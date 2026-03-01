import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import io
import hashlib
import urllib.parse
from googleapiclient.http import MediaIoBaseDownload

import drive_store as ds

# ==========================================
# 1. KOSMISCHE PARAMETER & KONSTANTEN (BACKEND)
# ==========================================
SPACING_27 = 27         
CACHE_TTL_27 = 27       
BLOCK_SEC_108 = 108     # Idempotenz-Sperrfrist

# Dharma-Status Logik (Workflow)
ST_OFFEN = "Offen"
ST_DRUCK = "Druckbereit"
ST_FINAL = "Final (AZK)"

st.set_page_config(page_title="R. Baumgartner AG - Projekt-Portal", layout="wide")

st.markdown(f"""
    <style>
        #MainMenu {{display: none;}} footer {{display: none;}} header {{visibility: hidden;}}
        .stApp, .main {{ background-color: #121212 !important; }}
        html, body, p, div, span, label, h1, h2, h3, li, td, th {{ 
            color: #E0E0E0 !important; font-family: 'Segoe UI', sans-serif; 
        }}
        .stButton>button {{ 
            background-color: #D4AF37 !important; color: #121212 !important; 
            transition: all 0.27s ease-in-out; border: none !important; font-weight: bold; border-radius: 4px; 
        }}
        .stButton>button:hover {{ background-color: #b5952f !important; transform: scale(1.02); }}
        .stTextInput>div>div>input, .stNumberInput>div>div>input, .stTextArea>div>div>textarea, .stSelectbox>div>div>div {{
            background-color: #1E1E1E !important; color: #E0E0E0 !important; border: 1px solid #D4AF37 !important;
        }}
        hr {{ border-color: #D4AF37 !important; opacity: 0.27; margin-top: {SPACING_27}px; margin-bottom: {SPACING_27}px; }}
        .block-container {{ padding-top: {SPACING_27}px; padding-bottom: {SPACING_27}px; }}
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. SYSTEM-HELFER & IDEMPOTENZ
# ==========================================
def init_session_state():
    if "logged_in" not in st.session_state: st.session_state["logged_in"] = False
    if "user_role" not in st.session_state: st.session_state["user_role"] = ""
    if "user_name" not in st.session_state: st.session_state["user_name"] = ""
    if "view" not in st.session_state: st.session_state["view"] = "Start"
    if "last_tx_time" not in st.session_state: st.session_state["last_tx_time"] = 0.0
    if "last_tx_hash" not in st.session_state: st.session_state["last_tx_hash"] = ""

def check_idempotency(data_string: str) -> bool:
    tx_hash = hashlib.md5(data_string.encode('utf-8')).hexdigest()
    now = time.time()
    if st.session_state["last_tx_hash"] == tx_hash:
        if (now - st.session_state["last_tx_time"]) < BLOCK_SEC_108:
            return False 
    st.session_state["last_tx_hash"] = tx_hash
    st.session_state["last_tx_time"] = now
    return True

def render_header():
    col_logo, col_name = st.columns([1, 6])
    with col_logo:
        try: st.image("logo.png", use_container_width=True)
        except Exception: st.markdown(f"<h1 style='margin-bottom:{SPACING_27}px;'>🏗️</h1>", unsafe_allow_html=True)
    with col_name:
        st.markdown("<h1 style='color:#D4AF37; margin-top:0px;'>R. Baumgartner AG</h1>", unsafe_allow_html=True)
    st.divider()

# ==========================================
# 3. DATEN-VALIDIERUNG & INTEGRITÄT
# ==========================================
def validate_project_data(df: pd.DataFrame) -> pd.DataFrame:
    """Die Auto-Aktiv-Garantie für fehlerfreie Projekt-Sichtbarkeit."""
    required_cols = ["Projekt_ID", "Auftragsnummer", "Projekt_Name", "Status", "Kunde_Name", "Kunde_Adresse", "Kunde_Email", "Kunde_Telefon", "Kunde_Kontakt", "Fuge_Zement", "Fuge_Silikon", "Asbest_Gefahr"]
    for col in required_cols:
        if col not in df.columns: df[col] = ""
        
    if not df.empty:
        for col in required_cols:
            df[col] = df[col].astype(str).replace({'nan': '', 'None': '', 'NaN': ''}).str.strip()
        df.loc[df['Asbest_Gefahr'] == '', 'Asbest_Gefahr'] = 'Nein'
        
        # Prithvi-Sicherung: Wenn der Status nicht explizit Pausiert oder Archiviert ist, ist er IMMER Aktiv
        df['Status'] = df['Status'].apply(lambda x: "Aktiv" if str(x).lower() not in ["pausiert", "archiviert"] else str(x).capitalize())
        
    return df

def validate_time_data(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["Start", "Ende", "Pause_Min", "R_Wohn_Bau_Min", "R_Bau_Wohn_Min", "Reisezeit_bezahlt_Min", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]
    for col in required_cols:
        if col not in df.columns: df[col] = ""
    if not df.empty:
        df.loc[df['Status'] == '', 'Status'] = ST_OFFEN
    return df

def validate_employee_data(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["Mitarbeiter_ID", "Name", "PIN", "Status"]
    for col in required_cols:
        if col not in df.columns: df[col] = ""
    if not df.empty:
        for col in required_cols:
            df[col] = df[col].astype(str).replace({'nan': '', 'None': '', 'NaN': ''}).str.strip()
        df.loc[df['PIN'] == '', 'PIN'] = '1234'
        df.loc[df['Status'] == '', 'Status'] = 'Aktiv'
    return df

# ==========================================
# 4. DATEI-MANAGEMENT (Google Drive)
# ==========================================
@st.cache_data(ttl=108, show_spinner=False)
def load_project_files_from_drive(_service, folder_id: str, project_name: str) -> list:
    if not folder_id: return []
    try:
        query = f"'{folder_id}' in parents and trashed = false"
        results = _service.files().list(q=query, pageSize=1000, fields="files(id, name)").execute()
        return [f for f in results.get('files', []) if project_name in f.get('name', '')][:108]
    except Exception: return []

@st.cache_data(ttl=1080, show_spinner=False)
def download_file_bytes(_service, file_id: str):
    try:
        request = _service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        return fh.getvalue()
    except Exception: return None

def delete_drive_assets(_service, keyword: str, folders: list):
    for fid in folders:
        if not fid: continue
        try:
            results = _service.files().list(q=f"'{fid}' in parents and trashed = false", pageSize=1000, fields="files(id, name)").execute()
            for f in results.get('files', []):
                if keyword in f.get('name', ''):
                    _service.files().delete(fileId=f['id']).execute()
        except Exception: pass

# ==========================================
# 5. GESCHÄFTSLOGIK
# ==========================================
def process_rapport(service, f_date, f_start, f_end, f_pause_min, f_arbeit, f_mat, f_bem, sel_proj, r_hin, r_rueck, P_FID, Z_FID, user_name):
    tx_string = f"RAPP_{f_date}_{f_start}_{f_end}_{f_arbeit[:10]}_{sel_proj}_{user_name}"
    if not check_idempotency(tx_string):
        st.warning("Datensatz wurde bereits erfasst. Sperre aktiv zur Vermeidung von Duplikaten.")
        return

    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_str = f_date.strftime("%Y-%m-%d")
    
    diff_hours = (datetime.combine(f_date, f_end) - datetime.combine(f_date, f_start)).total_seconds() / 3600.0
    work_hours = round(diff_hours - (f_pause_min / 60.0), 2)
    if work_hours < 0:
        st.error("Eingabefehler: Endzeit abzüglich Pause liegt vor Startzeit.")
        return

    reise_min_bezahlt = max(0, r_hin - 30) + max(0, r_rueck - 30)
    total_inkl_reise = round(work_hours + (reise_min_bezahlt / 60.0), 2)
    
    row_projekt = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": ST_OFFEN}
    row_zeit = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Start": f_start.strftime("%H:%M"), "Ende": f_end.strftime("%H:%M"), "Pause_Min": f_pause_min, "Stunden_Total": work_hours, "R_Wohn_Bau_Min": r_hin, "R_Bau_Wohn_Min": r_rueck, "Reisezeit_bezahlt_Min": reise_min_bezahlt, "Arbeitszeit_inkl_Reisezeit": total_inkl_reise, "Absenz_Typ": "", "Status": ST_OFFEN}
    
    save_to_drive(service, row_projekt, row_zeit, P_FID, Z_FID)
    st.success(f"Erfolgreich synchronisiert. (Total Std: {total_inkl_reise}h)")

def save_to_drive(service, row_p, row_z, P_FID, Z_FID):
    df_p, fid_p = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
    ds.save_csv(service, P_FID, "Baustellen_Rapport.csv", pd.concat([df_p, pd.DataFrame([row_p])], ignore_index=True), fid_p)
    df_z, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
    ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", pd.concat([validate_time_data(df_z), pd.DataFrame([row_z])], ignore_index=True), fid_z)
    st.cache_data.clear()

# ==========================================
# 6. MITARBEITER-PORTAL
# ==========================================
def render_mitarbeiter_portal(service, P_FID, Z_FID, FOTO_FID, PLAN_FID):
    user_name = st.session_state['user_name']
    col_back, col_title = st.columns([1, 4])
    with col_back:
        if st.button("🚪 Abmelden"): st.session_state["user_name"] = ""; st.session_state["view"] = "Start"; st.rerun()
    with col_title: st.subheader(f"📋 Personal-Portal: {user_name}")
    
    df_proj, _ = ds.read_csv(service, P_FID, "Projects.csv")
    df_proj = validate_project_data(df_proj)
    
    # Robuster Filter: Ignoriert dank Auto-Aktiv-Garantie keine Projekte mehr
    active_projs = []
    if not df_proj.empty:
        active_mask = df_proj["Status"].astype(str).str.strip().str.lower() == "aktiv"
        active_projs = df_proj[active_mask]["Projekt_Name"].tolist()
        active_projs = [p for p in active_projs if str(p).strip() != ""]
        
    sel_proj = st.selectbox("Aktuelles Projekt auswählen:", active_projs if active_projs else ["Keine aktiven Projekte gefunden"])
    
    if sel_proj != "Keine aktiven Projekte gefunden":
        matching_proj = df_proj[df_proj["Projekt_Name"] == sel_proj]
        if not matching_proj.empty:
            proj_data = matching_proj.iloc[0]
            with st.expander("ℹ️ Projekt-Informationen & Vorgaben", expanded=False):
                c_info, m_info = st.columns(2)
                with c_info:
                    st.write(f"**Kunde:** {proj_data.get('Kunde_Name', '-')} ({proj_data.get('Kunde_Kontakt', '-')})")
                    st.write(f"**Ort:** {proj_data.get('Kunde_Adresse', '-')}")
                with m_info:
                    st.write(f"**Fugen:** Zement: {proj_data.get('Fuge_Zement', '-')} | Silikon: {proj_data.get('Fuge_Silikon', '-')}")
                    if str(proj_data.get('Asbest_Gefahr', 'Nein')).strip().lower() == "ja": 
                        st.markdown("<p style='color:#ff4b4b; font-weight:bold;'>⚠️ SICHERHEITSHINWEIS: ASBEST VORHANDEN</p>", unsafe_allow_html=True)

    st.write(f"<div style='height: {SPACING_27}px;'></div>", unsafe_allow_html=True)
    t_arb, t_med = st.tabs(["🛠️ Arbeitszeit erfassen", "📤 Dokumente & Pläne"])
    
    with t_arb:
        with st.form("arb_form"):
            c1, c2, c3, c4 = st.columns(4)
            with c1: f_date = st.date_input("Datum", datetime.now())
            with c2: f_start = st.time_input("Arbeitsbeginn", datetime.strptime("07:00", "%H:%M").time())
            with c3: f_end = st.time_input("Arbeitsende", datetime.strptime("16:30", "%H:%M").time())
            with c4: f_pause = st.number_input("Pausen (Min)", min_value=0, value=30, step=15)
            
            st.divider()
            st.info("Hinweis: Fahrten über das Magazin gelten als Arbeitszeit. Bei Direktfahrten werden 30 Min. pro Weg abgezogen.")
            r1, r2 = st.columns(2)
            with r1: r_hin = st.number_input("Direktfahrt Hinweg (Min)", value=0, step=5)
            with r2: r_rueck = st.number_input("Direktfahrt Rückweg (Min)", value=0, step=5)
            
            st.divider()
            f_arbeit = st.text_area("Ausgeführte Arbeiten")
            f_mat = st.text_area("Materialeinsatz")
            f_bem = st.text_input("Bemerkungen / Besonderheiten")
            
            if st.form_submit_button("💾 Speichern & Übermitteln", type="primary"):
                with st.spinner("Übertrage Daten..."):
                    process_rapport(service, f_date, f_start, f_end, f_pause, f_arbeit, f_mat, f_bem, sel_proj, r_hin, r_rueck, P_FID, Z_FID, user_name)

    with t_med:
        files = st.file_uploader("Fotos hochladen", accept_multiple_files=True, type=['jpg','png','jpeg'])
        if st.button("📤 Upload starten", type="primary") and files:
            prog = st.progress(0)
            for idx, f in enumerate(files[:SPACING_27]):
                ds.upload_image(service, FOTO_FID, f"{sel_proj}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{f.name}", io.BytesIO(f.getvalue()), f.type)
                prog.progress((idx + 1) / len(files))
            st.success("Erfolgreich."); st.cache_data.clear(); time.sleep(1); st.rerun()
            
        st.divider()
        if st.button("🔄 Galerie laden"): st.cache_data.clear()
        if sel_proj != "Keine aktiven Projekte gefunden":
            all_files = load_project_files_from_drive(service, FOTO_FID, sel_proj) + load_project_files_from_drive(service, PLAN_FID, sel_proj)
            if all_files:
                cols = st.columns(2)
                for idx, img in enumerate(all_files):
                    with cols[idx % 2]:
                        img_b = download_file_bytes(service, img['id'])
                        if img_b:
                            if img['name'].lower().endswith(('.png', '.jpg', '.jpeg')): st.image(img_b, use_container_width=True)
                            else: st.download_button(f"📥 {img['name'][:15]}", data=img_b, file_name=img['name'])

# ==========================================
# 7. ADMIN DASHBOARD
# ==========================================
def render_admin_portal(service, P_FID, Z_FID, FOTO_FID, PLAN_FID, BASE_URL):
    col1, col2 = st.columns([4, 1])
    with col1: st.subheader("🛠️ Projektleitung & Administration")
    with col2:
        if st.button("🚪 Abmelden", use_container_width=True): st.session_state["logged_in"] = False; st.session_state["view"] = "Start"; st.rerun()
    
    t_week, t_ctrl, t_stam, t_docs, t_shiva = st.tabs(["🗓️ Wochenabschluss (Workflow)", "📊 Projekt-Controlling", "⚙️ Stammdaten", "📂 Dateien", "🗑️ System"])
    
    df_proj, fid_proj = ds.read_csv(service, P_FID, "Projects.csv")
    df_proj = validate_project_data(df_proj)
    active_projs = df_proj["Projekt_Name"].tolist() if not df_proj.empty else []
    active_projs = [p for p in active_projs if str(p).strip() != ""]
    if not active_projs: active_projs = ["Keine Projekte gefunden"]

    df_emp, fid_emp = ds.read_csv(service, P_FID, "Employees.csv")
    df_emp = validate_employee_data(df_emp)
    emp_list = [name for name in df_emp["Name"].tolist() if str(name).strip() != ""] if not df_emp.empty else ["Keine Mitarbeiter"]

    # -----------------------------
    # 7.1 WOCHENABSCHLUSS (4-Stufen Workflow)
    # -----------------------------
    with t_week:
        st.markdown("**Automatisierter Freigabe- und Druckprozess**")
        st.info("Schritt 1 (Erfassung) erfolgt durch die Mitarbeiter. Führen Sie hier Schritt 2 bis 4 durch.")
        
        sel_emp = st.selectbox("Personal-Ordner (Mitarbeiter):", emp_list, key="wa_emp")
        
        df_z, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
        if not df_z.empty and sel_emp != "Keine Mitarbeiter":
            df_z = validate_time_data(df_z)
            df_emp_z = df_z[df_z["Mitarbeiter"] == sel_emp].copy()
            
            if not df_emp_z.empty:
                df_emp_z['Sort'] = df_emp_z['Status'].map({ST_OFFEN: 1, ST_DRUCK: 2, ST_FINAL: 3}).fillna(4)
                df_emp_z = df_emp_z.sort_values(by=['Sort', 'Datum']).drop(columns=['Sort'])
                
                edit_z = st.data_editor(df_emp_z, num_rows="dynamic", use_container_width=True, key=f"ed_wa_{sel_emp}")
                
                st.divider()
                col_a, col_b, col_c = st.columns(3)
                
                with col_a:
                    if st.button("💾 Änderungen sichern", use_container_width=True):
                        df_z.set_index('Erfasst', inplace=True)
                        edit_z.set_index('Erfasst', inplace=True)
                        df_z = df_z[df_z.index.isin(edit_z.index) | (df_z["Mitarbeiter"] != sel_emp)]
                        df_z.update(edit_z)
                        df_z.reset_index(inplace=True)
                        ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                        st.cache_data.clear()
                        st.success("Tabelle aktualisiert.")
                        time.sleep(1); st.rerun()
                        
                    if st.button("🔒 Für Druck freigeben", use_container_width=True):
                        mask = (df_z["Mitarbeiter"] == sel_emp) & (df_z["Status"] == ST_OFFEN)
                        df_z.loc[mask, "Status"] = ST_DRUCK
                        ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                        st.cache_data.clear()
                        st.success("Zeiten gesperrt und druckbereit."); time.sleep(1); st.rerun()

                with col_b:
                    print_mask = (edit_z["Status"] == ST_DRUCK)
                    if not edit_z[print_mask].empty:
                        html_rows = ""
                        total_h = 0
                        for _, r in edit_z[print_mask].iterrows():
                            h = float(r.get('Arbeitszeit_inkl_Reisezeit', 0))
                            total_h += h
                            html_rows += f"<tr><td style='border:1px solid #aaaaaa; padding:8px;'>{r['Datum']}</td><td style='border:1px solid #aaaaaa; padding:8px;'><b>{r['Projekt']}</b></td><td style='border:1px solid #aaaaaa; padding:8px; text-align:center;'>{h}</td></tr>"
                        
                        html = f"""<html><body style="font-family:Arial;font-size:12px;background:#fff;color:#000;">
                        <h1 style="margin:0;font-size:22px;border-bottom:2px solid #000;padding-bottom:10px;">Wochenbericht / Arbeitszeit</h1>
                        <p style="font-size:14px;margin-top:10px;"><b>Mitarbeiter:</b> {sel_emp}</p>
                        <table style="width:100%; border-collapse:collapse; margin-top:20px;">
                            <tr>
                                <th style="border:1px solid #aaaaaa; padding:10px; width:20%; text-align:left; background-color:#f9f9f9;">Datum</th>
                                <th style="border:1px solid #aaaaaa; padding:10px; width:60%; text-align:left; background-color:#f9f9f9;">Projekt / Einsatzort</th>
                                <th style="border:1px solid #aaaaaa; padding:10px; width:20%; text-align:center; background-color:#f9f9f9;">Stunden Total</th>
                            </tr>
                            {html_rows}
                            <tr>
                                <td colspan="2" style="border:1px solid #aaaaaa; padding:10px; text-align:right;"><b>Gesamtsumme:</b></td>
                                <td style="border:1px solid #aaaaaa; padding:10px; text-align:center;"><b>{round(total_h, 2)} Std.</b></td>
                            </tr>
                        </table>
                        <div style="margin-top:54px; display:flex; justify-content:space-between;">
                            <div style="border-top:1px solid #000; width:45%; padding-top:10px;">Visum Administration</div>
                            <div style="border-top:1px solid #000; width:45%; padding-top:10px;">Unterschrift Mitarbeiter</div>
                        </div>
                        </body></html>"""
                        
                        st.download_button("🖨️ Wochenbericht drucken", html, f"Wochenbericht_{sel_emp.replace(' ','_')}.html", "text/html", use_container_width=True)
                    else:
                        st.button("🖨️ (Keine druckbereiten Daten)", disabled=True, use_container_width=True)

                with col_c:
                    if st.button("✅ In AZK buchen (Final)", type="primary", use_container_width=True):
                        mask = (df_z["Mitarbeiter"] == sel_emp) & (df_z["Status"] == ST_DRUCK)
                        df_z.loc[mask, "Status"] = ST_FINAL
                        ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                        st.cache_data.clear()
                        st.success("Erfolgreich ins finale Archiv übertragen!"); time.sleep(1); st.rerun()
            else:
                st.write("Keine Zeiteinträge für diesen Mitarbeiter gefunden.")

    # -----------------------------
    # 7.2 PROJEKT-CONTROLLING
    # -----------------------------
    with t_ctrl:
        st.markdown("**Projekt-Rapporte (Tätigkeiten & Material)**")
        st.info("Hier können Sie Tätigkeitsberichte einsehen, korrigieren oder Duplikate löschen.")
        
        df_hp, fid_hp = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
        if not df_hp.empty:
            edit_hp = st.data_editor(df_hp, num_rows="dynamic", use_container_width=True, key="ed_hp")
            if st.button("💾 Projekt-Rapporte aktualisieren"):
                ds.save_csv(service, P_FID, "Baustellen_Rapport.csv", edit_hp, fid_hp)
                st.cache_data.clear()
                st.success("Rapporte erfolgreich aktualisiert.")

    # -----------------------------
    # 7.3 STAMMDATEN (MIT DROPDOWNS)
    # -----------------------------
    with t_stam:
        st.markdown("**Projekt-Verwaltung**")
        proj_config = {"Status": st.column_config.SelectboxColumn("Status", options=["Aktiv", "Pausiert", "Archiviert"], required=True)}
        edit_proj = st.data_editor(df_proj, num_rows="dynamic", column_config=proj_config, key="ep", use_container_width=True)
        if st.button("💾 Projekte aktualisieren"): 
            clean_proj = edit_proj[edit_proj["Projekt_Name"].astype(str).str.strip() != ""]
            ds.save_csv(service, P_FID, "Projects.csv", clean_proj, fid_proj)
            st.cache_data.clear(); st.success("Gespeichert.")
        
        st.markdown("**Personal-Verwaltung**")
        emp_config = {"Status": st.column_config.SelectboxColumn("Status", options=["Aktiv", "Inaktiv"], required=True)}
        edit_emp = st.data_editor(df_emp, num_rows="dynamic", column_config=emp_config, key="ee", use_container_width=True)
        if st.button("💾 Personal aktualisieren"): 
            clean_emp = edit_emp[edit_emp["Name"].astype(str).str.strip() != ""]
            ds.save_csv(service, P_FID, "Employees.csv", clean_emp, fid_emp)
            st.cache_data.clear(); st.success("Gespeichert.")

    # -----------------------------
    # 7.4 DATEIEN
    # -----------------------------
    with t_docs:
        ap = st.selectbox("Projekt-Ordner:", active_projs, key="docs_sel")
        c_u1, c_u2 = st.columns(2)
        with c_u1:
            plan_f = st.file_uploader("📤 Pläne (PDF/Bilder)", accept_multiple_files=True, type=['pdf', 'jpg', 'png'])
            if st.button("Pläne hochladen") and plan_f and PLAN_FID and ap != "Keine Projekte gefunden":
                for f in plan_f: ds.upload_image(service, PLAN_FID, f"{ap}_PLAN_{f.name}", io.BytesIO(f.getvalue()), f.type)
                st.success("Upload erfolgreich."); st.cache_data.clear(); time.sleep(1); st.rerun()
        with c_u2:
            foto_f = st.file_uploader("📷 Projektfotos", accept_multiple_files=True, type=['jpg', 'png'])
            if st.button("Fotos hochladen") and foto_f and ap != "Keine Projekte gefunden":
                for f in foto_f: ds.upload_image(service, FOTO_FID, f"{ap}_ADMIN_{f.name}", io.BytesIO(f.getvalue()), f.type)
                st.success("Upload erfolgreich."); st.cache_data.clear(); time.sleep(1); st.rerun()
        
        st.divider()
        if st.button("🔄 Datei-Verzeichnis aktualisieren"): st.cache_data.clear()
        if ap != "Keine Projekte gefunden":
            files = load_project_files_from_drive(service, FOTO_FID, ap) + load_project_files_from_drive(service, PLAN_FID, ap)
            cols = st.columns(4)
            for i, img in enumerate(files):
                with cols[i % 4]:
                    b = download_file_bytes(service, img['id'])
                    if b:
                        if img['name'].lower().endswith(('.png','.jpg','.jpeg')): st.image(b)
                        else: st.download_button(f"📥 {img['name'][:15]}", b, img['name'])

    # -----------------------------
    # 7.5 SYSTEM-BEREINIGUNG
    # -----------------------------
    with t_shiva:
        st.error("🗑️ System-Bereinigung (Unwiderruflich)")
        typ = st.radio("Kategorie:", ["Projekt", "Mitarbeiter"])
        if st.checkbox("Löschvorgang verbindlich autorisieren"):
            if typ == "Projekt":
                tgt = st.selectbox("Zu löschendes Projekt:", active_projs)
                if st.button("🛑 Endgültig löschen") and tgt != "Keine Projekte gefunden":
                    df_proj = df_proj[df_proj["Projekt_Name"].astype(str).str.strip() != str(tgt).strip()]
                    ds.save_csv(service, P_FID, "Projects.csv", df_proj, fid_proj)
                    for file, id_key in [("Baustellen_Rapport.csv", P_FID), ("Arbeitszeit_AKZ.csv", Z_FID)]:
                        d_tmp, f_tmp = ds.read_csv(service, id_key, file)
                        ds.save_csv(service, id_key, file, d_tmp[d_tmp["Projekt"].astype(str).str.strip() != str(tgt).strip()], f_tmp)
                    delete_drive_assets(service, str(tgt).strip(), [FOTO_FID, PLAN_FID])
                    st.cache_data.clear(); st.success("Bereinigt."); time.sleep(2); st.rerun()
            else:
                tgt = st.selectbox("Zu löschender Mitarbeiter:", emp_list)
                if st.button("🛑 Endgültig löschen") and tgt != "Keine Mitarbeiter":
                    df_emp = df_emp[df_emp["Name"].astype(str).str.strip() != str(tgt).strip()]
                    ds.save_csv(service, P_FID, "Employees.csv", df_emp, fid_emp)
                    st.cache_data.clear(); st.success("Bereinigt."); time.sleep(2); st.rerun()

# ==========================================
# 8. SYSTEM-KERN (Boot-Sequenz)
# ==========================================
def main():
    init_session_state()
    render_header()

    try: 
        s = ds.get_drive_service()
        sec = st.secrets.get("general", st.secrets)
        P_FID, Z_FID = sec.get("PROJECT_REPORTS_FOLDER_ID", ""), sec.get("TIME_REPORTS_FOLDER_ID", "")
        FOTO_FID, PLAN_FID = sec.get("PHOTOS_FOLDER_ID", ""), sec.get("PLANS_FOLDER_ID", "")
        BASE_URL = sec.get("BASE_APP_URL", "https://8bv6gzagymvrdgnm8wrtrq.streamlit.app")
    except Exception: st.error("Systemfehler: Die Konfigurationsdateien sind unvollständig."); st.stop()
    if not s: st.warning("Verbindungsfehler: Laufwerk-Zugang fehlt."); st.stop()

    view = st.session_state["view"]
    
    if view == "Start":
        c1, c2 = st.columns(2)
        with c1: 
            if st.button("👷‍♂️ Personal-Zugang", use_container_width=True): st.session_state["view"] = "Mitarbeiter_Login"; st.rerun()
        with c2: 
            if st.button("🔐 Projektleitung", use_container_width=True): st.session_state["view"] = "Admin_Login"; st.rerun()

    elif view == "Admin_Login":
        if st.button("⬅️ Zurück zum Menü"): st.session_state["view"] = "Start"; st.rerun()
        if st.button("Login", type="primary") if st.text_input("Admin PIN", type="password") == str(sec.get("ADMIN_PIN", "1234")) else False:
            st.session_state.update({"logged_in": True, "user_role": "Admin", "view": "Admin_Dashboard"}); st.rerun()

    elif view == "Mitarbeiter_Login":
        if st.button("⬅️ Zurück zum Menü"): st.session_state["view"] = "Start"; st.rerun()
        
        df_emp, _ = ds.read_csv(s, P_FID, "Employees.csv")
        df_emp = validate_employee_data(df_emp) 
        
        if not df_emp.empty:
            active_mask = df_emp["Status"].astype(str).str.strip().str.lower() == "aktiv"
            emps = df_emp[active_mask]["Name"].tolist()
            emps = [name for name in emps if str(name).strip() != ""] 
        else:
            emps = []
            
        sel = st.selectbox("Mitarbeiterprofil:", emps if emps else ["Keine aktiven Profile"])
        pin_eingabe = st.text_input("Persönliche PIN", type="password")
        
        if st.button("Anmelden", type="primary") and sel != "Keine aktiven Profile":
            wahre_pin = str(df_emp[df_emp["Name"] == sel]["PIN"].iloc[0]).strip()
            if str(pin_eingabe).strip() == wahre_pin:
                st.session_state.update({"user_name": sel, "view": "Mitarbeiter_Dashboard"}); st.rerun()
            else:
                st.error("Authentifizierung fehlgeschlagen: PIN inkorrekt.")

    elif view == "Mitarbeiter_Dashboard": render_mitarbeiter_portal(s, P_FID, Z_FID, FOTO_FID, PLAN_FID)
    elif view == "Admin_Dashboard": render_admin_portal(s, P_FID, Z_FID, FOTO_FID, PLAN_FID, BASE_URL)

if __name__ == "__main__":
    main()
