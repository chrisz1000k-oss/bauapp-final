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
BLOCK_SEC_108 = 108     # Idempotenz-Sperrfrist gegen Doppelklicks

# Workflow Status
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
        
        /* Hervorhebung der Projekt-Infos */
        .info-box {{ background-color: #1E1E1E; border-left: 5px solid #D4AF37; padding: 15px; margin-bottom: 20px; border-radius: 4px; }}
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 2. SYSTEM-HELFER & IDEMPOTENZ (DOPPELKLICK-SCHUTZ)
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
    required_cols = ["Projekt_ID", "Auftragsnummer", "Projekt_Name", "Status", "Kunde_Name", "Kunde_Adresse", "Kunde_Email", "Kunde_Telefon", "Kunde_Kontakt", "Fuge_Zement", "Fuge_Silikon", "Asbest_Gefahr"]
    for col in required_cols:
        if col not in df.columns: df[col] = ""
    if not df.empty:
        for col in required_cols:
            df[col] = df[col].astype(str).replace({'nan': '', 'None': '', 'NaN': ''}).str.strip()
        df.loc[df['Asbest_Gefahr'] == '', 'Asbest_Gefahr'] = 'Nein'
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
# 5. GESCHÄFTSLOGIK (Speichern & Cache-Reset)
# ==========================================
def process_rapport(service, f_date, f_start, f_end, f_pause_min, f_arbeit, f_mat, f_bem, sel_proj, r_hin, r_rueck, P_FID, Z_FID, user_name):
    tx_string = f"RAPP_{f_date}_{f_start}_{f_end}_{f_arbeit[:10]}_{sel_proj}_{user_name}"
    if not check_idempotency(tx_string):
        st.warning("Dieser Datensatz wurde soeben gespeichert. Sperre aktiv zur Vermeidung von Duplikaten.")
        return

    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_str = f_date.strftime("%Y-%m-%d")
    
    diff_hours = (datetime.combine(f_date, f_end) - datetime.combine(f_date, f_start)).total_seconds() / 3600.0
    work_hours = round(diff_hours - (f_pause_min / 60.0), 2)
    if work_hours < 0:
        st.error("Eingabefehler: Die Endzeit abzüglich der Pause liegt vor der Startzeit.")
        return

    # SPV-KONFORME BERECHNUNG (30 Min. Abzug pro Weg bei Direktfahrt)
    reise_min_bezahlt = max(0, r_hin - 30) + max(0, r_rueck - 30)
    total_inkl_reise = round(work_hours + (reise_min_bezahlt / 60.0), 2)
    
    row_projekt = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": ST_OFFEN}
    row_zeit = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Start": f_start.strftime("%H:%M"), "Ende": f_end.strftime("%H:%M"), "Pause_Min": f_pause_min, "Stunden_Total": work_hours, "R_Wohn_Bau_Min": r_hin, "R_Bau_Wohn_Min": r_rueck, "Reisezeit_bezahlt_Min": reise_min_bezahlt, "Arbeitszeit_inkl_Reisezeit": total_inkl_reise, "Absenz_Typ": "", "Status": ST_OFFEN}
    
    save_to_drive(service, row_projekt, row_zeit, P_FID, Z_FID)
    st.success(f"Rapport erfolgreich synchronisiert. (Total Stunden: {total_inkl_reise}h)")

def process_absence_batch(service, start_date, end_date, f_hours, a_typ, f_bem, sel_proj, P_FID, Z_FID, user_name):
    tx_string = f"ABS_{start_date}_{end_date}_{a_typ}_{user_name}"
    if not check_idempotency(tx_string):
        st.warning("Abwesenheit wurde bereits verarbeitet. Sperre aktiv.")
        return

    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    days_diff = (end_date - start_date).days + 1
    
    r_proj, r_zeit = [], []
    for i in range(days_diff):
        date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        # Landet automatisch im Projekt-Controlling und in der AZK-Tabelle
        r_proj.append({"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Arbeit": f"Abwesenheit: {a_typ}", "Material": "", "Bemerkung": f_bem, "Status": ST_OFFEN})
        r_zeit.append({"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Start": "-", "Ende": "-", "Pause_Min": 0, "Stunden_Total": f_hours, "R_Wohn_Bau_Min": 0, "R_Bau_Wohn_Min": 0, "Reisezeit_bezahlt_Min": 0, "Arbeitszeit_inkl_Reisezeit": f_hours, "Absenz_Typ": a_typ, "Status": ST_OFFEN})
        
    save_to_drive_batch(service, r_proj, r_zeit, P_FID, Z_FID)
    st.success(f"Abwesenheit für {days_diff} Tag(e) gebucht und synchronisiert.")

def save_to_drive(service, row_p, row_z, P_FID, Z_FID):
    df_p, fid_p = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
    ds.save_csv(service, P_FID, "Baustellen_Rapport.csv", pd.concat([df_p, pd.DataFrame([row_p])], ignore_index=True), fid_p)
    df_z, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
    ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", pd.concat([validate_time_data(df_z), pd.DataFrame([row_z])], ignore_index=True), fid_z)
    st.cache_data.clear() 

def save_to_drive_batch(service, rows_p, rows_z, P_FID, Z_FID):
    df_p, fid_p = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
    ds.save_csv(service, P_FID, "Baustellen_Rapport.csv", pd.concat([df_p, pd.DataFrame(rows_p)], ignore_index=True), fid_p)
    df_z, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
    ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", pd.concat([validate_time_data(df_z), pd.DataFrame(rows_z)], ignore_index=True), fid_z)
    st.cache_data.clear()

# ==========================================
# 6. MITARBEITER-PORTAL (Mit zurückgekehrter Absenz-Funktion)
# ==========================================
def render_mitarbeiter_portal(service, P_FID, Z_FID, FOTO_FID, PLAN_FID):
    user_name = st.session_state['user_name']
    col_back, col_title = st.columns([1, 4])
    with col_back:
        if st.button("Abmelden"): st.session_state["user_name"] = ""; st.session_state["view"] = "Start"; st.rerun()
    with col_title: st.subheader(f"📋 Personal-Portal: {user_name}")
    
    df_proj, _ = ds.read_csv(service, P_FID, "Projects.csv")
    df_proj = validate_project_data(df_proj)
    
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
            asbest_warn = "<p style='color:#ff4b4b; font-weight:bold; margin-top:5px;'>⚠️ SICHERHEITSHINWEIS: ASBEST VORHANDEN</p>" if str(proj_data.get('Asbest_Gefahr', 'Nein')).strip().lower() == "ja" else ""
            
            st.markdown(f"""
            <div class="info-box">
                <h4 style="margin-top:0px; color:#D4AF37;">Projekt-Details</h4>
                <b>Kunde:</b> {proj_data.get('Kunde_Name', '-')} | <b>Ort:</b> {proj_data.get('Kunde_Adresse', '-')} | <b>Tel:</b> {proj_data.get('Kunde_Telefon', '-')}<br>
                <b>Material-Vorgaben:</b> Zementfuge: {proj_data.get('Fuge_Zement', '-')} | Silikonfuge: {proj_data.get('Fuge_Silikon', '-')}
                {asbest_warn}
            </div>
            """, unsafe_allow_html=True)

    st.write(f"<div style='height: 10px;'></div>", unsafe_allow_html=True)
    
    # DIE 4 SÄULEN DES MITARBEITERS (Absenz wieder da)
    t_arb, t_abs, t_med, t_hist = st.tabs(["🛠️ Rapport erfassen", "🏥 Abwesenheit", "📤 Medien & Dokumente", "📜 Projekt-Historie (Alle)"])
    
    with t_arb:
        with st.form("arb_form"):
            c1, c2, c3, c4 = st.columns(4)
            with c1: f_date = st.date_input("Datum", datetime.now())
            with c2: f_start = st.time_input("Arbeitsbeginn", datetime.strptime("07:00", "%H:%M").time())
            with c3: f_end = st.time_input("Arbeitsende", datetime.strptime("16:30", "%H:%M").time())
            with c4: f_pause = st.number_input("Pausen (Min)", min_value=0, value=30, step=15)
            
            st.divider()
            st.info("Hinweis: Fahrten über das Magazin gelten als Arbeitszeit. Bei Direktfahrten werden gemäß SPV 30 Min. pro Weg abgezogen.")
            r1, r2 = st.columns(2)
            with r1: r_hin = st.number_input("Direktfahrt Hinweg (Min)", value=0, step=5)
            with r2: r_rueck = st.number_input("Direktfahrt Rückweg (Min)", value=0, step=5)
            
            st.divider()
            f_arbeit = st.text_area("Ausgeführte Arbeiten")
            f_mat = st.text_area("Materialeinsatz")
            f_bem = st.text_input("Bemerkungen / Besonderheiten")
            
            if st.form_submit_button("💾 Speichern & Synchronisieren", type="primary"):
                with st.spinner("Übertrage Daten..."):
                    process_rapport(service, f_date, f_start, f_end, f_pause, f_arbeit, f_mat, f_bem, sel_proj, r_hin, r_rueck, P_FID, Z_FID, user_name)

    with t_abs:
        with st.form("abs_form"):
            st.markdown("**Meldung von Nicht-Präsenzzeiten (Urlaub/Krankheit)**")
            st.info("Tipp: Legen Sie für Urlaub/Krankheit im Admin-Bereich ein Projekt namens 'INTERN - Absenzen' an und wählen Sie dieses oben aus.")
            c1, c2 = st.columns(2)
            with c1: f_a_date_range = st.date_input("Zeitraum wählen", value=(datetime.now(), datetime.now()))
            with c2: f_a_hours = st.number_input("Soll-Stunden pro Tag", min_value=0.0, value=8.5, step=0.25)
            
            a_typ = st.selectbox("Kategorie", ["Ferien", "Krankheit", "Unfall (SUVA)", "Feiertag"])
            a_bem = st.text_input("Notizen")
            a_file = st.file_uploader("📄 Dokumenten-Upload (z.B. Arztzeugnis)", type=['pdf','jpg','png'])
            
            if st.form_submit_button("💾 Abwesenheit buchen", type="primary"):
                if isinstance(f_a_date_range, tuple):
                    start_date = f_a_date_range[0]
                    end_date = f_a_date_range[1] if len(f_a_date_range) == 2 else start_date
                else:
                    start_date = end_date = f_a_date_range
                
                if (end_date - start_date).days + 1 > 7:
                    st.error("Bitte buchen Sie maximal 7 Tage in einem Vorgang.")
                else:
                    with st.spinner("Verarbeite Block..."):
                        if a_file and a_typ == "Krankheit":
                            ds.upload_image(service, PLAN_FID, f"ZEUGNIS_{user_name}_{start_date}_{a_file.name}", io.BytesIO(a_file.getvalue()), a_file.type)
                        process_absence_batch(service, start_date, end_date, f_a_hours, a_typ, a_bem, sel_proj, P_FID, Z_FID, user_name)

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

    with t_hist:
        st.markdown(f"**Alle Berichte für: {sel_proj}**")
        df_hp, _ = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
        df_hz, _ = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
        
        if not df_hp.empty and not df_hz.empty and "Erfasst" in df_hz.columns:
            df_hz = validate_time_data(df_hz)
            df_merged = pd.merge(df_hp, df_hz[["Erfasst", "Arbeitszeit_inkl_Reisezeit"]], on="Erfasst", how="left")
            hist = df_merged[df_merged["Projekt"] == sel_proj].sort_values(by="Datum", ascending=False)
            
            if not hist.empty:
                for _, row in hist.head(50).iterrows():
                    stunden = row.get('Arbeitszeit_inkl_Reisezeit', '0')
                    with st.expander(f"📅 {row['Datum']} | 👷 {row['Mitarbeiter']} | ⏱️ {stunden} Std."):
                        st.write(f"**Tätigkeit:**\n{row.get('Arbeit', '-')}")
                        if str(row.get('Material', '')).strip():
                            st.write(f"**Material:**\n{row.get('Material', '-')}")
            else:
                st.write("Noch keine Berichte für dieses Projekt.")

# ==========================================
# 7. ADMIN DASHBOARD
# ==========================================
def render_admin_portal(service, P_FID, Z_FID, FOTO_FID, PLAN_FID, BASE_URL):
    col1, col2 = st.columns([4, 1])
    with col1: st.subheader("🛠️ Projektleitung & Administration")
    with col2:
        if st.button("Abmelden", use_container_width=True): st.session_state["logged_in"] = False; st.session_state["view"] = "Start"; st.rerun()
    
    t_week, t_ctrl, t_stam, t_docs, t_print, t_shiva = st.tabs(["🗓️ Wochenabschluss", "📊 Controlling", "⚙️ Stammdaten", "📂 Dateien", "🖨️ Projekt-Rapport (Drucken)", "🗑️ System"])
    
    df_proj, fid_proj = ds.read_csv(service, P_FID, "Projects.csv")
    df_proj = validate_project_data(df_proj)
    active_projs = df_proj["Projekt_Name"].tolist() if not df_proj.empty else []
    active_projs = [p for p in active_projs if str(p).strip() != ""]
    if not active_projs: active_projs = ["Keine Projekte gefunden"]

    df_emp, fid_emp = ds.read_csv(service, P_FID, "Employees.csv")
    df_emp = validate_employee_data(df_emp)
    emp_list = [name for name in df_emp["Name"].tolist() if str(name).strip() != ""] if not df_emp.empty else ["Keine Mitarbeiter"]

    # -----------------------------
    # 7.1 WOCHENABSCHLUSS (Mit manueller Dropdown-Kontrolle)
    # -----------------------------
    with t_week:
        sel_emp = st.selectbox("Mitarbeiter auswählen:", emp_list, key="wa_emp")
        df_z, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
        
        if not df_z.empty and sel_emp != "Keine Mitarbeiter":
            df_z = validate_time_data(df_z)
            df_emp_z = df_z[df_z["Mitarbeiter"] == sel_emp].copy()
            
            if not df_emp_z.empty:
                df_emp_z['Sort'] = df_emp_z['Status'].map({ST_OFFEN: 1, ST_DRUCK: 2, ST_FINAL: 3}).fillna(4)
                df_emp_z = df_emp_z.sort_values(by=['Sort', 'Datum']).drop(columns=['Sort'])
                
                wa_config = {
                    "Status": st.column_config.SelectboxColumn("Status", options=[ST_OFFEN, ST_DRUCK, ST_FINAL], required=True)
                }
                
                edit_z = st.data_editor(df_emp_z, num_rows="dynamic", use_container_width=True, column_config=wa_config, key=f"ed_wa_{sel_emp}")
                
                col_a, col_b = st.columns(2)
                with col_a:
                    if st.button("💾 Tabelle speichern", use_container_width=True):
                        df_z.set_index('Erfasst', inplace=True)
                        edit_z.set_index('Erfasst', inplace=True)
                        df_z = df_z[df_z.index.isin(edit_z.index) | (df_z["Mitarbeiter"] != sel_emp)]
                        df_z.update(edit_z)
                        df_z.reset_index(inplace=True)
                        ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                        st.cache_data.clear()
                        st.success("Tabelle aktualisiert.")
                        time.sleep(1); st.rerun()

    # -----------------------------
    # 7.2 PROJEKT-CONTROLLING
    # -----------------------------
    with t_ctrl:
        st.markdown("**Projekt-Rapporte (Tätigkeiten & Material)**")
        df_hp, fid_hp = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
        if not df_hp.empty:
            hp_config = {
                "Status": st.column_config.SelectboxColumn("Status", options=[ST_OFFEN, ST_DRUCK, ST_FINAL], required=True)
            }
            edit_hp = st.data_editor(df_hp, num_rows="dynamic", use_container_width=True, column_config=hp_config, key="ed_hp")
            if st.button("💾 Projekt-Rapporte aktualisieren"):
                ds.save_csv(service, P_FID, "Baustellen_Rapport.csv", edit_hp, fid_hp)
                st.cache_data.clear(); st.success("Rapporte erfolgreich aktualisiert.")

    # -----------------------------
    # 7.3 STAMMDATEN
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
    # 7.5 DRUCKEN (IMMER VERFÜGBAR - 3 SPALTEN)
    # -----------------------------
    with t_print:
        st.markdown("**Physischer Projekt-Rapport (Handschriftliches Backup)**")
        st.info("Generiert eine ausdruckbare Tabelle mit 3 Spalten und 15 Leerzeilen für die Baustelle.")
        
        print_proj = st.selectbox("Projekt für Ausdruck wählen:", active_projs, key="prnt_sel")
        if st.button("🖨️ PDF / Druckvorlage generieren") and print_proj != "Keine Projekte gefunden":
            
            matching_proj = df_proj[df_proj["Projekt_Name"] == print_proj]
            if not matching_proj.empty:
                pr = matching_proj.iloc[0]
                k_name, k_kontakt = pr.get('Kunde_Name', ''), pr.get('Kunde_Kontakt', '')
                k_adresse, k_telefon = pr.get('Kunde_Adresse', ''), pr.get('Kunde_Telefon', '')
                f_zem, f_sil = pr.get('Fuge_Zement', '-'), pr.get('Fuge_Silikon', '-')
                asb = "<p style='color:red;font-weight:bold;font-size:14px;border:1px solid red;padding:5px;'>⚠️ ASBEST VORHANDEN!</p>" if str(pr.get('Asbest_Gefahr', '')).lower() == "ja" else ""
            else:
                k_name, k_kontakt, k_adresse, k_telefon, f_zem, f_sil, asb = "", "", "", "", "-", "-", ""

            qr = f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={urllib.parse.quote(f'{BASE_URL}?projekt={urllib.parse.quote(print_proj)}')}"
            
            html_rows = "".join(["<tr><td style='border:1px solid #aaaaaa; height:45px;'></td><td style='border:1px solid #aaaaaa; height:45px;'></td><td style='border:1px solid #aaaaaa; height:45px;'></td></tr>" for _ in range(15)])
            
            html = f"""<html><body style="font-family:Arial;font-size:12px;background:#fff;color:#000;">
            <div style="display:flex;justify-content:space-between;border-bottom:2px solid #000;padding-bottom:15px;margin-bottom:20px;">
                <div style="width:40%;">
                    <h1 style="margin:0;font-size:22px;">R. Baumgartner AG</h1>
                    <p style="font-size:14px;margin-top:5px;"><b>Projekt-Rapport:</b> {print_proj}</p>
                    <p><b>Kunde:</b> {k_name} ({k_kontakt})<br><b>Ort:</b> {k_adresse}<br><b>Tel:</b> {k_telefon}</p>
                </div>
                <div style="width:40%;border-left:1px solid #ccc;padding-left:15px;">
                    <p style="margin-top:0;"><b>Material & Sicherheit:</b></p>
                    <p>Zementfuge: {f_zem}<br>Silikonfuge: {f_sil}</p>
                    {asb}
                </div>
                <div style="width:20%;text-align:right;">
                    <img src="{qr}" width="100"><p style="font-size:10px;margin-top:5px;">Schnell-Login Scanner</p>
                </div>
            </div>
            
            <table style="width:100%; border-collapse:collapse; margin-top:20px;">
                <tr>
                    <th style="border:1px solid #aaaaaa; padding:10px; width:15%; text-align:left; background-color:#f9f9f9;">Datum</th>
                    <th style="border:1px solid #aaaaaa; padding:10px; width:70%; text-align:left; background-color:#f9f9f9;">Ausgeführte Arbeiten / Material</th>
                    <th style="border:1px solid #aaaaaa; padding:10px; width:15%; text-align:center; background-color:#f9f9f9;">Stunden</th>
                </tr>
                {html_rows}
            </table>
            
            <div style="margin-top:54px; display:flex; justify-content:space-between;">
                <div style="border-top:1px solid #000; width:45%; padding-top:10px;">Visum Administration / Bauleitung</div>
                <div style="border-top:1px solid #000; width:45%; padding-top:10px;">Rechtsverbindliche Unterschrift Mitarbeiter</div>
            </div>
            </body></html>"""
            
            st.components.v1.html(html, height=500, scrolling=True)
            st.download_button("📄 HTML Druckvorlage herunterladen", html, f"Rapport_{print_proj}.html", "text/html", type="primary")

    # -----------------------------
    # 7.6 SYSTEM-BEREINIGUNG
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
