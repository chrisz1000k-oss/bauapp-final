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
# 1. SYSTEM-PARAMETER & KONSTANTEN
# ==========================================
SPACING_27 = 27         
CACHE_TTL_27 = 27       
BLOCK_SEC_108 = 108     # Idempotenz-Sperrfrist in Sekunden

# Status-Konstanten (Dharma-Workflow)
ST_DRAFT = "Entwurf"
ST_CHECKED = "Geprüft"
ST_READY = "Druckbereit"
ST_ARCHIVED = "Archiviert"

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
    """Kryptografischer Schutz vor Duplikaten (108 Sekunden Sperrfrist)."""
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
        df.loc[df['Status'] == '', 'Status'] = 'Aktiv'
    return df

def validate_time_data(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["Start", "Ende", "Pause_Min", "R_Wohn_Bau_Min", "R_Bau_Wohn_Min", "Reisezeit_bezahlt_Min", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]
    for col in required_cols:
        if col not in df.columns: df[col] = ""
    if not df.empty:
        df.loc[df['Status'] == '', 'Status'] = ST_DRAFT
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

# ==========================================
# 5. GESCHÄFTSLOGIK
# ==========================================
def process_rapport(service, f_date, f_start, f_end, f_pause_min, f_arbeit, f_mat, f_bem, sel_proj, r_hin, r_rueck, P_FID, Z_FID, user_name):
    tx_string = f"RAPP_{f_date}_{f_start}_{f_end}_{f_arbeit[:10]}_{sel_proj}_{user_name}"
    if not check_idempotency(tx_string):
        st.warning("Datensatz wurde bereits erfasst. Bitte 2 Minuten warten, um Duplikate zu vermeiden.")
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
    
    row_projekt = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": ST_DRAFT}
    row_zeit = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Start": f_start.strftime("%H:%M"), "Ende": f_end.strftime("%H:%M"), "Pause_Min": f_pause_min, "Stunden_Total": work_hours, "R_Wohn_Bau_Min": r_hin, "R_Bau_Wohn_Min": r_rueck, "Reisezeit_bezahlt_Min": reise_min_bezahlt, "Arbeitszeit_inkl_Reisezeit": total_inkl_reise, "Absenz_Typ": "", "Status": ST_DRAFT}
    
    save_to_drive(service, row_projekt, row_zeit, P_FID, Z_FID)
    st.success(f"Erfolgreich synchronisiert. (Netto: {work_hours}h | Reise: {reise_min_bezahlt}m | AZK Total: {total_inkl_reise}h)")

def process_absence_batch(service, start_date, end_date, f_hours, a_typ, f_bem, sel_proj, P_FID, Z_FID, user_name):
    tx_string = f"ABS_{start_date}_{end_date}_{a_typ}_{user_name}"
    if not check_idempotency(tx_string):
        st.warning("Abwesenheit wurde bereits verarbeitet.")
        return

    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    days_diff = (end_date - start_date).days + 1
    
    r_proj, r_zeit = [], []
    for i in range(days_diff):
        date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        r_proj.append({"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Arbeit": f"Abwesenheit: {a_typ}", "Material": "", "Bemerkung": f_bem, "Status": ST_DRAFT})
        r_zeit.append({"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": user_name, "Start": "-", "Ende": "-", "Pause_Min": 0, "Stunden_Total": f_hours, "R_Wohn_Bau_Min": 0, "R_Bau_Wohn_Min": 0, "Reisezeit_bezahlt_Min": 0, "Arbeitszeit_inkl_Reisezeit": f_hours, "Absenz_Typ": a_typ, "Status": ST_DRAFT})
        
    save_to_drive_batch(service, r_proj, r_zeit, P_FID, Z_FID)
    st.success(f"Abwesenheit über {days_diff} Tag(e) gebucht.")

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
    
    active_projs = df_proj[df_proj["Status"].astype(str).str.strip().str.lower() == "aktiv"]["Projekt_Name"].tolist() if not df_proj.empty else []
    active_projs = [p for p in active_projs if str(p).strip() != ""]
        
    sel_proj = st.selectbox("Aktuelles Projekt auswählen:", active_projs if active_projs else ["Keine aktiven Projekte"])
    
    if sel_proj != "Keine aktiven Projekte":
        matching_proj = df_proj[df_proj["Projekt_Name"] == sel_proj]
        if not matching_proj.empty:
            proj_data = matching_proj.iloc[0]
            with st.expander("ℹ️ Projekt-Informationen & Vorgaben", expanded=False):
                c_info, m_info = st.columns(2)
                with c_info:
                    st.write(f"**Kunde:** {proj_data.get('Kunde_Name')} ({proj_data.get('Kunde_Kontakt')})")
                    st.write(f"**Adresse:** {proj_data.get('Kunde_Adresse')}")
                    st.write(f"**Telefon:** {proj_data.get('Kunde_Telefon')}")
                with m_info:
                    st.write(f"**Materialvorgaben:** Zementfuge: {proj_data.get('Fuge_Zement')} | Silikonfuge: {proj_data.get('Fuge_Silikon')}")
                    if str(proj_data.get('Asbest_Gefahr')).strip().lower() == "ja": 
                        st.markdown("<p style='color:#ff4b4b; font-weight:bold;'>⚠️ SICHERHEITSHINWEIS: ASBEST VORHANDEN</p>", unsafe_allow_html=True)

    st.write(f"<div style='height: {SPACING_27}px;'></div>", unsafe_allow_html=True)
    t_arb, t_abs, t_med, t_hist = st.tabs(["🛠️ Arbeitszeit erfassen", "🏥 Abwesenheit", "📤 Dateien", "📜 Bestätigung & Historie"])
    
    with t_arb:
        with st.form("arb_form"):
            c1, c2, c3, c4 = st.columns(4)
            with c1: f_date = st.date_input("Datum", datetime.now())
            with c2: f_start = st.time_input("Arbeitsbeginn", datetime.strptime("07:00", "%H:%M").time())
            with c3: f_end = st.time_input("Arbeitsende", datetime.strptime("16:30", "%H:%M").time())
            with c4: f_pause = st.number_input("Pausen (Min)", min_value=0, value=30, step=15)
            
            st.divider()
            st.info("Hinweis: Fahrten über das Magazin gelten als reguläre Arbeitszeit. Bei Direktfahrten werden gemäß SPV 30 Min. pro Weg abgezogen.")
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
            c1, c2 = st.columns(2)
            with c1: drange = st.date_input("Zeitraum wählen (Max. 7 Tage)", value=(datetime.now(), datetime.now()))
            with c2: f_a_hours = st.number_input("Soll-Stunden pro Tag", min_value=0.0, value=8.5, step=0.25)
            
            a_typ = st.selectbox("Kategorie", ["Ferien", "Krankheit", "Unfall (SUVA)", "Feiertag"])
            a_bem = st.text_input("Notizen")
            a_file = st.file_uploader("📄 Dokumenten-Upload (z.B. Arztzeugnis)", type=['pdf','jpg','png'])
            
            if st.form_submit_button("💾 Abwesenheit buchen", type="primary"):
                with st.spinner("Verarbeite Block..."):
                    s_date = drange[0] if isinstance(drange, tuple) else drange
                    e_date = drange[1] if isinstance(drange, tuple) and len(drange)==2 else s_date
                    if (e_date - s_date).days + 1 > 7: st.error("Bitte max. 7 Tage pro Vorgang buchen.")
                    else:
                        if a_file and a_typ == "Krankheit": ds.upload_image(service, PLAN_FID, f"ZEUGNIS_{user_name}_{s_date}_{a_file.name}", io.BytesIO(a_file.getvalue()), a_file.type)
                        process_absence_batch(service, s_date, e_date, f_a_hours, a_typ, a_bem, sel_proj, P_FID, Z_FID, user_name)

    with t_med:
        files = st.file_uploader("Baustellen-Fotos hochladen", accept_multiple_files=True, type=['jpg','png','jpeg'])
        if st.button("📤 Upload starten", type="primary") and files:
            prog = st.progress(0)
            for idx, f in enumerate(files[:SPACING_27]):
                ds.upload_image(service, FOTO_FID, f"{sel_proj}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{f.name}", io.BytesIO(f.getvalue()), f.type)
                prog.progress((idx + 1) / len(files))
            st.success("Erfolgreich."); st.cache_data.clear(); time.sleep(1); st.rerun()
            
        st.divider()
        if st.button("🔄 Galerie laden"): st.cache_data.clear()
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
        st.markdown("<h4 style='color:#D4AF37;'>Schritt 3: Bestätigung durch Mitarbeiter</h4>", unsafe_allow_html=True)
        df_hp, _ = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
        df_hz, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
        
        if not df_hp.empty and not df_hz.empty and "Erfasst" in df_hz.columns:
            df_hz = validate_time_data(df_hz)
            df_m = pd.merge(df_hp, df_hz[["Erfasst", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]], on="Erfasst", how="left")
            hist = df_m[(df_m["Projekt"] == sel_proj) & (df_m["Mitarbeiter"] == user_name)].sort_values(by="Datum", ascending=False)
            
            for _, row in hist.head(SPACING_27).iterrows():
                stat = str(row.get('Status', ST_DRAFT))
                col = "🔴" if stat == ST_DRAFT else "🟡" if stat == ST_CHECKED else "🟢"
                titel = f"{col} Status: {stat} | Datum: {row['Datum']} | {row.get('Absenz_Typ', '')} Total: {row.get('Arbeitszeit_inkl_Reisezeit', '-')} Std."
                
                with st.expander(titel):
                    st.write(f"**Tätigkeit:** {row.get('Arbeit', '-')}")
                    if stat == ST_CHECKED:
                        st.info("Dieser Rapport wurde von der Administration geprüft. Bitte bestätigen Sie die Richtigkeit.")
                        if st.button(f"✍️ Daten bestätigen", key=f"sig_{row['Erfasst']}"):
                            idx_up = df_hz[df_hz['Erfasst'] == row['Erfasst']].index
                            if not idx_up.empty:
                                # Status-Flow: Geprüft -> Druckbereit
                                df_hz.loc[idx_up, "Status"] = ST_READY
                                ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", df_hz, fid_z)
                                st.cache_data.clear()
                                st.success("Bestätigt und zur Druckfreigabe übermittelt."); time.sleep(1); st.rerun()

# ==========================================
# 7. ADMIN DASHBOARD
# ==========================================
def render_admin_portal(service, P_FID, Z_FID, FOTO_FID, PLAN_FID, BASE_URL):
    col1, col2 = st.columns([4, 1])
    with col1: st.subheader("🛠️ Projektleitung & Administration")
    with col2:
        if st.button("🚪 Abmelden", use_container_width=True): st.session_state["logged_in"] = False; st.session_state["view"] = "Start"; st.rerun()
    
    t_ctrl, t_pers, t_stam, t_docs, t_print, t_alchemist, t_shiva = st.tabs(["📊 Projekt-Controlling", "📁 Personal-Ordner", "⚙️ Stammdaten", "📂 Dateien", "🖨️ Druck & Archiv", "📥 AZK-Export", "🗑️ Bereinigung"])
    
    df_proj, fid_proj = ds.read_csv(service, P_FID, "Projects.csv")
    df_proj = validate_project_data(df_proj)
    active_projs = df_proj["Projekt_Name"].tolist() if not df_proj.empty else []
    active_projs = [p for p in active_projs if str(p).strip() != ""]
    if not active_projs: active_projs = ["Keine Projekte gefunden"]
    
    # -----------------------------
    # 7.1 PROJEKT-CONTROLLING (DATEN-PARITÄT)
    # -----------------------------
    with t_ctrl:
        st.markdown("**Transparenz & Kostenerfassung**")
        ctrl_proj = st.selectbox("Projekt auswählen:", active_projs, key="ctrl_p")
        if ctrl_proj != "Keine Projekte gefunden":
            matching_proj = df_proj[df_proj["Projekt_Name"] == ctrl_proj]
            if not matching_proj.empty:
                pd_data = matching_proj.iloc[0]
                col_c1, col_c2 = st.columns(2)
                col_c1.info(f"**Kunde:** {pd_data.get('Kunde_Name')} | **Adresse:** {pd_data.get('Kunde_Adresse')}")
                col_c2.info(f"**Fugen:** Zement: {pd_data.get('Fuge_Zement')} / Silikon: {pd_data.get('Fuge_Silikon')} | **Asbest:** {pd_data.get('Asbest_Gefahr')}")
                
                df_hp, _ = ds.read_csv(service, P_FID, "Baustellen_Rapport.csv")
                df_hz, _ = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
                
                if not df_hp.empty and not df_hz.empty:
                    df_hz = validate_time_data(df_hz)
                    df_m = pd.merge(df_hp, df_hz[["Erfasst", "Arbeitszeit_inkl_Reisezeit"]], on="Erfasst", how="left")
                    df_m = df_m[df_m["Projekt"] == ctrl_proj]
                    
                    if not df_m.empty:
                        total_h = df_m["Arbeitszeit_inkl_Reisezeit"].sum()
                        st.metric("Total erfasste Stunden (Inkl. Reise)", round(total_h, 2))
                        st.dataframe(df_m[["Datum", "Mitarbeiter", "Arbeit", "Material", "Arbeitszeit_inkl_Reisezeit"]].sort_values("Datum", ascending=False), use_container_width=True)
                    else:
                        st.write("Noch keine Buchungen für dieses Projekt.")

    # -----------------------------
    # 7.2 PERSONAL-ORDNER (VALIDIERUNG)
    # -----------------------------
    with t_pers:
        st.markdown("**Schritt 2: Admin-Freigabe der AZK-Tabellen**")
        st.info("Ändern Sie den Status in der Spalte auf 'Geprüft', damit der Mitarbeiter das Dokument digital signieren kann.")
        
        df_emp, _ = ds.read_csv(service, P_FID, "Employees.csv")
        emp_list = [name for name in df_emp["Name"].tolist() if str(name).strip() != ""] if not df_emp.empty else ["Keine Mitarbeiter"]
        sel_emp = st.selectbox("Mitarbeiter-Ordner wählen:", emp_list)
        
        df_z, fid_z = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
        if not df_z.empty and sel_emp != "Keine Mitarbeiter":
            df_z = validate_time_data(df_z)
            # Filter nur für den gewählten Mitarbeiter
            df_emp_z = df_z[df_z["Mitarbeiter"] == sel_emp].copy()
            if not df_emp_z.empty:
                df_emp_z['Sort'] = df_emp_z['Status'].map({ST_DRAFT: 1, ST_CHECKED: 2, ST_READY: 3, ST_ARCHIVED: 4}).fillna(5)
                df_emp_z = df_emp_z.sort_values(by=['Sort', 'Datum']).drop(columns=['Sort'])
                
                edit_z = st.data_editor(df_emp_z, num_rows="dynamic", use_container_width=True, key=f"ed_{sel_emp}")
                
                if st.button("💾 Geprüfte Daten sichern", type="primary"):
                    # Update the master dataframe with the edited employee subset
                    df_z.set_index('Erfasst', inplace=True)
                    edit_z.set_index('Erfasst', inplace=True)
                    df_z.update(edit_z)
                    df_z.reset_index(inplace=True)
                    ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                    st.cache_data.clear()
                    st.success("Zeitkonto aktualisiert.")
            else:
                st.write("Noch keine Erfassungen für diesen Mitarbeiter.")

    # -----------------------------
    # 7.3 STAMMDATEN
    # -----------------------------
    with t_stam:
        edit_proj = st.data_editor(df_proj, num_rows="dynamic", key="ep", use_container_width=True)
        if st.button("💾 Projekte aktualisieren"): 
            clean_proj = edit_proj[edit_proj["Projekt_Name"].astype(str).str.strip() != ""]
            ds.save_csv(service, P_FID, "Projects.csv", clean_proj, fid_proj)
            st.cache_data.clear(); st.success("Gespeichert.")
        
        st.markdown("**Personal-Stammdaten & Zugangs-PIN**")
        edit_emp = st.data_editor(df_emp, num_rows="dynamic", key="ee", use_container_width=True)
        if st.button("💾 Personal aktualisieren"): 
            clean_emp = edit_emp[edit_emp["Name"].astype(str).str.strip() != ""]
            ds.save_csv(service, P_FID, "Employees.csv", clean_emp, fid_emp)
            st.cache_data.clear(); st.success("Gespeichert.")

    # -----------------------------
    # 7.4 DATEIEN
    # -----------------------------
    with t_docs:
        ap = st.selectbox("Projekt-Auswahl:", active_projs, key="docs_sel")
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
    # 7.5 DRUCK & ARCHIV (Physische Manifestation)
    # -----------------------------
    with t_print:
        st.markdown("**Schritt 4: Physischer Ausdruck & Archivierung**")
        st.info("Nur Einträge mit dem Status 'Druckbereit' (vom Mitarbeiter signiert) werden verarbeitet und nach dem Erzeugen des Dokuments automatisch archiviert.")
        
        print_proj = st.selectbox("Druck-Auswahl:", active_projs, key="prnt")
        
        if st.button("🖨️ Druckfreigabe erstellen") and print_proj != "Keine Projekte gefunden":
            # Lade AZK und checke, ob es "Druckbereit" Einträge für dieses Projekt gibt
            df_z_print, fid_z_print = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
            mask = (df_z_print["Projekt"] == print_proj) & (df_z_print["Status"] == ST_READY)
            
            if df_z_print[mask].empty:
                st.warning(f"Keine druckbereiten Datensätze für {print_proj} gefunden. Die Mitarbeiter müssen die Berichte zuerst bestätigen.")
            else:
                # Update Status auf Archiviert
                df_z_print.loc[mask, "Status"] = ST_ARCHIVED
                ds.save_csv(service, Z_FID, "Arbeitszeit_AKZ.csv", df_z_print, fid_z_print)
                st.cache_data.clear()
                st.success("System-Update: Datensätze wurden sicher im Archiv verschlossen.")
                
                # Generiere HTML Layout mit 15 Leerzeilen für manuelle Nachträge
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
                        <p style="margin-top:0;"><b>Spezifikationen:</b></p>
                        <p>Zementfuge: {f_zem}<br>Silikonfuge: {f_sil}</p>
                        {asb}
                    </div>
                    <div style="width:20%;text-align:right;">
                        <img src="{qr}" width="100"><p style="font-size:10px;margin-top:5px;">Dokument-ID</p>
                    </div>
                </div>
                
                <table style="width:100%; border-collapse:collapse; margin-top:20px;">
                    <tr>
                        <th style="border:1px solid #aaaaaa; padding:10px; width:15%; text-align:left; background-color:#f9f9f9;">Datum</th>
                        <th style="border:1px solid #aaaaaa; padding:10px; width:70%; text-align:left; background-color:#f9f9f9;">Ausgeführte Tätigkeit / Materialverbrauch (Details)</th>
                        <th style="border:1px solid #aaaaaa; padding:10px; width:15%; text-align:center; background-color:#f9f9f9;">Stunden</th>
                    </tr>
                    {html_rows}
                </table>
                
                <div style="margin-top:54px; display:flex; justify-content:space-between;">
                    <div style="border-top:1px solid #000; width:45%; padding-top:10px;">Visum Projektleitung</div>
                    <div style="border-top:1px solid #000; width:45%; padding-top:10px;">Rechtsverbindliche Unterschrift</div>
                </div>
                </body></html>"""
                
                st.components.v1.html(html, height=500, scrolling=True)
                st.download_button("📄 HTML Druckvorlage herunterladen", html, f"Rapport_{print_proj}.html", "text/html", type="primary")

    # -----------------------------
    # 7.6 AZK EXPORT
    # -----------------------------
    with t_alchemist:
        st.markdown("Generiert die standardisierte Excel-Datei (AZK) für die Buchhaltung.")
        df_z, _ = ds.read_csv(service, Z_FID, "Arbeitszeit_AKZ.csv")
        if not df_z.empty:
            df_z = validate_time_data(df_z)
            status = st.selectbox("Qualitäts-Filter:", ["Nur Archiviert/Druckbereit", "Alle Daten (Inkl. Entwürfe)"])
            df_ex = df_z[df_z["Status"].isin([ST_READY, ST_ARCHIVED])] if "Nur" in status else df_z
            if not df_ex.empty:
                df_ex['Datum'] = pd.to_datetime(df_ex['Datum'])
                monat = st.selectbox("Auswertungs-Zeitraum:", sorted(df_ex['Datum'].dt.strftime('%Y-%m').unique().tolist(), reverse=True))
                df_m = df_ex[df_ex['Datum'].dt.strftime('%Y-%m') == monat].copy()
                df_m = df_m[["Datum", "Mitarbeiter", "Projekt", "Stunden_Total", "Reisezeit_bezahlt_Min", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]]
                df_m['Datum'] = df_m['Datum'].dt.strftime('%d.%m.%Y')
                
                out = io.BytesIO()
                with pd.ExcelWriter(out, engine='openpyxl') as w: df_m.to_excel(w, index=False)
                st.download_button("📥 Excel-Export starten", out.getvalue(), f"AZK_{monat}.xlsx", type="primary")

    # -----------------------------
    # 7.7 BEREINIGUNG
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
                emp_list = [name for name in df_emp["Name"].tolist() if str(name).strip() != ""] if not df_emp.empty else ["Keine Mitarbeiter"]
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
