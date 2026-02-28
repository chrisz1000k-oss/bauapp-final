import streamlit as st
import pandas as pd
from datetime import datetime
import time
import io
import urllib.parse
from googleapiclient.http import MediaIoBaseDownload

import drive_store as ds

# ==========================================
# KONFIGURATION & NUMEROLOGIE (TATTVA)
# ==========================================
CACHE_TTL_SECONDS = 108        
MAX_IMAGE_BUFFER = 108         
PAGINATION_LIMIT = 27          

st.set_page_config(page_title="R. Baumgartner AG - Tempel", layout="wide")

# Das Akasha-Interface (Dark Mode CSS)
st.markdown("""
    <style>
        #MainMenu {display: none;}
        footer {display: none;}
        header {visibility: hidden;}
        
        /* Tamas-Basis */
        .stApp, .main { background-color: #121212 !important; }
        
        /* Soma-Silber Typografie */
        html, body, p, div, span, label, h1, h2, h3, h4, h5, h6, li, td, th { 
            color: #E0E0E0 !important; 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        
        /* Jupiter-Gold Akzente & Vayu-Flow */
        .stButton>button { 
            background-color: #D4AF37 !important; 
            color: #121212 !important; 
            transition: all 0.27s ease-in-out; 
            border: none !important;
            font-weight: bold;
            border-radius: 4px;
        }
        .stButton>button:hover { background-color: #b5952f !important; transform: scale(1.02); }
        
        /* Eingabefelder im Dunkelmodus */
        .stTextInput>div>div>input, .stNumberInput>div>div>input, .stTextArea>div>div>textarea, .stSelectbox>div>div>div, div[data-baseweb="select"]>div {
            background-color: #1E1E1E !important;
            color: #E0E0E0 !important;
            border: 1px solid #D4AF37 !important;
        }
        
        /* Expander & Tabs */
        .streamlit-expanderHeader { background-color: #1E1E1E !important; color: #D4AF37 !important; border-bottom: 1px solid #D4AF37; }
        .stTabs [data-baseweb="tab-list"] button { color: #E0E0E0; }
        .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] { color: #D4AF37 !important; border-bottom-color: #D4AF37 !important; }
        
        /* Trennlinien */
        hr { border-color: #D4AF37 !important; opacity: 0.27; margin-top: 27px; margin-bottom: 27px; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# FLIESSENDE HELFER & SHIVA (AUFLÖSUNG)
# ==========================================
@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_project_files_flowing(_service, folder_id: str, project_name: str) -> list:
    if not folder_id: return []
    query = f"'{folder_id}' in parents and name contains '{project_name}' and trashed = false"
    try:
        results = _service.files().list(q=query, fields="files(id, name)").execute()
        return results.get('files', [])[:MAX_IMAGE_BUFFER]
    except Exception:
        return []

@st.cache_data(ttl=CACHE_TTL_SECONDS * 10, show_spinner=False)
def get_file_bytes_flowing(_service, file_id: str):
    try:
        request = _service.files().get_media(fileId=file_id)
        file_handler = io.BytesIO()
        downloader = MediaIoBaseDownload(file_handler, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return file_handler.getvalue()
    except Exception:
        return None

def shiva_delete_assets(_service, keyword, folders):
    """Vernichtet alle physischen Dateien unwiderruflich aus dem Akasha (Drive)."""
    for fid in folders:
        if not fid: continue
        query = f"'{fid}' in parents and name contains '{keyword}'"
        try:
            results = _service.files().list(q=query, fields="files(id)").execute()
            for f in results.get('files', []):
                _service.files().delete(fileId=f['id']).execute()
        except Exception:
            pass

def init_cosmic_state():
    if "logged_in" not in st.session_state: st.session_state["logged_in"] = False
    if "user_role" not in st.session_state: st.session_state["user_role"] = ""
    if "user_name" not in st.session_state: st.session_state["user_name"] = ""
    if "view" not in st.session_state: st.session_state["view"] = "Start"

def render_header():
    col_logo, col_name = st.columns([1, 6])
    with col_logo:
        try: st.image("logo.png", use_container_width=True)
        except Exception: st.markdown("<h1 style='margin-bottom:27px;'>🏗️</h1>", unsafe_allow_html=True)
    with col_name:
        st.markdown("<h1 style='color:#D4AF37; margin-top:0px;'>R. Baumgartner AG</h1>", unsafe_allow_html=True)
    st.divider()

def align_project_dataframe(df):
    expected_cols = ["Projekt_ID", "Auftragsnummer", "Projekt_Name", "Status", "Kunde_Name", "Kunde_Adresse", "Kunde_Email", "Kunde_Telefon", "Kunde_Kontakt", "Fuge_Zement", "Fuge_Silikon", "Asbest_Gefahr"]
    for col in expected_cols:
        if col not in df.columns: df[col] = "Nein" if col == "Asbest_Gefahr" else ""
    return df

def align_zeit_dataframe(df):
    expected_cols = ["Pause_Min", "R_Wohn_Bau_Min", "R_Bau_Wohn_Min", "R_Mag_Bau_Min", "R_Bau_Mag_Min", "Reisezeit_bezahlt_Min", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = 0 if "Min" in col or "Arbeitszeit" in col else ("" if col == "Absenz_Typ" else "ENTWURF")
    return df

def process_rapport_saving(service, is_absenz, f_date, f_hours, f_pause, f_arbeit, f_mat, f_bem, sel_proj, r_wb, r_bw, r_mb, r_bm, absenz_typ, PROJEKT_FID, ZEIT_FID):
    if sel_proj == "Bitte Projekte im Admin-Bereich anlegen":
        st.error("Bitte wähle ein gültiges Projekt aus.")
        return
        
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if is_absenz:
        row_projekt = {"Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"), "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Arbeit": f"ABSENZ: {absenz_typ}", "Material": "", "Bemerkung": f_bem, "Status": "ENTWURF"}
        row_zeit = {"Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"), "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Start": "-", "Ende": "-", "Pause_Min": 0, "Stunden_Total": f_hours, "R_Wohn_Bau_Min": 0, "R_Bau_Wohn_Min": 0, "R_Mag_Bau_Min": 0, "R_Bau_Mag_Min": 0, "Reisezeit_bezahlt_Min": 0, "Arbeitszeit_inkl_Reisezeit": f_hours, "Absenz_Typ": absenz_typ, "Status": "ENTWURF"}
        msg = f"✅ Absenz ({absenz_typ}) für {f_hours} Std. im Akasha verankert."
    else:
        # SPV VAYU-NAVIGATION LOGIK (Dharma)
        bezahlt_wb = max(0, r_wb - 30)
        bezahlt_bw = max(0, r_bw - 30)
        bezahlt_mb = r_mb 
        bezahlt_bm = r_bm 
        
        reise_min_bezahlt = bezahlt_wb + bezahlt_bw + bezahlt_mb + bezahlt_bm
        reise_stunden_bezahlt = round(reise_min_bezahlt / 60, 2)
        total_inkl_reise = round(f_hours + reise_stunden_bezahlt, 2)
        
        row_projekt = {"Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"), "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": "ENTWURF"}
        row_zeit = {"Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"), "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Start": "-", "Ende": "-", "Pause_Min": f_pause, "Stunden_Total": f_hours, "R_Wohn_Bau_Min": r_wb, "R_Bau_Wohn_Min": r_bw, "R_Mag_Bau_Min": r_mb, "R_Bau_Mag_Min": r_bm, "Reisezeit_bezahlt_Min": reise_min_bezahlt, "Arbeitszeit_inkl_Reisezeit": total_inkl_reise, "Absenz_Typ": "", "Status": "ENTWURF"}
        msg = f"✅ Rapport erfasst. Vergütete Reisezeit: {reise_min_bezahlt} Min. AZK-Total: {total_inkl_reise} Std."

    df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
    df_p = pd.concat([df_p, pd.DataFrame([row_projekt])], ignore_index=True)
    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", df_p, fid_p)
    
    df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
    df_z = align_zeit_dataframe(df_z)
    df_z = pd.concat([df_z, pd.DataFrame([row_zeit])], ignore_index=True)
    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
    
    st.success(msg)

# ==========================================
# HAUPT-LOGIK (MANDALA)
# ==========================================
def main_flow():
    init_cosmic_state()
    render_header()

    try: service = ds.get_drive_service()
    except Exception: st.error("Kritischer Fehler im Prithvi-Fundament."); st.stop()
    if not service: st.warning("⚠️ Keine Verbindung zum Äther (Google Drive)."); st.stop()

    try:
        s = st.secrets.get("general", st.secrets)
        PHOTOS_FID = s.get("PHOTOS_FOLDER_ID", "")
        PROJEKT_FID = s.get("PROJECT_REPORTS_FOLDER_ID", "")
        ZEIT_FID = s.get("TIME_REPORTS_FOLDER_ID", "")
        PLAENE_FID = s.get("PLANS_FOLDER_ID", "")
        ADMIN_PIN = s.get("ADMIN_PIN", "1234")
        BASE_URL = s.get("BASE_APP_URL", "https://8bv6gzagymvrdgnm8wrtrq.streamlit.app")
    except Exception: st.error("Konfigurationsfehler in den Secrets."); st.stop()

    view = st.session_state["view"]

    # ---------------------------------------------------------
    # Pforten (Login)
    # ---------------------------------------------------------
    if view == "Start":
        st.markdown("<h3 style='text-align: center; color: #D4AF37;'>Wähle deinen Pfad</h3>", unsafe_allow_html=True)
        st.write("<div style='height: 27px;'></div>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            if st.button("👷‍♂️ Mitarbeiter-Bereich", use_container_width=True): st.session_state["view"] = "Mitarbeiter_Login"; st.rerun()
        with col2:
            if st.button("🔐 Admin-Bereich", use_container_width=True): st.session_state["view"] = "Admin_Login"; st.rerun()

    elif view == "Admin_Login":
        if st.button("⬅️ Zurück"): st.session_state["view"] = "Start"; st.rerun()
        st.subheader("🔐 Admin Zugang")
        pin_input = st.text_input("PIN eingeben", type="password")
        if st.button("Eintreten", type="primary"):
            if str(pin_input).strip() == str(ADMIN_PIN).strip():
                st.session_state["logged_in"] = True; st.session_state["user_role"] = "Admin"; st.session_state["view"] = "Admin_Dashboard"; st.rerun()
            else: st.error("Zugang verweigert.")

    elif view == "Mitarbeiter_Login":
        if st.button("⬅️ Zurück"): st.session_state["view"] = "Start"; st.rerun()
        st.subheader("👋 Identifikation")
        df_emp, _ = ds.read_csv(service, PROJEKT_FID, "Employees.csv")
        active_emps = df_emp[df_emp["Status"] == "Aktiv"]["Name"].tolist() if not df_emp.empty and "Status" in df_emp.columns else ["Bitte Stammdaten anlegen"]
        selected_employee = st.selectbox("Wer bist du?", active_emps)
        if st.button("Einloggen", type="primary"):
            if selected_employee != "Bitte Stammdaten anlegen":
                st.session_state["user_name"] = selected_employee; st.session_state["view"] = "Mitarbeiter_Dashboard"; st.rerun()

    # ---------------------------------------------------------
    # MITARBEITER DASHBOARD
    # ---------------------------------------------------------
    elif view == "Mitarbeiter_Dashboard":
        col_back, col_title = st.columns([1, 4])
        with col_back:
            if st.button("🚪 Logout"): st.session_state["user_name"] = ""; st.session_state["view"] = "Start"; st.rerun()
        with col_title: st.subheader(f"📋 Namaste, {st.session_state['user_name']}")
        
        df_proj, _ = ds.read_csv(service, PROJEKT_FID, "Projects.csv")
        df_proj = align_project_dataframe(df_proj)
        active_projs = df_proj[df_proj["Status"] == "Aktiv"]["Projekt_Name"].tolist() if not df_proj.empty else ["Bitte Projekte anlegen"]
        selected_project = st.selectbox("Aktuelles Projekt:", active_projs)
        
        if selected_project != "Bitte Projekte anlegen":
            proj_data = df_proj[df_proj["Projekt_Name"] == selected_project].iloc[0]
            with st.expander("ℹ️ Stoffliche Matrix (Kunde & Asbest)", expanded=False):
                c_info, m_info = st.columns(2)
                with c_info:
                    st.write(f"**Kunde:** {proj_data.get('Kunde_Name', '-')} | {proj_data.get('Kunde_Adresse', '-')}")
                    st.write(f"**Telefon:** {proj_data.get('Kunde_Telefon', '-')}")
                with m_info:
                    if str(proj_data.get('Asbest_Gefahr', 'Nein')).strip().lower() == "ja": 
                        st.markdown("<p style='color:#ff4b4b; font-weight:bold;'>⚠️ ASBEST VORHANDEN</p>", unsafe_allow_html=True)
        
        st.write("<div style='height: 27px;'></div>", unsafe_allow_html=True)
        t_arb, t_abs, t_med, t_hist = st.tabs(["🛠️ Arbeit", "🏥 Absenz", "📤 Medien", "📜 Signatur"])
        
        with t_arb:
            with st.form("arb_form"):
                c1, c2, c3 = st.columns(3)
                with c1: f_date = st.date_input("Datum", datetime.now())
                with c2: f_hours = st.number_input("Baustellen-Stunden", min_value=0.0, value=8.5, step=0.25)
                with c3: f_pause = st.number_input("Pausen (Min)", min_value=0, value=30, step=15)
                
                st.markdown("**🚗 Vayu-Navigation (Eingabe in Minuten pro Weg)**")
                r1, r2, r3, r4 = st.columns(4)
                with r1: r_wb = st.number_input("Wohnort ➔ Baustelle", value=0, step=5)
                with r2: r_bw = st.number_input("Baustelle ➔ Wohnort", value=0, step=5)
                with r3: r_mb = st.number_input("Magazin ➔ Baustelle", value=0, step=5)
                with r4: r_bm = st.number_input("Baustelle ➔ Magazin", value=0, step=5)
                
                f_arbeit = st.text_area("Ausgeführte Arbeiten")
                f_mat = st.text_area("Materialeinsatz")
                f_bem = st.text_input("Bemerkung / Behinderungen")
                
                if st.form_submit_button("💾 Rapport (Arbeit) einreichen", type="primary"):
                    process_rapport_saving(service, False, f_date, f_hours, f_pause, f_arbeit, f_mat, f_bem, selected_project, r_wb, r_bw, r_mb, r_bm, "", PROJEKT_FID, ZEIT_FID)

        with t_abs:
            with st.form("abs_form"):
                st.markdown("**Matrix der Nicht-Präsenz**")
                c1, c2 = st.columns(2)
                with c1: f_a_date = st.date_input("Datum (Absenz)", datetime.now())
                with c2: f_a_hours = st.number_input("Stunden (Absenz)", min_value=0.0, value=8.5, step=0.25)
                
                a_typ = st.selectbox("Grund", ["Ferien", "Krankheit", "Unfall (SUVA)", "Feiertag"])
                a_bem = st.text_input("Bemerkung")
                a_file = st.file_uploader("📄 Arztzeugnis (Nur Krankheit)", type=['pdf','jpg','png'])
                
                if st.form_submit_button("💾 Absenz verankern", type="primary"):
                    if a_file and a_typ == "Krankheit":
                        fname = f"ZEUGNIS_{st.session_state['user_name']}_{f_a_date}_{a_file.name}"
                        ds.upload_image(service, PLAENE_FID, fname, io.BytesIO(a_file.getvalue()), a_file.type)
                    process_rapport_saving(service, True, f_a_date, f_a_hours, 0, "", "", a_bem, selected_project, 0, 0, 0, 0, a_typ, PROJEKT_FID, ZEIT_FID)

        with t_med:
            col_up, col_gal = st.columns([1, 2])
            with col_up:
                files = st.file_uploader("Fotos hochladen", accept_multiple_files=True, type=['jpg','png','jpeg'])
                if st.button("📤 Upload", type="primary") and files:
                    prog = st.progress(0)
                    for idx, f in enumerate(files[:PAGINATION_LIMIT]):
                        fname = f"{selected_project}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{f.name}"
                        ds.upload_image(service, PHOTOS_FID, fname, io.BytesIO(f.getvalue()), f.type)
                        prog.progress((idx + 1) / len(files))
                    st.success("Im Akasha gespeichert."); st.cache_data.clear(); time.sleep(1); st.rerun()
            with col_gal:
                if st.button("🔄 Ansicht aktualisieren"): st.cache_data.clear()
                all_files = []
                if PHOTOS_FID: all_files.extend(load_project_files_flowing(service, PHOTOS_FID, selected_project))
                if PLAENE_FID: all_files.extend(load_project_files_flowing(service, PLAENE_FID, selected_project))
                if all_files:
                    cols = st.columns(2)
                    for idx, img in enumerate(all_files):
                        with cols[idx % 2]:
                            img_b = get_file_bytes_flowing(service, img['id'])
                            if img_b:
                                if img['name'].lower().endswith(('.png', '.jpg', '.jpeg')): st.image(img_b, use_container_width=True)
                                else: st.download_button(f"📥 {img['name'][:15]}", data=img_b, file_name=img['name'])

        with t_hist:
            st.markdown("<h4 style='color:#D4AF37;'>Das 2. Siegel: Digitale Signatur</h4>", unsafe_allow_html=True)
            df_hist_p, _ = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
            df_hist_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
            
            if not df_hist_p.empty and not df_hist_z.empty and "Erfasst" in df_hist_z.columns:
                df_hist_z = align_zeit_dataframe(df_hist_z)
                df_merged = pd.merge(df_hist_p, df_hist_z[["Erfasst", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]], on="Erfasst", how="left")
                df_proj_hist = df_merged[(df_merged["Projekt"] == selected_project) & (df_merged["Mitarbeiter"] == st.session_state["user_name"])].sort_values(by="Datum", ascending=False)
                
                for _, row in df_proj_hist.head(PAGINATION_LIMIT).iterrows():
                    status_str = str(row.get('Status', 'ENTWURF'))
                    status_color = "🔴" if status_str == "ENTWURF" else "🟡" if status_str == "FREIGEGEBEN" else "🟢"
                    titel = f"{status_color} {row['Datum']} | {row.get('Absenz_Typ', '')} Total: {row.get('Arbeitszeit_inkl_Reisezeit', '-')} Std."
                    
                    with st.expander(titel):
                        st.write(f"**Arbeit:** {row.get('Arbeit', '-')}")
                        if status_str == "FREIGEGEBEN":
                            st.info("Admin hat das 1. Siegel (Freigabe) gesetzt. Bitte bestätigen.")
                            if st.button(f"✍️ Signieren", key=f"sig_{row['Erfasst']}"):
                                idx_up = df_hist_z[df_hist_z['Erfasst'] == row['Erfasst']].index
                                if not idx_up.empty:
                                    df_hist_z.loc[idx_up, "Status"] = "SIGNIERT"
                                    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", df_hist_z, fid_z)
                                    st.success("Besiegelt!"); time.sleep(1); st.rerun()

    # ---------------------------------------------------------
    # ADMIN DASHBOARD
    # ---------------------------------------------------------
    elif view == "Admin_Dashboard":
        col1, col2 = st.columns([4, 1])
        with col1: st.subheader("🛠️ Admin Zentrale")
        with col2:
            if st.button("🚪 Logout", use_container_width=True): st.session_state["logged_in"] = False; st.session_state["view"] = "Start"; st.rerun()
        
        t_zeit, t_bau, t_stam, t_docs, t_print, t_shiva = st.tabs(["🕒 Validierung", "🏗️ Rapporte", "⚙️ Stammdaten", "📂 Medien", "🖨️ Druck", "🔥 Shiva"])
        
        with t_zeit:
            st.markdown("**Das 1. Siegel (Admin-Freigabe)**")
            df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
            if not df_z.empty:
                df_z = align_zeit_dataframe(df_z)
                df_z['Sort_Order'] = df_z['Status'].map({'ENTWURF': 1, 'FREIGEGEBEN': 2, 'SIGNIERT': 3}).fillna(4)
                df_z = df_z.sort_values(by=['Sort_Order', 'Datum']).drop(columns=['Sort_Order'])
                edit_z = st.data_editor(df_z, num_rows="dynamic", use_container_width=True)
                if st.button("💾 Speichern & Freigeben (AZK)", type="primary"):
                    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", edit_z, fid_z)
                    st.success("AZK synchronisiert.")

        with t_bau:
            df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
            if not df_p.empty:
                edit_p = st.data_editor(df_p, num_rows="dynamic", use_container_width=True)
                if st.button("💾 Speichern (Rapporte)", type="primary"):
                    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", edit_p, fid_p)
                    st.success("Gespeichert.")

        with t_stam:
            df_proj, fid_proj = ds.read_csv(service, PROJEKT_FID, "Projects.csv")
            df_proj = align_project_dataframe(df_proj)
            st.markdown("**🏗️ Projekte**")
            edit_proj = st.data_editor(df_proj, num_rows="dynamic", key="e_proj", use_container_width=True)
            if st.button("💾 Projekte Sichern"): ds.save_csv(service, PROJEKT_FID, "Projects.csv", edit_proj, fid_proj)

            df_emp, fid_emp = ds.read_csv(service, PROJEKT_FID, "Employees.csv")
            st.markdown("**👷‍♂️ Mitarbeiter**")
            if df_emp.empty or "Mitarbeiter_ID" not in df_emp.columns:
                df_emp = pd.DataFrame({"Mitarbeiter_ID": ["M01"], "Name": ["Christoph Schlorff"], "Status": ["Aktiv"]})
            edit_emp = st.data_editor(df_emp, num_rows="dynamic", key="e_emp", use_container_width=True)
            if st.button("💾 Mitarbeiter Sichern"): ds.save_csv(service, PROJEKT_FID, "Employees.csv", edit_emp, fid_emp)

        with t_docs:
            active_projs = df_proj["Projekt_Name"].tolist() if not df_proj.empty else ["Keine Projekte gefunden"]
            admin_sel_proj = st.selectbox("Projekt:", active_projs, key="admin_docs_sel")
            
            c_u1, c_u2 = st.columns(2)
            with c_u1:
                plan_f = st.file_uploader("📤 Pläne laden", accept_multiple_files=True, type=['pdf', 'jpg', 'png'])
                if st.button("Pläne ins Drive") and plan_f and PLAENE_FID:
                    for f in plan_f: ds.upload_image(service, PLAENE_FID, f"{admin_sel_proj}_PLAN_{f.name}", io.BytesIO(f.getvalue()), f.type)
                    st.success("Pläne im Akasha.")
            with c_u2:
                foto_f = st.file_uploader("📷 Fotos laden", accept_multiple_files=True, type=['jpg', 'png'])
                if st.button("Fotos ins Drive") and foto_f:
                    for f in foto_f: ds.upload_image(service, PHOTOS_FID, f"{admin_sel_proj}_ADMIN_{f.name}", io.BytesIO(f.getvalue()), f.type)
                    st.success("Fotos im Akasha.")
            
            st.divider()
            if st.button("🔄 Galerie laden"): st.cache_data.clear()
            admin_files = []
            if PHOTOS_FID: admin_files.extend(load_project_files_flowing(service, PHOTOS_FID, admin_sel_proj))
            if PLAENE_FID: admin_files.extend(load_project_files_flowing(service, PLAENE_FID, admin_sel_proj))
            if admin_files:
                cols = st.columns(4)
                for idx, img in enumerate(admin_files):
                    with cols[idx % 4]:
                        img_bytes = get_file_bytes_flowing(service, img['id'])
                        if img_bytes:
                            if img['name'].lower().endswith(('.png', '.jpg', '.jpeg')): st.image(img_bytes, use_container_width=True)
                            else: st.download_button(f"📥 {img['name'][:15]}", data=img_bytes, file_name=img['name'])

        with t_print:
            st.markdown("**Das 3. Siegel: Physische Manifestation (Druckvorlage)**")
            st.info("💡 Die Druckvorlage invertiert die Farben für den Papiertausdruck automatisch (Weißer Hintergrund).")
            print_proj = st.selectbox("Projekt für Druckvorlage:", active_projs, key="admin_print_sel")
            if st.button("🖨️ Druckvorlage generieren", type="primary") and print_proj != "Keine Projekte gefunden":
                safe_proj_name = urllib.parse.quote(print_proj)
                qr_url = f"{BASE_URL}?projekt={safe_proj_name}"
                qr_api_url = f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={urllib.parse.quote(qr_url)}"
                table_rows = "".join(["<tr><td></td><td></td><td></td><td></td><td></td></tr>" for _ in range(15)])
                
                # HTML erzwingt schwarz auf weiß für den physischen Druck
                html_content = f"""
                <!DOCTYPE html><html><head><title>Rapport - {print_proj}</title>
                <style>
                @page {{ size: A4; margin: 15mm; }} 
                body {{ font-family: Arial; font-size: 12px; background-color: #ffffff; color: #000000; margin:0; padding:0; }}
                .h-grid {{ display: flex; justify-content: space-between; border-bottom: 2px solid #000; padding-bottom: 27px; margin-bottom: 27px; }}
                table {{ width: 100%; border-collapse: collapse; }} th, td {{ border: 1px solid #000; padding: 10px; }} td {{ height: 50px; }}
                .sig-box {{ margin-top: 54px; border-top: 1px solid #000; width: 300px; padding-top: 10px; font-weight: bold; }}
                </style>
                </head><body>
                <div class="h-grid"><div><h1>R. Baumgartner AG</h1><p>Projekt: {print_proj}</p></div>
                <div><img src="{qr_api_url}" width="100"><p style="font-size:10px;">Zum digitalen Rapport</p></div></div>
                <table><thead><tr><th>Datum</th><th>Mitarbeiter</th><th>Stunden (Total)</th><th>Ausgeführte Arbeiten / Material</th><th>Notizen</th></tr></thead>
                <tbody>{table_rows}</tbody></table>
                <div class="sig-box">Handschriftliche Signatur (Physisches Siegel)</div>
                </body></html>
                """
                st.components.v1.html(html_content, height=500, scrolling=True)
                st.download_button("📄 HTML für Druck laden", data=html_content, file_name=f"Rapport_{print_proj}.html", mime="text/html")

        with t_shiva:
            st.markdown("<h2 style='color: #ff4b4b;'>🔥 Shiva-Modus (Auflösung)</h2>", unsafe_allow_html=True)
            st.warning("ACHTUNG: Vernichtung ist unwiderruflich. Dateien im Drive und Tabellen-Einträge werden gelöscht.")
            
            del_type = st.radio("Was soll aufgelöst werden?", ["Projekt", "Mitarbeiter"])
            sadashiva_check = st.checkbox("Sadashiva-Modus bestätigen")
            
            if del_type == "Projekt":
                del_target = st.selectbox("Projekt zur Vernichtung wählen:", active_projs)
                if st.button("🛑 Projekt Auflösen") and sadashiva_check:
                    df_proj = df_proj[df_proj["Projekt_Name"] != del_target]
                    ds.save_csv(service, PROJEKT_FID, "Projects.csv", df_proj, fid_proj)
                    
                    df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
                    df_p = df_p[df_p["Projekt"] != del_target]
                    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", df_p, fid_p)
                    
                    df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
                    df_z = df_z[df_z["Projekt"] != del_target]
                    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                    
                    shiva_delete_assets(service, del_target, [PHOTOS_FID, PLAENE_FID])
                    st.success(f"🔥 Projekt '{del_target}' wurde restlos getilgt."); time.sleep(2); st.rerun()
            else:
                del_emp = st.selectbox("Mitarbeiter zur Vernichtung wählen:", df_emp["Name"].tolist() if not df_emp.empty else [])
                if st.button("🛑 Mitarbeiter Auflösen") and sadashiva_check:
                    df_emp = df_emp[df_emp["Name"] != del_emp]
                    ds.save_csv(service, PROJEKT_FID, "Employees.csv", df_emp, fid_emp)
                    st.success(f"🔥 Mitarbeiter '{del_emp}' wurde aus den Stammdaten getilgt."); time.sleep(2); st.rerun()

if __name__ == "__main__":
    main_flow()
