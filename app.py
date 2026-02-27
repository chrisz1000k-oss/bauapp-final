import streamlit as st
import pandas as pd
from datetime import datetime
import time
import io
import urllib.parse
from googleapiclient.http import MediaIoBaseDownload

import drive_store as ds

# ==========================================
# KONFIGURATION & NUMEROLOGIE
# ==========================================
CACHE_TTL_SECONDS = 108        
MAX_IMAGE_BUFFER = 108         
PAGINATION_LIMIT = 27          

st.set_page_config(page_title="R. Baumgartner AG - BauApp", layout="wide")

st.markdown("""
    <style>
        #MainMenu {display: none;}
        footer {display: none;}
        header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# FLIESSENDE HELFER
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

def init_cosmic_state():
    if "logged_in" not in st.session_state: st.session_state["logged_in"] = False
    if "user_role" not in st.session_state: st.session_state["user_role"] = ""
    if "user_name" not in st.session_state: st.session_state["user_name"] = ""
    if "view" not in st.session_state: st.session_state["view"] = "Start"

def render_header():
    col_logo, col_name = st.columns([1, 6])
    with col_logo:
        try:
            st.image("logo.png", use_container_width=True)
        except Exception:
            st.markdown("<h1>🏗️</h1>", unsafe_allow_html=True)
    with col_name:
        st.markdown("<h1 style='color:#1E3A8A; margin-top:0px;'>R. Baumgartner AG</h1>", unsafe_allow_html=True)
    st.divider()

def process_rapport_saving(service, f_date, f_start, f_end, f_pause, f_reise, f_arbeit, f_mat, f_bem, sel_proj, PROJEKT_FID, ZEIT_FID):
    t1 = datetime.combine(f_date, f_start)
    t2 = datetime.combine(f_date, f_end)
    diff = (t2 - t1).total_seconds() / 3600
    hours = round(diff - f_pause, 2)
    
    if hours < 0:
        st.error("Fehler: Arbeitsende liegt vor Arbeitsbeginn!")
        return
    if sel_proj == "Bitte Projekte im Admin-Bereich anlegen":
        st.error("Bitte wähle ein gültiges Projekt aus.")
        return
        
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    row_projekt = {"Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"), "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": "DRAFT"}
    row_zeit = {"Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"), "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Start": f_start.strftime("%H:%M"), "Ende": f_end.strftime("%H:%M"), "Pause": f_pause, "Stunden_Total": hours, "Reise_Min": f_reise, "Status": "DRAFT"}
    
    df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
    df_p = pd.concat([df_p, pd.DataFrame([row_projekt])], ignore_index=True)
    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", df_p, fid_p)
    
    df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
    df_z = pd.concat([df_z, pd.DataFrame([row_zeit])], ignore_index=True)
    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
    
    st.success("✅ Rapport erfolgreich im System verankert.")

# ==========================================
# HAUPT-LOGIK
# ==========================================
def main_flow():
    init_cosmic_state()
    render_header()

    try:
        service = ds.get_drive_service()
    except Exception:
        st.error("Kritischer Fehler: secrets.toml ist nicht korrekt konfiguriert.")
        st.stop()

    if not service:
        st.warning("⚠️ Keine Verbindung zu Google Drive. Token fehlt.")
        st.stop()

    try:
        s = st.secrets.get("general", st.secrets)
        PHOTOS_FID = s.get("PHOTOS_FOLDER_ID", "")
        PROJEKT_FID = s.get("PROJECT_REPORTS_FOLDER_ID", "")
        ZEIT_FID = s.get("TIME_REPORTS_FOLDER_ID", "")
        PLAENE_FID = s.get("PLANS_FOLDER_ID", "")
        ADMIN_PIN = s.get("ADMIN_PIN", "1234")
        BASE_URL = s.get("BASE_APP_URL", "https://8bv6gzagymvrdgnm8wrtrq.streamlit.app")
    except Exception:
        st.error("Konfigurationsfehler in den Secrets.")
        st.stop()

    view = st.session_state["view"]

    # ---------------------------------------------------------
    # STARTSEITE
    # ---------------------------------------------------------
    if view == "Start":
        st.subheader("Bitte wählen Sie Ihren Bereich aus:")
        st.write("") 
        col1, col2 = st.columns(2)
        with col1:
            if st.button("👷‍♂️ Mitarbeiter-Bereich", use_container_width=True):
                st.session_state["view"] = "Mitarbeiter_Login"
                st.rerun()
        with col2:
            if st.button("🔐 Admin-Bereich", use_container_width=True):
                st.session_state["view"] = "Admin_Login"
                st.rerun()

    # ---------------------------------------------------------
    # LOGIN BEREICHE
    # ---------------------------------------------------------
    elif view == "Admin_Login":
        if st.button("⬅️ Zurück"):
            st.session_state["view"] = "Start"
            st.rerun()
        st.subheader("🔐 Admin Login")
        pin_input = st.text_input("PIN eingeben", type="password")
        if st.button("Eintreten", type="primary"):
            if str(pin_input).strip() == str(ADMIN_PIN).strip():
                st.session_state["logged_in"] = True
                st.session_state["user_role"] = "Admin"
                st.session_state["view"] = "Admin_Dashboard"
                st.rerun()
            else:
                st.error("Falsche PIN")

    elif view == "Mitarbeiter_Login":
        if st.button("⬅️ Zurück"):
            st.session_state["view"] = "Start"
            st.rerun()
        st.subheader("👋 Identifikation")
        df_emp, _ = ds.read_csv(service, PROJEKT_FID, "Employees.csv")
        if not df_emp.empty and "Status" in df_emp.columns:
            active_emps = df_emp[df_emp["Status"] == "Aktiv"]["Name"].tolist()
        else:
            active_emps = ["Bitte Stammdaten im Admin-Bereich anlegen"]
            
        selected_employee = st.selectbox("Wähle deinen Namen:", active_emps)
        if st.button("Einloggen", type="primary"):
            if selected_employee != "Bitte Stammdaten im Admin-Bereich anlegen":
                st.session_state["user_name"] = selected_employee
                st.session_state["view"] = "Mitarbeiter_Dashboard"
                st.rerun()
            else:
                st.error("Blockiert: Keine aktiven Mitarbeiter.")

    # ---------------------------------------------------------
    # MITARBEITER DASHBOARD
    # ---------------------------------------------------------
    elif view == "Mitarbeiter_Dashboard":
        col_back, col_title = st.columns([1, 4])
        with col_back:
            if st.button("🚪 Logout"):
                st.session_state["user_name"] = ""
                st.session_state["view"] = "Start"
                st.rerun()
        with col_title:
            st.subheader(f"📋 Rapport: {st.session_state['user_name']}")
        
        df_proj, _ = ds.read_csv(service, PROJEKT_FID, "Projects.csv")
        if not df_proj.empty and "Status" in df_proj.columns:
            active_projs = df_proj[df_proj["Status"] == "Aktiv"]["Projekt_Name"].tolist()
        else:
            active_projs = ["Bitte Projekte im Admin-Bereich anlegen"]

        selected_project = st.selectbox("Projekt:", active_projs)
        
        # 4 TABS: Inklusive Projekt-Historie (Transparenz)
        tab1, tab2, tab3, tab4 = st.tabs(["📝 Rapport", "📤 Upload", "🖼️ Pläne & Fotos", "📜 Projekt-Historie"])
        
        with tab1:
            with st.form("ma_form"):
                col_a, col_b = st.columns(2)
                with col_a:
                    f_date = st.date_input("Datum", datetime.now())
                    f_start = st.time_input("Start", datetime.strptime("07:00", "%H:%M").time())
                    f_end = st.time_input("Ende", datetime.strptime("16:30", "%H:%M").time())
                with col_b:
                    f_pause = st.number_input("Pause (Std)", value=0.5, step=0.25)
                    f_reise = st.number_input("Reise (Min)", value=0, step=15)
                
                f_arbeit = st.text_area("Arbeitsbeschrieb (Was wurde gemacht?)")
                f_mat = st.text_area("Materialeinsatz")
                f_bem = st.text_input("Bemerkung / Behinderungen")
                
                if st.form_submit_button("💾 Speichern", type="primary"):
                    process_rapport_saving(service, f_date, f_start, f_end, f_pause, f_reise, f_arbeit, f_mat, f_bem, selected_project, PROJEKT_FID, ZEIT_FID)

        with tab2:
            st.info(f"Bilder für: **{selected_project}**")
            files = st.file_uploader("Wählen", accept_multiple_files=True, type=['jpg','png','jpeg'])
            if st.button("📤 Hochladen", type="primary"):
                if files:
                    prog = st.progress(0)
                    for idx, f in enumerate(files[:PAGINATION_LIMIT]):
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        fname = f"{selected_project}_{ts}_{f.name}"
                        ds.upload_image(service, PHOTOS_FID, fname, io.BytesIO(f.getvalue()), f.type)
                        prog.progress((idx + 1) / len(files))
                    st.success("✅ Upload abgeschlossen.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()

        with tab3:
            if st.button("🔄 Ansicht aktualisieren"):
                st.cache_data.clear()
                st.rerun()
                
            # Kombiniert Pläne und Fotos
            all_files = []
            if PHOTOS_FID: all_files.extend(load_project_files_flowing(service, PHOTOS_FID, selected_project))
            if PLAENE_FID: all_files.extend(load_project_files_flowing(service, PLAENE_FID, selected_project))
            
            if not all_files:
                st.info("Noch keine Fotos oder Pläne vorhanden.")
            else:
                cols = st.columns(3)
                for idx, img in enumerate(all_files):
                    with cols[idx % 3]:
                        img_bytes = get_file_bytes_flowing(service, img['id'])
                        if img_bytes:
                            if img['name'].lower().endswith(('.png', '.jpg', '.jpeg')):
                                st.image(img_bytes, caption=img['name'], use_container_width=True)
                            else:
                                st.download_button(label=f"📥 {img['name']}", data=img_bytes, file_name=img['name'])
                                
        with tab4:
            st.markdown(f"**Letzte Tätigkeiten auf: {selected_project}**")
            df_hist, _ = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
            if not df_hist.empty and "Projekt" in df_hist.columns:
                # Filtern und sortieren (Echtzeit-Transparenz)
                df_proj_hist = df_hist[df_hist["Projekt"] == selected_project].sort_values(by="Datum", ascending=False)
                if df_proj_hist.empty:
                    st.info("Noch keine Rapporte für dieses Projekt.")
                else:
                    for _, row in df_proj_hist.head(20).iterrows(): # Zeigt die letzten 20 Einträge
                        st.markdown(f"🗓️ **{row['Datum']}** | 👷‍♂️ **{row['Mitarbeiter']}**")
                        st.write(f"_{row['Arbeit']}_")
                        st.divider()
            else:
                st.info("Keine Datenbasis gefunden.")

    # ---------------------------------------------------------
    # ADMIN DASHBOARD
    # ---------------------------------------------------------
    elif view == "Admin_Dashboard":
        col1, col2 = st.columns([4, 1])
        with col1:
            st.subheader("🛠️ Admin Zentrale")
        with col2:
            if st.button("🚪 Logout", use_container_width=True):
                st.session_state["logged_in"] = False
                st.session_state["view"] = "Start"
                st.rerun()
        
        t_zeit, t_bau, t_stam, t_docs, t_print = st.tabs(["🕒 AKZ", "🏗️ Rapporte", "⚙️ Stammdaten", "📂 Pläne & Medien", "🖨️ Druckvorlagen"])
        
        with t_zeit:
            df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
            if not df_z.empty:
                edit_z = st.data_editor(df_z, num_rows="dynamic", use_container_width=True)
                if st.button("💾 Speichern (AKZ)", type="primary"):
                    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", edit_z, fid_z)
                    st.success("Gespeichert.")

        with t_bau:
            df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
            if not df_p.empty:
                edit_p = st.data_editor(df_p, num_rows="dynamic", use_container_width=True)
                if st.button("💾 Speichern (Rapporte)", type="primary"):
                    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", edit_p, fid_p)
                    st.success("Gespeichert.")

        with t_stam:
            st.markdown("**🏗️ Projekte**")
            df_proj, fid_proj = ds.read_csv(service, PROJEKT_FID, "Projects.csv")
            if df_proj.empty or "Auftragsnummer" not in df_proj.columns:
                df_proj = pd.DataFrame({"Projekt_ID": ["P100"], "Auftragsnummer": ["A-01"], "Projekt_Name": ["Baustelle A"], "Status": ["Aktiv"]})
            
            edit_proj = st.data_editor(df_proj, num_rows="dynamic", key="e_proj", use_container_width=True)
            if st.button("💾 Projekte Sichern"):
                ds.save_csv(service, PROJEKT_FID, "Projects.csv", edit_proj, fid_proj)
                st.success("Aktualisiert.")

            st.markdown("**👷‍♂️ Mitarbeiter**")
            df_emp, fid_emp = ds.read_csv(service, PROJEKT_FID, "Employees.csv")
            if df_emp.empty or "Mitarbeiter_ID" not in df_emp.columns:
                df_emp = pd.DataFrame({"Mitarbeiter_ID": ["M01"], "Name": ["Christoph Schlorff"], "Status": ["Aktiv"]})
            
            edit_emp = st.data_editor(df_emp, num_rows="dynamic", key="e_emp", use_container_width=True)
            if st.button("💾 Mitarbeiter Sichern"):
                ds.save_csv(service, PROJEKT_FID, "Employees.csv", edit_emp, fid_emp)
                st.success("Aktualisiert.")

        with t_docs:
            st.markdown("**Admin-Souveränität: Dateien verwalten & einsehen**")
            df_proj, _ = ds.read_csv(service, PROJEKT_FID, "Projects.csv")
            active_projs = df_proj["Projekt_Name"].tolist() if not df_proj.empty else ["Keine Projekte gefunden"]
            admin_sel_proj = st.selectbox("Projekt auswählen:", active_projs, key="admin_docs_sel")
            
            col_up1, col_up2 = st.columns(2)
            with col_up1:
                plan_files = st.file_uploader("📤 PDF Pläne hochladen", accept_multiple_files=True, type=['pdf', 'jpg', 'png'])
                if st.button("Pläne ins Drive laden") and plan_files and PLAENE_FID:
                    for f in plan_files: ds.upload_image(service, PLAENE_FID, f"{admin_sel_proj}_PLAN_{f.name}", io.BytesIO(f.getvalue()), f.type)
                    st.success("Pläne hochgeladen.")
            with col_up2:
                foto_files = st.file_uploader("📷 Start-Fotos hochladen", accept_multiple_files=True, type=['jpg', 'png'])
                if st.button("Fotos ins Drive laden") and foto_files:
                    for f in foto_files: ds.upload_image(service, PHOTOS_FID, f"{admin_sel_proj}_ADMIN_{f.name}", io.BytesIO(f.getvalue()), f.type)
                    st.success("Fotos hochgeladen.")
            
            st.divider()
            st.markdown("👁️ **Aktive Projekt-Dateien (Galerie)**")
            if st.button("🔄 Galerie laden/aktualisieren"):
                st.cache_data.clear()
            
            admin_files = []
            if PHOTOS_FID: admin_files.extend(load_project_files_flowing(service, PHOTOS_FID, admin_sel_proj))
            if PLAENE_FID: admin_files.extend(load_project_files_flowing(service, PLAENE_FID, admin_sel_proj))
            
            if not admin_files:
                st.info("Dieses Projekt enthält noch keine Dateien.")
            else:
                cols = st.columns(4)
                for idx, img in enumerate(admin_files):
                    with cols[idx % 4]:
                        img_bytes = get_file_bytes_flowing(service, img['id'])
                        if img_bytes:
                            if img['name'].lower().endswith(('.png', '.jpg', '.jpeg')):
                                st.image(img_bytes, caption=img['name'], use_container_width=True)
                            else:
                                st.download_button(label=f"📥 {img['name'][:15]}...", data=img_bytes, file_name=img['name'])

        with t_print:
            st.markdown("**Output-Synthese: Arbeitsrapport inkl. QR-Code (Backup für Bauwagen)**")
            st.write("Wähle ein Projekt. Lade die HTML-Datei herunter und öffne sie im Browser (Doppelklick), um sie als perfektes PDF auszudrucken.")
            
            print_proj = st.selectbox("Projekt für Druckvorlage:", active_projs, key="admin_print_sel")
            
            if st.button("🖨️ Druckvorlage generieren", type="primary"):
                if print_proj != "Keine Projekte gefunden":
                    safe_proj_name = urllib.parse.quote(print_proj)
                    qr_url = f"{BASE_URL}?projekt={safe_proj_name}"
                    qr_api_url = f"https://api.qrserver.com/v1/create-qr-code/?size=200x200&data={urllib.parse.quote(qr_url)}"
                    
                    # HTML-Synthese für sauberen Druck
                    html_content = f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <title>Rapport - {print_proj}</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; padding: 40px; color: #333; }}
                            .header {{ display: flex; justify-content: space-between; border-bottom: 2px solid #1E3A8A; padding-bottom: 20px; }}
                            .title-area h1 {{ color: #1E3A8A; margin: 0; }}
                            .title-area h2 {{ margin: 5px 0 0 0; color: #555; }}
                            .qr-area img {{ border: 1px solid #ccc; padding: 5px; }}
                            .table {{ width: 100%; border-collapse: collapse; margin-top: 30px; }}
                            .table th, .table td {{ border: 1px solid #000; padding: 15px; text-align: left; }}
                            .table th {{ background-color: #f0f0f0; width: 30%; }}
                        </style>
                    </head>
                    <body>
                        <div class="header">
                            <div class="title-area">
                                <h1>R. Baumgartner AG</h1>
                                <h2>Arbeitsrapport (Backup)</h2>
                                <p><strong>Projekt:</strong> {print_proj}</p>
                            </div>
                            <div class="qr-area">
                                <img src="{qr_api_url}" alt="QR Code">
                                <p style="text-align:center; font-size:12px; margin:0;">Zum digitalen<br>Rapport scannen</p>
                            </div>
                        </div>
                        <table class="table">
                            <tr><th>Datum</th><td></td></tr>
                            <tr><th>Mitarbeiter Name</th><td></td></tr>
                            <tr><th>Arbeitszeit (Von - Bis)</th><td></td></tr>
                            <tr><th>Pause / Reisezeit</th><td></td></tr>
                            <tr><th style="height: 150px;">Ausgeführte Arbeiten</th><td></td></tr>
                            <tr><th style="height: 100px;">Material & Bemerkungen</th><td></td></tr>
                            <tr><th>Unterschrift</th><td></td></tr>
                        </table>
                    </body>
                    </html>
                    """
                    
                    st.components.v1.html(html_content, height=400, scrolling=True)
                    
                    st.download_button(
                        label="📄 HTML-Druckvorlage herunterladen (Zum Drucken öffnen)",
                        data=html_content,
                        file_name=f"Rapportvorlage_{print_proj}.html",
                        mime="text/html"
                    )

if __name__ == "__main__":
    main_flow()
