import streamlit as st
import pandas as pd
from datetime import datetime
import time
import io
from googleapiclient.http import MediaIoBaseDownload

import drive_store as ds

# --- KONFIGURATION ---
st.set_page_config(page_title="R. Baumgartner AG - BauApp", layout="wide")

st.markdown("""
    <style>
        #MainMenu {display: none;}
        footer {display: none;}
        header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

# --- HILFSFUNKTIONEN ---
@st.cache_data(ttl=60, show_spinner=False)
def load_project_images(_service, folder_id, project_name):
    query = f"'{folder_id}' in parents and name contains '{project_name}' and trashed = false"
    try:
        results = _service.files().list(q=query, fields="files(id, name)").execute()
        return results.get('files', [])
    except Exception:
        return []

@st.cache_data(ttl=3600, show_spinner=False)
def get_image_bytes(_service, file_id):
    try:
        request = _service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return fh.getvalue()
    except Exception:
        return None

# --- STATE INIT ---
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
if "user_role" not in st.session_state:
    st.session_state["user_role"] = ""
if "user_name" not in st.session_state:
    st.session_state["user_name"] = ""
if "view" not in st.session_state:
    st.session_state["view"] = "Start"

def main():
    try:
        service = ds.get_drive_service()
    except Exception:
        st.error("Kritischer Fehler: secrets.toml ist nicht korrekt konfiguriert.")
        st.stop()

    if not service:
        st.warning("⚠️ Keine Verbindung zu Google Drive. Token fehlt oder ist abgelaufen.")
        st.stop()

    try:
        PHOTOS_FID = st.secrets["general"]["PHOTOS_FOLDER_ID"] if "general" in st.secrets else st.secrets["PHOTOS_FOLDER_ID"]
        PROJEKT_RAPPORTE_FID = st.secrets["general"]["PROJECT_REPORTS_FOLDER_ID"] if "general" in st.secrets else st.secrets["PROJECT_REPORTS_FOLDER_ID"]
        ZEIT_RAPPORTE_FID = st.secrets["general"]["TIME_REPORTS_FOLDER_ID"] if "general" in st.secrets else st.secrets["TIME_REPORTS_FOLDER_ID"]
        PLAENE_FID = st.secrets.get("PLANS_FOLDER_ID", st.secrets.get("general", {}).get("PLANS_FOLDER_ID", ""))
        ADMIN_PIN = st.secrets.get("ADMIN_PIN", st.secrets.get("general", {}).get("ADMIN_PIN", "1234"))
    except KeyError as e:
        st.error(f"Konfigurationsfehler: Der Eintrag {e} fehlt.")
        st.stop()

    # --- KOPFZEILE ---
    col_logo, col_name = st.columns([1, 6])
    with col_logo:
        try:
            st.image("logo.png", use_container_width=True)
        except Exception:
            st.markdown("<h1>🏗️</h1>", unsafe_allow_html=True)
    with col_name:
        st.markdown("<h1 style='color:#1E3A8A; margin-top:0px;'>R. Baumgartner AG</h1>", unsafe_allow_html=True)
    st.divider()

    # =========================================================
    # ANSICHT 1: STARTSEITE
    # =========================================================
    if st.session_state["view"] == "Start":
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

    # =========================================================
    # ANSICHT 2: ADMIN LOGIN
    # =========================================================
    elif st.session_state["view"] == "Admin_Login":
        if st.button("⬅️ Zurück zur Startseite"):
            st.session_state["view"] = "Start"
            st.rerun()
            
        st.subheader("🔐 Admin Login")
        pin_input = st.text_input("PIN eingeben", type="password")
        
        if st.button("Anmelden", type="primary"):
            if str(pin_input).strip() == str(ADMIN_PIN).strip():
                st.session_state["logged_in"] = True
                st.session_state["user_role"] = "Admin"
                st.session_state["view"] = "Admin_Dashboard"
                st.rerun()
            else:
                st.error("Falsche PIN")

    # =========================================================
    # ANSICHT 3: MITARBEITER LOGIN (NEU: Strikte Schranke)
    # =========================================================
    elif st.session_state["view"] == "Mitarbeiter_Login":
        if st.button("⬅️ Zurück zur Startseite"):
            st.session_state["view"] = "Start"
            st.rerun()
            
        st.subheader("👋 Wer bist du?")
        
        # Mitarbeiter laden
        df_emp, _ = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Employees.csv")
        if not df_emp.empty and "Status" in df_emp.columns:
            active_emps = df_emp[df_emp["Status"] == "Aktiv"]["Name"].tolist()
        else:
            active_emps = ["Bitte Stammdaten im Admin-Bereich anlegen"]
            
        sel_emp = st.selectbox("Wähle deinen Namen:", active_emps)
        
        if st.button("Einloggen", type="primary"):
            if sel_emp != "Bitte Stammdaten im Admin-Bereich anlegen":
                st.session_state["user_name"] = sel_emp
                st.session_state["view"] = "Mitarbeiter_Dashboard"
                st.rerun()
            else:
                st.error("Keine aktiven Mitarbeiter gefunden.")

    # =========================================================
    # ANSICHT 4: MITARBEITER DASHBOARD
    # =========================================================
    elif st.session_state["view"] == "Mitarbeiter_Dashboard":
        col_back, col_title = st.columns([1, 4])
        with col_back:
            if st.button("🚪 Logout"):
                st.session_state["user_name"] = ""
                st.session_state["view"] = "Start"
                st.rerun()
        with col_title:
            st.subheader(f"📋 Rapportierung: {st.session_state['user_name']}")
        
        # Projekte laden
        df_proj, _ = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv")
        if not df_proj.empty and "Status" in df_proj.columns:
            # Zeigt Projekt_Name und Auftragsnummer an, falls vorhanden
            active_projs = df_proj[df_proj["Status"] == "Aktiv"]["Projekt_Name"].tolist()
        else:
            active_projs = ["Bitte Projekte im Admin-Bereich anlegen"]

        sel_proj = st.selectbox("Für welches Projekt rapportierst du?", active_projs)
        
        tab1, tab2, tab3 = st.tabs(["📝 Rapport", "📤 Fotos Hochladen", "🖼️ Galerie"])
        
        with tab1:
            with st.form("ma_form"):
                st.markdown("**Tagesdaten**")
                col_a, col_b = st.columns(2)
                with col_a:
                    f_date = st.date_input("Datum", datetime.now())
                    f_start = st.time_input("Start", datetime.strptime("07:00", "%H:%M").time())
                    f_end = st.time_input("Ende", datetime.strptime("16:30", "%H:%M").time())
                with col_b:
                    f_pause = st.number_input("Pause (Std)", value=0.5, step=0.25)
                    f_reise = st.number_input("Reisezeit (Min)", value=0, step=15)
                
                st.divider()
                st.markdown("**Details**")
                f_arbeit = st.text_area("Arbeitsbeschrieb (Was wurde gemacht?)")
                f_mat = st.text_area("Materialeinsatz")
                f_bem = st.text_input("Interne Bemerkung")
                
                if st.form_submit_button("💾 Rapport speichern", type="primary"):
                    t1 = datetime.combine(f_date, f_start)
                    t2 = datetime.combine(f_date, f_end)
                    diff = (t2 - t1).total_seconds() / 3600
                    hours = round(diff - f_pause, 2)
                    
                    if hours < 0:
                        st.error("Fehler: Arbeitsende liegt vor Arbeitsbeginn!")
                    elif sel_proj == "Bitte Projekte im Admin-Bereich anlegen":
                        st.error("Bitte wähle ein gültiges Projekt aus.")
                    else:
                        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        row_projekt = {
                            "Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"),
                            "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"],
                            "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": "DRAFT"
                        }
                        
                        row_zeit = {
                            "Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"),
                            "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"],
                            "Start": f_start.strftime("%H:%M"), "Ende": f_end.strftime("%H:%M"),
                            "Pause": f_pause, "Stunden_Total": hours, "Reise_Min": f_reise, "Status": "DRAFT"
                        }
                        
                        df_p, fid_p = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv")
                        df_p = pd.concat([df_p, pd.DataFrame([row_projekt])], ignore_index=True)
                        ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv", df_p, fid_p)
                        
                        df_z, fid_z = ds.read_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv")
                        df_z = pd.concat([df_z, pd.DataFrame([row_zeit])], ignore_index=True)
                        ds.save_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                        
                        st.success("✅ Rapport erfolgreich gespeichert.")

        with tab2:
            st.info(f"Fotos laden für Projekt: **{sel_proj}**")
            files = st.file_uploader("Bilder wählen", accept_multiple_files=True, type=['jpg','png','jpeg'])
            if st.button("📤 Fotos hochladen", type="primary"):
                if files:
                    prog = st.progress(0)
                    for idx, f in enumerate(files):
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        fname = f"{sel_proj}_{ts}_{f.name}"
                        ds.upload_image(service, PHOTOS_FID, fname, io.BytesIO(f.getvalue()), f.type)
                        prog.progress((idx + 1) / len(files))
                    st.success("✅ Bilder übertragen.")
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()

        with tab3:
            st.markdown(f"**Bilder für Projekt: {sel_proj}**")
            if st.button("🔄 Galerie aktualisieren"):
                st.cache_data.clear()
                st.rerun()
                
            images = load_project_images(service, PHOTOS_FID, sel_proj)
            if not images:
                st.info("Noch keine Fotos für dieses Projekt hochgeladen.")
            else:
                cols = st.columns(3)
                for idx, img in enumerate(images):
                    col = cols[idx % 3]
                    with col:
                        img_bytes = get_image_bytes(service, img['id'])
                        if img_bytes:
                            st.image(img_bytes, caption=img['name'], use_container_width=True)

    # =========================================================
    # ANSICHT 5: ADMIN DASHBOARD
    # =========================================================
    elif st.session_state["view"] == "Admin_Dashboard":
        col1, col2 = st.columns([4, 1])
        with col1:
            st.subheader("🛠️ Admin Zentrale")
        with col2:
            if st.button("🚪 Abmelden", use_container_width=True):
                st.session_state["logged_in"] = False
                st.session_state["view"] = "Start"
                st.rerun()
        
        t_zeit, t_bau, t_stam = st.tabs(["🕒 AKZ / Zeiten", "🏗️ Baustellen-Rapporte", "⚙️ Stammdaten"])
        
        with t_zeit:
            df_z, fid_z = ds.read_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv")
            if df_z.empty:
                st.info("Keine Zeitdaten vorhanden.")
            else:
                edit_z = st.data_editor(df_z, num_rows="dynamic", use_container_width=True)
                if st.button("💾 Zeit-Tabelle speichern", type="primary"):
                    ds.save_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv", edit_z, fid_z)
                    st.success("Gespeichert.")

        with t_bau:
            df_p, fid_p = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv")
            if df_p.empty:
                st.info("Keine Rapporte.")
            else:
                edit_p = st.data_editor(df_p, num_rows="dynamic", use_container_width=True)
                if st.button("💾 Projekt-Rapporte speichern", type="primary"):
                    ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv", edit_p, fid_p)
                    st.success("Gespeichert.")

        with t_stam:
            st.info("WICHTIG: Überschreiben Sie diese Tabellen komplett mit Ihren korrekten Daten.")
            
            st.markdown("**🏗️ Projekte verwalten**")
            df_proj, fid_proj = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv")
            # Neues Schema mit ID und Auftragsnummer
            if df_proj.empty or "Auftragsnummer" not in df_proj.columns:
                df_proj = pd.DataFrame({
                    "Projekt_ID": ["P100", "P101"], 
                    "Auftragsnummer": ["A-2026-01", "A-2026-02"],
                    "Projekt_Name": ["Baustelle A", "Baustelle B"], 
                    "Status": ["Aktiv", "Archiviert"]
                })
            edit_proj = st.data_editor(df_proj, num_rows="dynamic", key="e_proj", use_container_width=True)
            if st.button("💾 Projekte speichern"):
                ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv", edit_proj, fid_proj)
                st.success("Projekte aktualisiert.")

            st.markdown("**👷‍♂️ Mitarbeiter verwalten**")
            df_emp, fid_emp = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Employees.csv")
            # Neues Schema mit ID
            if df_emp.empty or "Mitarbeiter_ID" not in df_emp.columns:
                df_emp = pd.DataFrame({
                    "Mitarbeiter_ID": ["M01", "M02"],
                    "Name": ["Christoph Schlorff", "Temporär 1"], 
                    "Status": ["Aktiv", "Inaktiv"]
                })
            edit_emp = st.data_editor(df_emp, num_rows="dynamic", key="e_emp", use_container_width=True)
            if st.button("💾 Mitarbeiter speichern"):
                ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Employees.csv", edit_emp, fid_emp)
                st.success("Mitarbeiter aktualisiert.")

if __name__ == "__main__":
    main()
