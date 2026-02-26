import streamlit as st
import pandas as pd
from datetime import datetime
import time
import io
from googleapiclient.http import MediaIoBaseDownload

# Import unseres Backend-Moduls
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

# --- HILFSFUNKTIONEN FÜR BILDER ---
@st.cache_data(ttl=60, show_spinner=False)
def load_project_images(_service, folder_id, project_name):
    """Sucht in Drive nach Fotos, die zum Projekt gehören"""
    query = f"'{folder_id}' in parents and name contains '{project_name}' and trashed = false"
    try:
        results = _service.files().list(q=query, fields="files(id, name)").execute()
        return results.get('files', [])
    except Exception:
        return []

@st.cache_data(ttl=3600, show_spinner=False)
def get_image_bytes(_service, file_id):
    """Lädt die Bilddaten aus Drive herunter"""
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
    # 1. Verbindung herstellen
    try:
        service = ds.get_drive_service()
    except Exception:
        st.error("Kritischer Fehler: secrets.toml ist nicht korrekt konfiguriert.")
        st.stop()

    if not service:
        st.warning("⚠️ Keine Verbindung zu Google Drive. Token fehlt oder ist abgelaufen.")
        st.stop()

    # 2. IDs aus Secrets laden
    try:
        PHOTOS_FID = st.secrets["general"]["PHOTOS_FOLDER_ID"] if "general" in st.secrets else st.secrets["PHOTOS_FOLDER_ID"]
        PROJEKT_RAPPORTE_FID = st.secrets["general"]["PROJECT_REPORTS_FOLDER_ID"] if "general" in st.secrets else st.secrets["PROJECT_REPORTS_FOLDER_ID"]
        ZEIT_RAPPORTE_FID = st.secrets["general"]["TIME_REPORTS_FOLDER_ID"] if "general" in st.secrets else st.secrets["TIME_REPORTS_FOLDER_ID"]
        
        # PIN abrufen (versucht verschiedene Ebenen in der toml)
        ADMIN_PIN = st.secrets.get("ADMIN_PIN", st.secrets.get("general", {}).get("ADMIN_PIN", "1234"))
    except KeyError as e:
        st.error(f"Konfigurationsfehler: Der Eintrag {e} fehlt in den Streamlit Secrets")
        st.stop()

    # --- KOPFZEILE (Logo & Name) ---
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
    # ANSICHT 1: STARTSEITE (HAUPTMENÜ)
    # =========================================================
    if st.session_state["view"] == "Start":
        st.subheader("Bitte wählen Sie Ihren Bereich aus:")
        st.write("") 
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("👷‍♂️ Mitarbeiter-Bereich", use_container_width=True):
                st.session_state["view"] = "Mitarbeiter"
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
                st.session_state["user_name"] = "Administrator"
                st.session_state["view"] = "Admin_Dashboard"
                st.rerun()
            else:
                st.error("Falsche PIN")

    # =========================================================
    # ANSICHT 3: MITARBEITER BEREICH
    # =========================================================
    elif st.session_state["view"] == "Mitarbeiter":
        col_back, col_title = st.columns([1, 4])
        with col_back:
            if st.button("⬅️ Zurück"):
                st.session_state["view"] = "Start"
                st.rerun()
        with col_title:
            st.subheader("📋 Rapportierung")
        
        # --- Stammdaten laden (Mitarbeiter & Projekte) ---
        df_emp, _ = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Employees.csv")
        df_proj, _ = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv")
        
        # Aktive Mitarbeiter filtern
        if not df_emp.empty and "Status" in df_emp.columns:
            active_emps = df_emp[df_emp["Status"] == "Aktiv"]["Name"].tolist()
        else:
            active_emps = ["Max Mustermann", "Temporär 1"]
            
        # Aktive Projekte filtern
        if not df_proj.empty and "Status" in df_proj.columns:
            active_projs = df_proj[df_proj["Status"] == "Aktiv"]["Projekt"].tolist()
        else:
            active_projs = ["Allgemein", "Baustelle A"]

        # Dropdowns für Identifikation
        col_e, col_p = st.columns(2)
        with col_e:
            sel_emp = st.selectbox("Wer bist du?", active_emps)
            st.session_state["user_name"] = sel_emp
        with col_p:
            sel_proj = st.selectbox("Projekt auswählen", active_projs)
        
        tab1, tab2, tab3 = st.tabs(["📝 Rapport (Zeit & Arbeit)", "📤 Fotos Hochladen", "🖼️ Galerie Ansehen"])
        
        # TAB 1: Kombinierter Rapport
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
                    # Berechnungen
                    t1 = datetime.combine(f_date, f_start)
                    t2 = datetime.combine(f_date, f_end)
                    diff = (t2 - t1).total_seconds() / 3600
                    hours = round(diff - f_pause, 2)
                    
                    if hours < 0:
                        st.error("Fehler: Arbeitsende liegt vor Arbeitsbeginn!")
                    else:
                        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        # Datensatz Projekt
                        row_projekt = {
                            "Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"),
                            "Projekt": sel_proj, "Mitarbeiter": sel_emp,
                            "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": "DRAFT"
                        }
                        
                        # Datensatz Zeit
                        row_zeit = {
                            "Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"),
                            "Projekt": sel_proj, "Mitarbeiter": sel_emp,
                            "Start": f_start.strftime("%H:%M"), "Ende": f_end.strftime("%H:%M"),
                            "Pause": f_pause, "Stunden_Total": hours, "Reise_Min": f_reise, "Status": "DRAFT"
                        }
                        
                        # Speichern
                        df_p, fid_p = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv")
                        df_p = pd.concat([df_p, pd.DataFrame([row_projekt])], ignore_index=True)
                        ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv", df_p, fid_p)
                        
                        df_z, fid_z = ds.read_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv")
                        df_z = pd.concat([df_z, pd.DataFrame([row_zeit])], ignore_index=True)
                        ds.save_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                        
                        st.success("✅ Rapport erfolgreich gespeichert.")

        # TAB 2: Fotos Hochladen
        with tab2:
            st.info(f"Fotos laden in Ordner: **{sel_proj}**")
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
                    # Cache leeren, damit neue Bilder in der Galerie auftauchen
                    st.cache_data.clear()
                    time.sleep(1)
                    st.rerun()

        # TAB 3: Galerie Ansehen
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
    # ANSICHT 4: ADMIN DASHBOARD
    # =========================================================
    elif st.session_state["view"] == "Admin_Dashboard":
        col1, col2 = st.columns([4, 1])
        with col1:
            st.subheader("🛠️ Admin Zentrale")
        with col2:
            if st.button("🚪 Abmelden", use_container_width=True):
                st.session_state["logged_in"] = False
                st.session_state["user_role"] = ""
                st.session_state["view"] = "Start"
                st.rerun()
        
        t_zeit, t_bau, t_stam = st.tabs(["🕒 AKZ / Zeiten", "🏗️ Baustellen-Rapporte", "⚙️ Stammdaten"])
        
        # TAB 1: ZEITEN
        with t_zeit:
            st.markdown("**Arbeitszeit Kontrolle (AKZ)**")
            if st.button("🔄 Zeiten laden"):
                st.rerun()
            
            df_z, fid_z = ds.read_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv")
            if df_z.empty:
                st.info("Keine Zeitdaten vorhanden.")
            else:
                edit_z = st.data_editor(df_z, num_rows="dynamic", key="editor_zeit", use_container_width=True)
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("💾 Zeit-Tabelle speichern", type="primary"):
                        ds.save_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv", edit_z, fid_z)
                        st.success("Gespeichert.")
                with c2:
                    csv_z = edit_z.to_csv(index=False).encode('utf-8')
                    st.download_button("Excel Export (Zeiten)", csv_z, "Export_AKZ.csv", "text/csv")

        # TAB 2: BAUSTELLEN RAPPORTE
        with t_bau:
            st.markdown("**Tagesrapporte (Inhalt)**")
            df_p, fid_p = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv")
            if df_p.empty:
                st.info("Keine Rapporte.")
            else:
                edit_p = st.data_editor(df_p, num_rows="dynamic", key="editor_proj", use_container_width=True)
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("💾 Projekt-Rapporte speichern", type="primary"):
                        ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv", edit_p, fid_p)
                        st.success("Gespeichert.")
                with c2:
                    csv_p = edit_p.to_csv(index=False).encode('utf-8')
                    st.download_button("Excel Export (Rapporte)", csv_p, "Export_Projekt.csv", "text/csv")

        # TAB 3: STAMMDATEN (Erweitert)
        with t_stam:
            col_proj, col_emp = st.columns(2)
            
            # --- PROJEKTE ---
            with col_proj:
                st.markdown("**🏗️ Projekte verwalten**")
                df_proj, fid_proj = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv")
                if df_proj.empty:
                    df_proj = pd.DataFrame({"Projekt": ["Baustelle A", "Baustelle B"], "Status": ["Aktiv", "Archiviert"]})
                
                edit_proj = st.data_editor(df_proj, num_rows="dynamic", key="editor_s_proj", use_container_width=True)
                if st.button("💾 Projekte speichern"):
                    ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv", edit_proj, fid_proj)
                    st.success("Projekte aktualisiert.")

            # --- MITARBEITER ---
            with col_emp:
                st.markdown("**👷‍♂️ Mitarbeiter verwalten**")
                df_emp, fid_emp = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Employees.csv")
                if df_emp.empty:
                    df_emp = pd.DataFrame({"Name": ["Christoph Schlorff", "Temporär 1"], "Status": ["Aktiv", "Inaktiv"]})
                
                edit_emp = st.data_editor(df_emp, num_rows="dynamic", key="editor_s_emp", use_container_width=True)
                if st.button("💾 Mitarbeiter speichern"):
                    ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Employees.csv", edit_emp, fid_emp)
                    st.success("Mitarbeiter aktualisiert.")

if __name__ == "__main__":
    main()
