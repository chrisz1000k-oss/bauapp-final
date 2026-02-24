import streamlit as st
import pandas as pd
from datetime import datetime
import time
import io

# Import unseres Backend-Moduls
import drive_store as ds

# --- KONFIGURATION ---
st.set_page_config(page_title="BauApp Pro", layout="wide")

st.markdown("""
    <style>
        #MainMenu {display: none;}
        footer {display: none;}
        header {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

# --- STATE INIT (Neu strukturiert für klare Navigation) ---
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
if "user_role" not in st.session_state:
    st.session_state["user_role"] = ""
if "user_name" not in st.session_state:
    st.session_state["user_name"] = ""
if "view" not in st.session_state:
    st.session_state["view"] = "Start" # App startet jetzt zwingend im Hauptmenü

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

    # 2. IDs aus Secrets laden (4 Ordner)
    try:
        PHOTOS_FID = st.secrets["PHOTOS_FOLDER_ID"]
        PROJEKT_RAPPORTE_FID = st.secrets["PROJECT_REPORTS_FOLDER_ID"]
        ZEIT_RAPPORTE_FID = st.secrets["TIME_REPORTS_FOLDER_ID"]
        PLAENE_FID = st.secrets.get("PLANS_FOLDER_ID", "")
        
        # Falls ADMIN_PIN im Block [general] steht, holt Streamlit es so automatisch:
        ADMIN_PIN = st.secrets.get("ADMIN_PIN", st.secrets.get("general", {}).get("ADMIN_PIN", "1234"))
    except KeyError as e:
        st.error(f"Konfigurationsfehler: Der Eintrag {e} fehlt in den Streamlit Secrets")
        st.stop()


    # =========================================================
    # ANSICHT 1: STARTSEITE (HAUPTMENÜ)
    # =========================================================
    if st.session_state["view"] == "Start":
        st.title("🏗️ Willkommen bei der BauApp")
        st.write("Bitte wählen Sie Ihren Bereich aus:")
        
        st.write("") # Abstand
        col1, col2 = st.columns(2)
        with col1:
            if st.button("👷‍♂️ Mitarbeiter-Bereich", use_container_width=True):
                st.session_state["user_role"] = "Mitarbeiter"
                st.session_state["user_name"] = "Mitarbeiter"
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
            
        st.title("🔐 Admin Login")
        pin_input = st.text_input("PIN eingeben", type="password")
        
        if st.button("Anmelden", type="primary"):
            if pin_input == ADMIN_PIN:
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
            if st.button("⬅️ Zurück zum Start"):
                st.session_state["view"] = "Start"
                st.rerun()
        with col_title:
            st.header("📋 Rapportierung")
        
        # Projektliste laden
        df_proj, _ = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv")
        if not df_proj.empty and "Projekt" in df_proj.columns:
            projects = df_proj["Projekt"].tolist()
        else:
            projects = ["Allgemein", "Baustelle A", "Baustelle B"]

        sel_proj = st.selectbox("Projekt auswählen", projects)
        
        tab1, tab2 = st.tabs(["📝 Rapport (Zeit & Arbeit)", "📷 Fotos"])
        
        # TAB 1: Kombinierter Rapport
        with tab1:
            with st.form("ma_form"):
                st.subheader("Tagesdaten")
                col_a, col_b = st.columns(2)
                with col_a:
                    f_date = st.date_input("Datum", datetime.now())
                    f_start = st.time_input("Start", datetime.strptime("07:00", "%H:%M").time())
                    f_end = st.time_input("Ende", datetime.strptime("16:30", "%H:%M").time())
                with col_b:
                    f_pause = st.number_input("Pause (Std)", value=0.5, step=0.25)
                    f_reise = st.number_input("Reisezeit (Min)", value=0, step=15)
                
                st.divider()
                st.subheader("Details")
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
                        
                        # DATENSATZ A: Für die Projektleitung
                        row_projekt = {
                            "Erfasst": ts_str,
                            "Datum": f_date.strftime("%Y-%m-%d"),
                            "Projekt": sel_proj,
                            "Mitarbeiter": st.session_state["user_name"],
                            "Arbeit": f_arbeit,
                            "Material": f_mat,
                            "Bemerkung": f_bem,
                            "Status": "DRAFT"
                        }
                        
                        # DATENSATZ B: Für die Lohnbuchhaltung
                        row_zeit = {
                            "Erfasst": ts_str,
                            "Datum": f_date.strftime("%Y-%m-%d"),
                            "Projekt": sel_proj,
                            "Mitarbeiter": st.session_state["user_name"],
                            "Start": f_start.strftime("%H:%M"),
                            "Ende": f_end.strftime("%H:%M"),
                            "Pause": f_pause,
                            "Stunden_Total": hours,
                            "Reise_Min": f_reise,
                            "Status": "DRAFT"
                        }
                        
                        # Speichern in ORDNER PROJEKT RAPPORTE
                        df_p, fid_p = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv")
                        df_p = pd.concat([df_p, pd.DataFrame([row_projekt])], ignore_index=True)
                        ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv", df_p, fid_p)
                        
                        # Speichern in ORDNER ZEIT RAPPORTE
                        df_z, fid_z = ds.read_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv")
                        df_z = pd.concat([df_z, pd.DataFrame([row_zeit])], ignore_index=True)
                        ds.save_csv(service, ZEIT_RAPPORTE_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                        
                        st.success("✅ Rapport erfolgreich getrennt gespeichert (Projekt & Zeit).")

        # TAB 2: Fotos
        with tab2:
            st.info(f"Fotos laden in Ordner: {sel_proj}")
            files = st.file_uploader("Bilder wählen", accept_multiple_files=True, type=['jpg','png','jpeg'])
            
            if st.button("📤 Fotos hochladen", type="primary"):
                if files:
                    prog = st.progress(0)
                    for idx, f in enumerate(files):
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        fname = f"{sel_proj}_{ts}_{f.name}"
                        ds.upload_image(service, PHOTOS_FID, fname, io.BytesIO(f.getvalue()), f.type)
                        prog.progress((idx + 1) / len(files))
                    st.success("Bilder übertragen.")
                    time.sleep(1)
                    st.rerun()


    # =========================================================
    # ANSICHT 4: ADMIN DASHBOARD
    # =========================================================
    elif st.session_state["view"] == "Admin_Dashboard":
        col1, col2 = st.columns([4, 1])
        with col1:
            st.header("🛠️ Admin Zentrale")
        with col2:
            if st.button("🚪 Abmelden", use_container_width=True):
                st.session_state["logged_in"] = False
                st.session_state["user_role"] = ""
                st.session_state["view"] = "Start"
                st.rerun()
        
        t_zeit, t_bau, t_stam = st.tabs(["🕒 AKZ / Zeiten", "🏗️ Baustellen-Rapporte", "⚙️ Stammdaten"])
        
        # TAB 1: ZEITEN (AKZ)
        with t_zeit:
            st.subheader("Arbeitszeit Kontrolle (AKZ)")
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
            st.subheader("Tagesrapporte (Inhalt)")
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

        # TAB 3: STAMMDATEN
        with t_stam:
            st.subheader("Projekte verwalten")
            df_proj, fid_proj = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv")
            
            if df_proj.empty:
                df_proj = pd.DataFrame({"Projekt": ["Baustelle A", "Baustelle B"], "Status": ["Aktiv", "Aktiv"]})
            
            edit_proj = st.data_editor(df_proj, num_rows="dynamic", key="editor_s_proj")
            
            if st.button("Projekte speichern"):
                ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv", edit_proj, fid_proj)
                st.success("Projekte aktualisiert.")

if __name__ == "__main__":
    main()
