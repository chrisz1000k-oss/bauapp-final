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

# --- STATE INIT ---
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False
if "user_role" not in st.session_state:
    st.session_state["user_role"] = ""
if "user_name" not in st.session_state:
    st.session_state["user_name"] = ""

def main():
    # 1. Verbindung herstellen
    try:
        service = ds.get_drive_service()
    except Exception:
        st.error("Kritischer Fehler: secrets.toml ist nicht korrekt konfiguriert.")
        st.stop()

    if not service:
        st.warning("⚠️ Keine Verbindung zu Google Drive. Token fehlt.")
        st.stop()

    # 2. IDs aus Secrets laden (Jetzt 4 Ordner)
    try:
        # Ordner 1: Fotos
        PHOTOS_FID = st.secrets["PHOTOS_FOLDER_ID"]
        # Ordner 2: Arbeitsrapporte (Baustellen-Logbuch)
        PROJEKT_RAPPORTE_FID = st.secrets["PROJECT_REPORTS_FOLDER_ID"]
        # Ordner 3: Arbeitszeit (AKZ/Lohn)
        ZEIT_RAPPORTE_FID = st.secrets["TIME_REPORTS_FOLDER_ID"]
        # Ordner 4: Pläne (Optional, falls genutzt)
        PLAENE_FID = st.secrets.get("PLANS_FOLDER_ID", "")
        
        ADMIN_PIN = st.secrets["ADMIN_PIN"]
    except KeyError as e:
        st.error(f"Konfigurationsfehler: Der Eintrag {e} fehlt in der secrets.toml")
        st.stop()

    # --- LOGIN SCREEN ---
    if not st.session_state["logged_in"]:
        st.title("🔐 BauApp Login")
        col1, col2 = st.columns([1,2])
        with col1:
            pin = st.text_input("PIN eingeben", type="password")
            if st.button("Anmelden", type="primary"):
                if pin == ADMIN_PIN:
                    st.session_state["logged_in"] = True
                    st.session_state["user_role"] = "Admin"
                    st.session_state["user_name"] = "Administrator"
                    st.rerun()
                elif len(pin) > 0:
                    # Mitarbeiter Login
                    st.session_state["logged_in"] = True
                    st.session_state["user_role"] = "Mitarbeiter"
                    st.session_state["user_name"] = "Mitarbeiter" 
                    st.rerun()
        return

    # --- SIDEBAR ---
    st.sidebar.title(f"👤 {st.session_state['user_role']}")
    if st.sidebar.button("Abmelden"):
        st.session_state["logged_in"] = False
        st.rerun()

    # Projektliste laden (Liegt im Projekt-Ordner)
    df_proj, _ = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Projects.csv")
    if not df_proj.empty and "Projekt" in df_proj.columns:
        projects = df_proj["Projekt"].tolist()
    else:
        projects = ["Allgemein", "Baustelle A", "Baustelle B"]

    # =========================================================
    # ROLLE: MITARBEITER
    # =========================================================
    if st.session_state["user_role"] == "Mitarbeiter":
        st.header("📋 Rapportierung")
        
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
                    # 1. Berechnungen
                    t1 = datetime.combine(f_date, f_start)
                    t2 = datetime.combine(f_date, f_end)
                    diff = (t2 - t1).total_seconds() / 3600
                    hours = round(diff - f_pause, 2)
                    
                    if hours < 0:
                        st.error("Fehler: Arbeitsende liegt vor Arbeitsbeginn!")
                    else:
                        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        # DATENSATZ A: Für die Projektleitung (Beschreibung & Material)
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
                        
                        # DATENSATZ B: Für die Lohnbuchhaltung/AKZ (Zeit & Zahlen)
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
                        
                        # 2. Speichern in ORDNER PROJEKT RAPPORTE
                        df_p, fid_p = ds.read_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv")
                        df_p = pd.concat([df_p, pd.DataFrame([row_projekt])], ignore_index=True)
                        ds.save_csv(service, PROJEKT_RAPPORTE_FID, "Baustellen_Rapport.csv", df_p, fid_p)
                        
                        # 3. Speichern in ORDNER ZEIT RAPPORTE
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
                        # Name: Projekt_Datum_Original
                        fname = f"{sel_proj}_{ts}_{f.name}"
                        ds.upload_image(service, PHOTOS_FID, fname, io.BytesIO(f.getvalue()), f.type)
                        prog.progress((idx + 1) / len(files))
                    st.success("Bilder übertragen.")
                    time.sleep(1)
                    st.rerun()

    # =========================================================
    # ROLLE: ADMIN
    # =========================================================
    elif st.session_state["user_role"] == "Admin":
        st.header("🛠️ Admin Zentrale")
        
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
