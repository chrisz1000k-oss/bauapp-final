import streamlit as st
import pandas as pd
from datetime import datetime
import time
import io

# Import unseres neuen Backend-Moduls
import drive_store as ds

# --- KONFIGURATION ---
st.set_page_config(page_title="BauApp Pro", layout="wide")

# Verstecke Streamlit Standard-Elemente für sauberen Look
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
        st.warning("⚠️ Keine Verbindung zu Google Drive. Bitte Token prüfen (Schritt 2).")
        st.stop()

    # IDs aus Secrets laden
    try:
        PHOTOS_FID = st.secrets["PHOTOS_FOLDER_ID"]
        REPORTS_FID = st.secrets["REPORTS_FOLDER_ID"]
        ADMIN_PIN = st.secrets["ADMIN_PIN"]
    except KeyError as e:
        st.error(f"Fehlender Eintrag in secrets.toml: {e}")
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
                    # Login für Mitarbeiter (jeder PIN außer Admin geht vorerst)
                    st.session_state["logged_in"] = True
                    st.session_state["user_role"] = "Mitarbeiter"
                    st.session_state["user_name"] = "Mitarbeiter" 
                    st.rerun()
                else:
                    st.error("Bitte PIN eingeben.")
        return

    # --- HAUPTANWENDUNG ---
    # Sidebar
    st.sidebar.title(f"👤 {st.session_state['user_role']}")
    if st.sidebar.button("Abmelden"):
        st.session_state["logged_in"] = False
        st.session_state["user_role"] = ""
        st.rerun()

    # Projektliste laden
    df_proj, _ = ds.read_csv(service, REPORTS_FID, "Projects.csv")
    if not df_proj.empty and "Projekt" in df_proj.columns:
        projects = df_proj["Projekt"].tolist()
    else:
        projects = ["Allgemein", "Baustelle A (Beispiel)"]

    # ---------------------------------------------------------
    # ROLLE: MITARBEITER
    # ---------------------------------------------------------
    if st.session_state["user_role"] == "Mitarbeiter":
        st.header("📋 Tagesrapport & Fotos")
        
        sel_proj = st.selectbox("Projekt auswählen", projects)
        
        tab1, tab2 = st.tabs(["📝 Rapport erfassen", "📷 Fotos hochladen"])
        
        # TAB 1: RAPPORT
        with tab1:
            with st.form("ma_form"):
                col_a, col_b = st.columns(2)
                with col_a:
                    f_date = st.date_input("Datum", datetime.now())
                    f_start = st.time_input("Start", datetime.strptime("07:00", "%H:%M").time())
                    f_end = st.time_input("Ende", datetime.strptime("16:30", "%H:%M").time())
                with col_b:
                    f_pause = st.number_input("Pause (Std)", value=0.5, step=0.25)
                    f_reise = st.number_input("Reisezeit (Min)", value=0, step=15)
                
                f_arbeit = st.text_area("Arbeitsbeschrieb")
                f_mat = st.text_area("Material")
                f_bem = st.text_input("Bemerkung")
                
                if st.form_submit_button("Speichern", type="primary"):
                    # Berechnung
                    t1 = datetime.combine(f_date, f_start)
                    t2 = datetime.combine(f_date, f_end)
                    diff = (t2 - t1).total_seconds() / 3600
                    hours = round(diff - f_pause, 2)
                    
                    if hours < 0:
                        st.error("Fehler: Ende vor Start!")
                    else:
                        new_row = {
                            "Erfasst_Am": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Datum": f_date.strftime("%Y-%m-%d"),
                            "Projekt": sel_proj,
                            "Mitarbeiter": st.session_state["user_name"],
                            "Arbeit": f_arbeit,
                            "Material": f_mat,
                            "Stunden": hours,
                            "Reise_Min": f_reise,
                            "Bemerkung": f_bem,
                            "Status": "DRAFT" # Wichtig für Admin
                        }
                        
                        # Laden -> Anhängen -> Speichern
                        df_master, fid = ds.read_csv(service, REPORTS_FID, "Rapporte_Master.csv")
                        if df_master.empty:
                            df_master = pd.DataFrame([new_row])
                        else:
                            df_master = pd.concat([df_master, pd.DataFrame([new_row])], ignore_index=True)
                            
                        ds.save_csv(service, REPORTS_FID, "Rapporte_Master.csv", df_master, fid)
                        st.success("✅ Rapport gespeichert!")

        # TAB 2: FOTOS
        with tab2:
            st.info(f"Fotos werden gespeichert unter: {sel_proj}")
            files = st.file_uploader("Bilder wählen", accept_multiple_files=True, type=['jpg','png','jpeg'])
            
            if st.button("Fotos hochladen", type="primary"):
                if files:
                    prog = st.progress(0)
                    for idx, f in enumerate(files):
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        fname = f"{sel_proj}_{ts}_{f.name}"
                        ds.upload_image(service, PHOTOS_FID, fname, io.BytesIO(f.getvalue()), f.type)
                        prog.progress((idx + 1) / len(files))
                    st.success("✅ Upload abgeschlossen.")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.warning("Keine Bilder ausgewählt.")

    # ---------------------------------------------------------
    # ROLLE: ADMIN
    # ---------------------------------------------------------
    elif st.session_state["user_role"] == "Admin":
        st.header("🛠️ Admin Zentrale")
        
        adm_tab1, adm_tab2 = st.tabs(["Rapport Kontrolle", "Stammdaten"])
        
        with adm_tab1:
            st.caption("Hier bearbeiten Sie die Rapporte VOR der Finalisierung.")
            if st.button("🔄 Tabelle aktualisieren"):
                st.rerun()
                
            df_rep, fid_rep = ds.read_csv(service, REPORTS_FID, "Rapporte_Master.csv")
            
            if df_rep.empty:
                st.info("Keine Rapporte vorhanden.")
            else:
                # Editor
                edited_df = st.data_editor(df_rep, num_rows="dynamic", use_container_width=True)
                
                col_s, col_d = st.columns(2)
                with col_s:
                    if st.button("💾 Änderungen in Cloud speichern", type="primary"):
                        ds.save_csv(service, REPORTS_FID, "Rapporte_Master.csv", edited_df, fid_rep)
                        st.success("Datenbank aktualisiert.")
                
                with col_d:
                    csv_down = edited_df.to_csv(index=False).encode('utf-8')
                    st.download_button("🖨️ CSV Export (Excel)", csv_down, "Rapporte_Export.csv", "text/csv")

        with adm_tab2:
            st.subheader("Projekte verwalten")
            # Einfache Projektverwaltung
            df_p, fid_p = ds.read_csv(service, REPORTS_FID, "Projects.csv")
            if df_p.empty:
                # Initialisierung
                df_p = pd.DataFrame({"Projekt": ["Baustelle A", "Baustelle B"]})
            
            edited_projects = st.data_editor(df_p, num_rows="dynamic", key="proj_edit")
            if st.button("Projekte speichern"):
                ds.save_csv(service, REPORTS_FID, "Projects.csv", edited_projects, fid_p)
                st.success("Projektliste aktualisiert.")

if __name__ == "__main__":
    main()
