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

def align_project_dataframe(df):
    """Prithvi-Sicherung: Stellt sicher, dass alle neuen Dimensionen existieren, ohne alte Daten zu zerstören."""
    expected_cols = [
        "Projekt_ID", "Auftragsnummer", "Projekt_Name", "Status", 
        "Kunde_Name", "Kunde_Adresse", "Kunde_Email", "Kunde_Telefon", "Kunde_Kontakt",
        "Fuge_Zement", "Fuge_Silikon", "Asbest_Gefahr"
    ]
    for col in expected_cols:
        if col not in df.columns:
            # Asbest hat Tamas-Vigilanz (Standard: Unbekannt/Prüfen)
            df[col] = "Nein" if col == "Asbest_Gefahr" else ""
    return df

def process_rapport_saving(service, f_date, f_hours, f_arbeit, f_mat, f_bem, sel_proj, PROJEKT_FID, ZEIT_FID):
    if sel_proj == "Bitte Projekte im Admin-Bereich anlegen":
        st.error("Bitte wähle ein gültiges Projekt aus.")
        return
        
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    row_projekt = {"Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"), "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": "DRAFT"}
    row_zeit = {"Erfasst": ts_str, "Datum": f_date.strftime("%Y-%m-%d"), "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Start": "-", "Ende": "-", "Pause": 0, "Stunden_Total": f_hours, "Reise_Min": 0, "Status": "DRAFT"}
    
    df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
    df_p = pd.concat([df_p, pd.DataFrame([row_projekt])], ignore_index=True)
    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", df_p, fid_p)
    
    df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
    df_z = pd.concat([df_z, pd.DataFrame([row_zeit])], ignore_index=True)
    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
    
    st.success("✅ Rapport erfolgreich in der Essenz gespeichert.")

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
    # STARTSEITE & LOGIN
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
        df_proj = align_project_dataframe(df_proj) # Prithvi-Sicherung
        
        active_projs = df_proj[df_proj["Status"] == "Aktiv"]["Projekt_Name"].tolist() if not df_proj.empty else ["Bitte Projekte im Admin-Bereich anlegen"]
        selected_project = st.selectbox("Projekt:", active_projs)
        
        # --- SPIEGELUNG DER KUNDEN- & STOFFDATEN (Vayu-Interaktion) ---
        if selected_project != "Bitte Projekte im Admin-Bereich anlegen":
            proj_data = df_proj[df_proj["Projekt_Name"] == selected_project].iloc[0]
            with st.expander("ℹ️ Projekt-Matrix (Kunde & Material)", expanded=True):
                c_info, m_info = st.columns(2)
                with c_info:
                    st.markdown("**👤 Kunden-Kontakt**")
                    st.write(f"**Name:** {proj_data.get('Kunde_Name', '-')}")
                    st.write(f"**Adresse:** {proj_data.get('Kunde_Adresse', '-')}")
                    st.write(f"**Kontaktperson:** {proj_data.get('Kunde_Kontakt', '-')}")
                    st.write(f"**Telefon:** {proj_data.get('Kunde_Telefon', '-')}")
                with m_info:
                    st.markdown("**🧱 Stoffliche Alchemie**")
                    st.write(f"**Zementfuge:** {proj_data.get('Fuge_Zement', '-')}")
                    st.write(f"**Silikonfuge:** {proj_data.get('Fuge_Silikon', '-')}")
                    
                    # Tamas-Vigilanz (Asbest-Warnung)
                    asbest_status = str(proj_data.get('Asbest_Gefahr', 'Nein')).strip().lower()
                    if asbest_status == "ja":
                        st.error("⚠️ **ACHTUNG: ASBEST VORHANDEN!** Schutzmaßnahmen zwingend einhalten.")
                    else:
                        st.success("✅ Asbest-Status: Unbedenklich / Nein")
        st.write("") # Akasha Raum
        
        tab1, tab2, tab3, tab4 = st.tabs(["📝 Rapport", "📤 Upload", "🖼️ Pläne & Fotos", "📜 Projekt-Historie"])
        
        with tab1:
            with st.form("ma_form"):
                col_a, col_b = st.columns(2)
                with col_a:
                    f_date = st.date_input("Datum", datetime.now())
                with col_b:
                    f_hours = st.number_input("Gesamtstunden", min_value=0.0, value=8.5, step=0.25)
                
                f_arbeit = st.text_area("Ausgeführte Arbeiten")
                f_mat = st.text_area("Materialeinsatz")
                f_bem = st.text_input("Bemerkung / Behinderungen")
                
                if st.form_submit_button("💾 Speichern", type="primary"):
                    process_rapport_saving(service, f_date, f_hours, f_arbeit, f_mat, f_bem, selected_project, PROJEKT_FID, ZEIT_FID)

        with tab2:
            st.info(f"Medien für: **{selected_project}**")
            files = st.file_uploader("Wählen", accept_multiple_files=True, type=['jpg','png','jpeg'])
            if st.button("📤 Hochladen", type="primary") and files:
                prog = st.progress(0)
                for idx, f in enumerate(files[:PAGINATION_LIMIT]):
                    fname = f"{selected_project}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{f.name}"
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
            st.markdown(f"**Projekt-Historie: {selected_project}**")
            df_hist_p, _ = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
            df_hist_z, _ = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
            
            if not df_hist_p.empty and not df_hist_z.empty and "Erfasst" in df_hist_p.columns and "Erfasst" in df_hist_z.columns:
                df_merged = pd.merge(df_hist_p, df_hist_z[["Erfasst", "Stunden_Total"]], on="Erfasst", how="left")
                df_proj_hist = df_merged[df_merged["Projekt"] == selected_project].sort_values(by="Datum", ascending=False)
                
                if df_proj_hist.empty:
                    st.info("Noch keine Rapporte für dieses Projekt.")
                else:
                    for _, row in df_proj_hist.head(PAGINATION_LIMIT).iterrows():
                        with st.expander(f"🗓️ {row['Datum']} | 👷‍♂️ {row['Mitarbeiter']} | ⏱️ {row.get('Stunden_Total', '-')} Std."):
                            st.markdown(f"**Ausgeführte Arbeiten:**\n{row.get('Arbeit', '-')}")
                            st.markdown(f"**Materialeinsatz:**\n{row.get('Material', '-')}")
            else:
                st.info("Datenbasis noch unvollständig.")

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
        
        t_zeit, t_bau, t_stam, t_docs, t_print = st.tabs(["🕒 AKZ", "🏗️ Rapporte", "⚙️ Stammdaten", "📂 Medien", "🖨️ Druck"])
        
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
            st.markdown("**🏗️ Projekte & Kunden-Kunda**")
            df_proj, fid_proj = ds.read_csv(service, PROJEKT_FID, "Projects.csv")
            df_proj = align_project_dataframe(df_proj) # Prithvi-Sicherung erzwingt alle Spalten
            
            if df_proj.empty:
                df_proj = pd.DataFrame({"Projekt_ID": ["P100"], "Auftragsnummer": ["A-01"], "Projekt_Name": ["Baustelle A"], "Status": ["Aktiv"], "Kunde_Name": [""], "Kunde_Adresse": [""], "Kunde_Email": [""], "Kunde_Telefon": [""], "Kunde_Kontakt": [""], "Fuge_Zement": [""], "Fuge_Silikon": [""], "Asbest_Gefahr": ["Nein"]})
            
            edit_proj = st.data_editor(df_proj, num_rows="dynamic", key="e_proj", use_container_width=True)
            if st.button("💾 Projekte Sichern"): ds.save_csv(service, PROJEKT_FID, "Projects.csv", edit_proj, fid_proj)

            st.markdown("**👷‍♂️ Mitarbeiter**")
            df_emp, fid_emp = ds.read_csv(service, PROJEKT_FID, "Employees.csv")
            if df_emp.empty or "Mitarbeiter_ID" not in df_emp.columns:
                df_emp = pd.DataFrame({"Mitarbeiter_ID": ["M01"], "Name": ["Christoph Schlorff"], "Status": ["Aktiv"]})
            edit_emp = st.data_editor(df_emp, num_rows="dynamic", key="e_emp", use_container_width=True)
            if st.button("💾 Mitarbeiter Sichern"): ds.save_csv(service, PROJEKT_FID, "Employees.csv", edit_emp, fid_emp)

        with t_docs:
            st.markdown("**Admin-Souveränität: Dateien verwalten**")
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
            if st.button("🔄 Galerie laden/aktualisieren"): st.cache_data.clear()
            admin_files = []
            if PHOTOS_FID: admin_files.extend(load_project_files_flowing(service, PHOTOS_FID, admin_sel_proj))
            if PLAENE_FID: admin_files.extend(load_project_files_flowing(service, PLAENE_FID, admin_sel_proj))
            
            if admin_files:
                cols = st.columns(4)
                for idx, img in enumerate(admin_files):
                    with cols[idx % 4]:
                        img_bytes = get_file_bytes_flowing(service, img['id'])
                        if img_bytes:
                            if img['name'].lower().endswith(('.png', '.jpg', '.jpeg')): st.image(img_bytes, caption=img['name'], use_container_width=True)
                            else: st.download_button(label=f"📥 {img['name'][:15]}...", data=img_bytes, file_name=img['name'])

        with t_print:
            st.markdown("**Das Artefakt: Skalierbare Druckvorlage inkl. Projekt-Spezifikationen**")
            
            print_proj = st.selectbox("Projekt für Druckvorlage:", active_projs, key="admin_print_sel")
            
            if st.button("🖨️ Druckvorlage generieren", type="primary"):
                if print_proj != "Keine Projekte gefunden":
                    # Projekt-Daten für den Druck extrahieren
                    proj_row = df_proj[df_proj["Projekt_Name"] == print_proj].iloc[0]
                    k_name = proj_row.get("Kunde_Name", "")
                    k_adresse = proj_row.get("Kunde_Adresse", "")
                    k_kontakt = proj_row.get("Kunde_Kontakt", "")
                    f_zem = proj_row.get("Fuge_Zement", "")
                    f_sil = proj_row.get("Fuge_Silikon", "")
                    asbest = str(proj_row.get("Asbest_Gefahr", "Nein")).strip()
                    
                    asbest_html = f"<p style='color:red; font-weight:bold; font-size: 14px; margin-top:5px;'>⚠️ ASBEST GEFAHR: JA</p>" if asbest.lower() == "ja" else "<p style='color:green; font-size: 12px; margin-top:5px;'>✅ Asbest: Nein / Unbedenklich</p>"

                    safe_proj_name = urllib.parse.quote(print_proj)
                    qr_url = f"{BASE_URL}?projekt={safe_proj_name}"
                    qr_api_url = f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={urllib.parse.quote(qr_url)}"
                    
                    table_rows = "".join(["<tr><td></td><td></td><td></td><td></td><td></td></tr>" for _ in range(15)])
                    
                    # Akasha-Grid (3 Säulen im Header)
                    html_content = f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <title>Projekt-Rapport - {print_proj}</title>
                        <style>
                            @page {{ size: A4; margin: 15mm; }}
                            body {{ font-family: Arial, sans-serif; color: #333; font-size: 12px; margin: 0; padding: 0; }}
                            .header-grid {{ display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 2px solid #1E3A8A; padding-bottom: 27px; margin-bottom: 27px; }}
                            .col-firm {{ width: 30%; }}
                            .col-specs {{ width: 45%; border-left: 1px solid #ccc; padding-left: 27px; }}
                            .col-qr {{ width: 20%; text-align: right; }}
                            h1 {{ color: #1E3A8A; margin: 0; font-size: 24px; }}
                            h2 {{ margin: 5px 0 15px 0; color: #555; font-size: 16px; }}
                            h3 {{ margin: 0 0 5px 0; font-size: 14px; color: #1E3A8A; }}
                            .specs-text {{ margin: 2px 0; font-size: 12px; }}
                            .qr-area img {{ border: 1px solid #ccc; padding: 3px; width: 100px; height: 100px; }}
                            table {{ width: 100%; border-collapse: collapse; page-break-inside: auto; }}
                            tr {{ page-break-inside: avoid; page-break-after: auto; }}
                            th, td {{ border: 1px solid #000; padding: 10px; text-align: left; vertical-align: top; }}
                            th {{ background-color: #f0f0f0; }}
                            td {{ height: 50px; }}
                        </style>
                    </head>
                    <body>
                        <div class="header-grid">
                            <div class="col-firm">
                                <h1>R. Baumgartner AG</h1>
                                <h2>Arbeitsrapport (Backup)</h2>
                                <p class="specs-text"><strong>Projekt:</strong><br>{print_proj}</p>
                            </div>
                            <div class="col-specs">
                                <h3>Kunde & Ort</h3>
                                <p class="specs-text">{k_name}<br>{k_adresse}<br>Kontakt: {k_kontakt}</p>
                                <h3 style="margin-top: 10px;">Stoffliche Alchemie</h3>
                                <p class="specs-text">Zementfuge: {f_zem} | Silikonfuge: {f_sil}</p>
                                {asbest_html}
                            </div>
                            <div class="col-qr">
                                <div class="qr-area">
                                    <img src="{qr_api_url}" alt="QR Code">
                                    <p style="font-size:10px; margin-top:5px; text-align:center;">Zum digitalen<br>Rapport scannen</p>
                                </div>
                            </div>
                        </div>
                        <table>
                            <thead>
                                <tr>
                                    <th style="width: 10%;">Datum</th>
                                    <th style="width: 15%;">Mitarbeiter</th>
                                    <th style="width: 10%;">Stunden</th>
                                    <th style="width: 40%;">Ausgeführte Arbeiten</th>
                                    <th style="width: 25%;">Material / Notizen</th>
                                </tr>
                            </thead>
                            <tbody>
                                {table_rows}
                            </tbody>
                        </table>
                    </body>
                    </html>
                    """
                    
                    st.components.v1.html(html_content, height=500, scrolling=True)
                    st.download_button(
                        label="📄 Skalierbares A4-Raster herunterladen (Für Druck öffnen)",
                        data=html_content,
                        file_name=f"Rapport_Grid_{print_proj}.html",
                        mime="text/html"
                    )

if __name__ == "__main__":
    main_flow()
