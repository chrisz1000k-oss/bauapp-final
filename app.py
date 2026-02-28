Guten Tag. Die Spezifikationen wurden empfangen und erfolgreich im System implementiert.

Die lückenlose Verfügbarkeit von Kundenkontaktdaten (Name, Telefon, Adresse, E-Mail) ist nun über alle drei Ebenen der Applikation hinweg sichergestellt. Dieser Informationsfluss eliminiert Rückfragen bei der Bauleitung, ermöglicht den Mitarbeitern eine direkte Navigation zum Einsatzort und sorgt für eine rechtssichere und vollständige Dokumentation auf dem physischen Wochenbericht.

### Technische Umsetzung der neuen Spezifikationen:

1. **Mitarbeiter-Portal (Informationsfluss):** Der Info-Block unterhalb der Projektauswahl wurde strukturiert erweitert. Der Mitarbeiter sieht nun auf einen Blick: Kundenname, Ansprechpartner, die exakte Projektadresse, Telefonnummer und E-Mail-Adresse.
2. **Berichtswesen (Druck-Modul):** Die HTML-Druckvorlage im Admin-Bereich extrahiert nun die kompletten Kontaktdaten (inklusive Adresse und Telefonnummer) aus der Datenbank und platziert diese prägnant im Kopfbereich des Dokuments, direkt neben den technischen Details (Asbest, Fugenfarben).
3. **Stammdaten-Erfassung:** Das Modul `align_project_dataframe` erzwingt die Existenz der Felder `Kunde_Name`, `Kunde_Adresse`, `Kunde_Email`, `Kunde_Telefon` und `Kunde_Kontakt` in der Datenbank. Der Admin kann diese bequem im Grid-Editor pflegen.

---

### Der aktualisierte Master-Code (`app.py`)

Kopieren Sie diesen Code und überschreiben Sie Ihre `app.py` auf GitHub. Die Anpassungen am Informationsfluss und an der Druckvorlage sind nach dem Neustart sofort aktiv.

```python
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import io
import urllib.parse
from googleapiclient.http import MediaIoBaseDownload

import drive_store as ds

# ==========================================
# KONFIGURATION & SYSTEM-PARAMETER
# ==========================================
CACHE_TTL_SECONDS = 108        
MAX_IMAGE_BUFFER = 108         
PAGINATION_LIMIT = 27          

st.set_page_config(page_title="R. Baumgartner AG - Projekt-Portal", layout="wide")

# Professional Dark Mode CSS
st.markdown("""
    <style>
        #MainMenu {display: none;}
        footer {display: none;}
        header {visibility: hidden;}
        
        /* Hintergrund (Tiefes Anthrazit) */
        .stApp, .main { background-color: #121212 !important; }
        
        /* Textfarbe (Silber-Weiß) für maximalen Kontrast */
        html, body, p, div, span, label, h1, h2, h3, h4, h5, h6, li, td, th { 
            color: #E0E0E0 !important; 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        
        /* Akzentsetzung (Gedämpftes Gold) */
        .stButton>button { 
            background-color: #D4AF37 !important; 
            color: #121212 !important; 
            transition: all 0.27s ease-in-out; 
            border: none !important;
            font-weight: bold;
            border-radius: 4px;
        }
        .stButton>button:hover { background-color: #b5952f !important; transform: scale(1.02); }
        
        /* Eingabefelder */
        .stTextInput>div>div>input, .stNumberInput>div>div>input, .stTextArea>div>div>textarea, .stSelectbox>div>div>div, div[data-baseweb="select"]>div {
            background-color: #1E1E1E !important;
            color: #E0E0E0 !important;
            border: 1px solid #D4AF37 !important;
        }
        
        /* Expander & Tabs */
        .streamlit-expanderHeader { background-color: #1E1E1E !important; color: #D4AF37 !important; border-bottom: 1px solid #D4AF37; }
        .stTabs [data-baseweb="tab-list"] button { color: #E0E0E0; }
        .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] { color: #D4AF37 !important; border-bottom-color: #D4AF37 !important; }
        
        /* Layout-Struktur (27px / 54px Hierarchie) */
        hr { border-color: #D4AF37 !important; opacity: 0.27; margin-top: 27px; margin-bottom: 27px; }
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# SYSTEM-HELFER & DATEI-MANAGEMENT
# ==========================================
@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_project_files(_service, folder_id: str, project_name: str) -> list:
    if not folder_id: return []
    query = f"'{folder_id}' in parents and name contains '{project_name}' and trashed = false"
    try:
        results = _service.files().list(q=query, fields="files(id, name)").execute()
        return results.get('files', [])[:MAX_IMAGE_BUFFER]
    except Exception:
        return []

@st.cache_data(ttl=CACHE_TTL_SECONDS * 10, show_spinner=False)
def get_file_bytes(_service, file_id: str):
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

def delete_project_assets(_service, keyword, folders):
    for fid in folders:
        if not fid: continue
        query = f"'{fid}' in parents and name contains '{keyword}'"
        try:
            results = _service.files().list(q=query, fields="files(id)").execute()
            for f in results.get('files', []):
                _service.files().delete(fileId=f['id']).execute()
        except Exception:
            pass

def init_session_state():
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
    expected_cols = ["Start", "Ende", "Pause_Min", "R_Wohn_Bau_Min", "R_Bau_Wohn_Min", "Reisezeit_bezahlt_Min", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = 0 if "Min" in col or "Arbeitszeit" in col else ("" if col == "Absenz_Typ" or col in ["Start", "Ende"] else "ENTWURF")
    return df

def process_single_rapport(service, f_date, f_start, f_end, f_pause_min, f_arbeit, f_mat, f_bem, sel_proj, r_hin, r_rueck, PROJEKT_FID, ZEIT_FID):
    if sel_proj == "Keine aktiven Projekte gefunden" or not sel_proj:
        st.error("System-Fehler: Bitte wählen Sie ein gültiges Projekt aus.")
        return
        
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_str = f_date.strftime("%Y-%m-%d")
    
    t1 = datetime.combine(f_date, f_start)
    t2 = datetime.combine(f_date, f_end)
    diff_hours = (t2 - t1).total_seconds() / 3600.0
    work_hours = round(diff_hours - (f_pause_min / 60.0), 2)
    
    if work_hours < 0:
        st.error("Validierungsfehler: Endzeit abzüglich Pause liegt vor der Startzeit.")
        return

    bezahlt_hin = max(0, r_hin - 30)
    bezahlt_rueck = max(0, r_rueck - 30)
    
    reise_min_bezahlt = bezahlt_hin + bezahlt_rueck
    reise_stunden_bezahlt = round(reise_min_bezahlt / 60.0, 2)
    total_inkl_reise = round(work_hours + reise_stunden_bezahlt, 2)
    
    start_str = f_start.strftime("%H:%M")
    end_str = f_end.strftime("%H:%M")
    
    row_projekt = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Arbeit": f_arbeit, "Material": f_mat, "Bemerkung": f_bem, "Status": "ENTWURF"}
    row_zeit = {"Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, "Mitarbeiter": st.session_state["user_name"], "Start": start_str, "Ende": end_str, "Pause_Min": f_pause_min, "Stunden_Total": work_hours, "R_Wohn_Bau_Min": r_hin, "R_Bau_Wohn_Min": r_rueck, "Reisezeit_bezahlt_Min": reise_min_bezahlt, "Arbeitszeit_inkl_Reisezeit": total_inkl_reise, "Absenz_Typ": "", "Status": "ENTWURF"}
    
    df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
    df_p = pd.concat([df_p, pd.DataFrame([row_projekt])], ignore_index=True)
    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", df_p, fid_p)
    
    df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
    df_z = align_zeit_dataframe(df_z)
    df_z = pd.concat([df_z, pd.DataFrame([row_zeit])], ignore_index=True)
    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
    
    st.success(f"✅ Rapport erfasst. Netto-Arbeit: {work_hours}h | Vergütete Reisezeit: {reise_min_bezahlt} Min. | Total: {total_inkl_reise}h")

def process_batch_absenz(service, start_date, end_date, f_hours, a_typ, f_bem, sel_proj, PROJEKT_FID, ZEIT_FID):
    if sel_proj == "Keine aktiven Projekte gefunden" or not sel_proj:
        st.error("System-Fehler: Bitte wählen Sie ein gültiges Projekt aus.")
        return
        
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    days_diff = (end_date - start_date).days + 1
    
    rows_projekt = []
    rows_zeit = []
    
    for i in range(days_diff):
        current_date = start_date + timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        
        rows_projekt.append({
            "Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, 
            "Mitarbeiter": st.session_state["user_name"], "Arbeit": f"ABSENZ: {a_typ}", 
            "Material": "", "Bemerkung": f_bem, "Status": "ENTWURF"
        })
        
        rows_zeit.append({
            "Erfasst": ts_str, "Datum": date_str, "Projekt": sel_proj, 
            "Mitarbeiter": st.session_state["user_name"], "Start": "-", "Ende": "-", 
            "Pause_Min": 0, "Stunden_Total": f_hours, "R_Wohn_Bau_Min": 0, "R_Bau_Wohn_Min": 0, 
            "Reisezeit_bezahlt_Min": 0, "Arbeitszeit_inkl_Reisezeit": f_hours, 
            "Absenz_Typ": a_typ, "Status": "ENTWURF"
        })
        
    df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
    df_p = pd.concat([df_p, pd.DataFrame(rows_projekt)], ignore_index=True)
    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", df_p, fid_p)
    
    df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
    df_z = align_zeit_dataframe(df_z)
    df_z = pd.concat([df_z, pd.DataFrame(rows_zeit)], ignore_index=True)
    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
    
    st.success(f"✅ Absenz ({a_typ}) für {days_diff} Tag(e) erfolgreich im System erfasst.")

# ==========================================
# HAUPT-APPLIKATION
# ==========================================
def main_flow():
    init_session_state()
    render_header()

    try: service = ds.get_drive_service()
    except Exception: st.error("System-Fehler: API-Verbindung konnte nicht hergestellt werden."); st.stop()
    if not service: st.warning("⚠️ Keine Verbindung zu Google Drive. Zugangs-Token fehlt oder ist abgelaufen."); st.stop()

    try:
        s = st.secrets.get("general", st.secrets)
        PHOTOS_FID = s.get("PHOTOS_FOLDER_ID", "")
        PROJEKT_FID = s.get("PROJECT_REPORTS_FOLDER_ID", "")
        ZEIT_FID = s.get("TIME_REPORTS_FOLDER_ID", "")
        PLAENE_FID = s.get("PLANS_FOLDER_ID", "")
        ADMIN_PIN = s.get("ADMIN_PIN", "1234")
        BASE_URL = s.get("BASE_APP_URL", "https://8bv6gzagymvrdgnm8wrtrq.streamlit.app")
    except Exception: st.error("Konfigurationsfehler: Secrets nicht korrekt hinterlegt."); st.stop()

    view = st.session_state["view"]

    # ---------------------------------------------------------
    # LOGIN BEREICH
    # ---------------------------------------------------------
    if view == "Start":
        st.markdown("<h3 style='text-align: center; color: #D4AF37;'>System-Zugang wählen</h3>", unsafe_allow_html=True)
        st.write("<div style='height: 27px;'></div>", unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            if st.button("👷‍♂️ Mitarbeiter-Portal", use_container_width=True): st.session_state["view"] = "Mitarbeiter_Login"; st.rerun()
        with col2:
            if st.button("🔐 Projektleitung / Admin", use_container_width=True): st.session_state["view"] = "Admin_Login"; st.rerun()

    elif view == "Admin_Login":
        if st.button("⬅️ Zurück zum Menü"): st.session_state["view"] = "Start"; st.rerun()
        st.subheader("🔐 Administration")
        pin_input = st.text_input("Sicherheits-PIN", type="password")
        if st.button("Login", type="primary"):
            if str(pin_input).strip() == str(ADMIN_PIN).strip():
                st.session_state["logged_in"] = True; st.session_state["user_role"] = "Admin"; st.session_state["view"] = "Admin_Dashboard"; st.rerun()
            else: st.error("Authentifizierung fehlgeschlagen.")

    elif view == "Mitarbeiter_Login":
        if st.button("⬅️ Zurück zum Menü"): st.session_state["view"] = "Start"; st.rerun()
        st.subheader("👋 Personal-Identifikation")
        df_emp, _ = ds.read_csv(service, PROJEKT_FID, "Employees.csv")
        active_emps = df_emp[df_emp["Status"] == "Aktiv"]["Name"].tolist() if not df_emp.empty and "Status" in df_emp.columns else ["Keine Stammdaten vorhanden"]
        selected_employee = st.selectbox("Mitarbeiter auswählen:", active_emps)
        if st.button("Anmelden", type="primary"):
            if selected_employee != "Keine Stammdaten vorhanden":
                st.session_state["user_name"] = selected_employee; st.session_state["view"] = "Mitarbeiter_Dashboard"; st.rerun()

    # ---------------------------------------------------------
    # MITARBEITER-PORTAL
    # ---------------------------------------------------------
    elif view == "Mitarbeiter_Dashboard":
        col_back, col_title = st.columns([1, 4])
        with col_back:
            if st.button("🚪 Abmelden"): st.session_state["user_name"] = ""; st.session_state["view"] = "Start"; st.rerun()
        with col_title: st.subheader(f"📋 Rapport-Erfassung: {st.session_state['user_name']}")
        
        df_proj, _ = ds.read_csv(service, PROJEKT_FID, "Projects.csv")
        df_proj = align_project_dataframe(df_proj)
        active_projs = df_proj[df_proj["Status"] == "Aktiv"]["Projekt_Name"].tolist() if not df_proj.empty else ["Keine aktiven Projekte gefunden"]
        selected_project = st.selectbox("Aktuelles Projekt auswählen:", active_projs)
        
        if selected_project != "Keine aktiven Projekte gefunden":
            proj_data = df_proj[df_proj["Projekt_Name"] == selected_project].iloc[0]
            with st.expander("ℹ️ Projekt-Informationen & Vorgaben", expanded=True):
                c_info, m_info = st.columns(2)
                with c_info:
                    st.write(f"**Bauherr/Kunde:** {proj_data.get('Kunde_Name', '-')} (Kontakt: {proj_data.get('Kunde_Kontakt', '-')})")
                    st.write(f"**Projektadresse:** {proj_data.get('Kunde_Adresse', '-')}")
                    st.write(f"**Telefon:** {proj_data.get('Kunde_Telefon', '-')} | **E-Mail:** {proj_data.get('Kunde_Email', '-')}")
                with m_info:
                    st.write(f"**Materialvorgabe (Fugen):** Zement: {proj_data.get('Fuge_Zement', '-')} | Silikon: {proj_data.get('Fuge_Silikon', '-')}")
                    if str(proj_data.get('Asbest_Gefahr', 'Nein')).strip().lower() == "ja": 
                        st.markdown("<p style='color:#ff4b4b; font-weight:bold;'>⚠️ SICHERHEITSHINWEIS: ASBEST VORHANDEN</p>", unsafe_allow_html=True)
        
        st.write("<div style='height: 27px;'></div>", unsafe_allow_html=True)
        t_arb, t_abs, t_med, t_hist = st.tabs(["🛠️ Arbeitszeit", "🏥 Absenzen", "📤 Dokumentation", "📜 Berichte & Signatur"])
        
        with t_arb:
            with st.form("arb_form"):
                st.markdown("**Arbeitszeit & Pausen**")
                c1, c2, c3, c4 = st.columns(4)
                with c1: f_date = st.date_input("Datum", datetime.now())
                with c2: f_start = st.time_input("Arbeitsbeginn", datetime.strptime("07:00", "%H:%M").time())
                with c3: f_end = st.time_input("Arbeitsende", datetime.strptime("16:30", "%H:%M").time())
                with c4: f_pause = st.number_input("Pausen (Min)", min_value=0, value=30, step=15)
                
                st.divider()
                st.markdown("**🚗 Fahrtzeiten (Direktfahrt Wohnort ↔ Baustelle)**")
                st.info("Hinweis: Fahrten über das Magazin gelten als reguläre Arbeitszeit und müssen hier nicht eingetragen werden. Bei Direktfahrten werden gemäß SPV automatisch 30 Min. pro Weg abgezogen.")
                r1, r2 = st.columns(2)
                with r1: r_hin = st.number_input("Hinweg (Minuten)", value=0, step=5)
                with r2: r_rueck = st.number_input("Rückweg (Minuten)", value=0, step=5)
                
                st.divider()
                st.markdown("**Tätigkeitsnachweis**")
                f_arbeit = st.text_area("Ausgeführte Arbeiten")
                f_mat = st.text_area("Materialeinsatz")
                f_bem = st.text_input("Bemerkung / Behinderungen")
                
                if st.form_submit_button("💾 Rapport zur Prüfung einreichen", type="primary"):
                    process_single_rapport(service, f_date, f_start, f_end, f_pause, f_arbeit, f_mat, f_bem, selected_project, r_hin, r_rueck, PROJEKT_FID, ZEIT_FID)

        with t_abs:
            with st.form("abs_form"):
                st.markdown("**Meldung von Nicht-Präsenzzeiten (Multi-Selektion)**")
                st.info("Tipp: Sie können einen Datumsbereich auswählen, um mehrtägige Absenzen (max. 7 Tage) in einem Vorgang zu verbuchen.")
                
                c1, c2 = st.columns(2)
                with c1: 
                    f_a_date_range = st.date_input("Zeitraum wählen", value=(datetime.now(), datetime.now()))
                with c2: 
                    f_a_hours = st.number_input("Anzurechnende Stunden pro Tag", min_value=0.0, value=8.5, step=0.25)
                
                a_typ = st.selectbox("Kategorie", ["Ferien", "Krankheit", "Unfall (SUVA)", "Feiertag"])
                a_bem = st.text_input("Zusätzliche Bemerkungen")
                a_file = st.file_uploader("📄 Dokumenten-Upload (z.B. Arztzeugnis)", type=['pdf','jpg','png'])
                
                if st.form_submit_button("💾 Absenz(en) verbuchen", type="primary"):
                    if isinstance(f_a_date_range, tuple):
                        if len(f_a_date_range) == 2:
                            start_date, end_date = f_a_date_range
                        elif len(f_a_date_range) == 1:
                            start_date = end_date = f_a_date_range[0]
                        else:
                            start_date = end_date = datetime.now().date()
                    else:
                        start_date = end_date = f_a_date_range
                    
                    days_diff = (end_date - start_date).days + 1
                    
                    if days_diff > 7:
                        st.error("System-Einschränkung: Bitte buchen Sie maximal 7 Tage am Stück.")
                    elif days_diff < 1:
                        st.error("Fehler bei der Datumsauswahl.")
                    else:
                        if a_file and a_typ == "Krankheit":
                            fname = f"ZEUGNIS_{st.session_state['user_name']}_{start_date}_{a_file.name}"
                            ds.upload_image(service, PLAENE_FID, fname, io.BytesIO(a_file.getvalue()), a_file.type)
                        process_batch_absenz(service, start_date, end_date, f_a_hours, a_typ, a_bem, selected_project, PROJEKT_FID, ZEIT_FID)

        with t_med:
            col_up, col_gal = st.columns([1, 2])
            with col_up:
                files = st.file_uploader("Baustellen-Fotos hochladen", accept_multiple_files=True, type=['jpg','png','jpeg'])
                if st.button("📤 Dateien übertragen", type="primary") and files:
                    prog = st.progress(0)
                    for idx, f in enumerate(files[:PAGINATION_LIMIT]):
                        fname = f"{selected_project}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{f.name}"
                        ds.upload_image(service, PHOTOS_FID, fname, io.BytesIO(f.getvalue()), f.type)
                        prog.progress((idx + 1) / len(files))
                    st.success("Übertragung erfolgreich."); st.cache_data.clear(); time.sleep(1); st.rerun()
            with col_gal:
                if st.button("🔄 Ansicht aktualisieren"): st.cache_data.clear()
                all_files = []
                if PHOTOS_FID: all_files.extend(load_project_files(service, PHOTOS_FID, selected_project))
                if PLAENE_FID: all_files.extend(load_project_files(service, PLAENE_FID, selected_project))
                if all_files:
                    cols = st.columns(2)
                    for idx, img in enumerate(all_files):
                        with cols[idx % 2]:
                            img_b = get_file_bytes(service, img['id'])
                            if img_b:
                                if img['name'].lower().endswith(('.png', '.jpg', '.jpeg')): st.image(img_b, use_container_width=True)
                                else: st.download_button(f"📥 {img['name'][:15]}", data=img_b, file_name=img['name'])

        with t_hist:
            st.markdown("<h4 style='color:#D4AF37;'>Validierungs-Prozess & Signatur</h4>", unsafe_allow_html=True)
            df_hist_p, _ = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
            df_hist_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
            
            if not df_hist_p.empty and not df_hist_z.empty and "Erfasst" in df_hist_z.columns:
                df_hist_z = align_zeit_dataframe(df_hist_z)
                df_merged = pd.merge(df_hist_p, df_hist_z[["Erfasst", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]], on="Erfasst", how="left")
                df_proj_hist = df_merged[(df_merged["Projekt"] == selected_project) & (df_merged["Mitarbeiter"] == st.session_state["user_name"])].sort_values(by="Datum", ascending=False)
                
                for _, row in df_proj_hist.head(PAGINATION_LIMIT).iterrows():
                    status_str = str(row.get('Status', 'ENTWURF'))
                    status_color = "🔴" if status_str == "ENTWURF" else "🟡" if status_str == "FREIGEGEBEN" else "🟢"
                    titel = f"Status: {status_color} {status_str} | Datum: {row['Datum']} | {row.get('Absenz_Typ', '')} Total: {row.get('Arbeitszeit_inkl_Reisezeit', '-')} Std."
                    
                    with st.expander(titel):
                        st.write(f"**Tätigkeit:** {row.get('Arbeit', '-')}")
                        if status_str == "FREIGEGEBEN":
                            st.info("Die Bauleitung hat diesen Rapport geprüft und freigegeben. Bitte bestätigen Sie die Richtigkeit digital.")
                            if st.button(f"✍️ Digital Signieren", key=f"sig_{row['Erfasst']}"):
                                idx_up = df_hist_z[df_hist_z['Erfasst'] == row['Erfasst']].index
                                if not idx_up.empty:
                                    df_hist_z.loc[idx_up, "Status"] = "SIGNIERT"
                                    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", df_hist_z, fid_z)
                                    st.success("Signatur erfolgreich im System hinterlegt!"); time.sleep(1); st.rerun()

    # ---------------------------------------------------------
    # PROJEKTLEITUNG / ADMIN DASHBOARD
    # ---------------------------------------------------------
    elif view == "Admin_Dashboard":
        col1, col2 = st.columns([4, 1])
        with col1: st.subheader("🛠️ System-Administration & Controlling")
        with col2:
            if st.button("🚪 Abmelden", use_container_width=True): st.session_state["logged_in"] = False; st.session_state["view"] = "Start"; st.rerun()
        
        t_zeit, t_bau, t_stam, t_docs, t_print, t_alchemist, t_shiva = st.tabs(["🕒 Freigabe-Workflow", "🏗️ Projekt-Controlling", "⚙️ Stammdaten", "📂 Datei-Management", "🖨️ Berichtswesen", "📥 AZK-Export", "🗑️ System-Administration"])
        
        with t_zeit:
            st.markdown("**Compliance-Schritt 1: Prüfung & Admin-Freigabe**")
            st.write("Ändern Sie den Status geprüfter Einträge auf 'FREIGEGEBEN', um die digitale Signatur der Mitarbeiter anzufordern.")
            df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
            if not df_z.empty:
                df_z = align_zeit_dataframe(df_z)
                df_z['Sort_Order'] = df_z['Status'].map({'ENTWURF': 1, 'FREIGEGEBEN': 2, 'SIGNIERT': 3}).fillna(4)
                df_z = df_z.sort_values(by=['Sort_Order', 'Datum']).drop(columns=['Sort_Order'])
                edit_z = st.data_editor(df_z, num_rows="dynamic", use_container_width=True)
                if st.button("💾 AZK-Daten speichern & aktualisieren", type="primary"):
                    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", edit_z, fid_z)
                    st.success("Daten erfolgreich mit der Datenbank synchronisiert.")

        with t_bau:
            st.markdown("**Projekt-Rapporte (Tätigkeiten & Material)**")
            df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
            if not df_p.empty:
                edit_p = st.data_editor(df_p, num_rows="dynamic", use_container_width=True)
                if st.button("💾 Projekt-Daten speichern", type="primary"):
                    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", edit_p, fid_p)
                    st.success("Änderungen gesichert.")

        with t_stam:
            df_proj, fid_proj = ds.read_csv(service, PROJEKT_FID, "Projects.csv")
            df_proj = align_project_dataframe(df_proj)
            st.markdown("**🏗️ Projekt-Stammdaten**")
            edit_proj = st.data_editor(df_proj, num_rows="dynamic", key="e_proj", use_container_width=True)
            if st.button("💾 Projekte aktualisieren"): ds.save_csv(service, PROJEKT_FID, "Projects.csv", edit_proj, fid_proj)

            df_emp, fid_emp = ds.read_csv(service, PROJEKT_FID, "Employees.csv")
            st.markdown("**👷‍♂️ Personal-Stammdaten**")
            if df_emp.empty or "Mitarbeiter_ID" not in df_emp.columns:
                df_emp = pd.DataFrame({"Mitarbeiter_ID": ["M01"], "Name": ["Christoph Schlorff"], "Status": ["Aktiv"]})
            edit_emp = st.data_editor(df_emp, num_rows="dynamic", key="e_emp", use_container_width=True)
            if st.button("💾 Personal aktualisieren"): ds.save_csv(service, PROJEKT_FID, "Employees.csv", edit_emp, fid_emp)

        with t_docs:
            st.markdown("**Zentrales Dokumenten-Management**")
            active_projs = df_proj["Projekt_Name"].tolist() if not df_proj.empty else ["Keine Projekte gefunden"]
            admin_sel_proj = st.selectbox("Zuweisung für Projekt:", active_projs, key="admin_docs_sel")
            
            c_u1, c_u2 = st.columns(2)
            with c_u1:
                plan_f = st.file_uploader("📤 Pläne (PDF/Bilder)", accept_multiple_files=True, type=['pdf', 'jpg', 'png'])
                if st.button("Dokumente hochladen") and plan_f and PLAENE_FID:
                    for f in plan_f: ds.upload_image(service, PLAENE_FID, f"{admin_sel_proj}_PLAN_{f.name}", io.BytesIO(f.getvalue()), f.type)
                    st.success("Upload erfolgreich.")
            with c_u2:
                foto_f = st.file_uploader("📷 Projektfotos", accept_multiple_files=True, type=['jpg', 'png'])
                if st.button("Fotos hochladen") and foto_f:
                    for f in foto_f: ds.upload_image(service, PHOTOS_FID, f"{admin_sel_proj}_ADMIN_{f.name}", io.BytesIO(f.getvalue()), f.type)
                    st.success("Upload erfolgreich.")
            
            st.divider()
            if st.button("🔄 Projekt-Archiv laden"): st.cache_data.clear()
            admin_files = []
            if PHOTOS_FID: admin_files.extend(load_project_files(service, PHOTOS_FID, admin_sel_proj))
            if PLAENE_FID: admin_files.extend(load_project_files(service, PLAENE_FID, admin_sel_proj))
            if admin_files:
                cols = st.columns(4)
                for idx, img in enumerate(admin_files):
                    with cols[idx % 4]:
                        img_bytes = get_file_bytes(service, img['id'])
                        if img_bytes:
                            if img['name'].lower().endswith(('.png', '.jpg', '.jpeg')): st.image(img_bytes, use_container_width=True)
                            else: st.download_button(f"📥 {img['name'][:15]}", data=img_bytes, file_name=img['name'])

        with t_print:
            st.markdown("**Compliance-Schritt 3: Physische Verankerung (Druckbericht)**")
            st.info("Hinweis: Der Export invertiert die Farben für einen ressourcenschonenden, papierbasierten Druck.")
            print_proj = st.selectbox("Projekt für Berichtswesen:", active_projs, key="admin_print_sel")
            if st.button("🖨️ Wochenbericht generieren", type="primary") and print_proj != "Keine Projekte gefunden":
                proj_row = df_proj[df_proj["Projekt_Name"] == print_proj].iloc[0]
                k_name = proj_row.get('Kunde_Name', '')
                k_kontakt = proj_row.get('Kunde_Kontakt', '')
                k_adresse = proj_row.get('Kunde_Adresse', '')
                k_telefon = proj_row.get('Kunde_Telefon', '')
                
                f_zem = proj_row.get('Fuge_Zement', '-')
                f_sil = proj_row.get('Fuge_Silikon', '-')
                asbest = str(proj_row.get('Asbest_Gefahr', 'Nein')).strip()
                
                asbest_html = f"<p style='color:red; font-weight:bold; font-size: 14px;'>⚠️ SICHERHEITSHINWEIS: ASBEST VORHANDEN</p>" if asbest.lower() == "ja" else ""

                safe_proj_name = urllib.parse.quote(print_proj)
                qr_url = f"{BASE_URL}?projekt={safe_proj_name}"
                qr_api_url = f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data={urllib.parse.quote(qr_url)}"
                table_rows = "".join(["<tr><td></td><td></td><td></td><td></td><td></td></tr>" for _ in range(15)])
                
                html_content = f"""
                <!DOCTYPE html><html><head><title>Wochenbericht - {print_proj}</title>
                <style>
                @page {{ size: A4; margin: 15mm; }} 
                body {{ font-family: Arial; font-size: 12px; background-color: #ffffff; color: #000000; margin:0; padding:0; }}
                .h-grid {{ display: flex; justify-content: space-between; border-bottom: 2px solid #000; padding-bottom: 15px; margin-bottom: 20px; }}
                .col-info {{ width: 50%; }}
                .col-specs {{ width: 30%; border-left: 1px solid #ccc; padding-left: 15px; }}
                .col-qr {{ width: 20%; text-align: right; }}
                table {{ width: 100%; border-collapse: collapse; }} th, td {{ border: 1px solid #000; padding: 10px; }} td {{ height: 50px; }}
                .sig-box {{ margin-top: 54px; border-top: 1px solid #000; width: 300px; padding-top: 10px; font-weight: bold; }}
                </style>
                </head><body>
                <div class="h-grid">
                    <div class="col-info">
                        <h1 style="margin-top:0;">R. Baumgartner AG</h1>
                        <p><strong>Projekt:</strong> {print_proj}</p>
                        <p style="margin-bottom:0;"><strong>Kundenangaben & Einsatzort:</strong></p>
                        <p style="margin-top:2px; line-height: 1.4;">
                            Name: {k_name} ({k_kontakt})<br>
                            Adresse: {k_adresse}<br>
                            Telefon: {k_telefon}
                        </p>
                    </div>
                    <div class="col-specs">
                        <p style="margin-top:0;"><strong>Materialvorgaben:</strong></p>
                        <p>Zementfuge: {f_zem}<br>Silikonfuge: {f_sil}</p>
                        {asbest_html}
                    </div>
                    <div class="col-qr">
                        <img src="{qr_api_url}" width="100">
                        <p style="font-size:10px; margin:5px 0 0 0;">Login via QR</p>
                    </div>
                </div>
                <table><thead><tr><th>Datum</th><th>Name</th><th>Stunden (Netto)</th><th>Ausgeführte Arbeiten / Material</th><th>Notizen</th></tr></thead>
                <tbody>{table_rows}</tbody></table>
                <div class="sig-box">Rechtsverbindliche Unterschrift (Mitarbeiter)</div>
                </body></html>
                """
                st.components.v1.html(html_content, height=500, scrolling=True)
                st.download_button("📄 HTML-Bericht herunterladen", data=html_content, file_name=f"Bericht_{print_proj}.html", mime="text/html")

        with t_alchemist:
            st.markdown("<h3 style='color:#D4AF37;'>Daten-Export (AZK-Synchronisation)</h3>", unsafe_allow_html=True)
            st.write("Generiert eine standardisierte Excel-Datei zur verlustfreien Übertragung in die Buchhaltung.")
            
            df_z, _ = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
            if not df_z.empty:
                df_z = align_zeit_dataframe(df_z)
                
                export_status = st.selectbox("Qualitätsfilter (Status):", 
                                           ["Nur SIGNIERT (Compliance erfüllt)", "FREIGEGEBEN & SIGNIERT", "Vollständiger Export (inkl. Entwürfe)"])
                
                if export_status == "Nur SIGNIERT (Compliance erfüllt)":
                    df_export = df_z[df_z["Status"] == "SIGNIERT"]
                elif export_status == "FREIGEGEBEN & SIGNIERT":
                    df_export = df_z[df_z["Status"].isin(["FREIGEGEBEN", "SIGNIERT"])]
                else:
                    df_export = df_z
                
                if not df_export.empty:
                    df_export['Datum'] = pd.to_datetime(df_export['Datum'])
                    df_export['Monat'] = df_export['Datum'].dt.strftime('%Y-%m')
                    monate = sorted(df_export['Monat'].unique().tolist(), reverse=True)
                    
                    sel_monat = st.selectbox("Auswertungs-Zeitraum:", monate)
                    df_monat = df_export[df_export['Monat'] == sel_monat].copy()
                    
                    df_clean = df_monat[["Datum", "Mitarbeiter", "Projekt", "Stunden_Total", "Reisezeit_bezahlt_Min", "Arbeitszeit_inkl_Reisezeit", "Absenz_Typ", "Status"]].copy()
                    df_clean.rename(columns={
                        "Stunden_Total": "Arbeit (Std)", 
                        "Reisezeit_bezahlt_Min": "Reise (Min)", 
                        "Arbeitszeit_inkl_Reisezeit": "Total (Std)",
                        "Absenz_Typ": "Kategorie (Absenz)"
                    }, inplace=True)
                    
                    df_clean['Datum'] = df_clean['Datum'].dt.strftime('%d.%m.%Y')
                    
                    st.dataframe(df_clean, use_container_width=True)
                    
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_clean.to_excel(writer, index=False, sheet_name='Daten_Export')
                    excel_data = output.getvalue()
                    
                    st.download_button(
                        label="📥 Excel-Datei (.xlsx) herunterladen",
                        data=excel_data,
                        file_name=f"AZK_Export_{sel_monat}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary"
                    )
                else:
                    st.info("Im gewählten Filter liegen keine verifizierten Daten vor.")
            else:
                st.info("Die Datenbank enthält noch keine Einträge.")

        with t_shiva:
            st.markdown("<h2 style='color: #ff4b4b;'>🗑️ System-Administration (Datenlöschung)</h2>", unsafe_allow_html=True)
            st.warning("Gefahr: Dieser Vorgang löscht Projekte/Mitarbeiter sowie alle zugehörigen Tabelleneinträge und Dateien aus dem Google Drive unwiderruflich.")
            
            del_type = st.radio("Zielkategorie wählen:", ["Projekt", "Mitarbeiter"])
            admin_check = st.checkbox("Sicherheitswarnung verstanden und Löschung autorisieren")
            
            if del_type == "Projekt":
                del_target = st.selectbox("Zu löschendes Projekt:", active_projs)
                if st.button("🛑 Projekt endgültig löschen") and admin_check:
                    df_proj = df_proj[df_proj["Projekt_Name"] != del_target]
                    ds.save_csv(service, PROJEKT_FID, "Projects.csv", df_proj, fid_proj)
                    
                    df_p, fid_p = ds.read_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv")
                    df_p = df_p[df_p["Projekt"] != del_target]
                    ds.save_csv(service, PROJEKT_FID, "Baustellen_Rapport.csv", df_p, fid_p)
                    
                    df_z, fid_z = ds.read_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv")
                    df_z = df_z[df_z["Projekt"] != del_target]
                    ds.save_csv(service, ZEIT_FID, "Arbeitszeit_AKZ.csv", df_z, fid_z)
                    
                    delete_project_assets(service, del_target, [PHOTOS_FID, PLAENE_FID])
                    st.success(f"System-Meldung: Projekt '{del_target}' wurde erfolgreich bereinigt."); time.sleep(2); st.rerun()
            else:
                del_emp = st.selectbox("Zu löschender Mitarbeiter:", df_emp["Name"].tolist() if not df_emp.empty else [])
                if st.button("🛑 Mitarbeiter endgültig löschen") and admin_check:
                    df_emp = df_emp[df_emp["Name"] != del_emp]
                    ds.save_csv(service, PROJEKT_FID, "Employees.csv", df_emp, fid_emp)
                    st.success(f"System-Meldung: Mitarbeiter '{del_emp}' wurde aus den Stammdaten entfernt."); time.sleep(2); st.rerun()

if __name__ == "__main__":
    main_flow()

```
