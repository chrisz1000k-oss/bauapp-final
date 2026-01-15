import io
import os
import time as sys_time
import base64
import html
from datetime import datetime, time, timedelta
from uuid import uuid4

import pandas as pd
import streamlit as st
import qrcode

# Google Libraries
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# ==========================================
# KONFIGURATION & KONSTANTEN
# ==========================================
st.set_page_config(page_title="BauApp", layout="wide")

# CSS: UI etwas aufräumen
st.markdown(
    """
    <style>
    #MainMenu {display: none !important;}
    [data-testid="stToolbar"] {display: none !important;}
    footer {display: none !important;}
    </style>
    """,
    unsafe_allow_html=True,
)

# Basis-URL (für QR-Codes)
BASE_APP_URL = "https://8bv6gzagymvrdgnm8wrtrq.streamlit.app"

# Dateinamen für CSVs (Stammdaten)
PROJECTS_CSV = "Projects.csv"
EMPLOYEES_CSV = "Employees.csv"
CLOSURES_CSV = "Closures.csv"

# Ordner-IDs (DIESE MÜSSEN IN DEINEN SECRETS STEHEN ODER HIER HARDCODED SEIN)
# Falls sie nicht in st.secrets sind, trage sie hier direkt ein:
PHOTOS_FOLDER_ID = st.secrets.get("PHOTOS_FOLDER_ID", "DEINE_PHOTOS_ID_HIER")
REPORTS_FOLDER_ID = st.secrets.get("REPORTS_FOLDER_ID", "DEINE_REPORTS_ID_HIER")

# Spalten-Definition für den Rapport
RAPPORT_COLS = [
    "ID", "Datum", "Wochentag", "Kalenderwoche",
    "Projekt", "ProjektID",
    "Kunde", "Objekt", "Auftragsnr",  # Kundendaten aus Projects.csv
    "Mitarbeiter", "EmployeeID",
    "Start", "Ende", "Pause", "Stunden_num",
    "Material", "Bemerkung",
    # Fahrtzeiten SPV
    "AnkunftMagazin", "AbfahrtMagazin",
    "ReiseHomeToSiteMin", "ReiseSiteToHomeMin",
    "ReiseDirektMin", "ReiseBezahltMin",
    "ReiseRegel", "Reisezeit_h", # Bleibt für Kompatibilität
    "ErstelltAm", "ErstelltVon"
]

# ==========================================
# GOOGLE DRIVE AUTH & HELPER
# ==========================================
def get_drive_service():
    """Erstellt den Google Drive Service aus st.secrets."""
    if "GOOGLE_REFRESH_TOKEN" not in st.secrets:
        st.error("Google Refresh Token fehlt in st.secrets!")
        return None
    
    creds = Credentials(
        None,
        refresh_token=st.secrets["GOOGLE_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=st.secrets["GOOGLE_CLIENT_ID"],
        client_secret=st.secrets["GOOGLE_CLIENT_SECRET"],
    )
    return build("drive", "v3", credentials=creds)

def list_files(folder_id, mime_prefix=None):
    """Listet Dateien in einem Drive-Ordner auf."""
    srv = get_drive_service()
    if not srv: return []
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        if mime_prefix:
            query += f" and mimeType contains '{mime_prefix}'"
        
        results = srv.files().list(
            q=query,
            fields="files(id, name, mimeType, createdTime)",
            pageSize=1000
        ).execute()
        return results.get("files", [])
    except Exception as e:
        st.error(f"Fehler beim Listen von Dateien: {e}")
        return []

def download_csv(file_name_or_id):
    """Lädt eine CSV aus Drive herunter (sucht erst nach Namen, dann ID)."""
    srv = get_drive_service()
    if not srv: return pd.DataFrame()
    
    try:
        # 1) Versuche, Datei per ID zu laden (falls String wie eine ID aussieht)
        #    Einfacher: Wir suchen erst nach Namen im Root oder allgemein
        #    Hier vereinfacht: Wir suchen nach Namen.
        
        # Suche File ID anhand Namen (in allen Ordnern, oder spezifisch Reports)
        # Wir suchen global oder im Reports Folder? Stammdaten liegen oft im Root oder Config Folder.
        # Der Einfachheit halber suchen wir "name = '...'"
        q = f"name = '{file_name_or_id}' and trashed=false"
        res = srv.files().list(q=q, fields="files(id)").execute()
        files = res.get("files", [])
        
        if not files:
            # Falls nicht gefunden, gib leeres DF zurück
            return pd.DataFrame()
        
        file_id = files[0]["id"]
        
        request = srv.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        return pd.read_csv(fh)
    except Exception:
        return pd.DataFrame()

def download_file_by_id(file_id):
    """Lädt Datei direkt per ID (für Rapporte im Reports-Folder)."""
    srv = get_drive_service()
    if not srv: return None
    try:
        request = srv.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return pd.read_csv(fh)
    except Exception:
        return pd.DataFrame()

def update_file_in_drive(file_id, csv_content_str):
    """Überschreibt eine existierende Datei in Drive."""
    srv = get_drive_service()
    if not srv: return False
    try:
        media = MediaIoBaseUpload(io.BytesIO(csv_content_str.encode("utf-8")), mimetype="text/csv")
        srv.files().update(fileId=file_id, media_body=media).execute()
        return True
    except Exception as e:
        st.error(f"Fehler beim Update: {e}")
        return False

def create_file_in_drive(folder_id, filename, csv_content_str):
    """Erstellt eine neue Datei in Drive."""
    srv = get_drive_service()
    if not srv: return None
    try:
        file_metadata = {"name": filename, "parents": [folder_id], "mimeType": "text/csv"}
        media = MediaIoBaseUpload(io.BytesIO(csv_content_str.encode("utf-8")), mimetype="text/csv")
        file = srv.files().create(body=file_metadata, media_body=media, fields="id").execute()
        return file.get("id")
    except Exception as e:
        st.error(f"Fehler beim Erstellen: {e}")
        return None

# ==========================================
# DATA LOADING FUNCTIONS
# ==========================================
def get_projects():
    """Lädt Projects.csv und gibt Liste & Dict zurück."""
    df = download_csv(PROJECTS_CSV)
    if df.empty:
        # Fallback Mockdaten, falls CSV fehlt
        return ["Beispielprojekt"], pd.DataFrame({"Projekt": ["Beispielprojekt"]})
    # Erwarte Spalte "Projekt"
    if "Projekt" not in df.columns:
        return [], df
    
    # Filtern: Nur aktive? (Optional, hier nehmen wir alle)
    projects = sorted(df["Projekt"].dropna().unique().tolist())
    return projects, df

def get_employees():
    """Lädt Employees.csv."""
    df = download_csv(EMPLOYEES_CSV)
    if df.empty:
        return ["Mitarbeiter A"], pd.DataFrame()
    if "Name" in df.columns:
        return sorted(df["Name"].dropna().unique().tolist()), df
    return [], df

def get_reports_df(project_name):
    """
    Sucht {Project}_Reports.csv im REPORTS_FOLDER_ID.
    Gibt (DataFrame, file_id) zurück.
    """
    srv = get_drive_service()
    filename = f"{project_name}_Reports.csv"
    
    # Suche im Reports-Ordner
    q = f"name = '{filename}' and '{REPORTS_FOLDER_ID}' in parents and trashed=false"
    res = srv.files().list(q=q, fields="files(id)").execute()
    files = res.get("files", [])
    
    if not files:
        # Noch keine Datei -> Leeres DF
        return pd.DataFrame(columns=RAPPORT_COLS), None
    
    file_id = files[0]["id"]
    df = download_file_by_id(file_id)
    
    # Sicherstellen, dass alle Spalten da sind
    for col in RAPPORT_COLS:
        if col not in df.columns:
            df[col] = None
            
    return df, file_id

def save_report_entry(project_name, entry_dict):
    """Lädt aktuellen Rapport, fügt Zeile hinzu, speichert zurück."""
    df, file_id = get_reports_df(project_name)
    
    new_row = pd.DataFrame([entry_dict])
    df = pd.concat([df, new_row], ignore_index=True)
    
    csv_str = df.to_csv(index=False)
    
    if file_id:
        update_file_in_drive(file_id, csv_str)
    else:
        create_file_in_drive(REPORTS_FOLDER_ID, f"{project_name}_Reports.csv", csv_str)

# ==========================================
# HELPER UI
# ==========================================
def get_week_start_end(dt_date):
    start = dt_date - timedelta(days=dt_date.weekday())
    end = start + timedelta(days=6)
    return start, end

# ==========================================
# MAIN APP
# ==========================================
def main():
    # --- Sidebar ---
    st.sidebar.image("https://cdn-icons-png.flaticon.com/512/25/25694.png", width=50) # Platzhalter Logo
    st.sidebar.title("BauApp Menü")
    
    app_mode = st.sidebar.radio("Bereich wählen", ["Mitarbeiter (Rapport)", "Admin / Büro"])
    
    # Projekte laden
    project_list, df_projects = get_projects()
    
    if app_mode == "Mitarbeiter (Rapport)":
        show_employee_mode(project_list, df_projects)
    else:
        show_admin_mode(project_list)

# ==========================================
# 1) MITARBEITER BEREICH
# ==========================================
def show_employee_mode(project_list, df_projects):
    st.title("👷‍♂️ Tagesrapport erfassen")
    
    # Projektauswahl
    sel_proj = st.selectbox("Projekt auswählen", project_list)
    
    # Kundendaten ermitteln (für Anzeige & Speichern)
    proj_row = df_projects[df_projects["Projekt"] == sel_proj].iloc[0] if not df_projects.empty and sel_proj in df_projects["Projekt"].values else {}
    kunde_val = proj_row.get("Kunde", "")
    objekt_val = proj_row.get("Objekt", "")
    auftrag_val = proj_row.get("Auftragsnr", "")
    
    if kunde_val:
        st.info(f"**Kunde:** {kunde_val} | **Objekt:** {objekt_val}")

    # Mitarbeiter laden
    emp_list, df_emp = get_employees()
    sel_emp = st.selectbox("Mitarbeiter", emp_list)
    
    # ID des Mitarbeiters finden
    emp_id = ""
    if not df_emp.empty and "Name" in df_emp.columns and "EmployeeID" in df_emp.columns:
        row = df_emp[df_emp["Name"] == sel_emp]
        if not row.empty:
            emp_id = row.iloc[0]["EmployeeID"]

    # Datum
    date_val = st.date_input("Datum", datetime.now())
    
    # Zeit-Eingabe
    c1, c2, c3 = st.columns(3)
    t_start = c1.time_input("Start", time(7, 0))
    t_end = c2.time_input("Ende", time(16, 30))
    t_pause = c3.number_input("Pause (Minuten)", 0, 120, 30, step=5)
    
    # Berechnung Stunden
    dt_start = datetime.combine(date_val, t_start)
    dt_end = datetime.combine(date_val, t_end)
    if dt_end < dt_start:
        dt_end += timedelta(days=1) # Nachtschicht
    
    diff = (dt_end - dt_start).total_seconds() / 3600.0
    stunden_netto = max(0.0, diff - (t_pause / 60.0))
    st.write(f"**Arbeitszeit:** {stunden_netto:.2f} Stunden")
    
    # --- FAHRTZEITEN (SPV NEU) ---
    # defaults
    ank_mag = None
    abd_mag = None
    reise_home_to_site_min = 0
    reise_site_to_home_min = 0
    reise_direkt_total_min = 0
    reise_bezahlt_min = 0
    reise_bezahlt_h = 0.0
    reise_regel = "SPV: Bezahlt nur Direktfahrt Zuhause↔Baustelle, abzüglich 30 Min Selbstbehalt PRO RICHTUNG."

    with st.expander("🚗 Fahrtzeiten (SPV-konform)", expanded=False):
        st.caption(
            "Regel: Magazin↔Baustelle ist **nicht** bezahlte Fahrtzeit. "
            "Bezahlt wird nur Direktfahrt Zuhause↔Baustelle, **minus 30 Min Selbstbehalt pro Richtung**."
        )
        ec1, ec2, ec3 = st.columns(3)
        
        # Magazinzeiten nur als Info (nicht bezahlt)
        has_ank = ec1.checkbox("Ankunft Magazin erfassen", value=False, key="ank_mag_chk")
        ank_mag = ec1.time_input("Ankunft Magazin (Uhrzeit)", time(0, 0), key="ank_mag_time") if has_ank else None
        
        has_abd = ec2.checkbox("Abfahrt Magazin erfassen", value=False, key="abd_mag_chk")
        abd_mag = ec2.time_input("Abfahrt Magazin (Uhrzeit)", time(0, 0), key="abd_mag_time") if has_abd else None
        
        # Direktfahrten (nur diese zählen für bezahlte Fahrtzeit)
        reise_home_to_site_min = int(ec3.number_input("Direkt: Zuhause → Baustelle (Min)", 0, 600, 0, 5, key="reise_h2s_min"))
        reise_site_to_home_min = int(ec3.number_input("Direkt: Baustelle → Zuhause (Min)", 0, 600, 0, 5, key="reise_s2h_min"))
        
        reise_direkt_total_min = reise_home_to_site_min + reise_site_to_home_min
        
        # 30 Min Selbstbehalt PRO RICHTUNG (nur wenn diese Richtung erfasst ist)
        bezahlt_h2s = max(0, reise_home_to_site_min - 30) if reise_home_to_site_min > 0 else 0
        bezahlt_s2h = max(0, reise_site_to_home_min - 30) if reise_site_to_home_min > 0 else 0
        
        reise_bezahlt_min = bezahlt_h2s + bezahlt_s2h
        reise_bezahlt_h = round(reise_bezahlt_min / 60.0, 2)
        
        # Anzeige zur Kontrolle
        if reise_direkt_total_min > 0:
            st.info(
                f"Direkt: H→B **{reise_home_to_site_min} Min** (bezahlt **{bezahlt_h2s}**), "
                f"B→H **{reise_site_to_home_min} Min** (bezahlt **{bezahlt_s2h}**) → "
                f"Bezahlt gesamt: **{reise_bezahlt_min} Min** (= **{reise_bezahlt_h} h**)"
            )
        else:
            st.info("Keine Direktfahrt erfasst → bezahlte Fahrtzeit = 0")

    # --- MATERIAL & BEMERKUNG ---
    st.subheader("Material & Beschreibung")
    col_m1, col_m2 = st.columns(2)
    material = col_m1.text_area("Materialeinsatz", height=100, placeholder="z.B. 5x Zementsäcke, 10m Kabel...")
    bemerkung = col_m2.text_area("Arbeitsbeschrieb / Bemerkung", height=100, placeholder="Was wurde gemacht?")
    
    # --- SPEICHERN ---
    if st.button("💾 Rapport Speichern", use_container_width=True, type="primary"):
        # Daten sammeln
        entry = {
            "ID": str(uuid4())[:8],
            "Datum": str(date_val),
            "Wochentag": date_val.strftime("%A"),
            "Kalenderwoche": date_val.isocalendar()[1],
            "Projekt": sel_proj,
            "ProjektID": str(proj_row.get("ProjektID", "")),
            "Kunde": str(kunde_val),
            "Objekt": str(objekt_val),
            "Auftragsnr": str(auftrag_val),
            "Mitarbeiter": sel_emp,
            "EmployeeID": str(emp_id),
            "Start": str(t_start),
            "Ende": str(t_end),
            "Pause": t_pause,
            "Stunden_num": round(stunden_netto, 2),
            "Material": material,
            "Bemerkung": bemerkung,
            # SPV Felder
            "AnkunftMagazin": str(ank_mag) if ank_mag else "",
            "AbfahrtMagazin": str(abd_mag) if abd_mag else "",
            "ReiseHomeToSiteMin": reise_home_to_site_min,
            "ReiseSiteToHomeMin": reise_site_to_home_min,
            "ReiseDirektMin": reise_direkt_total_min,
            "ReiseBezahltMin": reise_bezahlt_min,
            "ReiseRegel": reise_regel,
            "Reisezeit_h": reise_bezahlt_h, # Für Kompatibilität
            "ErstelltAm": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ErstelltVon": sel_emp
        }
        
        save_report_entry(sel_proj, entry)
        st.success("✅ Rapport erfolgreich gespeichert!")
        sys_time.sleep(1)
        st.rerun()

    # --- ANZEIGE DES AKTUELLEN PROJEKTS (TABELLE) ---
    st.divider()
    st.markdown("### 📋 Erfasste Rapporte für dieses Projekt")
    df_curr, _ = get_reports_df(sel_proj)
    if not df_curr.empty:
        # Sortieren nach Datum absteigend
        if "Datum" in df_curr.columns:
            df_curr = df_curr.sort_values("Datum", ascending=False)
        
        # Einfache Ansicht für Mitarbeiter
        cols_show = ["Datum", "Mitarbeiter", "Stunden_num", "Reisezeit_h", "Material", "Bemerkung"]
        st.dataframe(df_curr[cols_show], use_container_width=True)
    else:
        st.info("Noch keine Einträge für dieses Projekt.")

    # --- WOCHENABSCHLUSS (SIGNATUR) ---
    st.divider()
    with st.expander("✒️ Woche abschliessen (Signatur)"):
        st.caption("Zeigt Summen der aktuellen Woche und erlaubt Signatur (PIN).")
        pw = st.text_input("PIN eingeben", type="password")
        if st.button("Woche anzeigen"):
            # Filtern auf aktuelle Woche des gewählten Datums
            kw = date_val.isocalendar()[1]
            if "Kalenderwoche" in df_curr.columns:
                df_week = df_curr[df_curr["Kalenderwoche"].astype(str) == str(kw)]
                st.write(f"**KW {kw}** - Summe Stunden: {df_week['Stunden_num'].sum() if not df_week.empty else 0}")
                st.write(f"Summe Reise (h): {df_week['Reisezeit_h'].sum() if not df_week.empty else 0}")
                st.dataframe(df_week)
            else:
                st.warning("Keine KW-Spalte gefunden.")

# ==========================================
# 2) ADMIN BEREICH
# ==========================================
def show_admin_mode(project_list):
    st.title("🔐 Admin / Büro")
    
    pwd = st.text_input("Admin-Passwort", type="password")
    if pwd != "admin123": # Beispielpasswort
        st.warning("Bitte Passwort eingeben.")
        return

    tab1, tab2, tab3 = st.tabs(["Übersicht", "📤 Exporte & Editor", "Einstellungen"])
    
    # --- TAB 1: ÜBERSICHT ---
    with tab1:
        st.write("Wähle ein Projekt, um Status zu sehen.")
        sel = st.selectbox("Projekt", project_list, key="adm_proj")
        df_r, _ = get_reports_df(sel)
        st.metric("Anzahl Rapporte", len(df_r))
        if not df_r.empty and "Stunden_num" in df_r.columns:
            st.metric("Gesamtstunden", df_r["Stunden_num"].sum())

    # --- TAB 2: EXPORTE & EDITOR ---
    with tab2:
        st.subheader("Rapporte bearbeiten & exportieren")
        
        sel_exp = st.selectbox("Projekt für Export wählen", project_list, key="exp_proj")
        
        # 1. DATEN LADEN
        df_r, file_id = get_reports_df(sel_exp)
        
        if df_r.empty:
            st.info("Keine Daten vorhanden.")
        else:
            # 2. EDITOR (Änderungen speichern)
            st.markdown("### ✏️ Rapporte bearbeiten (vor Export)")
            st.caption("Du kannst hier Daten korrigieren. Klicke danach auf 'Änderungen speichern', um die Cloud-Datei zu aktualisieren.")
            
            # Wichtig: Datum in DateTime konvertieren für Editor, falls möglich
            if "Datum" in df_r.columns:
                df_r["Datum"] = pd.to_datetime(df_r["Datum"], errors='coerce')

            edited_df = st.data_editor(
                df_r, 
                num_rows="dynamic", 
                use_container_width=True,
                key="data_editor_admin"
            )
            
            # SPEICHERN BUTTON
            if st.button("💾 Änderungen in Cloud speichern", type="primary"):
                # Konvertiere Datum zurück zu String YYYY-MM-DD für CSV
                save_df = edited_df.copy()
                if "Datum" in save_df.columns:
                    save_df["Datum"] = save_df["Datum"].dt.strftime("%Y-%m-%d")
                
                csv_str = save_df.to_csv(index=False)
                
                if file_id:
                    success = update_file_in_drive(file_id, csv_str)
                    if success:
                        st.success(f"Datei '{sel_exp}_Reports.csv' erfolgreich aktualisiert!")
                        sys_time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Fehler beim Speichern in Drive.")
                else:
                    st.error("Keine File-ID gefunden (Datei existiert nicht?).")

            st.divider()
            
            # 3. EXPORT (CSV / AZK)
            # Wir nutzen hier 'edited_df' (also den Stand aus dem Editor), 
            # damit Änderungen sofort im Download sind, auch ohne Speichern (optional).
            # Besser: Wir nehmen edited_df.
            
            st.markdown("### 📥 Download (Daten aus Editor)")
            
            # a) Komplett-Export
            csv_all = edited_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                f"⬇️ {sel_exp}_Full.csv",
                csv_all,
                f"{sel_exp}_Full.csv",
                "text/csv"
            )
            
            # b) AZK-Export (Formatierte Spalten)
            # Filterung / Umbennung für Lohnbüro
            st.markdown("#### AZK-Format")
            # Beispielhafte Transformation
            try:
                # Kopie für AZK
                df_azk = edited_df.copy()
                # Sicherstellen dass Spalten existieren
                req_cols = ["Datum", "Mitarbeiter", "EmployeeID", "Stunden_num", "Reisezeit_h", "Material", "Bemerkung"]
                for c in req_cols:
                    if c not in df_azk.columns:
                        df_azk[c] = ""
                
                df_azk_out = df_azk[req_cols].rename(columns={
                    "Stunden_num": "Stunden",
                    "Reisezeit_h": "Reisezeit",
                    "Stunden_num": "Arbeitszeit"
                })
                
                csv_azk = df_azk_out.to_csv(index=False).encode("utf-8")
                st.download_button(
                    f"⬇️ AZK_Export_{sel_exp}.csv",
                    csv_azk,
                    f"AZK_{sel_exp}.csv",
                    "text/csv"
                )
            except Exception as e:
                st.error(f"Fehler beim AZK-Export: {e}")

    # --- TAB 3: EINSTELLUNGEN ---
    with tab3:
        st.write("System-Info & Debug")
        st.write(f"Photos Folder ID: `{PHOTOS_FOLDER_ID}`")
        st.write(f"Reports Folder ID: `{REPORTS_FOLDER_ID}`")
        st.markdown(f"[📂 Öffne Reports in Drive](https://drive.google.com/drive/folders/{REPORTS_FOLDER_ID})")


if __name__ == "__main__":
    main()
