import io
import os
import time as sys_time
from datetime import datetime, time, timedelta

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# =========================
# PAGE CONFIG & LOGO
# =========================
st.set_page_config(page_title="BauApp", layout="wide")

logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
if os.path.exists(logo_path):
    st.sidebar.image(logo_path, use_container_width=True)

# =========================
# HELPER: SECRETS (ROBUST)
# =========================
def sget(key: str, default: str = "") -> str:
    """Holt einen Wert aus secrets sicher als String."""
    try:
        val = st.secrets.get(key, default)
        return str(val).strip()
    except Exception:
        return default

def require_secret(key: str) -> str:
    """Stoppt die App, wenn ein Secret fehlt."""
    val = sget(key, "")
    if not val:
        st.error(f"Fehlendes Secret: {key}")
        st.stop()
    return val

# =========================
# SECRETS LOADING
# =========================
# 1. Google Drive Auth (Flache Struktur)
GOOGLE_CLIENT_ID = require_secret("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = require_secret("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = require_secret("GOOGLE_REFRESH_TOKEN")

# 2. Ordner IDs
PHOTOS_FOLDER_ID = require_secret("PHOTOS_FOLDER_ID")
UPLOADS_FOLDER_ID = require_secret("UPLOADS_FOLDER_ID")
REPORTS_FOLDER_ID = require_secret("REPORTS_FOLDER_ID")

# 3. Admin PIN
ADMIN_PIN = sget("ADMIN_PIN", "1234") # Fallback 1234, falls nicht gesetzt

# 4. Upload Service (Spezialbehandlung für Sektion [upload_service])
upload_section = st.secrets.get("upload_service")

# Fallback: Falls Streamlit die Sektion nicht als Dict liefert, versuchen wir es anders
UPLOAD_SERVICE_URL = ""
UPLOAD_SERVICE_TOKEN = ""

if isinstance(upload_section, dict):
    UPLOAD_SERVICE_URL = str(upload_section.get("url", "")).strip().rstrip("/")
    UPLOAD_SERVICE_TOKEN = str(upload_section.get("token", "")).strip()
else:
    # Falls User es doch flach eingetragen hat oder Streamlit komisch parst
    st.error("Fehler: [upload_service] muss in secrets.toml als Sektion definiert sein.")
    st.stop()

if not UPLOAD_SERVICE_URL or not UPLOAD_SERVICE_TOKEN:
    st.error("Fehlende Werte in [upload_service]: url oder token leer.")
    st.stop()

# =========================
# GOOGLE DRIVE CLIENT
# =========================
def get_drive_service():
    try:
        creds = Credentials(
            token=None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        # Refresh erzwingen, falls nötig
        if not creds.valid:
            creds.refresh(Request())
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        st.error(f"Google Drive Auth Fehler: {e}")
        st.stop()

drive = get_drive_service()

# =========================
# DRIVE HELPER FUNCTIONS
# =========================
def list_files(folder_id: str):
    """Listet Dateien in einem Ordner auf."""
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        res = drive.files().list(
            q=q, 
            pageSize=200, 
            fields="files(id,name,mimeType,createdTime)",
            orderBy="createdTime desc"
        ).execute()
        return res.get("files", [])
    except Exception as e:
        st.error(f"Fehler beim Listen von Dateien: {e}")
        return []

def download_bytes(file_id: str):
    try:
        request = drive.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return fh.getvalue()
    except Exception:
        return None

def upload_bytes_to_drive(data: bytes, folder_id: str, filename: str, mimetype: str = "text/csv"):
    try:
        file_metadata = {'name': filename, 'parents': [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
        drive.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return True
    except Exception as e:
        st.error(f"Upload Fehler: {e}")
        return False

def update_file_in_drive(file_id: str, data: bytes, mimetype: str = "text/csv"):
    try:
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
        drive.files().update(fileId=file_id, media_body=media).execute()
        return True
    except Exception as e:
        st.error(f"Update Fehler: {e}")
        return False

def delete_file(file_id: str):
    try:
        drive.files().delete(fileId=file_id).execute()
        return True
    except Exception as e:
        st.error(f"Löschen Fehler: {e}")
        return False

# =========================
# PROJECTS LOGIC
# =========================
PROJECTS_CSV_NAME = "Projects.csv"
PROJECTS_COLS = ["Projekt", "Status"]

def get_projects_df():
    # Suche Projects.csv im Reports Ordner
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f['name'] == PROJECTS_CSV_NAME), None)
    
    if csv_file:
        data = download_bytes(csv_file['id'])
        if data:
            return pd.read_csv(io.BytesIO(data)), csv_file['id']
    
    # Neu erstellen wenn nicht da
    return pd.DataFrame(columns=PROJECTS_COLS), None

def save_projects_df(df, file_id=None):
    csv_data = df.to_csv(index=False).encode("utf-8")
    if file_id:
        update_file_in_drive(file_id, csv_data)
    else:
        upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, PROJECTS_CSV_NAME)

def get_active_projects():
    df, _ = get_projects_df()
    if df.empty: return []
    if "Status" in df.columns:
        return df[df["Status"] == "aktiv"]["Projekt"].tolist()
    return df["Projekt"].tolist()

# =========================
# RAPPORTE LOGIC
# =========================
RAPPORT_COLS = ["Datum", "Projekt", "Mitarbeiter", "Start", "Ende", "Pause_h", "Stunden", "Material", "Bemerkung"]

def get_reports_df(project_name):
    filename = f"{project_name}_Reports.csv"
    files = list_files(REPORTS_FOLDER_ID) # Ggf. REPORTS_SUBFOLDER_NAME nutzen wenn gewünscht, hier einfachheitshalber im Hauptordner oder man sucht rekursiv.
    # Wir nutzen hier den REPORTS_FOLDER_ID direkt, wie in Ihrer alten Logik oft der Fall.
    
    # Optional: Falls Sie Unterordner nutzen wollten:
    # Wir bleiben beim simplen Ansatz: Alles in REPORTS_FOLDER_ID
    
    csv_file = next((f for f in files if f['name'] == filename), None)
    if csv_file:
        data = download_bytes(csv_file['id'])
        if data:
            return pd.read_csv(io.BytesIO(data)), csv_file['id']
    
    return pd.DataFrame(columns=RAPPORT_COLS), None

def save_report(project_name, new_data_dict):
    df, file_id = get_reports_df(project_name)
    new_row = pd.DataFrame([new_data_dict])
    df = pd.concat([df, new_row], ignore_index=True)
    
    csv_data = df.to_csv(index=False).encode("utf-8")
    if file_id:
        update_file_in_drive(file_id, csv_data)
    else:
        upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, f"{project_name}_Reports.csv")

# =========================
# CLOUD RUN UPLOAD WIDGET
# =========================
def cloudrun_upload_widget(*, project: str, bucket: str, title: str, help_text: str):
    """
    bucket: "photos" oder "uploads" (Pläne)
    project: Projektname
    """
    # HTML Template ohne f-Strings im JS Teil, um Python-Fehler zu vermeiden
    html = r"""
    <div style="border:1px solid #ddd; padding:15px; border-radius:10px; background-color:#f9f9f9; margin-bottom:20px;">
      <div style="font-weight:bold; margin-bottom:10px;">__TITLE__</div>
      <div style="font-size:12px; color:#555; margin-bottom:10px;">__HELP__</div>

      <input id="fileInput" type="file" multiple accept="image/*,application/pdf" style="margin-bottom:10px; width:100%;" />
      
      <button id="uploadBtn" style="background-color:#FF4B4B; color:white; border:none; padding:10px 20px; border-radius:5px; cursor:pointer; font-weight:bold;">
        📤 Hochladen
      </button>
      
      <div id="status" style="margin-top:10px; font-size:14px; white-space: pre-wrap;"></div>
    </div>

    <script>
    (function() {
      const url = "__URL__";
      const token = "__TOKEN__";
      const bucket = "__BUCKET__";
      const project = "__PROJECT__";

      const btn = document.getElementById("uploadBtn");
      const input = document.getElementById("fileInput");
      const status = document.getElementById("status");

      btn.onclick = async function() {
        if (!input.files || input.files.length === 0) {
            status.innerText = "❌ Bitte Datei wählen.";
            return;
        }

        btn.disabled = true;
        btn.style.opacity = "0.6";
        status.innerText = "⏳ Upload startet...";
        
        let success = 0;
        let errors = 0;

        for (let i=0; i<input.files.length; i++) {
            const file = input.files[i];
            status.innerText = "⏳ Lade Datei " + (i+1) + " von " + input.files.length + " hoch: " + file.name;
            
            const fd = new FormData();
            fd.append("file", file);
            fd.append("project", project);
            fd.append("upload_type", bucket === "uploads" ? "plan" : "photo"); // Mapping für Backend

            try {
                const resp = await fetch(url + "/upload", {
                    method: "POST",
                    headers: { "X-Upload-Token": token },
                    body: fd
                });
                
                if (resp.ok) success++;
                else {
                    errors++;
                    console.error("Fehler bei " + file.name);
                }
            } catch(e) {
                errors++;
                console.error(e);
            }
        }

        btn.disabled = false;
        btn.style.opacity = "1.0";
        input.value = ""; 

        if (errors === 0) {
            status.innerHTML = "<span style='color:green; font-weight:bold'>✅ Alle " + success + " Dateien erfolgreich! Bitte Liste aktualisieren.</span>";
        } else {
            status.innerHTML = "<span style='color:red'>⚠️ " + success + " erfolgreich, " + errors + " fehlgeschlagen.</span>";
        }
      };
    })();
    </script>
    """
    
    # Python-seitiges Ersetzen der Platzhalter
    html = (html
        .replace("__TITLE__", title)
        .replace("__HELP__", help_text)
        .replace("__URL__", UPLOAD_SERVICE_URL)
        .replace("__TOKEN__", UPLOAD_SERVICE_TOKEN)
        .replace("__BUCKET__", bucket)
        .replace("__PROJECT__", project)
    )
    
    components.html(html, height=250)

# =========================
# MAIN UI
# =========================
st.sidebar.title("Navigation")
mode = st.sidebar.radio("Bereich", ["👷 Mitarbeiter", "🛠️ Admin"])

# -------------------------
# 👷 MITARBEITER BEREICH
# -------------------------
if mode == "👷 Mitarbeiter":
    st.title("👷 Mitarbeiterbereich")
    
    active_projects = get_active_projects()
    if not active_projects:
        st.warning("Keine aktiven Projekte gefunden. Bitte Admin kontaktieren.")
        st.stop()
        
    project = st.selectbox("Projekt wählen", active_projects)
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📝 Rapport", "📷 Fotos", "📂 Pläne"])
    
    # --- TAB 1: RAPPORT (Erfassen + Einblick) ---
    with tab1:
        st.subheader("Rapport erfassen")
        
        c1, c2, c3 = st.columns(3)
        date_val = c1.date_input("Datum", datetime.now())
        ma_val = c1.text_input("Mitarbeiter Name")
        
        start_val = c2.time_input("Start", time(7,0))
        end_val = c2.time_input("Ende", time(16,0))
        
        pause_val = c3.number_input("Pause (Std)", 0.0, 5.0, 0.5, 0.25)
        mat_val = c3.text_input("Material")
        
        rem_val = st.text_area("Bemerkung")
        
        # Stunden Berechnung
        dt_start = datetime.combine(datetime.today(), start_val)
        dt_end = datetime.combine(datetime.today(), end_val)
        if dt_end < dt_start: dt_end += timedelta(days=1)
        duration = (dt_end - dt_start).total_seconds() / 3600 - pause_val
        duration = round(max(0.0, duration), 2)
        
        st.info(f"Berechnete Stunden: {duration}")
        
        if st.button("✅ Speichern", type="primary"):
            if not ma_val:
                st.error("Bitte Name eingeben.")
            else:
                row = {
                    "Datum": str(date_val),
                    "Projekt": project,
                    "Mitarbeiter": ma_val,
                    "Start": str(start_val),
                    "Ende": str(end_val),
                    "Pause_h": pause_val,
                    "Stunden": duration,
                    "Material": mat_val,
                    "Bemerkung": rem_val
                }
                save_report(project, row)
                st.success("Rapport gespeichert!")
                sys_time.sleep(1)
                st.rerun()

        st.divider()
        st.subheader("Verlauf (Letzte Einträge)")
        df_hist, _ = get_reports_df(project)
        if not df_hist.empty:
            st.dataframe(df_hist.tail(10), use_container_width=True)
        else:
            st.caption("Noch keine Einträge.")

    # --- TAB 2: FOTOS (Cloud Run Upload) ---
    with tab2:
        st.subheader("Fotos")
        
        cloudrun_upload_widget(
            project=project,
            bucket="photos",
            title="Neues Foto hochladen",
            help_text="Wähle Kamera oder Galerie. Funktioniert stabil auch bei schlechtem Netz."
        )
        
        if st.button("🔄 Fotos aktualisieren"):
            st.rerun()
            
        # Anzeige
        files = list_files(PHOTOS_FOLDER_ID)
        # Filter: Dateiname beginnt mit "Projektname_"
        proj_files = [f for f in files if f['name'].startswith(project + "_")]
        
        if proj_files:
            for f in proj_files[:20]: # Limit 20 um Ladezeit zu sparen
                st.write(f"🖼️ {f['name']}")
                # Optional: Hier könnte man st.image(download_bytes(f['id'])) nutzen, 
                # aber das kostet viel Traffic/Ladezeit. Namen reichen oft als Bestätigung.
                # Wenn Bild gewünscht:
                # data = download_bytes(f['id'])
                # if data: st.image(data, width=300)
        else:
            st.info("Keine Fotos vorhanden.")

    # --- TAB 3: PLÄNE (Read Only) ---
    with tab3:
        st.subheader("Pläne & Dokumente")
        files = list_files(UPLOADS_FOLDER_ID)
        proj_files = [f for f in files if f['name'].startswith(project + "_")]
        
        if proj_files:
            for f in proj_files:
                c1, c2 = st.columns([0.8, 0.2])
                c1.write(f"📄 {f['name']}")
                data = download_bytes(f['id'])
                if data:
                    c2.download_button("⬇️", data, f['name'])
        else:
            st.info("Keine Dokumente hinterlegt.")

# -------------------------
# 🛠️ ADMIN BEREICH
# -------------------------
elif mode == "🛠️ Admin":
    st.title("Admin Bereich")
    
    pin = st.text_input("Admin PIN eingeben", type="password")
    if pin != ADMIN_PIN:
        st.stop()
        
    st.success("Angemeldet")
    
    # Projektverwaltung
    st.subheader("Projekte verwalten")
    df_proj, proj_file_id = get_projects_df()
    
    c1, c2 = st.columns(2)
    new_proj = c1.text_input("Neues Projekt anlegen")
    if c2.button("➕ Anlegen") and new_proj:
        new_row = pd.DataFrame([{"Projekt": new_proj, "Status": "aktiv"}])
        df_proj = pd.concat([df_proj, new_row], ignore_index=True)
        save_projects_df(df_proj, proj_file_id)
        st.success(f"Projekt {new_proj} angelegt.")
        st.rerun()
        
    # Tabelle anzeigen / Status ändern
    if not df_proj.empty:
        st.dataframe(df_proj, use_container_width=True)
        
        # Archivieren / Reaktivieren Logik könnte hier erweitert werden
        # Wir belassen es simpel bei der Tabelle oder fügen Selectbox hinzu:
        all_projs = df_proj["Projekt"].tolist()
        if all_projs:
            to_archive = st.selectbox("Projekt Status ändern (aktiv/archiviert)", all_projs)
            new_status = st.radio("Neuer Status", ["aktiv", "archiviert"], horizontal=True)
            if st.button("Status speichern"):
                df_proj.loc[df_proj["Projekt"] == to_archive, "Status"] = new_status
                save_projects_df(df_proj, proj_file_id)
                st.success("Status geändert.")
                st.rerun()

    st.divider()
    
    # Pläne Upload für Admin
    st.subheader("Pläne hochladen")
    active_projs = get_active_projects()
    if active_projs:
        upload_proj = st.selectbox("Ziel-Projekt für Plan", active_projs)
        
        cloudrun_upload_widget(
            project=upload_proj,
            bucket="uploads", # Das signalisiert dem Backend: ab in den Plan-Ordner
            title="Plan/PDF hochladen",
            help_text="Dateien werden im Ordner 'Pläne' gespeichert."
        )
        
        if st.button("🔄 Pläne aktualisieren"):
            st.rerun()
            
        # Liste und Löschen
        st.caption("Vorhandene Dateien:")
        files = list_files(UPLOADS_FOLDER_ID)
        proj_files = [f for f in files if f['name'].startswith(upload_proj + "_")]
        for f in proj_files:
            c1, c2 = st.columns([0.8, 0.2])
            c1.write(f"📄 {f['name']}")
            if c2.button("🗑", key=f"del_{f['id']}"):
                delete_file(f['id'])
                st.success("Gelöscht")
                sys_time.sleep(0.5)
                st.rerun()
    else:
        st.info("Erstelle zuerst ein aktives Projekt.")
