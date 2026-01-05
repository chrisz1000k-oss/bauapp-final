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
# 1. KONFIGURATION & INIT
# =========================
st.set_page_config(page_title="BauApp", layout="wide")

# Logo laden (falls vorhanden)
logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
if os.path.exists(logo_path):
    st.sidebar.image(logo_path, use_container_width=True)

# =========================
# 2. SECRETS LADEN (NACH PROTOKOLL)
# =========================
def sget(key: str, default: str = "") -> str:
    """Holt einen Wert sicher als String."""
    try:
        val = st.secrets.get(key, default)
        return str(val).strip()
    except Exception:
        return default

def require_secret(key: str) -> str:
    """Stoppt die App hart, wenn ein Secret fehlt."""
    val = sget(key, "")
    if not val:
        st.error(f"Fehler: Secret '{key}' fehlt in secrets.toml")
        st.stop()
    return val

# A) Google Drive & Ordner (Einzelwerte)
GOOGLE_CLIENT_ID = require_secret("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = require_secret("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = require_secret("GOOGLE_REFRESH_TOKEN")

PHOTOS_FOLDER_ID = require_secret("PHOTOS_FOLDER_ID")
UPLOADS_FOLDER_ID = require_secret("UPLOADS_FOLDER_ID")
REPORTS_FOLDER_ID = require_secret("REPORTS_FOLDER_ID")

ADMIN_PIN = sget("ADMIN_PIN", "1234")

# B) Upload Service (Sektion [upload_service])
upload_section = st.secrets.get("upload_service")

# Prüfung gemäß Protokoll: Muss eine Sektion (Dict) sein
if not upload_section:
    st.error("Konfigurations-Fehler: Die Sektion [upload_service] fehlt in secrets.toml oder steht nicht am Ende der Datei.")
    st.stop()

# Hinweis: Streamlit Secrets können sich wie Dicts verhalten, auch wenn sie Proxy-Objekte sind.
# Wir versuchen direkt auf die Keys zuzugreifen.
try:
    UPLOAD_SERVICE_URL = str(upload_section["url"]).strip().rstrip("/")
    UPLOAD_SERVICE_TOKEN = str(upload_section["token"]).strip()
except KeyError:
    st.error("Konfigurations-Fehler: 'url' oder 'token' fehlen INNERHALB der Sektion [upload_service].")
    st.stop()

if not UPLOAD_SERVICE_URL or not UPLOAD_SERVICE_TOKEN:
    st.error("Konfigurations-Fehler: Werte in [upload_service] sind leer.")
    st.stop()

# =========================
# 3. GOOGLE DRIVE CLIENT
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
        if not creds.valid:
            creds.refresh(Request())
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        st.error(f"Google Drive Verbindungsfehler: {e}")
        st.stop()

drive = get_drive_service()

# =========================
# 4. DRIVE FUNKTIONEN
# =========================
def list_files(folder_id: str):
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        res = drive.files().list(
            q=q, pageSize=200, 
            fields="files(id,name,mimeType,createdTime)",
            orderBy="createdTime desc"
        ).execute()
        return res.get("files", [])
    except Exception:
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
        st.error(f"Speicherfehler: {e}")
        return False

def update_file_in_drive(file_id: str, data: bytes, mimetype: str = "text/csv"):
    try:
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
        drive.files().update(fileId=file_id, media_body=media).execute()
        return True
    except Exception:
        return False

def delete_file(file_id: str):
    try:
        drive.files().delete(fileId=file_id).execute()
        return True
    except Exception:
        return False

# =========================
# 5. DATEN LOGIK (Projekte & Rapporte)
# =========================
PROJECTS_CSV_NAME = "Projects.csv"
PROJECTS_COLS = ["Projekt", "Status"]

def get_projects_df():
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f['name'] == PROJECTS_CSV_NAME), None)
    
    if csv_file:
        data = download_bytes(csv_file['id'])
        if data:
            return pd.read_csv(io.BytesIO(data)), csv_file['id']
    return pd.DataFrame(columns=PROJECTS_COLS), None

def save_projects_df(df, file_id=None):
    csv_data = df.to_csv(index=False).encode("utf-8")
    if file_id: update_file_in_drive(file_id, csv_data)
    else: upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, PROJECTS_CSV_NAME)

def get_active_projects():
    df, _ = get_projects_df()
    if df.empty: return []
    if "Status" in df.columns:
        return df[df["Status"] == "aktiv"]["Projekt"].tolist()
    return df["Projekt"].tolist()

def get_reports_df(project_name):
    filename = f"{project_name}_Reports.csv"
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f['name'] == filename), None)
    if csv_file:
        data = download_bytes(csv_file['id'])
        if data: return pd.read_csv(io.BytesIO(data)), csv_file['id']
    return pd.DataFrame(columns=["Datum", "Projekt", "Mitarbeiter", "Start", "Ende", "Pause_h", "Stunden", "Material", "Bemerkung"]), None

def save_report(project_name, row_dict):
    df, file_id = get_reports_df(project_name)
    df = pd.concat([df, pd.DataFrame([row_dict])], ignore_index=True)
    csv_data = df.to_csv(index=False).encode("utf-8")
    if file_id: update_file_in_drive(file_id, csv_data)
    else: upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, f"{project_name}_Reports.csv")

# =========================
# 6. UPLOAD WIDGET (Cloud Run)
# =========================
def cloudrun_upload_widget(*, project: str, bucket: str, title: str, help_text: str):
    # Stabile HTML/JS Implementierung ohne f-String Konflikte
    html = r"""
    <div style="border:1px solid #ddd; padding:15px; border-radius:10px; background-color:#f9f9f9; margin-bottom:20px;">
      <div style="font-weight:bold; margin-bottom:5px;">__TITLE__</div>
      <div style="font-size:12px; color:#555; margin-bottom:10px;">__HELP__</div>
      <input id="fileInput" type="file" multiple accept="image/*,application/pdf" style="margin-bottom:10px; width:100%;" />
      <button id="uploadBtn" style="background-color:#FF4B4B; color:white; border:none; padding:10px 20px; border-radius:5px; cursor:pointer; font-weight:bold; width:100%;">
        📤 Hochladen
      </button>
      <div id="status" style="margin-top:10px; font-size:14px;"></div>
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
            status.innerText = "❌ Bitte Datei wählen."; return;
        }
        btn.disabled = true;
        btn.style.opacity = "0.6";
        status.innerText = "⏳ Upload läuft...";
        
        let success = 0; let errors = 0;
        for (let i=0; i<input.files.length; i++) {
            const file = input.files[i];
            status.innerText = "⏳ Lade Datei " + (i+1) + " von " + input.files.length + " hoch...";
            const fd = new FormData();
            fd.append("file", file);
            fd.append("project", project);
            fd.append("upload_type", bucket === "uploads" ? "plan" : "photo");
            try {
                const resp = await fetch(url + "/upload", {
                    method: "POST", headers: { "X-Upload-Token": token }, body: fd
                });
                if (resp.ok) success++; else errors++;
            } catch(e) { errors++; }
        }
        btn.disabled = false; btn.style.opacity = "1.0"; input.value = ""; 
        if (errors === 0) status.innerHTML = "<span style='color:green; font-weight:bold'>✅ " + success + " Datei(en) erfolgreich!</span>";
        else status.innerHTML = "<span style='color:red'>⚠️ " + errors + " Fehler aufgetreten.</span>";
      };
    })();
    </script>
    """
    html = html.replace("__TITLE__", title).replace("__HELP__", help_text)\
               .replace("__URL__", UPLOAD_SERVICE_URL).replace("__TOKEN__", UPLOAD_SERVICE_TOKEN)\
               .replace("__BUCKET__", bucket).replace("__PROJECT__", project)
    components.html(html, height=220)

# =========================
# 7. UI LOGIK
# =========================
st.sidebar.title("Menü")
mode = st.sidebar.radio("Bereich", ["👷 Mitarbeiter", "🛠️ Admin"])

if mode == "👷 Mitarbeiter":
    st.title("👷 Mitarbeiterbereich")
    active_projects = get_active_projects()
    if not active_projects:
        st.warning("Keine aktiven Projekte.")
        st.stop()
    
    project = st.selectbox("Projekt wählen", active_projects)
    t1, t2, t3 = st.tabs(["📝 Rapport", "📷 Fotos", "📂 Pläne"])
    
    with t1:
        c1, c2, c3 = st.columns(3)
        date_val = c1.date_input("Datum", datetime.now())
        ma_val = c1.text_input("Name")
        start_val = c2.time_input("Start", time(7,0))
        end_val = c2.time_input("Ende", time(16,0))
        pause_val = c3.number_input("Pause (h)", 0.0, 5.0, 0.5, 0.25)
        mat_val = c3.text_input("Material")
        rem_val = st.text_area("Bemerkung")
        
        # Berechnung
        dt_start = datetime.combine(datetime.today(), start_val)
        dt_end = datetime.combine(datetime.today(), end_val)
        if dt_end < dt_start: dt_end += timedelta(days=1)
        dur = round(max(0.0, (dt_end - dt_start).total_seconds()/3600 - pause_val), 2)
        st.info(f"Stunden: {dur}")
        
        if st.button("Speichern", type="primary"):
            if ma_val:
                save_report(project, {"Datum": str(date_val), "Projekt": project, "Mitarbeiter": ma_val, 
                                      "Start": str(start_val), "Ende": str(end_val), "Pause_h": pause_val, 
                                      "Stunden": dur, "Material": mat_val, "Bemerkung": rem_val})
                st.success("Gespeichert!"); sys_time.sleep(1); st.rerun()
            else: st.error("Name fehlt.")
        
        st.divider()
        st.caption("Verlauf (Letzte Einträge)")
        df_h, _ = get_reports_df(project)
        if not df_h.empty: st.dataframe(df_h.tail(5), use_container_width=True)

    with t2:
        cloudrun_upload_widget(project=project, bucket="photos", title="Foto hochladen", help_text="Kamera/Galerie wählbar.")
        if st.button("🔄 Aktualisieren", key="ref_photos"): st.rerun()
        files = list_files(PHOTOS_FOLDER_ID)
        for f in [x for x in files if x['name'].startswith(project + "_")][:20]:
            st.write(f"🖼️ {f['name']}")

    with t3:
        files = list_files(UPLOADS_FOLDER_ID)
        for f in [x for x in files if x['name'].startswith(project + "_")]:
            c1, c2 = st.columns([0.8, 0.2])
            c1.write(f"📄 {f['name']}")
            d = download_bytes(f['id'])
            if d: c2.download_button("⬇️", d, f['name'])

elif mode == "🛠️ Admin":
    st.title("Admin")
    if st.text_input("PIN", type="password") != ADMIN_PIN: st.stop()
    
    st.subheader("Projekte")
    df_p, pid = get_projects_df()
    new_p = st.text_input("Neues Projekt")
    if st.button("Anlegen") and new_p:
        df_p = pd.concat([df_p, pd.DataFrame([{"Projekt": new_p, "Status": "aktiv"}])], ignore_index=True)
        save_projects_df(df_p, pid); st.rerun()
    if not df_p.empty: st.dataframe(df_p)

    st.divider()
    st.subheader("Pläne Upload")
    projs = get_active_projects()
    if projs:
        sel_p = st.selectbox("Projekt", projs)
        cloudrun_upload_widget(project=sel_p, bucket="uploads", title="Plan hochladen", help_text="Für Dokumente/Pläne.")
        if st.button("🔄 Aktualisieren", key="ref_admin"): st.rerun()
        for f in [x for x in list_files(UPLOADS_FOLDER_ID) if x['name'].startswith(sel_p + "_")]:
            c1, c2 = st.columns([0.8, 0.2])
            c1.write(f"📄 {f['name']}")
            if c2.button("🗑", key=f['id']): delete_file(f['id']); st.rerun()
