import io
import os
import time as sys_time
from datetime import datetime, time, timedelta
from uuid import uuid4

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
# SECRETS (ROBUST)
# =========================
def sget(key: str, default: str = "") -> str:
    try:
        val = st.secrets.get(key, default)
        return str(val).strip()
    except Exception:
        return default

def require_secret(key: str) -> str:
    val = sget(key, "")
    if not val:
        st.error(f"Fehlendes Secret: {key}")
        st.stop()
    return val

GOOGLE_CLIENT_ID = require_secret("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = require_secret("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = require_secret("GOOGLE_REFRESH_TOKEN")

PHOTOS_FOLDER_ID = require_secret("PHOTOS_FOLDER_ID")
UPLOADS_FOLDER_ID = require_secret("UPLOADS_FOLDER_ID")
REPORTS_FOLDER_ID = require_secret("REPORTS_FOLDER_ID")

ADMIN_PIN = sget("ADMIN_PIN", "1234")

# Upload Service (Sektion [upload_service])
upload_section = st.secrets.get("upload_service")
if not upload_section:
    st.error("Fehler: Sektion [upload_service] fehlt in secrets.toml.")
    st.stop()

try:
    UPLOAD_SERVICE_URL = str(upload_section["url"]).strip().rstrip("/")
    UPLOAD_SERVICE_TOKEN = str(upload_section["token"]).strip()
except KeyError:
    st.error("Fehler: In [upload_service] fehlen 'url' oder 'token'.")
    st.stop()

if not UPLOAD_SERVICE_URL or not UPLOAD_SERVICE_TOKEN:
    st.error("Fehler: [upload_service] url/token leer.")
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
        if not creds.valid:
            creds.refresh(Request())
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        st.error(f"Google Drive Auth Fehler: {e}")
        st.stop()

drive = get_drive_service()

# =========================
# DRIVE HELPERS
# =========================
def list_files(folder_id: str, *, mime_prefix: str | None = None):
    """
    Listet Dateien im Ordner.
    mime_prefix="image/" -> nur Bilder
    mime_prefix=None -> alles
    """
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        if mime_prefix:
            q += f" and mimeType contains '{mime_prefix}'"

        res = drive.files().list(
            q=q,
            pageSize=200,
            fields="files(id,name,mimeType,createdTime)",
            orderBy="createdTime desc",
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

def upload_bytes_to_drive(data: bytes, folder_id: str, filename: str, mimetype: str):
    try:
        file_metadata = {"name": filename, "parents": [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
        drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
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
    """Nur im Admin UI verwenden."""
    try:
        drive.files().delete(fileId=file_id).execute()
        return True
    except Exception as e:
        st.error(f"Löschen Fehler: {e}")
        return False

def make_prefixed_filename(project: str, original_name: str) -> str:
    """Damit deine startswith(project + '_')-Filter immer greifen."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{project}_{ts}_{original_name}"

# =========================
# PROJECTS + REPORTS
# =========================
PROJECTS_CSV_NAME = "Projects.csv"
PROJECTS_COLS = ["Projekt", "Status"]
RAPPORT_COLS = ["Datum", "Projekt", "Mitarbeiter", "Start", "Ende", "Pause_h", "Stunden", "Material", "Bemerkung"]

def get_projects_df():
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f["name"] == PROJECTS_CSV_NAME), None)
    if csv_file:
        data = download_bytes(csv_file["id"])
        if data:
            return pd.read_csv(io.BytesIO(data)), csv_file["id"]
    return pd.DataFrame(columns=PROJECTS_COLS), None

def save_projects_df(df, file_id=None):
    csv_data = df.to_csv(index=False).encode("utf-8")
    if file_id:
        return update_file_in_drive(file_id, csv_data, mimetype="text/csv")
    return upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, PROJECTS_CSV_NAME, "text/csv")

def get_active_projects():
    df, _ = get_projects_df()
    if df.empty:
        return []
    if "Status" in df.columns:
        return df[df["Status"] == "aktiv"]["Projekt"].tolist()
    return df["Projekt"].tolist()

def get_reports_df(project_name: str):
    filename = f"{project_name}_Reports.csv"
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f["name"] == filename), None)
    if csv_file:
        data = download_bytes(csv_file["id"])
        if data:
            return pd.read_csv(io.BytesIO(data)), csv_file["id"]
    return pd.DataFrame(columns=RAPPORT_COLS), None

def save_report(project_name: str, row_dict: dict) -> bool:
    df, file_id = get_reports_df(project_name)
    df = pd.concat([df, pd.DataFrame([row_dict])], ignore_index=True)
    csv_data = df.to_csv(index=False).encode("utf-8")
    if file_id:
        return update_file_in_drive(file_id, csv_data, mimetype="text/csv")
    return upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, f"{project_name}_Reports.csv", "text/csv")

# =========================
# CLOUD RUN UPLOAD WIDGET (MOBILE STABLE)
# =========================
def cloudrun_upload_widget(
    *,
    project: str,
    bucket: str,
    title: str,
    help_text: str,
    accept: str,
    multiple: bool = True,
    height: int = 230,
    debug: bool = False,
):
    """
    bucket: "photos" oder "uploads"
    accept: "image/*" für Fotos, "application/pdf,image/*" für Dokumente
    debug: zeigt Serverantworten an (hilft beim Testen)
    """
    uid = str(uuid4()).replace("-", "")
    dbg = "true" if debug else "false"

    html = r"""
    <div style="border:1px solid #ddd; padding:15px; border-radius:10px; background-color:#f9f9f9; margin-bottom:20px;">
      <div style="font-weight:bold; margin-bottom:5px;">__TITLE__</div>
      <div style="font-size:12px; color:#555; margin-bottom:10px;">__HELP__</div>

      <input id="fileInput___UID__" type="file" __MULT__ accept="__ACCEPT__" style="margin-bottom:10px; width:100%;" />

      <button id="uploadBtn___UID__" style="background-color:#FF4B4B; color:white; border:none; padding:10px 20px; border-radius:5px; cursor:pointer; font-weight:bold; width:100%;">
        📤 Hochladen
      </button>

      <div id="status___UID__" style="margin-top:10px; font-size:14px; white-space: pre-wrap;"></div>
    </div>

    <script>
    (function() {
      const url = "__URL__";
      const token = "__TOKEN__";
      const bucket = "__BUCKET__";
      const project = "__PROJECT__";
      const debug = __DEBUG__;

      const btn = document.getElementById("uploadBtn___UID__");
      const input = document.getElementById("fileInput___UID__");
      const status = document.getElementById("status___UID__");

      btn.onclick = async function() {
        if (!input.files || input.files.length === 0) {
          status.innerText = "❌ Bitte Datei wählen.";
          return;
        }

        btn.disabled = true;
        btn.style.opacity = "0.6";
        status.innerText = "⏳ Upload läuft...";

        let success = 0;
        let errors = 0;

        for (let i = 0; i < input.files.length; i++) {
          const file = input.files[i];
          status.innerText = "⏳ Lade Datei " + (i+1) + " von " + input.files.length + " hoch: " + file.name;

          const fd = new FormData();
          fd.append("file", file);
          fd.append("project", project);

          // entscheidend fürs Backend
          const legacyType = (bucket === "uploads") ? "plan" : "photo";
          fd.append("upload_type", legacyType);

          try {
            const resp = await fetch(url + "/upload", {
              method: "POST",
              headers: { "X-Upload-Token": token },
              body: fd
            });

            const text = await resp.text();

            if (resp.ok) {
              success++;
              if (debug) {
                status.innerText = "✅ OK:
