import io
import os
import re
from datetime import datetime

import pandas as pd
import streamlit as st
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# -------------------------
# Page
# -------------------------
st.set_page_config(page_title="BauApp", layout="wide")

# -------------------------
# Helpers: secrets
# -------------------------
def sget(key: str, default: str = "") -> str:
    try:
        v = st.secrets.get(key, default)
        return str(v).strip()
    except Exception:
        return default

def require_secret(key: str) -> str:
    v = sget(key, "")
    if not v:
        st.error(f"Fehlendes Secret: {key}")
        st.stop()
    return v

# -------------------------
# Secrets: Drive OAuth
# -------------------------
GOOGLE_CLIENT_ID = require_secret("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = require_secret("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = require_secret("GOOGLE_REFRESH_TOKEN")

PHOTOS_FOLDER_ID = require_secret("PHOTOS_FOLDER_ID")
UPLOADS_FOLDER_ID = require_secret("UPLOADS_FOLDER_ID")
REPORTS_FOLDER_ID = require_secret("REPORTS_FOLDER_ID")

# Upload service secrets (Streamlit Cloud → Settings → Secrets)
UPLOAD_SERVICE_URL = require_secret("upload_service")["url"] if isinstance(st.secrets.get("upload_service"), dict) else ""
UPLOAD_SERVICE_TOKEN = require_secret("upload_service")["token"] if isinstance(st.secrets.get("upload_service"), dict) else ""
if not UPLOAD_SERVICE_URL or not UPLOAD_SERVICE_TOKEN:
    st.error("Fehlende [upload_service] Secrets: url / token")
    st.stop()

# Optional Admin PIN
ADMIN_PIN = sget("ADMIN_PIN", "")

# -------------------------
# Google Drive client
# -------------------------
def drive_client():
    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)

drive = drive_client()

def drive_list(folder_id: str):
    q = f"'{folder_id}' in parents and trashed=false"
    res = drive.files().list(
        q=q,
        pageSize=200,
        fields="files(id,name,modifiedTime,webViewLink,mimeType,size)",
        orderBy="modifiedTime desc",
    ).execute()
    return res.get("files", [])

def drive_download_bytes(file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()

def sanitize_project(name: str) -> str:
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    return name

# -------------------------
# Cloud Run Upload Widget (NO f-string braces!)
# -------------------------
def cloudrun_upload_widget(*, project: str, bucket: str, title: str, help_text: str):
    """
    bucket: "photos" or "uploads"
    project: project name used as prefix server-side
    """
    # We inject values via simple placeholder replace (no f-string!)
    html = r"""
<div style="border:1px solid rgba(255,255,255,0.12); padding:16px; border-radius:12px; max-width:520px;">
  <div style="font-size:18px; font-weight:700; margin-bottom:8px;">__TITLE__</div>
  <div style="opacity:0.8; font-size:13px; margin-bottom:12px;">__HELP__</div>

  <input id="file" type="file" accept="image/*,application/pdf" capture="environment" style="margin-bottom:12px;" />
  <div style="display:flex; gap:10px; align-items:center;">
    <button id="btn" style="background:#ff4b4b; color:white; border:none; padding:10px 14px; border-radius:10px; font-weight:700; cursor:pointer;">
      📤 Hochladen
    </button>
    <div id="status" style="font-size:13px; opacity:0.9;"></div>
  </div>

  <div style="height:10px;"></div>
  <div style="background:rgba(255,255,255,0.08); border-radius:999px; overflow:hidden; height:10px;">
    <div id="bar" style="width:0%; height:10px; background:#ff4b4b;"></div>
  </div>
</div>

<script>
(function() {
  const uploadUrl = "__UPLOAD_URL__";
  const token = "__TOKEN__";
  const bucket = "__BUCKET__";
  const project = "__PROJECT__";

  const btn = document.getElementById("btn");
  const status = document.getElementById("status");
  const bar = document.getElementById("bar");
  const fileInput = document.getElementById("file");

  function setStatus(msg) {
    status.textContent = msg;
  }
  function setProgress(pct) {
    bar.style.width = String(pct) + "%";
  }

  btn.addEventListener("click", async function() {
    try {
      const f = fileInput.files && fileInput.files[0];
      if (!f) {
        setStatus("Bitte zuerst eine Datei wählen.");
        setProgress(0);
        return;
      }

      setStatus("Upload startet …");
      setProgress(5);

      const form = new FormData();
      form.append("file", f, f.name);
      form.append("bucket", bucket);
      form.append("project", project);

      const resp = await fetch(uploadUrl.replace(/\/$/, "") + "/upload", {
        method: "POST",
        headers: { "X-Upload-Token": token },
        body: form
      });

      const txt = await resp.text();
      let payload = null;
      try { payload = JSON.parse(txt); } catch(e) {}

      if (!resp.ok) {
        const detail = payload && (payload.detail || payload.error || payload.message) ? (payload.detail || payload.error || payload.message) : txt;
        setStatus("❌ Upload fehlgeschlagen (" + resp.status + "): " + detail);
        setProgress(0);
        return;
      }

      setStatus("✅ Upload erfolgreich. Bitte unten 'Liste aktualisieren' klicken.");
      setProgress(100);

    } catch (err) {
      setStatus("❌ Fehler: " + (err && err.message ? err.message : String(err)));
      setProgress(0);
    }
  });
})();
</script>
"""
    html = (html
        .replace("__TITLE__", title)
        .replace("__HELP__", help_text)
        .replace("__UPLOAD_URL__", UPLOAD_SERVICE_URL)
        .replace("__TOKEN__", UPLOAD_SERVICE_TOKEN)
        .replace("__BUCKET__", bucket)
        .replace("__PROJECT__", project)
    )
    st.components.v1.html(html, height=260, scrolling=False)

# -------------------------
# Projects / simple state
# -------------------------
# Minimal: projects list comes from a Drive file "Projects.csv" inside REPORTS_FOLDER_ID or Uploads folder
# If you already have a stable project system, we can plug it in. For now: fallback list.
DEFAULT_PROJECTS = ["Baustelle Müller", "Baustelle Beispiel"]

@st.cache_data(ttl=30)
def get_projects():
    # If you have Projects.csv in REPORTS_FOLDER_ID, we read it.
    files = drive_list(REPORTS_FOLDER_ID)
    proj = [f for f in files if f["name"].lower() == "projects.csv"]
    if proj:
        data = drive_download_bytes(proj[0]["id"])
        df = pd.read_csv(io.BytesIO(data))
        # expecting columns: Projekt, Status
        if "Projekt" in df.columns:
            df = df[df.get("Status", "aktiv").fillna("aktiv").str.lower().eq("aktiv")] if "Status" in df.columns else df
            vals = [sanitize_project(x) for x in df["Projekt"].dropna().astype(str).tolist()]
            return vals or DEFAULT_PROJECTS
    return DEFAULT_PROJECTS

def project_prefix(project: str) -> str:
    # same prefix used server-side: project__filename
    return sanitize_project(project)

# -------------------------
# Reports for employee view
# -------------------------
def report_filename_for_project(project: str) -> str:
    # Keep compatible with your earlier format
    safe = project.replace("/", "_")
    return f"{safe}_Reports.csv"

def find_drive_file_by_name(folder_id: str, name: str):
    q = f"name = '{name}' and '{folder_id}' in parents and trashed=false"
    res = drive.files().list(q=q, pageSize=5, fields="files(id,name,modifiedTime)").execute().get("files", [])
    return res[0] if res else None

def load_project_reports(project: str) -> pd.DataFrame:
    file_name = report_filename_for_project(project)
    f = find_drive_file_by_name(REPORTS_FOLDER_ID, file_name)
    if not f:
        return pd.DataFrame()
    raw = drive_download_bytes(f["id"])
    try:
        df = pd.read_csv(io.BytesIO(raw))
        return df
    except Exception:
        return pd.DataFrame()

# -------------------------
# UI
# -------------------------
st.title("👷 BauApp")

role_tabs = st.tabs(["👷 Mitarbeiter", "🛠️ Admin"])

# ===== Mitarbeiter =====
with role_tabs[0]:
    projects = get_projects()
    project = st.selectbox("Projekt", projects, index=0)
    project = sanitize_project(project)

    t1, t2, t3 = st.tabs(["📋 Rapport", "📷 Fotos", "📁 Pläne"])

    # --- Rapport tab (employee view + insight) ---
    with t1:
        st.subheader("Rapporte (Einblick)")
        df = load_project_reports(project)

        if df.empty:
            st.info("Noch keine Rapporte für dieses Projekt gefunden.")
        else:
            # Show last entries
            st.caption("Letzte Einträge (aktuell aus Google Drive gelesen)")
            st.dataframe(df.tail(20), use_container_width=True)

            # Download full CSV
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Gesamten Rapport als CSV herunterladen",
                data=csv_bytes,
                file_name=report_filename_for_project(project),
                mime="text/csv",
            )

        st.divider()
        st.caption("Hinweis: Das ist nur der Einblick. Rapport-Erfassung bleibt wie in deiner bestehenden Logik – wenn du willst, baue ich das Formular wieder komplett ein.")

    # --- Photos tab (Cloud Run upload) ---
    with t2:
        st.subheader("Fotos")
        st.caption("Foto-Upload läuft über Cloud Run (stabil auf Android/iPhone). Nach dem Upload bitte unten auf „Liste aktualisieren“ klicken.")

        cloudrun_upload_widget(
            project=project_prefix(project),
            bucket="photos",
            title="Foto auswählen oder aufnehmen",
            help_text="Auf dem Handy kannst du Kamera oder Galerie wählen. Upload startet über den roten Button.",
        )

        if st.button("🔄 Liste aktualisieren"):
            st.cache_data.clear()

        st.divider()
        # Display photos for this project by prefix
        files = drive_list(PHOTOS_FOLDER_ID)
        prefix = project_prefix(project) + "__"
        proj_files = [f for f in files if f["name"].startswith(prefix)]
        if not proj_files:
            st.info("Noch keine Fotos für dieses Projekt.")
        else:
            st.write(f"Gefundene Fotos: {len(proj_files)}")
            for f in proj_files[:50]:
                st.write("🖼️", f["name"])

    # --- Plans tab (read-only for employees) ---
    with t3:
        st.subheader("Pläne / Dokumente (Anzeige)")
        files = drive_list(UPLOADS_FOLDER_ID)
        prefix = project_prefix(project) + "__"
        proj_files = [f for f in files if f["name"].startswith(prefix)]
        if not proj_files:
            st.info("Keine Pläne/Dokumente für dieses Projekt gefunden.")
        else:
            for f in proj_files[:100]:
                st.write("📄", f["name"])

# ===== Admin =====
with role_tabs[1]:
    if ADMIN_PIN:
        pin = st.text_input("Admin PIN", type="password")
        if pin != ADMIN_PIN:
            st.warning("PIN erforderlich.")
            st.stop()

    st.subheader("Admin – Uploads (Smartphone stabil)")

    projects = get_projects()
    project = st.selectbox("Projekt (für Upload-Zuordnung)", projects, index=0, key="admin_project")
    project = sanitize_project(project)

    st.caption("Admin-Datei-Upload läuft ebenfalls über Cloud Run (damit es am Handy zuverlässig ist).")

    cloudrun_upload_widget(
        project=project_prefix(project),
        bucket="uploads",
        title="Plan/Dokument hochladen (PDF/Bild)",
        help_text="Wähle Datei am Handy/PC und lade hoch. Danach unten Liste aktualisieren.",
    )

    if st.button("🔄 Liste aktualisieren", key="admin_refresh"):
        st.cache_data.clear()

    st.divider()
    st.subheader("Dateien im Upload-Ordner (Projekt)")
    files = drive_list(UPLOADS_FOLDER_ID)
    prefix = project_prefix(project) + "__"
    proj_files = [f for f in files if f["name"].startswith(prefix)]
    if not proj_files:
        st.info("Noch keine Uploads für dieses Projekt.")
    else:
        for f in proj_files[:200]:
            st.write("📄", f["name"])
