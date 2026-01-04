import io
import os
import time as sys_time
from datetime import datetime, time, timedelta

import pandas as pd
import streamlit as st

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# =========================
# PAGE
# =========================
st.set_page_config(page_title="BauApp - R. Baumgartner", layout="wide")

# Sidebar logo (robust path)
logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
if os.path.exists(logo_path):
    st.sidebar.image(logo_path, use_container_width=True)

# =========================
# CONSTANTS
# =========================
SCOPES = ["https://www.googleapis.com/auth/drive"]

PROJECTS_CSV_NAME = "Projects.csv"
REPORTS_SUBFOLDER_NAME = "Rapporte"

PROJECTS_COLS = ["Projekt", "Status"]  # Status: aktiv | archiviert

RAPPORT_COLUMNS = [
    "Datum",
    "Projekt",
    "Mitarbeiter",
    "Start",
    "Ende",
    "Pause_h",
    "Stunden",
    "Material",
    "Bemerkung",
]

# =========================
# SECRETS (STRICT)
# =========================
def require_secret(key: str) -> str:
    if key not in st.secrets or str(st.secrets[key]).strip() == "":
        st.error(f"Fehlendes Geheimnis: {key}")
        st.stop()
    return str(st.secrets[key]).strip()

def require_section(section: str) -> dict:
    if section not in st.secrets:
        st.error(f"Fehlende Sektion in secrets: [{section}]")
        st.stop()
    data = dict(st.secrets[section])
    if not data:
        st.error(f"Sektion [{section}] ist leer.")
        st.stop()
    return data

PHOTOS_FOLDER_ID  = require_secret("PHOTOS_FOLDER_ID")
UPLOADS_FOLDER_ID = require_secret("UPLOADS_FOLDER_ID")
REPORTS_FOLDER_ID = require_secret("REPORTS_FOLDER_ID")
ADMIN_PASSWORD    = require_secret("ADMIN_PASSWORD")

# =========================
# AUTH (OAuth Refresh Token)
# =========================
def authenticate_drive():
    oauth = require_section("google_auth")
    for k in ["client_id", "client_secret", "refresh_token"]:
        if k not in oauth or str(oauth[k]).strip() == "":
            st.error(f"Fehlender Key in [google_auth]: {k}")
            st.stop()

    try:
        creds = Credentials(
            token=None,
            refresh_token=oauth["refresh_token"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=oauth["client_id"],
            client_secret=oauth["client_secret"],
            scopes=SCOPES,
        )
        creds.refresh(Request())
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        st.error(f"OAuth/Drive Fehler: {e}")
        st.stop()

drive = authenticate_drive()

# =========================
# DRIVE HELPERS
# =========================
def drive_list(query: str, fields: str, page_size: int = 200):
    return drive.files().list(q=query, fields=fields, pageSize=page_size).execute().get("files", [])

def find_file_in_folder_by_name(folder_id: str, name: str):
    try:
        q = f"name = '{name}' and '{folder_id}' in parents and trashed=false"
        files = drive_list(q, "files(id,name,modifiedTime)", page_size=20)
        return files[0]["id"] if files else None
    except Exception as e:
        st.error(f"Drive-Suche Fehler: {e}")
        return None

def list_files(folder_id: str):
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        return drive_list(q, "files(id,name,mimeType,createdTime)", page_size=200)
    except Exception as e:
        st.error(f"Drive-Liste Fehler: {e}")
        return []

def download_bytes(file_id: str):
    try:
        req = drive.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        dl = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        return fh.getvalue()
    except Exception as e:
        st.error(f"Download Fehler: {e}")
        return None

def upload_bytes_to_folder(folder_id: str, filename: str, content_bytes: bytes, mimetype: str):
    try:
        meta = {"name": filename, "parents": [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(content_bytes), mimetype=mimetype, resumable=False)
        drive.files().create(body=meta, media_body=media, fields="id").execute()
        return True
    except Exception as e:
        st.error(f"Upload Fehler: {e}")
        return False

def update_file_bytes(file_id: str, content_bytes: bytes, mimetype: str):
    try:
        media = MediaIoBaseUpload(io.BytesIO(content_bytes), mimetype=mimetype, resumable=False)
        drive.files().update(fileId=file_id, media_body=media).execute()
        return True
    except Exception as e:
        st.error(f"Update Fehler: {e}")
        return False

def upload_streamlit_file(uploaded_file, folder_id: str, filename: str):
    try:
        meta = {"name": filename, "parents": [folder_id]}
        media = MediaIoBaseUpload(
            io.BytesIO(uploaded_file.getvalue()),
            mimetype=uploaded_file.type or "application/octet-stream",
            resumable=False
        )
        drive.files().create(body=meta, media_body=media, fields="id").execute()
        return True
    except Exception as e:
        st.error(f"Upload Fehler: {e}")
        return False

def delete_file(file_id: str):
    try:
        drive.files().delete(fileId=file_id).execute()
        return True
    except Exception as e:
        st.error(f"Löschen Fehler: {e}")
        return False

def ensure_subfolder(parent_id: str, folder_name: str) -> str:
    try:
        q = (
            f"mimeType='application/vnd.google-apps.folder' and "
            f"name='{folder_name}' and '{parent_id}' in parents and trashed=false"
        )
        files = drive_list(q, "files(id,name)", page_size=5)
        if files:
            return files[0]["id"]

        meta = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        created = drive.files().create(body=meta, fields="id").execute()
        return created["id"]
    except Exception as e:
        st.error(f"Unterordner Fehler: {e}")
        st.stop()

REPORTS_HOME_FOLDER_ID = ensure_subfolder(REPORTS_FOLDER_ID, REPORTS_SUBFOLDER_NAME)

# =========================
# PROJECTS (Drive + Archive)
# =========================
def save_projects_df(df: pd.DataFrame):
    fid = find_file_in_folder_by_name(REPORTS_FOLDER_ID, PROJECTS_CSV_NAME)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    if fid:
        update_file_bytes(fid, csv_bytes, "text/csv")
    else:
        upload_bytes_to_folder(REPORTS_FOLDER_ID, PROJECTS_CSV_NAME, csv_bytes, "text/csv")

def load_projects_df() -> pd.DataFrame:
    fid = find_file_in_folder_by_name(REPORTS_FOLDER_ID, PROJECTS_CSV_NAME)

    if not fid:
        defaults = ["Neubau Müller", "Sanierung West", "Dachstock Meier"]
        df = pd.DataFrame({"Projekt": defaults, "Status": ["aktiv"] * len(defaults)})
        save_projects_df(df)  # create immediately
        return df

    content = download_bytes(fid)
    if not content:
        return pd.DataFrame(columns=PROJECTS_COLS)

    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception:
        return pd.DataFrame(columns=PROJECTS_COLS)

    if "Projekt" not in df.columns:
        df["Projekt"] = ""

    if "Status" not in df.columns:
        df["Status"] = "aktiv"
        save_projects_df(df)

    df["Projekt"] = df["Projekt"].astype(str).str.strip()
    df["Status"] = df["Status"].astype(str).str.strip().str.lower()

    df = df[df["Projekt"] != ""].copy()
    df.loc[~df["Status"].isin(["aktiv", "archiviert"]), "Status"] = "aktiv"

    return df[PROJECTS_COLS].copy()

def load_projects(include_archived: bool = False) -> list[str]:
    df = load_projects_df()
    if include_archived:
        return df["Projekt"].tolist()
    return df[df["Status"] != "archiviert"]["Projekt"].tolist()

def add_or_restore_project(project_name: str):
    p = (project_name or "").strip()
    if not p:
        return False

    df = load_projects_df()
    mask = df["Projekt"].astype(str).str.strip() == p
    if mask.any():
        df.loc[mask, "Status"] = "aktiv"
    else:
        df = pd.concat([df, pd.DataFrame([{"Projekt": p, "Status": "aktiv"}])], ignore_index=True)

    save_projects_df(df)
    return True

def set_project_status(project_name: str, status: str):
    p = (project_name or "").strip()
    if not p:
        return False

    df = load_projects_df()
    mask = df["Projekt"].astype(str).str.strip() == p
    if not mask.any():
        return False

    df.loc[mask, "Status"] = status
    save_projects_df(df)
    return True

# =========================
# RAPPORTS (clean schema)
# =========================
def report_csv_name(project: str) -> str:
    return f"{project}_Reports.csv"

def get_report_file_id(project: str):
    return find_file_in_folder_by_name(REPORTS_HOME_FOLDER_ID, report_csv_name(project))

def normalize_reports_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=RAPPORT_COLUMNS)

    # remove known legacy cols
    for c in list(df.columns):
        lc = str(c).strip().lower()
        if lc in ["timestamp", "erfasst_am", "erfasst am", "pause", "pause2", "pause_2"]:
            df = df.drop(columns=[c], errors="ignore")

    rename_map = {}
    if "Pause (h)" in df.columns and "Pause_h" not in df.columns:
        rename_map["Pause (h)"] = "Pause_h"
    if rename_map:
        df = df.rename(columns=rename_map)

    for col in RAPPORT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[RAPPORT_COLUMNS]
    df["Datum"] = df["Datum"].astype(str)
    return df

def load_reports(project: str) -> pd.DataFrame:
    fid = get_report_file_id(project)
    if not fid:
        return pd.DataFrame(columns=RAPPORT_COLUMNS)

    content = download_bytes(fid)
    if not content:
        return pd.DataFrame(columns=RAPPORT_COLUMNS)

    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception:
        df = pd.DataFrame()

    df_clean = normalize_reports_df(df)

    # migration write-back
    if list(df_clean.columns) != list(df.columns):
        update_file_bytes(fid, df_clean.to_csv(index=False).encode("utf-8"), "text/csv")

    return df_clean

def append_report(project: str, row: dict):
    fid = get_report_file_id(project)
    df_new = normalize_reports_df(pd.DataFrame([row]))

    if fid:
        old = load_reports(project)
        df = pd.concat([old, df_new], ignore_index=True)
        update_file_bytes(fid, df.to_csv(index=False).encode("utf-8"), "text/csv")
    else:
        upload_bytes_to_folder(
            REPORTS_HOME_FOLDER_ID,
            report_csv_name(project),
            df_new.to_csv(index=False).encode("utf-8"),
            "text/csv"
        )

# =========================
# UI
# =========================
st.sidebar.header("BauApp")
role = st.sidebar.radio("Bereich:", ["Mitarbeiter", "Admin"])

projects_active = load_projects(include_archived=False)

# ===== Mitarbeiter =====
if role == "Mitarbeiter":
    st.title("👷 Mitarbeiter")

    if not projects_active:
        st.warning("Keine aktiven Projekte vorhanden. (Admin: Projekte anlegen oder aus Archiv wiederherstellen)")
        st.stop()

    project = st.selectbox("Projekt:", projects_active)

    tab1, tab2, tab3 = st.tabs(["📝 Rapport", "📷 Fotos", "📂 Pläne"])

    with tab1:
        with st.form("rapport_form"):
            d = st.date_input("Datum", datetime.now())
            name = st.text_input("Name")

            c1, c2 = st.columns(2)
            t_start = c1.time_input("Start", value=time(7, 0))
            t_end = c2.time_input("Ende", value=time(16, 30))

            pause_h = st.number_input("Pause (h)", 0.0, 2.0, 0.5, 0.25)
            material = st.text_area("Material")
            bemerkung = st.text_area("Bemerkung")

            submitted = st.form_submit_button("Senden")

        if submitted:
            dt_start = datetime.combine(d, t_start)
            dt_end = datetime.combine(d, t_end)
            if dt_end < dt_start:
                dt_end += timedelta(days=1)

            hours = ((dt_end - dt_start).total_seconds() / 3600) - float(pause_h)

            append_report(project, {
                "Datum": d.strftime("%Y-%m-%d"),
                "Projekt": project,
                "Mitarbeiter": name,
                "Start": t_start.strftime("%H:%M"),
                "Ende": t_end.strftime("%H:%M"),
                "Pause_h": float(pause_h),
                "Stunden": round(hours, 2),
                "Material": material,
                "Bemerkung": bemerkung,
            })

            st.success("Rapport gespeichert.")
            sys_time.sleep(0.2)
            st.rerun()

    with tab2:
        st.caption("📱 Foto-Upload läuft über den BauApp-Upload-Service (Cloud Run). "
                   "Auf Android/iPhone kannst du damit Kamera oder Galerie auswählen. "
                   "Nach dem Upload bitte unten auf „🔄 Liste aktualisieren“ klicken.")

        # --- Upload-Service Konfiguration aus Streamlit Secrets ---
        if "upload_service" not in st.secrets or "url" not in st.secrets["upload_service"] or "token" not in st.secrets["upload_service"]:
            st.error("Upload-Service ist nicht konfiguriert. Bitte in Streamlit Secrets [upload_service] url + token setzen.")
            st.stop()

        upload_url = str(st.secrets["upload_service"]["url"]).strip().rstrip("/")
        upload_token = str(st.secrets["upload_service"]["token"]).strip()

        # --- Stabiler Upload via HTML + fetch (kein st.file_uploader / keine WebSockets) ---
        # Hinweis: Absichtlich KEIN Streamlit-Button für Upload, damit kein rerun den Upload abbricht.
        html = f"""
        <div style="border:1px solid rgba(255,255,255,0.12); border-radius:12px; padding:14px; max-width:520px;">
          <div style="font-family: sans-serif; font-size: 14px; margin-bottom: 8px;">
            <b>Foto auswählen oder aufnehmen</b>
          </div>

          <input
            type="file"
            id="bauappFile"
            accept="image/*"
            style="margin-bottom: 10px; width: 100%;"
          />

          <button
            type="button"
            onclick="bauappUpload()"
            style="background:#ff4b4b; color:white; border:none; padding:10px 14px; border-radius:8px; cursor:pointer; font-size:14px;"
          >
            📤 Hochladen
          </button>

          <div id="bauappStatus" style="margin-top:10px; font-family:sans-serif; font-size:14px;"></div>
          <div style="margin-top:8px; font-family:sans-serif; font-size:12px; opacity:0.75;">
            Tipp: Wenn du nach dem Upload das neue Bild nicht sofort siehst, klicke unten auf „Liste aktualisieren“.
          </div>
        </div>

        <script>
          async function bauappUpload() {{
            const status = document.getElementById("bauappStatus");
            const input = document.getElementById("bauappFile");

            if (!input || !input.files || input.files.length === 0) {{
              status.innerText = "❌ Bitte zuerst ein Foto auswählen.";
              return;
            }}

            const file = input.files[0];

            // Build form data
            const fd = new FormData();
            fd.append("project", "{project}");
            fd.append("file", file, file.name);

            status.innerText = "⏳ Upload läuft...";

            try {{
              const res = await fetch("{upload_url}/upload", {{
                method: "POST",
                headers: {{
                  "x-upload-token": "{upload_token}"
                }},
                body: fd
              }});

              if (res.ok) {{
                status.innerText = "✅ Upload erfolgreich. Bitte unten „Liste aktualisieren“ klicken.";
              }} else {{
                let msg = "";
                try {{
                  const j = await res.json();
                  msg = (j && (j.detail || j.message)) ? (j.detail || j.message) : "";
                }} catch (e) {{}}
                status.innerText = "❌ Upload fehlgeschlagen (" + res.status + "). " + msg;
              }}
            }} catch (err) {{
              status.innerText = "❌ Netzwerkfehler beim Upload. Bitte Verbindung prüfen und erneut versuchen.";
            }}
          }}
        </script>
        """
        st.components.v1.html(html, height=260)

        colA, colB = st.columns([0.35, 0.65])
        with colA:
            if st.button("🔄 Liste aktualisieren", key="refresh_photos"):
                st.rerun()
        with colB:
            st.caption("Hinweis: Der Upload startet über den roten HTML-Button oben. "
                       "Der Refresh-Button lädt nur die Foto-Liste neu.")

        st.divider()

        files = list_files(PHOTOS_FOLDER_ID)
        shown = False
        for f in files:
            if f["name"].startswith(project + "_"):
                data = download_bytes(f["id"])
                if data:
                    try:
                        st.image(data, width=320)
                    except Exception:
                        st.download_button(
                            f"⬇️ {f['name']} (Anzeige nicht möglich)",
                            data=data,
                            file_name=f["name"],
                            key=f"dl_{f['id']}"
                        )
                    shown = True
        if not shown:
            st.info("Keine Fotos für dieses Projekt vorhanden.")


    with tab3:
        files = list_files(UPLOADS_FOLDER_ID)
        found = False
        for f in files:
            if f["name"].startswith(project + "_"):
                found = True
                st.download_button(
                    f"⬇️ {f['name']}",
                    data=download_bytes(f["id"]),
                    file_name=f["name"],
                    key=f["id"]
                )
        if not found:
            st.info("Keine Pläne/Dokumente vorhanden.")

# ===== Admin =====
else:
    pw = st.sidebar.text_input("Passwort", type="password")
    if pw != ADMIN_PASSWORD:
        st.info("Bitte Admin-Passwort eingeben.")
        st.stop()

    st.title("🛠️ Admin")

    dfp = load_projects_df()
    active_projects = dfp[dfp["Status"] != "archiviert"]["Projekt"].tolist()
    archived_projects = dfp[dfp["Status"] == "archiviert"]["Projekt"].tolist()

    st.subheader("Projektverwaltung")

    new_proj = st.text_input("Projekt anlegen oder archiviertes reaktivieren")
    if st.button("➕ Projekt speichern"):
        ok = add_or_restore_project(new_proj)
        if ok:
            st.success("Projekt gespeichert.")
            st.rerun()
        else:
            st.warning("Bitte Projektname eingeben.")

    if active_projects:
        proj = st.selectbox("Aktives Projekt", active_projects)
        c1, c2 = st.columns([0.7, 0.3])
        confirm = c1.checkbox("Archivieren (Drive-Daten bleiben)")
        if c2.button("🗃️ Archivieren") and confirm:
            set_project_status(proj, "archiviert")
            st.success("Projekt archiviert.")
            st.rerun()
    else:
        proj = None
        st.info("Keine aktiven Projekte vorhanden.")

    st.divider()
    st.subheader("Archivierte Projekte")
    if archived_projects:
        arch = st.selectbox("Archiv", archived_projects)
        if st.button("♻️ Wiederherstellen"):
            set_project_status(arch, "aktiv")
            st.success("Projekt wiederhergestellt.")
            st.rerun()
    else:
        st.info("Keine archivierten Projekte vorhanden.")

    t_reports, t_plans, t_photos = st.tabs(["📄 Rapporte", "📂 Pläne", "📷 Fotos"])

    with t_reports:
        if not proj:
            st.info("Kein aktives Projekt gewählt.")
        else:
            df = load_reports(proj)
            if df.empty:
                st.info("Keine Rapporte vorhanden.")
            else:
                st.dataframe(df, use_container_width=True)
                st.download_button(
                    "⬇️ Rapporte als CSV herunterladen",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name=report_csv_name(proj),
                    mime="text/csv"
                )

    with t_plans:
        if not proj:
            st.info("Kein aktives Projekt gewählt.")
        else:
            st.caption("Upload über Formular (stabil).")
            with st.form("plan_upload_form", clear_on_submit=True):
                up_p = st.file_uploader("Plan/Dokument auswählen", key="plan_upload_file")
                submit = st.form_submit_button("⬆️ Dokument hochladen")

            if submit:
                if up_p is None:
                    st.warning("Bitte zuerst eine Datei auswählen.")
                else:
                    fname = f"{proj}_{up_p.name}"
                    ok = upload_streamlit_file(up_p, UPLOADS_FOLDER_ID, fname)
                    if ok:
                        st.success("Dokument hochgeladen.")
                        sys_time.sleep(0.2)
                        st.rerun()

            files = list_files(UPLOADS_FOLDER_ID)
            any_ = False
            for f in files:
                if f["name"].startswith(proj + "_"):
                    any_ = True
                    c1, c2 = st.columns([0.8, 0.2])
                    c1.write(f["name"])
                    if c2.button("🗑 Löschen", key=f"del_plan_{f['id']}"):
                        delete_file(f["id"])
                        st.rerun()

            if not any_:
                st.info("Keine Pläne/Dokumente vorhanden.")

    with t_photos:
        if not proj:
            st.info("Kein aktives Projekt gewählt.")
        else:
            files = list_files(PHOTOS_FOLDER_ID)
            any_ = False
            for f in files:
                if f["name"].startswith(proj + "_"):
                    any_ = True
                    data = download_bytes(f["id"])
                    if data:
                        st.image(data, width=220)
                    if st.button("🗑 Foto löschen", key=f"del_photo_{f['id']}"):
                        delete_file(f["id"])
                        st.rerun()

            if not any_:
                st.info("Keine Fotos vorhanden.")
