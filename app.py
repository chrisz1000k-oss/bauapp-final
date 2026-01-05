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
# PAGE
# =========================
st.set_page_config(page_title="BauApp - R. Baumgartner", layout="wide")

logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
if os.path.exists(logo_path):
    st.sidebar.image(logo_path, use_container_width=True)

SCOPES = ["https://www.googleapis.com/auth/drive"]

PROJECTS_CSV_NAME = "Projects.csv"
REPORTS_SUBFOLDER_NAME = "Rapporte"

PROJECTS_COLS = ["Projekt", "Status"]  # aktiv | archiviert

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
# SECRETS
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

UPLOAD_SERVICE = require_section("upload_service")
UPLOAD_SERVICE_URL = str(UPLOAD_SERVICE.get("url", "")).strip().rstrip("/")
UPLOAD_SERVICE_TOKEN = str(UPLOAD_SERVICE.get("token", "")).strip()
if not UPLOAD_SERVICE_URL or not UPLOAD_SERVICE_TOKEN:
    st.error("Fehlende upload_service Konfiguration in secrets: [upload_service] url/token")
    st.stop()


# =========================
# GOOGLE DRIVE AUTH
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

def ensure_subfolder(parent_id: str, folder_name: str) -> str:
    try:
        q = (
            f"mimeType='application/vnd.google-apps.folder' and "
            f"name='{folder_name}' and '{parent_id}' in parents and trashed=false"
        )
        files = drive_list(q, "files(id,name)", page_size=5)
        if files:
            return files[0]["id"]

        meta = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
        created = drive.files().create(body=meta, fields="id").execute()
        return created["id"]
    except Exception as e:
        st.error(f"Unterordner Fehler: {e}")
        st.stop()

REPORTS_HOME_FOLDER_ID = ensure_subfolder(REPORTS_FOLDER_ID, REPORTS_SUBFOLDER_NAME)

def find_file_in_folder_by_name(folder_id: str, name: str):
    try:
        q = f"name = '{name}' and '{folder_id}' in parents and trashed=false"
        files = drive_list(q, "files(id,name,modifiedTime)", page_size=10)
        return files[0]["id"] if files else None
    except Exception as e:
        st.error(f"Drive-Suche Fehler: {e}")
        return None

def list_files(folder_id: str):
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        return drive_list(q, "files(id,name,mimeType,createdTime,modifiedTime)", page_size=200)
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
        return b""


def upload_bytes_to_drive(data: bytes, folder_id: str, filename: str, mimetype: str = "application/octet-stream"):
    try:
        meta = {"name": filename, "parents": [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=False)
        drive.files().create(body=meta, media_body=media, fields="id").execute()
        return True
    except Exception as e:
        st.error(f"Upload Fehler: {e}")
        return False


# =========================
# PROJECTS CSV
# =========================
def load_projects_df() -> pd.DataFrame:
    file_id = find_file_in_folder_by_name(REPORTS_FOLDER_ID, PROJECTS_CSV_NAME)
    if not file_id:
        df = pd.DataFrame(columns=PROJECTS_COLS)
        upload_bytes_to_drive(df.to_csv(index=False).encode("utf-8"), REPORTS_FOLDER_ID, PROJECTS_CSV_NAME, "text/csv")
        return df

    raw = download_bytes(file_id)
    if not raw:
        return pd.DataFrame(columns=PROJECTS_COLS)

    df = pd.read_csv(io.BytesIO(raw))
    for c in PROJECTS_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[PROJECTS_COLS].fillna("")
    return df

def save_projects_df(df: pd.DataFrame):
    file_id = find_file_in_folder_by_name(REPORTS_FOLDER_ID, PROJECTS_CSV_NAME)
    data = df.to_csv(index=False).encode("utf-8")
    if file_id:
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype="text/csv", resumable=False)
        drive.files().update(fileId=file_id, media_body=media).execute()
    else:
        upload_bytes_to_drive(data, REPORTS_FOLDER_ID, PROJECTS_CSV_NAME, "text/csv")


def add_or_restore_project(name: str) -> bool:
    name = (name or "").strip()
    if not name:
        return False
    df = load_projects_df()
    if (df["Projekt"] == name).any():
        df.loc[df["Projekt"] == name, "Status"] = "aktiv"
    else:
        df = pd.concat([df, pd.DataFrame([{"Projekt": name, "Status": "aktiv"}])], ignore_index=True)
    save_projects_df(df)
    return True

def archive_project(name: str):
    df = load_projects_df()
    df.loc[df["Projekt"] == name, "Status"] = "archiviert"
    save_projects_df(df)

def restore_project(name: str):
    df = load_projects_df()
    df.loc[df["Projekt"] == name, "Status"] = "aktiv"
    save_projects_df(df)


# =========================
# REPORTS CSV per project
# =========================
def reports_filename(project: str) -> str:
    return f"{project}_Reports.csv"

def load_project_reports(project: str) -> pd.DataFrame:
    fname = reports_filename(project)
    fid = find_file_in_folder_by_name(REPORTS_HOME_FOLDER_ID, fname)
    if not fid:
        return pd.DataFrame(columns=RAPPORT_COLUMNS)

    raw = download_bytes(fid)
    if not raw:
        return pd.DataFrame(columns=RAPPORT_COLUMNS)

    df = pd.read_csv(io.BytesIO(raw))
    # Migration / Column cleanup
    for c in RAPPORT_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[RAPPORT_COLUMNS].fillna("")
    return df

def save_project_reports(project: str, df: pd.DataFrame):
    fname = reports_filename(project)
    fid = find_file_in_folder_by_name(REPORTS_HOME_FOLDER_ID, fname)
    data = df.to_csv(index=False).encode("utf-8")

    if fid:
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype="text/csv", resumable=False)
        drive.files().update(fileId=fid, media_body=media).execute()
    else:
        upload_bytes_to_drive(data, REPORTS_HOME_FOLDER_ID, fname, "text/csv")


# =========================
# HTML UPLOAD WIDGET (Cloud Run)
# =========================
def cloudrun_upload_widget(project: str, target: str, label: str, accept: str, multiple: bool = False, height: int = 310):
    """
    target: "photos" | "uploads"
    """
    prj = (project or "").replace('"', "").strip()
    tgt = target.replace('"', "").strip()

    mult_attr = "multiple" if multiple else ""
    html = f"""
    <div style="border:1px solid rgba(255,255,255,0.12); border-radius:12px; padding:14px; max-width:560px;">
      <div style="font-family:sans-serif; font-size:14px; margin-bottom:10px;">
        <b>{label}</b><br/>
        <span style="opacity:0.75; font-size:12px;">Upload läuft über Cloud Run (stabil auf Handy). Danach ggf. „Liste aktualisieren“ klicken.</span>
      </div>

      <input
        type="file"
        id="bauappFile"
        accept="{accept}"
        {mult_attr}
        style="margin-bottom: 10px; width: 100%;"
      />

      <button
        type="button"
        id="bauappBtn"
        style="background:#ff4b4b; color:white; border:none; padding:10px 14px; border-radius:8px; cursor:pointer; font-size:14px;"
      >
        📤 Hochladen
      </button>

      <div id="bauappStatus" style="margin-top:10px; font-family:sans-serif; font-size:14px;"></div>
      <div id="bauappProgressWrap" style="margin-top:8px; display:none;">
        <div style="height:10px; background:rgba(255,255,255,0.12); border-radius:999px; overflow:hidden;">
          <div id="bauappProgress" style="height:10px; width:0%; background:#22c55e;"></div>
        </div>
      </div>
    </div>

    <script>
      const input = document.getElementById("bauappFile");
      const btn = document.getElementById("bauappBtn");
      const status = document.getElementById("bauappStatus");
      const wrap = document.getElementById("bauappProgressWrap");
      const bar = document.getElementById("bauappProgress");

      function setStatus(msg) {{
        status.innerText = msg;
      }}
      function setProgress(p) {{
        wrap.style.display = "block";
        bar.style.width = String(p) + "%";
      }}

      async function uploadOne(file) {{
        return new Promise((resolve) => {{
          const fd = new FormData();
          fd.append("project", "{prj}");
          fd.append("target", "{tgt}");
          fd.append("file", file, file.name);

          const xhr = new XMLHttpRequest();
          xhr.open("POST", "{UPLOAD_SERVICE_URL}/upload", true);
          xhr.setRequestHeader("X-Upload-Token", "{UPLOAD_SERVICE_TOKEN}");

          xhr.upload.onprogress = (e) => {{
            if (e.lengthComputable) {{
              const p = Math.round((e.loaded / e.total) * 100);
              setProgress(p);
            }}
          }};

          xhr.onload = () => {{
            let data = {{}};
            try {{ data = JSON.parse(xhr.responseText || "{{}}"); }} catch(e) {{}}
            if (xhr.status >= 200 && xhr.status < 300) {{
              resolve({{ok:true, filename: data.filename || file.name}});
            }} else {{
              resolve({{ok:false, err: (data.detail || xhr.responseText || "Upload-Fehler")}});
            }}
          }};

          xhr.onerror = () => resolve({{ok:false, err:"Netzwerkfehler"}});
          xhr.send(fd);
        }});
      }}

      btn.onclick = async () => {{
        if (!input.files || input.files.length === 0) {{
          setStatus("❌ Bitte zuerst eine Datei auswählen.");
          return;
        }}

        btn.disabled = true;
        btn.style.opacity = "0.7";
        setProgress(0);

        const files = Array.from(input.files);
        let okCount = 0;

        for (let i = 0; i < files.length; i++) {{
          setStatus(`⏳ Upload ${i+1} / ${files.length} …`);
          const res = await uploadOne(files[i]);
          if (res.ok) {{
            okCount += 1;
            setStatus(`✅ ${okCount}/${files.length} hochgeladen`);
          }} else {{
            setStatus(`❌ Fehler bei Datei ${i+1}: ${res.err}`);
            break;
          }}
          setProgress(0);
        }}

        setStatus("✅ Upload abgeschlossen. Bitte ggf. „Liste aktualisieren“ klicken.");
        btn.disabled = false;
        btn.style.opacity = "1.0";
        input.value = "";
      }};
    </script>
    """
    components.html(html, height=height)


# =========================
# UI: MODE
# =========================
mode = st.sidebar.radio("Bereich", ["Mitarbeiter", "Admin"], horizontal=False)


# =========================
# MITARBEITER
# =========================
if mode == "Mitarbeiter":
    st.title("👷 Mitarbeiterbereich")

    dfp = load_projects_df()
    active = dfp[dfp["Status"] != "archiviert"]["Projekt"].tolist()
    if not active:
        st.info("Keine aktiven Projekte vorhanden.")
        st.stop()

    project = st.selectbox("Projekt:", active)

    tab1, tab2, tab3 = st.tabs(["🧾 Rapport", "📷 Fotos", "📁 Pläne"])

    # -------- Rapport
    with tab1:
        st.subheader("Rapport erfassen")

        col1, col2, col3 = st.columns(3)
        with col1:
            datum = st.date_input("Datum", datetime.now().date())
            mitarbeiter = st.text_input("Mitarbeiter", "")
        with col2:
            start = st.time_input("Start", time(7, 0))
            ende = st.time_input("Ende", time(16, 0))
        with col3:
            pause_h = st.number_input("Pause (h)", min_value=0.0, max_value=6.0, step=0.25, value=0.5)
            material = st.text_input("Material", "")

        bemerkung = st.text_area("Bemerkung", "")

        # Stunden berechnen
        def calc_hours(s: time, e: time, pause: float) -> float:
            dt_s = datetime.combine(datetime.now().date(), s)
            dt_e = datetime.combine(datetime.now().date(), e)
            if dt_e < dt_s:
                dt_e += timedelta(days=1)
            hours = (dt_e - dt_s).total_seconds() / 3600.0
            hours = max(0.0, hours - float(pause))
            return round(hours, 2)

        stunden = calc_hours(start, ende, pause_h)
        st.write(f"**Stunden:** {stunden}")

        if st.button("✅ Rapport speichern"):
            df = load_project_reports(project)
            new_row = {
                "Datum": str(datum),
                "Projekt": project,
                "Mitarbeiter": mitarbeiter.strip(),
                "Start": start.strftime("%H:%M"),
                "Ende": ende.strftime("%H:%M"),
                "Pause_h": float(pause_h),
                "Stunden": float(stunden),
                "Material": material.strip(),
                "Bemerkung": bemerkung.strip(),
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            save_project_reports(project, df)
            st.success("Rapport gespeichert.")
            sys_time.sleep(0.2)
            st.rerun()

        st.divider()
        st.subheader("📌 Aktuelle Rapporte im Projekt")

        df_show = load_project_reports(project)
        if df_show.empty:
            st.info("Noch keine Rapporte vorhanden.")
        else:
            # Neueste oben
            df_show2 = df_show.copy()
            # Sort grob nach Datum/Start (best effort)
            try:
                df_show2["__dt"] = pd.to_datetime(df_show2["Datum"], errors="coerce")
                df_show2 = df_show2.sort_values(["__dt", "Start"], ascending=[False, False]).drop(columns=["__dt"])
            except Exception:
                pass

            st.dataframe(df_show2, use_container_width=True, height=320)

            csv_bytes = df_show.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Rapporte als CSV herunterladen",
                data=csv_bytes,
                file_name=reports_filename(project),
                mime="text/csv",
            )

    # -------- Fotos
    with tab2:
        st.subheader("📷 Fotos")
        st.caption("Upload ist mobil stabil (Cloud Run).")

        cloudrun_upload_widget(
            project=project,
            target="photos",
            label="Foto auswählen oder aufnehmen",
            accept="image/*",
            multiple=False,
            height=320,
        )

        if st.button("🔄 Liste aktualisieren", key="refresh_photos"):
            st.rerun()

        st.divider()
        files = list_files(PHOTOS_FOLDER_ID)
        shown = False
        for f in files:
            if f["name"].startswith(project + "_"):
                data = download_bytes(f["id"])
                if data:
                    # Bilder anzeigen, sonst Download
                    if (f.get("mimeType") or "").startswith("image/"):
                        try:
                            st.image(data, width=340)
                        except Exception:
                            st.download_button(f"⬇️ {f['name']}", data=data, file_name=f["name"], key=f"ph_{f['id']}")
                    else:
                        st.download_button(f"⬇️ {f['name']}", data=data, file_name=f["name"], key=f"ph_{f['id']}")
                    shown = True
        if not shown:
            st.info("Keine Fotos für dieses Projekt vorhanden.")

    # -------- Pläne / Dokumente (Mitarbeiter nur Download)
    with tab3:
        st.subheader("📁 Pläne / Dokumente")
        files = list_files(UPLOADS_FOLDER_ID)
        found = False
        for f in files:
            if f["name"].startswith(project + "_"):
                found = True
                st.download_button(
                    f"⬇️ {f['name']}",
                    data=download_bytes(f["id"]),
                    file_name=f["name"],
                    key=f"pl_{f['id']}",
                )
        if not found:
            st.info("Keine Pläne/Dokumente vorhanden.")


# =========================
# ADMIN
# =========================
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
        if add_or_restore_project(new_proj):
            st.success("Projekt gespeichert.")
            st.rerun()
        else:
            st.warning("Bitte Projektname eingeben.")

    colA, colB = st.columns(2)
    with colA:
        st.write("Aktive Projekte")
        to_archive = st.selectbox("Archivieren", [""] + active_projects)
        if st.button("📦 Archivieren") and to_archive:
            archive_project(to_archive)
            st.success("Archiviert.")
            st.rerun()

    with colB:
        st.write("Archivierte Projekte")
        to_restore = st.selectbox("Wiederherstellen", [""] + archived_projects)
        if st.button("♻️ Wiederherstellen") and to_restore:
            restore_project(to_restore)
            st.success("Wiederhergestellt.")
            st.rerun()

    st.divider()
    st.subheader("📁 Pläne/Dokumente hochladen (mobil stabil)")

    if not active_projects:
        st.info("Keine aktiven Projekte vorhanden.")
        st.stop()

    up_project = st.selectbox("Projekt für Upload", active_projects, key="admin_upload_project")

    cloudrun_upload_widget(
        project=up_project,
        target="uploads",
        label="Dokument(e) auswählen und hochladen (PDF, Bilder, Office, …)",
        accept="*/*",
        multiple=True,
        height=340,
    )

    if st.button("🔄 Liste aktualisieren", key="refresh_admin_uploads"):
        st.rerun()

    st.divider()
    st.subheader("Vorhandene Pläne/Dokumente")

    files = list_files(UPLOADS_FOLDER_ID)
    found = False
    for f in files:
        if f["name"].startswith(up_project + "_"):
            found = True
            st.download_button(
                f"⬇️ {f['name']}",
                data=download_bytes(f["id"]),
                file_name=f["name"],
                key=f"adm_dl_{f['id']}",
            )
    if not found:
        st.info("Keine Dateien für dieses Projekt vorhanden.")
