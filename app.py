import io
import os
import time as sys_time
import uuid
from datetime import datetime, time, timedelta

import pandas as pd
import streamlit as st

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# =========================
# SETTINGS
# =========================
st.set_page_config(page_title="BauApp - R. Baumgartner", layout="wide")

# Logo oben links
logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
if os.path.exists(logo_path):
    st.sidebar.image(logo_path, use_container_width=True)
else:
    st.sidebar.warning("logo.png nicht gefunden (muss im gleichen Ordner wie app.py liegen).")

SCOPES = ["https://www.googleapis.com/auth/drive"]

PROJECTS_CSV_NAME = "Projects.csv"
REPORTS_SUBFOLDER_NAME = "Rapporte"  # Unterordner in REPORTS_FOLDER_ID

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
def find_file_in_folder_by_name(folder_id: str, name: str):
    try:
        q = f"name = '{name}' and '{folder_id}' in parents and trashed=false"
        res = drive.files().list(q=q, fields="files(id,name)", pageSize=10).execute()
        files = res.get("files", [])
        return files[0]["id"] if files else None
    except Exception as e:
        st.error(f"Drive-Suche Fehler: {e}")
        return None

def list_files(folder_id: str):
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        res = drive.files().list(
            q=q,
            fields="files(id,name,mimeType,createdTime)",
            pageSize=200
        ).execute()
        return res.get("files", [])
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
            mimetype=uploaded_file.type,
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
    """Return folder_id of (parent/folder_name). Create if missing."""
    try:
        q = (
            f"mimeType='application/vnd.google-apps.folder' and "
            f"name='{folder_name}' and '{parent_id}' in parents and trashed=false"
        )
        res = drive.files().list(q=q, fields="files(id,name)", pageSize=5).execute()
        files = res.get("files", [])
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

# Alle Rapport-Dateien landen hier:
REPORTS_HOME_FOLDER_ID = ensure_subfolder(REPORTS_FOLDER_ID, REPORTS_SUBFOLDER_NAME)

# =========================
# PROJECTS (persist + archive in Drive)
# =========================
PROJECTS_COLS = ["Projekt", "Status"]  # Status: aktiv | archiviert

def load_projects_df() -> pd.DataFrame:
    fid = find_file_in_folder_by_name(REPORTS_FOLDER_ID, PROJECTS_CSV_NAME)

    # Nur beim allerersten Start Defaults erzeugen:
    if not fid:
        defaults = ["Neubau Müller", "Sanierung West", "Dachstock Meier"]
        return pd.DataFrame({"Projekt": defaults, "Status": ["aktiv"] * len(defaults)})

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
        # Auto-Migration zurückschreiben
        save_projects_df(df)

    # Cleanup
    df["Projekt"] = df["Projekt"].astype(str).str.strip()
    df["Status"] = df["Status"].astype(str).str.strip().str.lower().replace({"active": "aktiv"})

    # Leere raus
    df = df[df["Projekt"] != ""].copy()

    # Nur erlaubte Status
    df.loc[~df["Status"].isin(["aktiv", "archiviert"]), "Status"] = "aktiv"

    return df[PROJECTS_COLS].copy()

def save_projects_df(df: pd.DataFrame):
    fid = find_file_in_folder_by_name(REPORTS_FOLDER_ID, PROJECTS_CSV_NAME)

    # Falls Datei noch nicht existiert: jetzt erstellen (damit Defaults persistiert sind)
    if not fid:
        upload_bytes_to_folder(
            REPORTS_FOLDER_ID,
            PROJECTS_CSV_NAME,
            df.to_csv(index=False).encode("utf-8"),
            "text/csv"
        )
        return

    update_file_bytes(fid, df.to_csv(index=False).encode("utf-8"), "text/csv")

def load_projects(include_archived: bool = False) -> list[str]:
    df = load_projects_df()

    # Wenn Projects.csv noch NICHT existiert, erzeugen wir sie jetzt (Defaults persistieren)
    fid = find_file_in_folder_by_name(REPORTS_FOLDER_ID, PROJECTS_CSV_NAME)
    if not fid:
        save_projects_df(df)

    if include_archived:
        return df["Projekt"].tolist()

    return df[df["Status"] != "archiviert"]["Projekt"].tolist()

def set_project_status(project: str, status: str):
    df = load_projects_df()
    mask = df["Projekt"].astype(str).str.strip() == project.strip()
    if mask.any():
        df.loc[mask, "Status"] = status
        save_projects_df(df)

def add_or_restore_project(project: str):
    p = project.strip()
    if not p:
        return

    df = load_projects_df()
    mask = df["Projekt"].astype(str).str.strip() == p

    if mask.any():
        # existiert -> wenn archiviert, wieder aktiv setzen
        df.loc[mask, "Status"] = "aktiv"
    else:
        df = pd.concat([df, pd.DataFrame([{"Projekt": p, "Status": "aktiv"}])], ignore_index=True)

    save_projects_df(df)

# =========================
# RAPPORTS (clean schema)
# =========================
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

def report_csv_name(project: str) -> str:
    return f"{project}_Reports.csv"

def get_report_file_id(project: str):
    return find_file_in_folder_by_name(REPORTS_HOME_FOLDER_ID, report_csv_name(project))

def normalize_reports_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=RAPPORT_COLUMNS)

    # bekannte unnötige Spalten entfernen
    for c in list(df.columns):
        lc = str(c).strip().lower()
        if lc in ["timestamp", "erfasst_am", "erfasst am", "pause", "pause2", "pause_2"]:
            df = df.drop(columns=[c], errors="ignore")

    # Falls alte Spaltennamen existieren
    rename_map = {}
    if "Pause (h)" in df.columns and "Pause_h" not in df.columns:
        rename_map["Pause (h)"] = "Pause_h"
    if rename_map:
        df = df.rename(columns=rename_map)

    # fehlende Spalten ergänzen
    for col in RAPPORT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # Reihenfolge fix
    df = df[RAPPORT_COLUMNS]

    # Datum als Text
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

    # Auto-Migration zurückschreiben
    if set(df_clean.columns) != set(df.columns) or list(df_clean.columns) != list(df.columns):
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
projects = load_projects(include_archived=False)

st.sidebar.header("BauApp")
role = st.sidebar.radio("Bereich:", ["Mitarbeiter", "Admin"])

# ===== Mitarbeiter =====
if role == "Mitarbeiter":
    st.title("👷 Mitarbeiter")

    if not projects:
        st.warning("Keine aktiven Projekte vorhanden. (Admin: Projekte anlegen oder aus Archiv wiederherstellen)")
        st.stop()

    project = st.selectbox("Projekt:", projects)

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

            if st.form_submit_button("Senden"):
                dt_start = datetime.combine(d, t_start)
                dt_end = datetime.combine(d, t_end)
                if dt_end < dt_start:
                    dt_end += timedelta(days=1)

                hours = ((dt_end - dt_start).total_seconds() / 3600) - pause_h

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
        st.caption("Tipp: Auf dem Smartphone können Fotos als HEIC (iPhone) oder WEBP (Android) kommen.")

        # Stabiler Upload via FORM (wie bei Plänen im Admin)
        with st.form("photo_upload_form", clear_on_submit=True):
            up = st.file_uploader(
                "Foto hochladen",
                type=["jpg", "jpeg", "png", "heic", "webp"],
                key="photo_upload_file"
            )
            submit_photo = st.form_submit_button("📤 Foto speichern")

        if submit_photo:
            if up is None:
                st.warning("Bitte zuerst ein Foto auswählen.")
            else:
                fname = f"{project}_{up.name}"
                if upload_streamlit_file(up, PHOTOS_FOLDER_ID, fname):
                    st.success("Foto hochgeladen.")
                    sys_time.sleep(0.2)
                    st.rerun()

        files = list_files(PHOTOS_FOLDER_ID)
        shown = False
        for f in files:
            if f["name"].startswith(project + "_"):
                data = download_bytes(f["id"])
                if data:
                    st.image(data, width=320)
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

    # Projekte (aktiv + archiviert)
    dfp = load_projects_df()
    active_projects = dfp[dfp["Status"] != "archiviert"]["Projekt"].tolist()
    archived_projects = dfp[dfp["Status"] == "archiviert"]["Projekt"].tolist()

    if active_projects:
        proj = st.selectbox("Projekt (aktiv):", active_projects, key="admin_proj")
    else:
        st.warning("Keine aktiven Projekte vorhanden.")
        proj = None

    st.subheader("Projektverwaltung (im Drive, mit Archiv)")

    # --- Projekt hinzufügen / wiederherstellen ---
    new_proj = st.text_input("Neues Projekt anlegen (oder archiviertes wieder aktivieren)")
    if st.button("➕ Projekt hinzufügen / wiederherstellen"):
        p = new_proj.strip()
        if not p:
            st.warning("Bitte Projektname eingeben.")
        else:
            add_or_restore_project(p)
            st.success("Projekt aktiv (neu oder wiederhergestellt).")
            st.rerun()

    # --- Projekt archivieren (statt löschen) ---
    if active_projects:
        del_proj = st.selectbox("Projekt archivieren (aus App entfernen)", active_projects, key="del_proj")
        confirm = st.checkbox("Ja, ich will dieses Projekt archivieren (Drive-Daten bleiben erhalten).")
        if st.button("🗃️ Projekt archivieren") and confirm:
            set_project_status(del_proj, "archiviert")
            st.success("Projekt archiviert. (Daten bleiben im Drive, Projekt verschwindet aus der App)")
            st.rerun()

    # --- Archiv-Panel: anzeigen & wiederherstellen ---
    st.divider()
    st.subheader("Archivierte Projekte")

    if not archived_projects:
        st.info("Keine archivierten Projekte vorhanden.")
    else:
        sel_arch = st.selectbox("Archiviertes Projekt auswählen", archived_projects, key="arch_sel")

        c1, c2 = st.columns(2)
        if c1.button("♻️ Wiederherstellen (aktiv)"):
            set_project_status(sel_arch, "aktiv")
            st.success("Projekt wiederhergestellt.")
            st.rerun()

        if c2.button("📋 Nur anzeigen (nichts ändern)"):
            st.info(f"Archiviert: {sel_arch}")

    # Tabs
    t_reports, t_plans, t_photos = st.tabs(["📄 Rapporte", "📂 Pläne", "📷 Fotos"])

    with t_reports:
        if proj is None:
            st.info("Wähle zuerst ein aktives Projekt.")
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

    # =========================
    # ADMIN -> PLÄNE (FIXED)
    # Upload via FORM (stabil)
    # =========================
    with t_plans:
        if proj is None:
            st.info("Wähle zuerst ein aktives Projekt.")
        else:
            st.caption("Stabiler Upload über Formular (verhindert UI-Fehler beim Button-Klick).")

            with st.form("plan_upload_form", clear_on_submit=True):
                up_p = st.file_uploader("Plan/Dokument auswählen", key="plan_upload_file")
                submit = st.form_submit_button("⬆️ Dokument hochladen")

            if submit:
                if up_p is None:
                    st.warning("Bitte zuerst eine Datei auswählen.")
                else:
                    try:
                        fname = f"{proj}_{up_p.name}"
                        ok = upload_streamlit_file(up_p, UPLOADS_FOLDER_ID, fname)
                        if ok:
                            st.success("Dokument hochgeladen.")
                        else:
                            st.error("Upload fehlgeschlagen (siehe Fehlermeldung oben).")
                    except Exception as e:
                        st.error(f"Upload-Exception: {e}")

            if st.button("🔄 Aktualisieren"):
                pass

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
        if proj is None:
            st.info("Wähle zuerst ein aktives Projekt.")
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
