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
def cloudrun_upload_widget(*, project: str, bucket: str, title: str, help_text: str, accept: str, multiple: bool = True, height: int = 230):
    """
    bucket: "photos" oder "uploads"
    accept: "image/*" für Fotos, "application/pdf,image/*" für Dokumente
    """
    uid = str(uuid4()).replace("-", "")

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
          fd.append("upload_type", bucket === "uploads" ? "plan" : "photo");

          try {
            const resp = await fetch(url + "/upload", {
              method: "POST",
              headers: { "X-Upload-Token": token },
              body: fd
            });

            if (resp.ok) success++;
            else errors++;
          } catch(e) {
            errors++;
          }
        }

        btn.disabled = false;
        btn.style.opacity = "1.0";
        input.value = "";

        if (errors === 0) {
          status.innerHTML = "<span style='color:green; font-weight:bold'>✅ " + success + " Datei(en) erfolgreich!</span>";
        } else {
          status.innerHTML = "<span style='color:red'>⚠️ " + success + " erfolgreich, " + errors + " fehlgeschlagen.</span>";
        }
      };
    })();
    </script>
    """

    html = (html
        .replace("__TITLE__", title)
        .replace("__HELP__", help_text)
        .replace("__URL__", UPLOAD_SERVICE_URL)
        .replace("__TOKEN__", UPLOAD_SERVICE_TOKEN)
        .replace("__BUCKET__", bucket)
        .replace("__PROJECT__", project)
        .replace("__ACCEPT__", accept)
        .replace("__UID__", uid)
        .replace("__MULT__", "multiple" if multiple else "")
    )

    components.html(html, height=height)

# =========================
# SAFE IMAGE PREVIEW
# =========================
def image_preview_from_drive(file_name: str, mime: str | None, file_id: str):
    """
    Fotos sollen lesbar sein -> wir listen nur image/*.
    Trotzdem schützen wir gegen defekte Dateien.
    """
    data = download_bytes(file_id)
    if not data:
        st.warning("Konnte Bild nicht laden.")
        return

    try:
        st.image(data, use_container_width=True)
    except Exception:
        st.warning("Bild konnte nicht angezeigt werden (Format/Upload defekt).")

# =========================
# UI
# =========================
st.sidebar.title("Menü")
mode = st.sidebar.radio("Bereich", ["👷 Mitarbeiter", "🛠️ Admin"])

# -------------------------
# 👷 MITARBEITER
# -------------------------
if mode == "👷 Mitarbeiter":
    st.title("👷 Mitarbeiterbereich")

    active_projects = get_active_projects()
    if not active_projects:
        st.warning("Keine aktiven Projekte.")
        st.stop()

    project = st.selectbox("Projekt wählen", active_projects)
    t1, t2, t3 = st.tabs(["📝 Rapport", "📷 Fotos", "📂 Pläne"])

    # --- RAPPORT ---
    with t1:
        st.subheader("Rapport erfassen")
        c1, c2, c3 = st.columns(3)

        date_val = c1.date_input("Datum", datetime.now())
        ma_val = c1.text_input("Name")
        start_val = c2.time_input("Start", time(7, 0))
        end_val = c2.time_input("Ende", time(16, 0))
        pause_val = c3.number_input("Pause (h)", 0.0, 5.0, 0.5, 0.25)
        mat_val = c3.text_input("Material")
        rem_val = st.text_area("Bemerkung")

        dt_start = datetime.combine(datetime.today(), start_val)
        dt_end = datetime.combine(datetime.today(), end_val)
        if dt_end < dt_start:
            dt_end += timedelta(days=1)

        dur = round(max(0.0, (dt_end - dt_start).total_seconds() / 3600 - pause_val), 2)
        st.info(f"Stunden: {dur}")

        if st.button("✅ Speichern", type="primary"):
            if not ma_val.strip():
                st.error("Name fehlt.")
            else:
                ok = save_report(project, {
                    "Datum": str(date_val),
                    "Projekt": project,
                    "Mitarbeiter": ma_val.strip(),
                    "Start": str(start_val),
                    "Ende": str(end_val),
                    "Pause_h": pause_val,
                    "Stunden": dur,
                    "Material": mat_val,
                    "Bemerkung": rem_val
                })
                if ok:
                    st.success("Gespeichert!")
                    sys_time.sleep(0.4)
                    st.rerun()
                else:
                    st.error("Speichern fehlgeschlagen (Drive). Bitte erneut versuchen.")

        st.divider()

        cA, cB = st.columns([0.7, 0.3])
        cA.subheader("📌 Rapporte im Projekt (aktuell)")
        if cB.button("🔄 Bericht aktualisieren", key="refresh_reports_emp"):
            st.rerun()

        df_h, _ = get_reports_df(project)
        if df_h.empty:
            st.info("Noch keine Rapporte für dieses Projekt vorhanden.")
        else:
            try:
                df_view = df_h.copy()
                df_view["Datum"] = pd.to_datetime(df_view["Datum"], errors="coerce")
                df_view = df_view.sort_values(["Datum"], ascending=False)
            except Exception:
                df_view = df_h
            st.dataframe(df_view.tail(50), use_container_width=True)

    # --- FOTOS (LESBAR) ---
    with t2:
        st.subheader("Fotos")

        # Upload nur Bilder
        cloudrun_upload_widget(
            project=project,
            bucket="photos",
            title="Foto(s) hochladen",
            help_text="Nur Bilder (Kamera/Galerie).",
            accept="image/*",
            multiple=True,
            height=240,
        )

        if st.button("🔄 Fotos aktualisieren", key="refresh_photos_emp"):
            st.rerun()

        # WICHTIG: Nur echte Bilder listen (Drive Query Filter)
        files = list_files(PHOTOS_FOLDER_ID, mime_prefix="image/")
        proj_photos = [x for x in files if x["name"].startswith(project + "_")][:40]

        if not proj_photos:
            st.info("Keine Fotos vorhanden.")
        else:
            for f in proj_photos:
                # Mitarbeiter: NUR ansehen, NICHT löschen
                with st.expander(f"🖼️ {f['name']}", expanded=False):
                    image_preview_from_drive(f["name"], f.get("mimeType"), f["id"])

    # --- PLÄNE / DOKUMENTE (DOWNLOAD OK) ---
    with t3:
        st.subheader("Pläne & Dokumente")

        if st.button("🔄 Pläne aktualisieren", key="refresh_docs_emp"):
            st.rerun()

        files = list_files(UPLOADS_FOLDER_ID)
        proj_docs = [x for x in files if x["name"].startswith(project + "_")][:100]

        if not proj_docs:
            st.info("Keine Dokumente hinterlegt.")
        else:
            for f in proj_docs:
                d = download_bytes(f["id"])
                if not d:
                    continue
                c1, c2 = st.columns([0.8, 0.2])
                c1.write(f"📄 {f['name']}")
                c2.download_button("⬇️", d, file_name=f["name"])

# -------------------------
# 🛠️ ADMIN
# -------------------------
elif mode == "🛠️ Admin":
    st.title("Admin")

    pin = st.text_input("PIN", type="password")
    if pin != ADMIN_PIN:
        st.stop()

    st.success("Angemeldet")

    tabA, tabB, tabC = st.tabs(["📌 Projekte", "📂 Uploads & Vorschau", "🧾 Rapporte"])

    # --- Projekte ---
    with tabA:
        st.subheader("Projekte verwalten")
        df_p, pid = get_projects_df()

        c1, c2 = st.columns([0.7, 0.3])
        new_p = c1.text_input("Neues Projekt")
        if c2.button("➕ Anlegen") and new_p.strip():
            df_p = pd.concat([df_p, pd.DataFrame([{"Projekt": new_p.strip(), "Status": "aktiv"}])], ignore_index=True)
            save_projects_df(df_p, pid)
            st.success("Projekt angelegt.")
            st.rerun()

        if df_p.empty:
            st.info("Noch keine Projekte.")
        else:
            st.dataframe(df_p, use_container_width=True)

            st.divider()
            st.subheader("Projekt Status ändern")
            all_projs = df_p["Projekt"].tolist()
            sel = st.selectbox("Projekt wählen", all_projs, key="admin_status_proj")
            new_status = st.radio("Neuer Status", ["aktiv", "archiviert"], horizontal=True, key="admin_status_radio")
            if st.button("Status speichern", key="admin_save_status"):
                df_p.loc[df_p["Projekt"] == sel, "Status"] = new_status
                save_projects_df(df_p, pid)
                st.success("Status geändert.")
                st.rerun()

    # --- Uploads & Vorschau ---
    with tabB:
        st.subheader("Uploads (mobil + PC)")

        projs = get_active_projects()
        if not projs:
            st.info("Erstelle zuerst ein aktives Projekt.")
            st.stop()

        sel_p = st.selectbox("Projekt", projs, key="admin_sel_project_upload")

        cX, cY = st.columns(2)

        with cX:
            st.markdown("### 📷 Fotos (Upload)")
            cloudrun_upload_widget(
                project=sel_p,
                bucket="photos",
                title="Foto(s) hochladen",
                help_text="Nur Bilder (Kamera/Galerie).",
                accept="image/*",
                multiple=True,
                height=240,
            )

        with cY:
            st.markdown("### 📄 Pläne/Dokumente (Upload)")
            cloudrun_upload_widget(
                project=sel_p,
                bucket="uploads",
                title="Dokument(e) hochladen",
                help_text="PDF/Bilder (Download genügt).",
                accept="application/pdf,image/*",
                multiple=True,
                height=240,
            )

        st.divider()

        # Fotos Vorschau (lesbar)
        c1, c2 = st.columns([0.7, 0.3])
        c1.subheader("📷 Fotos – Vorschau")
        if c2.button("🔄 Aktualisieren", key="admin_refresh_photos"):
            st.rerun()

        files_ph = list_files(PHOTOS_FOLDER_ID, mime_prefix="image/")
        admin_photos = [x for x in files_ph if x["name"].startswith(sel_p + "_")][:60]

        if not admin_photos:
            st.info("Keine Fotos vorhanden.")
        else:
            for f in admin_photos:
                with st.expander(f"🖼️ {f['name']}", expanded=False):
                    image_preview_from_drive(f["name"], f.get("mimeType"), f["id"])
                    # Admin darf löschen
                    if st.button("🗑 Foto löschen", key=f"adm_del_photo_{f['id']}"):
                        delete_file(f["id"])
                        st.success("Gelöscht.")
                        sys_time.sleep(0.2)
                        st.rerun()

        st.divider()

        # Dokumente Download-Liste + Löschen
        c1, c2 = st.columns([0.7, 0.3])
        c1.subheader("📄 Pläne/Dokumente – Download")
        if c2.button("🔄 Aktualisieren", key="admin_refresh_docs"):
            st.rerun()

        files_docs = list_files(UPLOADS_FOLDER_ID)
        admin_docs = [x for x in files_docs if x["name"].startswith(sel_p + "_")][:150]

        if not admin_docs:
            st.info("Keine Dokumente vorhanden.")
        else:
            for f in admin_docs:
                d = download_bytes(f["id"])
                if not d:
                    continue
                a1, a2, a3 = st.columns([0.65, 0.2, 0.15])
                a1.write(f"📄 {f['name']}")
                a2.download_button("⬇️ Download", d, file_name=f["name"])
                if a3.button("🗑", key=f"adm_del_doc_{f['id']}"):
                    delete_file(f["id"])
                    st.success("Gelöscht.")
                    sys_time.sleep(0.2)
                    st.rerun()

    # --- Rapporte ---
    with tabC:
        st.subheader("Rapporte ansehen")

        projs = get_active_projects()
        if not projs:
            st.info("Keine aktiven Projekte.")
            st.stop()

        sel_rp = st.selectbox("Projekt", projs, key="admin_sel_reports")
        if st.button("🔄 Rapporte aktualisieren", key="admin_refresh_reports"):
            st.rerun()

        df_r, _ = get_reports_df(sel_rp)
        if df_r.empty:
            st.info("Keine Rapporte vorhanden.")
        else:
            try:
                df_view = df_r.copy()
                df_view["Datum"] = pd.to_datetime(df_view["Datum"], errors="coerce")
                df_view = df_view.sort_values(["Datum"], ascending=False)
            except Exception:
                df_view = df_r

            st.dataframe(df_view, use_container_width=True)
            st.download_button(
                "⬇️ Rapporte als CSV herunterladen",
                df_view.to_csv(index=False).encode("utf-8"),
                file_name=f"{sel_rp}_Reports.csv",
                mime="text/csv",
            )
