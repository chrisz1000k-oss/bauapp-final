import io
import os
import re
import time as sys_time
import base64
import html
from datetime import datetime, time, timedelta
from uuid import uuid4
from urllib.parse import quote

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import qrcode

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


# =========================
# PAGE CONFIG / CHROME / LOGO
# =========================
st.set_page_config(page_title="BauApp", layout="wide")

st.markdown(
    """
    <style>
    #MainMenu {display: none !important;}
    [data-testid="stToolbar"] {display: none !important;}
    [data-testid="stHeader"] {display: none !important;}
    [data-testid="stStatusWidget"] {display: none !important;}
    footer {display: none !important;}
    </style>
    """,
    unsafe_allow_html=True,
)

logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
if os.path.exists(logo_path):
    st.sidebar.image(logo_path, use_container_width=True)


# =========================
# SECRETS (ROBUST)
# =========================
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


ADMIN_PIN = sget("ADMIN_PIN", "1234")
BASE_APP_URL = sget("BASE_APP_URL", "").strip().rstrip("/")

GOOGLE_CLIENT_ID = require_secret("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = require_secret("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = require_secret("GOOGLE_REFRESH_TOKEN")

PHOTOS_FOLDER_ID = require_secret("PHOTOS_FOLDER_ID")
PLANS_FOLDER_ID = require_secret("PLANS_FOLDER_ID")
PROJECT_REPORTS_FOLDER_ID = require_secret("PROJECT_REPORTS_FOLDER_ID")
TIME_REPORTS_FOLDER_ID = require_secret("TIME_REPORTS_FOLDER_ID")

upload_section = st.secrets.get("upload_service")
if not upload_section:
    st.error("Fehler: Sektion [upload_service] fehlt in Secrets.")
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
def list_files(folder_id: str, *, mime_prefix: str | None = None, page_size: int = 200):
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        if mime_prefix:
            q += f" and mimeType contains '{mime_prefix}'"

        res = (
            drive.files()
            .list(
                q=q,
                pageSize=page_size,
                fields="files(id,name,mimeType,createdTime)",
                orderBy="createdTime desc",
            )
            .execute()
        )
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
        meta = {"name": filename, "parents": [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
        drive.files().create(body=meta, media_body=media, fields="id").execute()
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
# SAFE IMAGE PREVIEW
# =========================
def image_preview_from_drive(file_id: str):
    data = download_bytes(file_id)
    if not data:
        st.warning("Konnte Bild nicht laden.")
        return
    try:
        st.image(data, use_container_width=True)
    except Exception:
        st.warning("Bild konnte nicht angezeigt werden (Format/Upload defekt).")


# =========================
# QR CODE
# =========================
def generate_project_qr(project_name: str) -> bytes:
    if not BASE_APP_URL:
        # Not fatal, but QR should still be possible
        base = "https://DEINE-APP.streamlit.app"
    else:
        base = BASE_APP_URL

    safe_project = quote(project_name)
    link = f"{base}?embed=true&project={safe_project}"

    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(link)
    qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# =========================
# CLOUD RUN UPLOAD WIDGET
# =========================
def cloudrun_upload_widget(
    *,
    project: str,
    upload_type: str,  # "photo" | "plan"
    title: str,
    help_text: str,
    accept: str,
    multiple: bool = True,
    height: int = 230,
):
    uid = str(uuid4()).replace("-", "")

    html_code = r"""
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
      const project = "__PROJECT__";
      const uploadType = "__UPLOAD_TYPE__";

      const btn = document.getElementById("uploadBtn___UID__");
      const input = document.getElementById("fileInput___UID__");
      const status = document.getElementById("status___UID__");

      function setStatus(msg) { status.innerText = msg; }

      btn.onclick = async function() {
        if (!input.files || input.files.length === 0) {
          setStatus("❌ Bitte Datei wählen.");
          return;
        }

        btn.disabled = true;
        btn.style.opacity = "0.6";

        let success = 0;
        let errors = 0;

        for (let i = 0; i < input.files.length; i++) {
          const file = input.files[i];
          setStatus("⏳ Lade Datei " + (i+1) + " von " + input.files.length + " hoch: " + file.name);

          const fd = new FormData();
          fd.append("file", file);
          fd.append("project", project);
          fd.append("upload_type", uploadType);

          try {
            const resp = await fetch(url + "/upload", {
              method: "POST",
              headers: { "X-Upload-Token": token },
              body: fd
            });

            if (resp.ok) {
              success++;
            } else {
              errors++;
              let t = "";
              try { t = await resp.text(); } catch(e) {}
              setStatus("❌ Fehler (" + resp.status + ") bei " + file.name + (t ? ("\n" + t) : ""));
            }
          } catch(e) {
            errors++;
            setStatus("❌ Netzwerk/Fetch Fehler bei " + file.name);
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

    html_code = (
        html_code.replace("__TITLE__", title)
        .replace("__HELP__", help_text)
        .replace("__URL__", UPLOAD_SERVICE_URL)
        .replace("__TOKEN__", UPLOAD_SERVICE_TOKEN)
        .replace("__PROJECT__", project)
        .replace("__UPLOAD_TYPE__", upload_type)
        .replace("__ACCEPT__", accept)
        .replace("__UID__", uid)
        .replace("__MULT__", "multiple" if multiple else "")
    )

    components.html(html_code, height=height)


# =========================
# DATA MODEL (CSV FILES)
# =========================
PROJECTS_CSV_NAME = "Projects.csv"
PROJECTS_COLS = [
    "ProjektID",
    "Projekt",
    "Status",
    "Auftragsnr",
    "Objekt",
    "Kunde",
    "Telefon",
    "Kontaktperson",
    "Kontakttelefon",
]

EMPLOYEES_CSV_NAME = "Employees.csv"
EMPLOYEES_COLS = ["EmployeeID", "Name", "Rolle", "Stundenlohn", "PIN", "Status"]

# Pro Projekt: "<Projekt>_Reports.csv" im PROJECT_REPORTS_FOLDER_ID
RAPPORT_COLS = [
    "Datum",
    "Projekt",
    "ProjektID",
    "Auftragsnr",
    "Objekt",
    "Kunde",
    "Telefon",
    "Kontaktperson",
    "Kontakttelefon",
    "EmployeeID",
    "Mitarbeiter",
    "AnkunftMagazin",
    "AbfahrtMagazin",
    "ReiseHomeToSiteMin",
    "ReiseSiteToHomeMin",
    "ReiseDirektMin",
    "ReiseBezahltMin",
    "ReiseRegel",
    "Reisezeit_h",
    "Start",
    "Ende",
    "Pause_h",
    "Stunden",
    "Material",
    "Bemerkung",
]

# Zeit / Wochenabschluss / AZK in TIME_REPORTS_FOLDER_ID
CLOSURES_CSV_NAME = "Closures.csv"
CLOSURES_COLS = [
    "EmployeeID",
    "Mitarbeiter",
    "Jahr",
    "KW",
    "Von",
    "Bis",
    "TotalStunden",
    "TotalReisezeit_h",
    "Timestamp",
    "Signed",
]

AZK_CSV_NAME = "AZK.csv"
AZK_COLS = ["Jahr", "KW", "EmployeeID", "Mitarbeiter", "ProjektID", "Projekt", "TotalStunden", "TotalReisezeit_h", "Timestamp"]


def _ensure_cols(df: pd.DataFrame, cols: list[str], defaults: dict | None = None) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    if defaults:
        for c, dv in defaults.items():
            if c in df.columns:
                df[c] = df[c].fillna(dv)
    return df[cols]


def _read_csv_from_folder(folder_id: str, filename: str, cols: list[str], defaults: dict | None = None):
    files = list_files(folder_id, page_size=500)
    hit = next((f for f in files if f["name"] == filename), None)
    if not hit:
        return pd.DataFrame(columns=cols), None
    data = download_bytes(hit["id"])
    if not data:
        return pd.DataFrame(columns=cols), hit["id"]
    df = pd.read_csv(io.BytesIO(data))
    df = _ensure_cols(df, cols, defaults=defaults)
    return df, hit["id"]


def _write_csv_to_folder(folder_id: str, filename: str, df: pd.DataFrame, file_id: str | None):
    payload = df.to_csv(index=False).encode("utf-8")
    if file_id:
        return update_file_in_drive(file_id, payload, mimetype="text/csv")
    return upload_bytes_to_drive(payload, folder_id, filename, "text/csv")


def ensure_project_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "ProjektID" not in df.columns:
        df["ProjektID"] = ""
    for idx, row in df.iterrows():
        name = str(row.get("Projekt", "")).strip()
        pid = str(row.get("ProjektID", "")).strip()
        if name and not pid:
            df.at[idx, "ProjektID"] = str(uuid4())[:8]
    return df


def get_projects_df():
    df, fid = _read_csv_from_folder(
        PROJECT_REPORTS_FOLDER_ID,
        PROJECTS_CSV_NAME,
        PROJECTS_COLS,
        defaults={"Status": "aktiv"},
    )
    if "Status" in df.columns:
        df["Status"] = df["Status"].astype(str).replace("nan", "").fillna("").replace("", "aktiv")
    df = ensure_project_ids(df)
    df = _ensure_cols(df, PROJECTS_COLS, defaults={"Status": "aktiv"})
    return df, fid


def save_projects_df(df: pd.DataFrame, file_id: str | None):
    df2 = ensure_project_ids(df)
    if "Status" in df2.columns:
        df2["Status"] = df2["Status"].astype(str).replace("nan", "").fillna("").replace("", "aktiv")
    df2 = _ensure_cols(df2, PROJECTS_COLS, defaults={"Status": "aktiv"})
    return _write_csv_to_folder(PROJECT_REPORTS_FOLDER_ID, PROJECTS_CSV_NAME, df2, file_id)


def get_active_projects():
    df, _ = get_projects_df()
    if df.empty:
        return []
    return df[df["Status"].astype(str) == "aktiv"]["Projekt"].astype(str).tolist()


def get_all_projects():
    df, _ = get_projects_df()
    if df.empty:
        return []
    return df["Projekt"].astype(str).tolist()


def get_project_record(project_name: str, projects_df: pd.DataFrame | None = None) -> dict:
    df = projects_df
    if df is None:
        df, _ = get_projects_df()
    if df.empty:
        return {}
    hit = df[df["Projekt"].astype(str) == str(project_name)]
    if hit.empty:
        return {}
    rec = hit.iloc[0].to_dict()
    for k in list(rec.keys()):
        v = rec.get(k)
        if v is None or str(v).lower() == "nan":
            rec[k] = ""
        else:
            rec[k] = str(v).strip()
    return rec


def reports_filename(project_name: str) -> str:
    return f"{project_name}_Reports.csv"


def get_reports_df(project_name: str):
    df, fid = _read_csv_from_folder(
        PROJECT_REPORTS_FOLDER_ID,
        reports_filename(project_name),
        RAPPORT_COLS,
        defaults={"Pause_h": 0.0, "Stunden": 0.0, "Reisezeit_h": 0.0},
    )
    # numeric safety
    for c in ["Pause_h", "Stunden", "Reisezeit_h"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df, fid


def save_reports_df(project_name: str, df: pd.DataFrame, file_id: str | None) -> bool:
    df2 = _ensure_cols(
        df,
        RAPPORT_COLS,
        defaults={"Pause_h": 0.0, "Stunden": 0.0, "Reisezeit_h": 0.0},
    )
    for c in ["Pause_h", "Stunden", "Reisezeit_h"]:
        df2[c] = pd.to_numeric(df2[c], errors="coerce").fillna(0.0)
    return _write_csv_to_folder(PROJECT_REPORTS_FOLDER_ID, reports_filename(project_name), df2, file_id)


def append_report(project_name: str, row: dict) -> bool:
    df, fid = get_reports_df(project_name)
    df2 = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    return save_reports_df(project_name, df2, fid)


def get_employees_df():
    df, fid = _read_csv_from_folder(
        PROJECT_REPORTS_FOLDER_ID,
        EMPLOYEES_CSV_NAME,
        EMPLOYEES_COLS,
        defaults={"Status": "aktiv", "Stundenlohn": 0.0},
    )
    # normalize
    for c in ["EmployeeID", "Name", "Rolle", "PIN", "Status"]:
        df[c] = df[c].astype(str).replace("nan", "").fillna("").str.strip()
    df["Stundenlohn"] = pd.to_numeric(df["Stundenlohn"], errors="coerce").fillna(0.0)
    df["Status"] = df["Status"].apply(lambda x: x if x in ["aktiv", "inaktiv"] else "aktiv")
    return df, fid


def save_employees_df(df: pd.DataFrame, file_id: str | None) -> bool:
    df2 = _ensure_cols(df, EMPLOYEES_COLS, defaults={"Status": "aktiv", "Stundenlohn": 0.0})
    # cleanup
    df2["EmployeeID"] = df2["EmployeeID"].astype(str).replace("nan", "").fillna("").str.strip()
    df2["Name"] = df2["Name"].astype(str).replace("nan", "").fillna("").str.strip()
    df2["PIN"] = df2["PIN"].astype(str).replace("nan", "").fillna("").str.strip()
    df2["Status"] = df2["Status"].apply(lambda x: x if x in ["aktiv", "inaktiv"] else "aktiv")
    df2["Stundenlohn"] = pd.to_numeric(df2["Stundenlohn"], errors="coerce").fillna(0.0)

    # Auto-ID if missing
    for idx, row in df2.iterrows():
        if not str(row.get("EmployeeID", "")).strip() and str(row.get("Name", "")).strip():
            base = re.sub(r"[^A-Za-z0-9]+", "", str(row["Name"]).upper())[:6]
            df2.at[idx, "EmployeeID"] = base or str(uuid4())[:6].upper()

    return _write_csv_to_folder(PROJECT_REPORTS_FOLDER_ID, EMPLOYEES_CSV_NAME, df2, file_id)


def get_closures_df():
    df, fid = _read_csv_from_folder(
        TIME_REPORTS_FOLDER_ID,
        CLOSURES_CSV_NAME,
        CLOSURES_COLS,
        defaults={"Signed": True},
    )
    for c in ["TotalStunden", "TotalReisezeit_h"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df, fid


def append_closure(row: dict) -> bool:
    df, fid = get_closures_df()
    df2 = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df2 = _ensure_cols(df2, CLOSURES_COLS, defaults={"Signed": True})
    return _write_csv_to_folder(TIME_REPORTS_FOLDER_ID, CLOSURES_CSV_NAME, df2, fid)


def get_azk_df():
    df, fid = _read_csv_from_folder(
        TIME_REPORTS_FOLDER_ID,
        AZK_CSV_NAME,
        AZK_COLS,
        defaults=None,
    )
    return df, fid


def upsert_azk_row(row: dict) -> bool:
    df, fid = get_azk_df()
    df2 = pd.DataFrame([row])
    df2 = _ensure_cols(df2, AZK_COLS)

    if df.empty:
        out = df2
    else:
        key_cols = ["Jahr", "KW", "EmployeeID", "ProjektID"]
        for k in key_cols:
            df[k] = df[k].astype(str)
            df2[k] = df2[k].astype(str)
        mask = (
            (df["Jahr"] == str(row.get("Jahr")))
            & (df["KW"] == str(row.get("KW")))
            & (df["EmployeeID"] == str(row.get("EmployeeID")))
            & (df["ProjektID"] == str(row.get("ProjektID")))
        )
        if mask.any():
            idx = df[mask].index[0]
            for c in AZK_COLS:
                df.at[idx, c] = df2.iloc[0][c]
            out = df
        else:
            out = pd.concat([df, df2], ignore_index=True)

    out = _ensure_cols(out, AZK_COLS)
    return _write_csv_to_folder(TIME_REPORTS_FOLDER_ID, AZK_CSV_NAME, out, fid)


# =========================
# WEEK HELPERS
# =========================
def iso_year_week(d) -> tuple[int, int]:
    iso = d.isocalendar()
    return int(iso[0]), int(iso[1])


def week_date_range(year: int, week: int) -> tuple[datetime, datetime]:
    start = datetime.fromisocalendar(year, week, 1)
    end = datetime.fromisocalendar(year, week, 7)
    return start, end


# =========================
# UI MODE
# =========================
st.sidebar.title("Menü")

default_mode = 0
if st.query_params.get("mode") == "admin":
    default_mode = 1

mode = st.sidebar.radio("Bereich", ["👷 Mitarbeiter", "🛠️ Admin"], index=default_mode)

target_project_from_qr = st.query_params.get("project", None)


# =========================
# 👷 MITARBEITER
# =========================
if mode == "👷 Mitarbeiter":
    st.title("👷 Mitarbeiterbereich")

    if BASE_APP_URL:
        emp_link = f"{BASE_APP_URL}?embed=true"
        st.info(
            "📲 **BauApp als Verknüpfung speichern**\n\n"
            f"👉 Öffne diesen Link: {emp_link}\n\n"
            "• iPhone (Safari): Teilen → Zum Home-Bildschirm\n"
            "• Android (Chrome): ⋮ → Zum Startbildschirm hinzufügen / App installieren"
        )
        st.link_button("🔗 Mitarbeiter-Link öffnen", emp_link)

        admin_link_public = f"{BASE_APP_URL}?mode=admin"
        with st.expander("🛠️ Admin öffnen"):
            st.code(admin_link_public)
            st.link_button("🔐 Admin-Link öffnen", admin_link_public)

    active_projects = get_active_projects()
    if not active_projects:
        st.warning("Keine aktiven Projekte.")
        st.stop()

    default_index = 0
    if target_project_from_qr and target_project_from_qr in active_projects:
        default_index = active_projects.index(target_project_from_qr)
        st.success(f"📍 Direkteinstieg via QR-Code: {target_project_from_qr}")
    elif target_project_from_qr:
        st.warning(f"Projekt '{target_project_from_qr}' ist nicht aktiv oder existiert nicht.")

    project = st.selectbox("Projekt wählen", active_projects, index=default_index)
    projects_df, _ = get_projects_df()
    proj_rec = get_project_record(project, projects_df=projects_df)

    # Projektdaten anzeigen (wie Scan)
    if proj_rec:
        info_parts = []
        for k, label in [
            ("Auftragsnr", "Auftragsnr."),
            ("Objekt", "Objekt"),
            ("Kunde", "Kunde"),
            ("Telefon", "Telefon"),
            ("Kontaktperson", "Kontaktperson"),
            ("Kontakttelefon", "Kontakt-Tel."),
        ]:
            if str(proj_rec.get(k, "")).strip():
                info_parts.append(f"**{label}:** {proj_rec.get(k)}")
        if info_parts:
            st.markdown("#### Projektdaten")
            st.info(" · ".join(info_parts))

    t1, t2, t3 = st.tabs(["📝 Rapport", "📷 Fotos", "📂 Pläne"])

    # -------- RAPPORT --------
    with t1:
        st.subheader("Rapport erfassen")

        df_emp, _ = get_employees_df()
        df_emp_active = df_emp[df_emp["Status"] == "aktiv"].copy() if not df_emp.empty else df_emp

        c1, c2, c3 = st.columns(3)
        date_val = c1.date_input("Datum", datetime.now())

        ma_sel = None
        if df_emp_active.empty:
            st.warning("Keine Mitarbeiter vorhanden. Admin → Mitarbeiter anlegen.")
        else:
            emp_options = df_emp_active.apply(
                lambda r: f'{r["Name"]} ({r["EmployeeID"]})', axis=1
            ).tolist()
            emp_map = {emp_options[i]: df_emp_active.iloc[i].to_dict() for i in range(len(emp_options))}
            ma_key = c1.selectbox("Mitarbeiter", emp_options)
            ma_sel = emp_map.get(ma_key)

        # Fahrtzeiten (SPV)
        ank_mag = None
        abd_mag = None
        reise_home_to_site_min = 0
        reise_site_to_home_min = 0
        reise_direkt_total_min = 0
        reise_bezahlt_min = 0
        reise_bezahlt_h = 0.0
        reise_regel = "SPV: Bezahlt nur Direktfahrt Zuhause↔Baustelle; pro Richtung 30 Min Selbstbehalt. Magazinfahrten unbezahlt."

        with st.expander("🚗 Fahrtzeiten (SPV-konform)", expanded=False):
            st.caption(
                "Regel: Magazin↔Baustelle ist nicht bezahlte Fahrtzeit. "
                "Bezahlt wird nur Direktfahrt Zuhause↔Baustelle, pro Richtung minus 30 Min Selbstbehalt."
            )
            ec1, ec2, ec3 = st.columns(3)
            has_ank = ec1.checkbox("Ankunft Magazin erfassen", value=False, key="ank_mag_chk")
            ank_mag = ec1.time_input("Ankunft Magazin (Uhrzeit)", time(0, 0), key="ank_mag_time") if has_ank else None

            has_abd = ec2.checkbox("Abfahrt Magazin erfassen", value=False, key="abd_mag_chk")
            abd_mag = ec2.time_input("Abfahrt Magazin (Uhrzeit)", time(0, 0), key="abd_mag_time") if has_abd else None

            reise_home_to_site_min = int(ec3.number_input("Direkt: Zuhause → Baustelle (Min)", 0, 600, 0, 5))
            reise_site_to_home_min = int(ec3.number_input("Direkt: Baustelle → Zuhause (Min)", 0, 600, 0, 5))

            reise_direkt_total_min = reise_home_to_site_min + reise_site_to_home_min
            bezahlt_h2s = max(0, reise_home_to_site_min - 30) if reise_home_to_site_min > 0 else 0
            bezahlt_s2h = max(0, reise_site_to_home_min - 30) if reise_site_to_home_min > 0 else 0

            reise_bezahlt_min = bezahlt_h2s + bezahlt_s2h
            reise_bezahlt_h = round(reise_bezahlt_min / 60.0, 2)

            st.info(
                f"Direkt: H→B {reise_home_to_site_min} (bezahlt {bezahlt_h2s}), "
                f"B→H {reise_site_to_home_min} (bezahlt {bezahlt_s2h}) → "
                f"Bezahlt: {reise_bezahlt_min} Min (= {reise_bezahlt_h} h)"
            )

        start_val = c2.time_input("Start", time(7, 0))
        end_val = c2.time_input("Ende", time(16, 0))
        pause_val = c3.number_input("Pause (h)", 0.0, 5.0, 0.5, 0.25)

        mat_val = c3.text_input("Material")
        rem_val = st.text_area("Bemerkung")

        dt_start = datetime.combine(datetime.today(), start_val)
        dt_end = datetime.combine(datetime.today(), end_val)
        if dt_end < dt_start:
            dt_end += timedelta(days=1)

        dur = round(max(0.0, (dt_end - dt_start).total_seconds() / 3600 - float(pause_val)), 2)
        st.info(f"Arbeitsstunden (ohne Fahrt): {dur}")

        if st.button("✅ Speichern", type="primary"):
            if not ma_sel:
                st.error("Mitarbeiter fehlt.")
            else:
                ok = append_report(
                    project,
                    {
                        "Datum": str(date_val),
                        "Projekt": project,
                        "ProjektID": str(proj_rec.get("ProjektID", "")).strip(),
                        "Auftragsnr": str(proj_rec.get("Auftragsnr", "")).strip(),
                        "Objekt": str(proj_rec.get("Objekt", "")).strip(),
                        "Kunde": str(proj_rec.get("Kunde", "")).strip(),
                        "Telefon": str(proj_rec.get("Telefon", "")).strip(),
                        "Kontaktperson": str(proj_rec.get("Kontaktperson", "")).strip(),
                        "Kontakttelefon": str(proj_rec.get("Kontakttelefon", "")).strip(),
                        "EmployeeID": str(ma_sel.get("EmployeeID", "")).strip(),
                        "Mitarbeiter": str(ma_sel.get("Name", "")).strip(),
                        "AnkunftMagazin": str(ank_mag) if ank_mag else "",
                        "AbfahrtMagazin": str(abd_mag) if abd_mag else "",
                        "ReiseHomeToSiteMin": int(reise_home_to_site_min),
                        "ReiseSiteToHomeMin": int(reise_site_to_home_min),
                        "ReiseDirektMin": int(reise_direkt_total_min),
                        "ReiseBezahltMin": int(reise_bezahlt_min),
                        "ReiseRegel": str(reise_regel),
                        "Reisezeit_h": float(reise_bezahlt_h),
                        "Start": str(start_val),
                        "Ende": str(end_val),
                        "Pause_h": float(pause_val),
                        "Stunden": float(dur),
                        "Material": str(mat_val),
                        "Bemerkung": str(rem_val),
                    },
                )
                if ok:
                    st.success("Gespeichert ✅")
                    sys_time.sleep(0.2)
                    st.rerun()
                else:
                    st.error("Speichern fehlgeschlagen (Drive).")

        st.divider()
        st.subheader("Woche abschliessen (Signatur)")

        if not ma_sel:
            st.info("Mitarbeiter auswählen, um eine Woche abzuschliessen.")
        else:
            emp_id = str(ma_sel.get("EmployeeID", "")).strip()

            # Alle Projekt-Reports zusammenziehen (nur für den Mitarbeiter)
            frames = []
            for p in get_all_projects():
                dfr, _ = get_reports_df(p)
                if not dfr.empty:
                    frames.append(dfr)

            df_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=RAPPORT_COLS)
            if not df_all.empty:
                df_all["Datum_dt"] = pd.to_datetime(df_all["Datum"], errors="coerce").dt.date
                df_all = df_all[df_all["EmployeeID"].astype(str).str.strip() == emp_id]

            weeks = []
            if not df_all.empty:
                for d in df_all["Datum_dt"].dropna().unique():
                    y, w = iso_year_week(d)
                    weeks.append((y, w))
            today = datetime.now().date()
            cy, cw = iso_year_week(today)
            if (cy, cw) not in weeks:
                weeks = [(cy, cw)] + weeks
            weeks = sorted(set(weeks), reverse=True)

            week_labels = [f"{y}-KW{w:02d}" for (y, w) in weeks] if weeks else [f"{cy}-KW{cw:02d}"]
            chosen = st.selectbox("Kalenderwoche", week_labels, index=0)
            y = int(chosen.split("-KW")[0])
            w = int(chosen.split("-KW")[1])
            w_start, w_end = week_date_range(y, w)
            st.caption(f"Zeitraum: {w_start.date()} – {w_end.date()}")

            df_week = df_all.copy()
            if not df_week.empty:
                mask = (df_week["Datum_dt"] >= w_start.date()) & (df_week["Datum_dt"] <= w_end.date())
                df_week = df_week[mask].copy()

            total_hours = float(pd.to_numeric(df_week.get("Stunden", 0), errors="coerce").fillna(0).sum()) if not df_week.empty else 0.0
            total_travel = float(pd.to_numeric(df_week.get("Reisezeit_h", 0), errors="coerce").fillna(0).sum()) if not df_week.empty else 0.0

            csum1, csum2 = st.columns(2)
            csum1.metric("Total Arbeitsstunden", round(total_hours, 2))
            csum2.metric("Total Reisezeit (h)", round(total_travel, 2))

            pin_in = st.text_input("PIN eingeben (nur für Signatur)", type="password")

            if st.button("✅ Woche abschliessen", type="primary"):
                pin_expected = str(ma_sel.get("PIN", "")).strip()
                if not pin_expected:
                    st.error("Für diesen Mitarbeiter ist kein PIN hinterlegt (Admin: Mitarbeiter bearbeiten).")
                elif str(pin_in).strip() != pin_expected:
                    st.error("PIN falsch. Keine Unterschrift.")
                else:
                    okc = append_closure(
                        {
                            "EmployeeID": emp_id,
                            "Mitarbeiter": str(ma_sel.get("Name", "")).strip(),
                            "Jahr": int(y),
                            "KW": int(w),
                            "Von": str(w_start.date()),
                            "Bis": str(w_end.date()),
                            "TotalStunden": round(total_hours, 2),
                            "TotalReisezeit_h": round(total_travel, 2),
                            "Timestamp": datetime.now().isoformat(timespec="seconds"),
                            "Signed": True,
                        }
                    )
                    if okc:
                        # AZK updaten: pro Projekt in dieser Woche summieren
                        if not df_week.empty:
                            df_week["ProjektID"] = df_week["ProjektID"].astype(str)
                            df_week["Projekt"] = df_week["Projekt"].astype(str)
                            df_week["Stunden_num"] = pd.to_numeric(df_week["Stunden"], errors="coerce").fillna(0.0)
                            df_week["Reise_num"] = pd.to_numeric(df_week["Reisezeit_h"], errors="coerce").fillna(0.0)

                            grp = (
                                df_week.groupby(["ProjektID", "Projekt"], dropna=False)[["Stunden_num", "Reise_num"]]
                                .sum()
                                .reset_index()
                            )
                            for _, rr in grp.iterrows():
                                _ = upsert_azk_row(
                                    {
                                        "Jahr": str(y),
                                        "KW": str(w),
                                        "EmployeeID": emp_id,
                                        "Mitarbeiter": str(ma_sel.get("Name", "")).strip(),
                                        "ProjektID": str(rr.get("ProjektID", "")).strip(),
                                        "Projekt": str(rr.get("Projekt", "")).strip(),
                                        "TotalStunden": round(float(rr.get("Stunden_num", 0.0)), 2),
                                        "TotalReisezeit_h": round(float(rr.get("Reise_num", 0.0)), 2),
                                        "Timestamp": datetime.now().isoformat(timespec="seconds"),
                                    }
                                )
                        st.success("Woche abgeschlossen / signiert ✅ (Closures + AZK aktualisiert)")
                    else:
                        st.error("Konnte Signatur nicht speichern (Drive).")

        st.divider()
        st.subheader("Rapporte im Projekt (aktuell)")

        df_h, _ = get_reports_df(project)
        if df_h.empty:
            st.info("Noch keine Rapporte für dieses Projekt.")
        else:
            df_view = df_h.copy()
            df_view["Datum"] = pd.to_datetime(df_view["Datum"], errors="coerce")
            df_view = df_view.sort_values(["Datum"], ascending=False)
            st.dataframe(df_view.head(100), use_container_width=True)

    # -------- FOTOS --------
    with t2:
        st.subheader("Fotos")

        cloudrun_upload_widget(
            project=project,
            upload_type="photo",
            title="Foto(s) hochladen",
            help_text="Nur Bilder (Kamera/Galerie).",
            accept="image/*",
            multiple=True,
            height=240,
        )

        if st.button("🔄 Fotos aktualisieren", key="refresh_photos_emp"):
            st.rerun()

        files = list_files(PHOTOS_FOLDER_ID, mime_prefix="image/", page_size=500)
        proj_photos = [x for x in files if x["name"].startswith(project + "_")][:120]

        if not proj_photos:
            st.info("Keine Fotos vorhanden.")
        else:
            for f in proj_photos:
                with st.expander(f"🖼️ {f['name']}", expanded=False):
                    image_preview_from_drive(f["id"])

    # -------- PLÄNE --------
    with t3:
        st.subheader("Pläne & Dokumente")

        if st.button("🔄 Pläne aktualisieren", key="refresh_docs_emp"):
            st.rerun()

        files = list_files(PLANS_FOLDER_ID, page_size=500)
        proj_docs = [x for x in files if x["name"].startswith(project + "_")][:300]

        if not proj_docs:
            st.info("Keine Dokumente hinterlegt.")
        else:
            for f in proj_docs:
                d = download_bytes(f["id"])
                if not d:
                    continue
                c1, c2 = st.columns([0.8, 0.2])
                c1.write(f"📄 {f['name']}")
                c2.download_button("⬇️ Download", d, file_name=f["name"])


# =========================
# 🛠️ ADMIN
# =========================
else:
    st.title("🛠️ Admin")
    pin = st.text_input("PIN", type="password")
    if pin != ADMIN_PIN:
        st.stop()

    st.success("Angemeldet ✅")

    if BASE_APP_URL:
        admin_link = f"{BASE_APP_URL}?mode=admin"
        st.info(f"Admin-Link: {admin_link}")
        st.link_button("🔗 Admin-Link öffnen", admin_link)

    tabA, tabB, tabC, tabD, tabE = st.tabs(
        ["📌 Projekte", "📂 Uploads & Übersicht", "🧾 Rapporte", "👤 Mitarbeiter", "📤 Exporte"]
    )

    # --- Projekte ---
    with tabA:
        st.subheader("Projekte verwalten")
        df_p, pid = get_projects_df()

        c1, c2 = st.columns([0.7, 0.3])
        new_p = c1.text_input("Neues Projekt")

        if c2.button("➕ Anlegen") and new_p.strip():
            project_name = new_p.strip()
            df_p = pd.concat([df_p, pd.DataFrame([{"Projekt": project_name, "Status": "aktiv"}])], ignore_index=True)
            ok = save_projects_df(df_p, pid)
            if ok:
                st.success(f"Projekt '{project_name}' angelegt ✅")
                st.divider()
                st.subheader(f"QR-Code für {project_name}")
                qr_bytes = generate_project_qr(project_name)
                st.image(qr_bytes, width=220)
                st.download_button("⬇️ QR-Code (.png)", qr_bytes, file_name=f"QR_{project_name}.png", mime="image/png")
                sys_time.sleep(0.2)
                st.rerun()
            else:
                st.error("Konnte Projects.csv nicht speichern (Drive).")

        if df_p.empty:
            st.info("Noch keine Projekte.")
        else:
            st.caption("Stammdaten pflegen (werden im Mitarbeiter-Rapport angezeigt).")
            df_edit = st.data_editor(
                df_p,
                use_container_width=True,
                num_rows="dynamic",
                disabled=["ProjektID"],
                key="proj_editor",
            )
            if st.button("💾 Projekte speichern", type="primary"):
                ok = save_projects_df(df_edit, pid)
                if ok:
                    st.success("Projekte gespeichert ✅")
                    sys_time.sleep(0.2)
                    st.rerun()
                else:
                    st.error("Konnte Projects.csv nicht speichern (Drive).")

            st.divider()
            st.subheader("Projekt Status ändern")
            all_projs = df_p["Projekt"].astype(str).tolist()
            sel = st.selectbox("Projekt wählen", all_projs, key="admin_status_proj")
            new_status = st.radio("Neuer Status", ["aktiv", "archiviert"], horizontal=True)
            if st.button("Status speichern"):
                df_p.loc[df_p["Projekt"] == sel, "Status"] = new_status
                save_projects_df(df_p, pid)
                st.success("Status geändert ✅")
                sys_time.sleep(0.2)
                st.rerun()

    # --- Uploads & Übersicht ---
    with tabB:
        st.subheader("Uploads & Übersicht (wie Mitarbeiter, plus Löschen)")
        projs_all = get_all_projects()
        if not projs_all:
            st.info("Erstelle zuerst ein Projekt.")
            st.stop()

        sel_p = st.selectbox("Projekt", projs_all, key="admin_sel_project_upload")

        cX, cY = st.columns(2)

        with cX:
            st.markdown("### 📷 Fotos (Cloud Run)")
            cloudrun_upload_widget(
                project=sel_p,
                upload_type="photo",
                title="Foto(s) hochladen",
                help_text="Nur Bilder (Kamera/Galerie).",
                accept="image/*",
                multiple=True,
                height=240,
            )

        with cY:
            st.markdown("### 📄 Pläne/Dokumente (direkt nach Drive)")
            admin_docs_files = st.file_uploader(
                "Dokumente auswählen",
                type=None,
                accept_multiple_files=True,
                key="admin_docs_uploader",
            )

            if st.button("📤 Dokument(e) speichern", type="primary"):
                if not admin_docs_files:
                    st.warning("Bitte zuerst Dokumente auswählen.")
                else:
                    ok_n, fail_n = 0, 0
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    for f in admin_docs_files:
                        try:
                            data = f.getvalue()
                            mime = getattr(f, "type", None) or "application/octet-stream"
                            filename = f"{sel_p}_{ts}_{f.name}"
                            if upload_bytes_to_drive(data, PLANS_FOLDER_ID, filename, mime):
                                ok_n += 1
                            else:
                                fail_n += 1
                        except Exception:
                            fail_n += 1

                    if fail_n == 0:
                        st.success(f"✅ {ok_n} Datei(en) gespeichert.")
                    else:
                        st.warning(f"⚠️ {ok_n} ok, {fail_n} fehlgeschlagen.")
                    sys_time.sleep(0.2)
                    st.rerun()

        st.divider()
        tabF, tabDocs = st.tabs(["📷 Fotos – Übersicht", "📄 Pläne/Dokumente – Übersicht"])

        with tabF:
            c1, c2 = st.columns([0.7, 0.3])
            c1.subheader("Fotos – Vorschau")
            if c2.button("🔄 Aktualisieren", key="admin_refresh_photos"):
                st.rerun()

            files_ph = list_files(PHOTOS_FOLDER_ID, mime_prefix="image/", page_size=500)
            admin_photos = [x for x in files_ph if x["name"].startswith(sel_p + "_")][:240]

            if not admin_photos:
                st.info("Keine Fotos vorhanden.")
            else:
                cols = st.columns(3)
                for idx, f in enumerate(admin_photos):
                    col = cols[idx % 3]
                    with col:
                        with st.expander(f"🖼️ {f['name']}", expanded=False):
                            image_preview_from_drive(f["id"])
                            if st.button("🗑 Foto löschen", key=f"adm_del_photo_{f['id']}"):
                                delete_file(f["id"])
                                st.success("Gelöscht ✅")
                                sys_time.sleep(0.2)
                                st.rerun()

        with tabDocs:
            c1, c2 = st.columns([0.7, 0.3])
            c1.subheader("Pläne/Dokumente – Download")
            if c2.button("🔄 Aktualisieren", key="admin_refresh_docs"):
                st.rerun()

            files_docs = list_files(PLANS_FOLDER_ID, page_size=500)
            admin_docs = [x for x in files_docs if x["name"].startswith(sel_p + "_")][:500]

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
                        st.success("Gelöscht ✅")
                        sys_time.sleep(0.2)
                        st.rerun()

    # --- Rapporte ---
    with tabC:
        st.subheader("Rapporte ansehen (pro Projekt)")
        projs = get_all_projects()
        if not projs:
            st.info("Keine Projekte vorhanden.")
            st.stop()

        sel_rp = st.selectbox("Projekt", projs, key="admin_sel_reports")
        if st.button("🔄 Aktualisieren", key="admin_refresh_reports"):
            st.rerun()

        df_r, _ = get_reports_df(sel_rp)
        if df_r.empty:
            st.info("Keine Rapporte vorhanden.")
        else:
            df_view = df_r.copy()
            df_view["Datum"] = pd.to_datetime(df_view["Datum"], errors="coerce")
            df_view = df_view.sort_values(["Datum"], ascending=False)
            st.dataframe(df_view, use_container_width=True)
            st.download_button(
                "⬇️ CSV herunterladen",
                df_view.to_csv(index=False).encode("utf-8"),
                file_name=f"{sel_rp}_Reports.csv",
            )

    # --- Mitarbeiter ---
    with tabD:
        st.subheader("Mitarbeiter verwalten")
        st.caption("PIN wird nur für Wochenabschluss/Signatur verwendet (kein Login).")

        df_emp, emp_fid = get_employees_df()
        if df_emp.empty:
            df_emp = pd.DataFrame(columns=EMPLOYEES_COLS)

        edited = st.data_editor(
            df_emp,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "EmployeeID": st.column_config.TextColumn("EmployeeID"),
                "Name": st.column_config.TextColumn("Name"),
                "Rolle": st.column_config.TextColumn("Rolle"),
                "Stundenlohn": st.column_config.NumberColumn("Stundenlohn", step=0.05),
                "PIN": st.column_config.TextColumn("PIN"),
                "Status": st.column_config.SelectboxColumn("Status", options=["aktiv", "inaktiv"]),
            },
        )

        if st.button("💾 Mitarbeiter speichern", type="primary"):
            ok = save_employees_df(edited, emp_fid)
            if ok:
                st.success("Mitarbeiter gespeichert ✅")
                sys_time.sleep(0.2)
                st.rerun()
            else:
                st.error("Konnte Employees.csv nicht speichern (Drive).")

        st.divider()
        st.subheader("Wochenabschlüsse / Signaturen (Closures)")
        df_cl, cl_fid = get_closures_df()
        if df_cl.empty:
            st.info("Noch keine Wochenabschlüsse vorhanden.")
        else:
            st.dataframe(df_cl.sort_values(["Jahr", "KW"], ascending=False), use_container_width=True)
            st.download_button(
                "⬇️ Closures.csv herunterladen",
                df_cl.to_csv(index=False).encode("utf-8"),
                file_name="Closures.csv",
            )

            st.caption("Admin kann einzelne Einträge entfernen (reversibel).")
            del_idx = st.number_input("Zeilenindex löschen (0..n-1)", min_value=0, max_value=max(0, len(df_cl) - 1), value=0, step=1)
            if st.button("🗑️ Signatur-Zeile löschen"):
                df2 = df_cl.drop(index=int(del_idx)).reset_index(drop=True)
                ok = _write_csv_to_folder(TIME_REPORTS_FOLDER_ID, CLOSURES_CSV_NAME, df2, cl_fid)
                if ok:
                    st.success("Gelöscht ✅")
                    sys_time.sleep(0.2)
                    st.rerun()
                else:
                    st.error("Löschen fehlgeschlagen (Drive).")

        st.divider()
        st.subheader("AZK (Arbeitszeit-Kontrolle)")
        df_azk, _ = get_azk_df()
        if df_azk.empty:
            st.info("Noch keine AZK Daten.")
        else:
            st.dataframe(df_azk.sort_values(["Jahr", "KW"], ascending=False), use_container_width=True)
            st.download_button("⬇️ AZK.csv herunterladen", df_azk.to_csv(index=False).encode("utf-8"), file_name="AZK.csv")

    # --- Exporte ---
    with tabE:
        st.subheader("Exporte (mit Bearbeitung vor Export)")

        projs = get_all_projects()
        if not projs:
            st.info("Keine Projekte vorhanden.")
            st.stop()

        sel = st.selectbox("Projekt", projs, key="exp_proj")
        df_r, rep_file_id = get_reports_df(sel)

        if df_r.empty:
            st.info("Keine Rapporte für dieses Projekt.")
        else:
            st.markdown("### ✏️ Rapporte bearbeiten (vor Export)")
            edited_df = st.data_editor(df_r, use_container_width=True, num_rows="dynamic", key=f"edit_reports_{sel}")

            if st.button("💾 Änderungen speichern", type="primary"):
                ok = save_reports_df(sel, edited_df, rep_file_id)
                if ok:
                    st.success("Gespeichert ✅ (Drive aktualisiert)")
                    sys_time.sleep(0.2)
                    st.rerun()
                else:
                    st.error("Speichern fehlgeschlagen (Drive).")

            st.divider()

            # Normalize
            df_r2 = edited_df.copy()
            df_r2["Datum_dt"] = pd.to_datetime(df_r2["Datum"], errors="coerce").dt.date
            df_r2["Stunden_num"] = pd.to_numeric(df_r2["Stunden"], errors="coerce").fillna(0.0)
            df_r2["Reise_num"] = pd.to_numeric(df_r2.get("Reisezeit_h", 0), errors="coerce").fillna(0.0)

            # Baustellenrapporte Export (CSV)
            st.markdown("### Export: Baustellenrapporte (CSV)")
            export_cols = [
                "Jahr", "KW", "Auftrag erfasst!!", "Rechnung gestellt", "Zeit Stempel",
                "Firma", "Baustelle", "MA", "Datum",
                "Vormittag ab", "Vormittag bis", "Std. Vorm.",
                "Nachmittag ab", "Nachmittag bis", "Std. Nachm.",
                "Kürzel", "Notizen/Spezieles", "Total Std. V. & N."
            ]

            rows = []
            for _, r in df_r2.iterrows():
                d = r.get("Datum_dt")
                if pd.isna(d) or d is None:
                    continue
                y, w = iso_year_week(d)
                rows.append({
                    "Jahr": y,
                    "KW": w,
                    "Auftrag erfasst!!": "",
                    "Rechnung gestellt": "",
                    "Zeit Stempel": "",
                    "Firma": "RBAG",
                    "Baustelle": sel,
                    "MA": str(r.get("Mitarbeiter", "")),
                    "Datum": str(d),
                    "Vormittag ab": str(r.get("Start", "")),
                    "Vormittag bis": str(r.get("Ende", "")),
                    "Std. Vorm.": float(r.get("Stunden_num", 0.0)),
                    "Nachmittag ab": "",
                    "Nachmittag bis": "",
                    "Std. Nachm.": "",
                    "Kürzel": str(r.get("EmployeeID", "")),
                    "Notizen/Spezieles": str(r.get("Bemerkung", "")),
                    "Total Std. V. & N.": float(r.get("Stunden_num", 0.0)),
                })

            df_exp = pd.DataFrame(rows, columns=export_cols)
            st.download_button(
                "⬇️ Baustellenrapporte_export.csv",
                df_exp.to_csv(index=False).encode("utf-8"),
                file_name=f"{sel}_Baustellenrapporte_export.csv",
            )

            # AZK Basisexport (CSV)
            st.markdown("### Export: AZK Basis (CSV)")
            df_azk_out = pd.DataFrame({
                "Datum": df_r2["Datum_dt"].astype(str),
                "Projekt": sel,
                "EmployeeID": df_r2.get("EmployeeID", "").astype(str),
                "Mitarbeiter": df_r2.get("Mitarbeiter", "").astype(str),
                "Stunden": df_r2["Stunden_num"],
                "Reisezeit_h": df_r2["Reise_num"],
                "Material": df_r2.get("Material", "").astype(str),
                "Bemerkung": df_r2.get("Bemerkung", "").astype(str),
            })
            st.download_button(
                "⬇️ AZK_Rapporte_export.csv",
                df_azk_out.to_csv(index=False).encode("utf-8"),
                file_name=f"{sel}_AZK_Rapporte_export.csv",
            )
