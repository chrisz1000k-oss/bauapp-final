import io
import os
import re
import time as sys_time
import base64
import html as html_lib
from dataclasses import dataclass
from datetime import datetime, time, timedelta, date
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


# =========================================================
# UI / PAGE
# =========================================================
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

BASE_APP_URL = st.secrets.get("BASE_APP_URL", "").strip()
# Falls nicht gesetzt: Streamlit setzt die korrekte URL sowieso für den Nutzer; QR-Codes brauchen sie aber fix.
if not BASE_APP_URL:
    # Fallback: funktionieren tut die App auch ohne – QR-Code-Link kann dann manuell ersetzt werden.
    BASE_APP_URL = "https://REPLACE_WITH_YOUR_STREAMLIT_URL"


logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
if os.path.exists(logo_path):
    st.sidebar.image(logo_path, use_container_width=True)


# =========================================================
# SECRETS (robust + sauber)
# =========================================================
def _sget(key: str, default: str = "") -> str:
    try:
        v = st.secrets.get(key, default)
        return str(v).strip()
    except Exception:
        return default


def _require(key: str) -> str:
    v = _sget(key, "")
    if not v:
        st.error(f"Fehlendes Secret: {key}")
        st.stop()
    return v


GOOGLE_CLIENT_ID = _require("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = _require("GOOGLE_CLIENT_SECRET")
GOOGLE_REFRESH_TOKEN = _require("GOOGLE_REFRESH_TOKEN")

# Neue saubere Keys
PHOTOS_FOLDER_ID = _require("PHOTOS_FOLDER_ID")
PLANS_FOLDER_ID = _sget("PLANS_FOLDER_ID", "")
PROJECT_REPORTS_FOLDER_ID = _sget("PROJECT_REPORTS_FOLDER_ID", "")
TIME_REPORTS_FOLDER_ID = _sget("TIME_REPORTS_FOLDER_ID", "")

# Backward kompatibel (falls du noch alte Keys drin hast)
if not PLANS_FOLDER_ID:
    PLANS_FOLDER_ID = _sget("UPLOADS_FOLDER_ID", "")
if not PROJECT_REPORTS_FOLDER_ID:
    PROJECT_REPORTS_FOLDER_ID = _sget("REPORTS_FOLDER_ID", "")
if not TIME_REPORTS_FOLDER_ID:
    # falls noch nicht getrennt: wenigstens funktionsfähig
    TIME_REPORTS_FOLDER_ID = PROJECT_REPORTS_FOLDER_ID

if not PLANS_FOLDER_ID or not PROJECT_REPORTS_FOLDER_ID or not TIME_REPORTS_FOLDER_ID:
    st.error(
        "Fehlende Ordner-IDs: Bitte setze PLANS_FOLDER_ID, PROJECT_REPORTS_FOLDER_ID, TIME_REPORTS_FOLDER_ID "
        "(oder als Fallback UPLOADS_FOLDER_ID / REPORTS_FOLDER_ID)."
    )
    st.stop()

ADMIN_PIN = _sget("ADMIN_PIN", "1234")

upload_section = st.secrets.get("upload_service")
if not upload_section:
    st.error("Fehler: Sektion [upload_service] fehlt in secrets.")
    st.stop()

UPLOAD_SERVICE_URL = str(upload_section.get("url", "")).strip().rstrip("/")
UPLOAD_SERVICE_TOKEN = str(upload_section.get("token", "")).strip()

if not UPLOAD_SERVICE_URL or not UPLOAD_SERVICE_TOKEN:
    st.error("Fehler: [upload_service] url/token leer oder fehlt.")
    st.stop()


# =========================================================
# GOOGLE DRIVE SERVICE
# =========================================================
@st.cache_resource(show_spinner=False)
def get_drive_service():
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


drive = get_drive_service()


# =========================================================
# DRIVE HELPERS
# =========================================================
def drive_list_files(folder_id: str, *, mime_contains: str | None = None, limit: int = 400) -> list[dict]:
    try:
        q = f"'{folder_id}' in parents and trashed=false"
        if mime_contains:
            q += f" and mimeType contains '{mime_contains}'"
        res = (
            drive.files()
            .list(
                q=q,
                pageSize=min(limit, 1000),
                fields="files(id,name,mimeType,createdTime,size)",
                orderBy="createdTime desc",
            )
            .execute()
        )
        return res.get("files", [])
    except Exception as e:
        st.error(f"Drive: Fehler beim Listen: {e}")
        return []


def drive_download_bytes(file_id: str) -> bytes | None:
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


def drive_upload_bytes(data: bytes, folder_id: str, filename: str, mimetype: str) -> str | None:
    try:
        meta = {"name": filename, "parents": [folder_id]}
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
        res = drive.files().create(body=meta, media_body=media, fields="id").execute()
        return res.get("id")
    except Exception as e:
        st.error(f"Drive: Upload Fehler: {e}")
        return None


def drive_update_file(file_id: str, data: bytes, mimetype: str = "text/csv") -> bool:
    try:
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
        drive.files().update(fileId=file_id, media_body=media).execute()
        return True
    except Exception as e:
        st.error(f"Drive: Update Fehler: {e}")
        return False


def drive_delete(file_id: str) -> bool:
    try:
        drive.files().delete(fileId=file_id).execute()
        return True
    except Exception as e:
        st.error(f"Drive: Löschen Fehler: {e}")
        return False


# =========================================================
# CLOUD RUN UPLOAD WIDGET (mobil stabil)
# =========================================================
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

    html = (
        html.replace("__TITLE__", title)
        .replace("__HELP__", help_text)
        .replace("__URL__", UPLOAD_SERVICE_URL)
        .replace("__TOKEN__", UPLOAD_SERVICE_TOKEN)
        .replace("__PROJECT__", project)
        .replace("__UPLOAD_TYPE__", upload_type)
        .replace("__ACCEPT__", accept)
        .replace("__UID__", uid)
        .replace("__MULT__", "multiple" if multiple else "")
    )

    components.html(html, height=height)


# =========================================================
# DATA MODEL (CSV in Drive)
# =========================================================
PROJECTS_CSV = "Projects.csv"
EMPLOYEES_CSV = "Employees.csv"

CLOSURES_CSV = "Closures.csv"   # Wochenabschlüsse/Signaturen
AZK_CSV = "AZK.csv"             # Auswertung (MA/Woche/Projekt Summen)

PROJECTS_COLS = [
    "ProjektID", "Projekt", "Status",
    "Auftragsnr", "Objekt", "Kunde", "Telefon",
    "Kontaktperson", "Kontakttelefon",
]
EMPLOYEES_COLS = ["EmployeeID", "Name", "Rolle", "Stundenlohn", "PIN", "Status"]

# Baustellenrapport pro Projekt -> Datei: {Projekt}_Reports.csv in PROJECT_REPORTS_FOLDER_ID
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

CLOSURES_COLS = [
    "Projekt",
    "ProjektID",
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

AZK_COLS = ["Jahr", "KW", "EmployeeID", "Mitarbeiter", "ProjektID", "Projekt", "TotalStunden", "TotalReisezeit_h", "Timestamp"]


def _csv_find(folder_id: str, filename: str) -> tuple[pd.DataFrame, str | None]:
    files = drive_list_files(folder_id, limit=800)
    hit = next((f for f in files if f["name"] == filename), None)
    if not hit:
        return pd.DataFrame(), None
    data = drive_download_bytes(hit["id"])
    if not data:
        return pd.DataFrame(), hit["id"]
    return pd.read_csv(io.BytesIO(data)), hit["id"]


def _csv_save(folder_id: str, filename: str, df: pd.DataFrame, file_id: str | None) -> bool:
    raw = df.to_csv(index=False).encode("utf-8")
    if file_id:
        return drive_update_file(file_id, raw, mimetype="text/csv")
    return drive_upload_bytes(raw, folder_id, filename, "text/csv") is not None


def get_projects() -> tuple[pd.DataFrame, str | None]:
    df, fid = _csv_find(PROJECT_REPORTS_FOLDER_ID, PROJECTS_CSV)
    if df.empty:
        df = pd.DataFrame(columns=PROJECTS_COLS)
    for c in PROJECTS_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[PROJECTS_COLS]
    df["Status"] = df["Status"].fillna("aktiv").replace("", "aktiv")
    return df, fid


def save_projects(df: pd.DataFrame, fid: str | None) -> bool:
    df2 = df.copy()
    for c in PROJECTS_COLS:
        if c not in df2.columns:
            df2[c] = ""
    # ProjektID stabil erzeugen
    for idx, r in df2.iterrows():
        name = str(r.get("Projekt", "")).strip()
        pid = str(r.get("ProjektID", "")).strip()
        if name and not pid:
            df2.at[idx, "ProjektID"] = str(uuid4())[:8]
    df2["Status"] = df2["Status"].fillna("aktiv").replace("", "aktiv")
    df2 = df2[PROJECTS_COLS]
    return _csv_save(PROJECT_REPORTS_FOLDER_ID, PROJECTS_CSV, df2, fid)


def get_employees() -> tuple[pd.DataFrame, str | None]:
    df, fid = _csv_find(PROJECT_REPORTS_FOLDER_ID, EMPLOYEES_CSV)
    if df.empty:
        df = pd.DataFrame(columns=EMPLOYEES_COLS)
    for c in EMPLOYEES_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[EMPLOYEES_COLS]
    df["Status"] = df["Status"].fillna("aktiv").replace("", "aktiv")
    return df, fid


def save_employees(df: pd.DataFrame, fid: str | None) -> bool:
    df2 = df.copy()
    for c in EMPLOYEES_COLS:
        if c not in df2.columns:
            df2[c] = ""
    df2 = df2[EMPLOYEES_COLS]
    df2["Status"] = df2["Status"].fillna("aktiv").replace("", "aktiv")

    # EmployeeID generieren wenn fehlt
    for idx, r in df2.iterrows():
        name = str(r.get("Name", "")).strip()
        eid = str(r.get("EmployeeID", "")).strip()
        if name and not eid:
            base = re.sub(r"[^A-Za-z0-9]+", "", name.upper())[:6]
            df2.at[idx, "EmployeeID"] = (base or str(uuid4())[:6]).upper()

    return _csv_save(PROJECT_REPORTS_FOLDER_ID, EMPLOYEES_CSV, df2, fid)


def get_project_record(project_name: str, projects_df: pd.DataFrame) -> dict:
    if projects_df is None or projects_df.empty:
        return {}
    hit = projects_df[projects_df["Projekt"].astype(str) == str(project_name)]
    if hit.empty:
        return {}
    rec = hit.iloc[0].to_dict()
    # normalize NaN
    for k, v in list(rec.items()):
        if v is None:
            rec[k] = ""
        else:
            sv = str(v)
            rec[k] = "" if sv.lower() == "nan" else sv
    return rec


def report_filename(project_name: str) -> str:
    return f"{project_name}_Reports.csv"


def get_reports(project_name: str) -> tuple[pd.DataFrame, str | None]:
    df, fid = _csv_find(PROJECT_REPORTS_FOLDER_ID, report_filename(project_name))
    if df.empty:
        df = pd.DataFrame(columns=RAPPORT_COLS)
    for c in RAPPORT_COLS:
        if c not in df.columns:
            df[c] = "" if c not in ["Pause_h", "Stunden", "Reisezeit_h"] else 0.0
    df = df[RAPPORT_COLS]
    return df, fid


def append_report(project_name: str, row: dict) -> bool:
    df, fid = get_reports(project_name)
    df2 = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    return _csv_save(PROJECT_REPORTS_FOLDER_ID, report_filename(project_name), df2, fid)


def save_reports(project_name: str, df: pd.DataFrame, fid: str | None) -> bool:
    df2 = df.copy()
    for c in RAPPORT_COLS:
        if c not in df2.columns:
            df2[c] = "" if c not in ["Pause_h", "Stunden", "Reisezeit_h"] else 0.0
    df2 = df2[RAPPORT_COLS]
    return _csv_save(PROJECT_REPORTS_FOLDER_ID, report_filename(project_name), df2, fid)


def get_closures() -> tuple[pd.DataFrame, str | None]:
    df, fid = _csv_find(TIME_REPORTS_FOLDER_ID, CLOSURES_CSV)
    if df.empty:
        df = pd.DataFrame(columns=CLOSURES_COLS)
    for c in CLOSURES_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[CLOSURES_COLS]
    return df, fid


def append_closure(row: dict) -> bool:
    df, fid = get_closures()
    df2 = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    return _csv_save(TIME_REPORTS_FOLDER_ID, CLOSURES_CSV, df2, fid)


def get_azk() -> tuple[pd.DataFrame, str | None]:
    df, fid = _csv_find(TIME_REPORTS_FOLDER_ID, AZK_CSV)
    if df.empty:
        df = pd.DataFrame(columns=AZK_COLS)
    for c in AZK_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[AZK_COLS]
    return df, fid


def upsert_azk(row: dict) -> bool:
    df, fid = get_azk()
    df2 = pd.DataFrame([row])
    for c in AZK_COLS:
        if c not in df2.columns:
            df2[c] = ""
    df2 = df2[AZK_COLS]

    if df.empty:
        out = df2
    else:
        key_cols = ["Jahr", "KW", "EmployeeID", "ProjektID"]
        for k in key_cols:
            df[k] = df[k].astype(str)
            df2[k] = df2[k].astype(str)

        mask = (
            (df["Jahr"] == str(row.get("Jahr"))) &
            (df["KW"] == str(row.get("KW"))) &
            (df["EmployeeID"] == str(row.get("EmployeeID"))) &
            (df["ProjektID"] == str(row.get("ProjektID")))
        )
        if mask.any():
            idx = df[mask].index[0]
            for c in AZK_COLS:
                df.at[idx, c] = df2.iloc[0][c]
            out = df
        else:
            out = pd.concat([df, df2], ignore_index=True)

    return _csv_save(TIME_REPORTS_FOLDER_ID, AZK_CSV, out, fid)


# =========================================================
# TIME HELPERS
# =========================================================
def iso_year_week(d: date) -> tuple[int, int]:
    y, w, _ = d.isocalendar()
    return int(y), int(w)


def week_range(year: int, week: int) -> tuple[date, date]:
    start = datetime.fromisocalendar(year, week, 1).date()
    end = datetime.fromisocalendar(year, week, 7).date()
    return start, end


def hours_to_hhmm(h: float) -> str:
    try:
        m = int(round(float(h) * 60))
        return f"{m//60:02d}:{m%60:02d}"
    except Exception:
        return ""


# =========================================================
# QR CODE
# =========================================================
def generate_project_qr(project_name: str) -> bytes:
    safe_project = quote(project_name)
    link = f"{BASE_APP_URL}?embed=true&project={safe_project}"

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


# =========================================================
# PREVIEW HELPERS
# =========================================================
def safe_image_preview(file_id: str):
    data = drive_download_bytes(file_id)
    if not data:
        st.warning("Konnte Bild nicht laden.")
        return
    try:
        st.image(data, use_container_width=True)
    except Exception:
        st.warning("Bild konnte nicht angezeigt werden (Format/Upload defekt).")


# =========================================================
# NAV / DEEP LINK
# =========================================================
st.sidebar.title("Menü")

default_mode = 0
if st.query_params.get("mode") == "admin":
    default_mode = 1

mode = st.sidebar.radio("Bereich", ["👷 Mitarbeiter", "🛠️ Admin"], index=default_mode)

target_project = st.query_params.get("project", None)


# =========================================================
# COMMON: Load projects once
# =========================================================
projects_df, projects_fid = get_projects()

active_projects = projects_df[projects_df["Status"] == "aktiv"]["Projekt"].tolist() if not projects_df.empty else []
all_projects = projects_df["Projekt"].tolist() if not projects_df.empty else []


# =========================================================
# 👷 MITARBEITER
# =========================================================
if mode == "👷 Mitarbeiter":
    st.title("👷 Mitarbeiterbereich")

    emp_link = f"{BASE_APP_URL}?embed=true"
    st.info(
        "📲 **BauApp als Verknüpfung speichern**\n\n"
        f"👉 {emp_link}\n\n"
        "• iPhone (Safari): Teilen → Zum Home-Bildschirm\n"
        "• Android (Chrome): ⋮ → Zum Startbildschirm hinzufügen / App installieren"
    )
    st.link_button("🔗 Mitarbeiter-Link öffnen", emp_link)

    admin_link = f"{BASE_APP_URL}?mode=admin"
    with st.expander("🛠️ Admin öffnen"):
        st.code(admin_link)
        st.link_button("🔐 Admin-Link öffnen", admin_link)

    if not active_projects:
        st.warning("Keine aktiven Projekte vorhanden (Admin: Projekte anlegen).")
        st.stop()

    default_idx = 0
    if target_project and target_project in active_projects:
        default_idx = active_projects.index(target_project)
        st.success(f"📍 Direkteinstieg via QR-Code: {target_project}")

    project = st.selectbox("Projekt wählen", active_projects, index=default_idx)

    proj_rec = get_project_record(project, projects_df)
    if proj_rec:
        info = []
        if proj_rec.get("Auftragsnr"): info.append(f"**Auftragsnr.:** {proj_rec.get('Auftragsnr')}")
        if proj_rec.get("Objekt"): info.append(f"**Objekt:** {proj_rec.get('Objekt')}")
        if proj_rec.get("Kunde"): info.append(f"**Kunde:** {proj_rec.get('Kunde')}")
        if proj_rec.get("Telefon"): info.append(f"**Telefon:** {proj_rec.get('Telefon')}")
        if proj_rec.get("Kontaktperson"): info.append(f"**Kontaktperson:** {proj_rec.get('Kontaktperson')}")
        if proj_rec.get("Kontakttelefon"): info.append(f"**Kontakt-Tel.:** {proj_rec.get('Kontakttelefon')}")
        if info:
            st.markdown("#### Projektdaten")
            st.info(" · ".join(info))

    t1, t2, t3 = st.tabs(["📝 Rapport", "📷 Fotos", "📂 Pläne"])

    # -------------------------
    # Rapport erfassen
    # -------------------------
    with t1:
        st.subheader("Rapport erfassen")

        df_emp, emp_fid = get_employees()
        df_emp = df_emp.copy()
        df_emp["Status"] = df_emp["Status"].astype(str).str.lower().replace("nan", "").replace("", "aktiv")
        df_emp_active = df_emp[df_emp["Status"] == "aktiv"]

        c1, c2, c3 = st.columns(3)
        date_val = c1.date_input("Datum", datetime.now().date())

        ma_sel = None
        if df_emp_active.empty:
            st.warning("Keine Mitarbeiter vorhanden (Admin: Mitarbeiter anlegen).")
        else:
            options = df_emp_active.apply(
                lambda r: f'{r["Name"]} ({r["EmployeeID"]})',
                axis=1
            ).tolist()
            sel_key = c1.selectbox("Mitarbeiter", options)
            idx = options.index(sel_key)
            ma_sel = df_emp_active.iloc[idx].to_dict()

        start_val = c2.time_input("Start", time(7, 0))
        end_val = c2.time_input("Ende", time(16, 0))
        pause_h = c3.number_input("Pause (h)", 0.0, 5.0, 0.5, 0.25)
        material = c3.text_input("Material")
        remark = st.text_area("Bemerkung")

        # Fahrtzeiten (SPV-konform)
        ank_mag = ""
        abd_mag = ""
        h2s = 0
        s2h = 0
        reise_direkt = 0
        reise_bezahlt_min = 0
        reise_bezahlt_h = 0.0
        reise_regel = "SPV: Bezahlt nur Direktfahrt Zuhause↔Baustelle; pro Richtung 30 Min Selbstbehalt. Magazinfahrten unbezahlt."

        with st.expander("🚗 Fahrtzeiten (SPV-konform)", expanded=False):
            st.caption(
                "Regel: Magazin↔Baustelle ist **nicht** bezahlte Fahrtzeit. "
                "Bezahlt wird nur Direktfahrt Zuhause↔Baustelle, **pro Richtung minus 30 Min Selbstbehalt**."
            )
            ec1, ec2, ec3 = st.columns(3)
            if ec1.checkbox("Ankunft Magazin erfassen", value=False):
                ank_mag = str(ec1.time_input("Ankunft Magazin (Uhrzeit)", time(0, 0)))
            if ec2.checkbox("Abfahrt Magazin erfassen", value=False):
                abd_mag = str(ec2.time_input("Abfahrt Magazin (Uhrzeit)", time(0, 0)))

            h2s = int(ec3.number_input("Direkt: Zuhause → Baustelle (Min)", 0, 600, 0, 5))
            s2h = int(ec3.number_input("Direkt: Baustelle → Zuhause (Min)", 0, 600, 0, 5))
            reise_direkt = h2s + s2h

            bezahlt_h2s = max(0, h2s - 30) if h2s > 0 else 0
            bezahlt_s2h = max(0, s2h - 30) if s2h > 0 else 0
            reise_bezahlt_min = bezahlt_h2s + bezahlt_s2h
            reise_bezahlt_h = round(reise_bezahlt_min / 60.0, 2)

            st.info(
                f"Direkt: H→B {h2s} Min (bezahlt {bezahlt_h2s}), "
                f"B→H {s2h} Min (bezahlt {bezahlt_s2h}) → "
                f"Bezahlt gesamt: {reise_bezahlt_min} Min (= {reise_bezahlt_h} h)"
            )

        # Stunden berechnen
        dt_start = datetime.combine(datetime.today(), start_val)
        dt_end = datetime.combine(datetime.today(), end_val)
        if dt_end < dt_start:
            dt_end += timedelta(days=1)

        hours = round(max(0.0, (dt_end - dt_start).total_seconds() / 3600 - float(pause_h)), 2)
        st.info(f"Stunden: {hours}")

        if st.button("✅ Speichern", type="primary"):
            if not ma_sel:
                st.error("Mitarbeiter fehlt.")
            else:
                row = {
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
                    "AnkunftMagazin": ank_mag,
                    "AbfahrtMagazin": abd_mag,
                    "ReiseHomeToSiteMin": int(h2s),
                    "ReiseSiteToHomeMin": int(s2h),
                    "ReiseDirektMin": int(reise_direkt),
                    "ReiseBezahltMin": int(reise_bezahlt_min),
                    "ReiseRegel": reise_regel,
                    "Reisezeit_h": float(reise_bezahlt_h),
                    "Start": str(start_val),
                    "Ende": str(end_val),
                    "Pause_h": float(pause_h),
                    "Stunden": float(hours),
                    "Material": str(material),
                    "Bemerkung": str(remark),
                }
                ok = append_report(project, row)
                if ok:
                    st.success("Gespeichert ✅")
                    sys_time.sleep(0.2)
                    st.rerun()
                else:
                    st.error("Speichern fehlgeschlagen (Drive).")

        st.divider()
        st.subheader("Woche abschliessen (Signatur)")

        if ma_sel:
            df_r, _fid = get_reports(project)
            if not df_r.empty:
                df_r2 = df_r.copy()
                df_r2["Datum_dt"] = pd.to_datetime(df_r2["Datum"], errors="coerce").dt.date
                df_r2 = df_r2[df_r2["EmployeeID"].astype(str).str.strip() == str(ma_sel.get("EmployeeID", "")).strip()]
            else:
                df_r2 = pd.DataFrame()

            weeks = []
            if not df_r2.empty and "Datum_dt" in df_r2.columns:
                for d in df_r2["Datum_dt"].dropna().unique():
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
            w_start, w_end = week_range(y, w)
            st.caption(f"Zeitraum: {w_start} – {w_end}")

            if not df_r2.empty:
                mask = (df_r2["Datum_dt"] >= w_start) & (df_r2["Datum_dt"] <= w_end)
                df_week = df_r2[mask].copy()
                total_hours = float(pd.to_numeric(df_week["Stunden"], errors="coerce").fillna(0).sum())
                total_travel = float(pd.to_numeric(df_week["Reisezeit_h"], errors="coerce").fillna(0).sum())
            else:
                total_hours = 0.0
                total_travel = 0.0

            m1, m2 = st.columns(2)
            m1.metric("Total Arbeitsstunden", round(total_hours, 2))
            m2.metric("Total Reisezeit (h)", round(total_travel, 2))

            pin_in = st.text_input("PIN eingeben (nur für Signatur)", type="password")
            if st.button("✅ Woche abschliessen", type="primary"):
                expected = str(ma_sel.get("PIN", "")).strip()
                if not expected:
                    st.error("Für diesen Mitarbeiter ist kein PIN hinterlegt (Admin: Mitarbeiter bearbeiten).")
                elif str(pin_in).strip() != expected:
                    st.error("PIN falsch. Keine Unterschrift.")
                else:
                    okc = append_closure({
                        "Projekt": project,
                        "ProjektID": str(proj_rec.get("ProjektID", "")).strip(),
                        "EmployeeID": str(ma_sel.get("EmployeeID", "")).strip(),
                        "Mitarbeiter": str(ma_sel.get("Name", "")).strip(),
                        "Jahr": str(y),
                        "KW": str(w),
                        "Von": str(w_start),
                        "Bis": str(w_end),
                        "TotalStunden": round(total_hours, 2),
                        "TotalReisezeit_h": round(total_travel, 2),
                        "Timestamp": datetime.now().isoformat(timespec="seconds"),
                        "Signed": True,
                    })
                    if okc:
                        _ = upsert_azk({
                            "Jahr": str(y),
                            "KW": str(w),
                            "EmployeeID": str(ma_sel.get("EmployeeID", "")).strip(),
                            "Mitarbeiter": str(ma_sel.get("Name", "")).strip(),
                            "ProjektID": str(proj_rec.get("ProjektID", "")).strip(),
                            "Projekt": project,
                            "TotalStunden": round(total_hours, 2),
                            "TotalReisezeit_h": round(total_travel, 2),
                            "Timestamp": datetime.now().isoformat(timespec="seconds"),
                        })
                        st.success("Woche abgeschlossen / signiert ✅ (AZK aktualisiert)")
                    else:
                        st.error("Signatur speichern fehlgeschlagen (Drive).")

    # -------------------------
    # Fotos
    # -------------------------
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

        if st.button("🔄 Fotos aktualisieren"):
            st.rerun()

        files = drive_list_files(PHOTOS_FOLDER_ID, mime_contains="image/")
        proj_photos = [x for x in files if x["name"].startswith(project + "_")][:120]

        if not proj_photos:
            st.info("Keine Fotos vorhanden.")
        else:
            for f in proj_photos:
                with st.expander(f"🖼️ {f['name']}", expanded=False):
                    safe_image_preview(f["id"])

    # -------------------------
    # Pläne & Dokumente
    # -------------------------
    with t3:
        st.subheader("Pläne & Dokumente")

        if st.button("🔄 Pläne aktualisieren"):
            st.rerun()

        files = drive_list_files(PLANS_FOLDER_ID, limit=600)
        proj_docs = [x for x in files if x["name"].startswith(project + "_")][:300]

        if not proj_docs:
            st.info("Keine Dokumente hinterlegt.")
        else:
            for f in proj_docs:
                data = drive_download_bytes(f["id"])
                if not data:
                    continue
                c1, c2 = st.columns([0.8, 0.2])
                c1.write(f"📄 {f['name']}")
                c2.download_button("⬇️ Download", data, file_name=f["name"])


# =========================================================
# 🛠️ ADMIN
# =========================================================
else:
    st.title("🛠️ Admin")

    pin = st.text_input("PIN", type="password")
    if pin != ADMIN_PIN:
        st.stop()

    st.success("Angemeldet ✅")

    admin_link = f"{BASE_APP_URL}?mode=admin"
    st.info(f"🔐 Admin-Link: {admin_link}")
    st.link_button("🔗 Admin-Link öffnen", admin_link)

    tabA, tabB, tabC, tabD, tabE = st.tabs(
        ["📌 Projekte", "📂 Übersicht & Uploads", "🧾 Rapporte", "👤 Mitarbeiter", "📤 Zeitdaten (AZK/Signaturen)"]
    )

    # -------------------------
    # Projekte
    # -------------------------
    with tabA:
        st.subheader("Projekte verwalten")

        df_p, p_fid = get_projects()

        c1, c2 = st.columns([0.7, 0.3])
        new_p = c1.text_input("Neues Projekt")
        if c2.button("➕ Anlegen") and new_p.strip():
            name = new_p.strip()
            df_p = pd.concat([df_p, pd.DataFrame([{"Projekt": name, "Status": "aktiv"}])], ignore_index=True)
            if save_projects(df_p, p_fid):
                st.success(f"Projekt '{name}' angelegt.")
                qr = generate_project_qr(name)
                st.image(qr, width=220)
                st.download_button("⬇️ QR-Code (.png)", qr, file_name=f"QR_{name}.png", mime="image/png")
                sys_time.sleep(0.2)
                st.rerun()
            else:
                st.error("Konnte Projects.csv nicht speichern.")

        st.divider()
        if df_p.empty:
            st.info("Noch keine Projekte.")
        else:
            st.caption("Projekt-Stammdaten pflegen (werden im Mitarbeiterbereich angezeigt).")
            edited = st.data_editor(
                df_p,
                use_container_width=True,
                num_rows="dynamic",
                disabled=["ProjektID"],
            )
            if st.button("💾 Projekte speichern", type="primary"):
                if save_projects(edited, p_fid):
                    st.success("Gespeichert ✅")
                    sys_time.sleep(0.2)
                    st.rerun()
                else:
                    st.error("Speichern fehlgeschlagen (Drive).")

    # -------------------------
    # Übersicht & Uploads (wie Mitarbeiter + Löschen)
    # -------------------------
    with tabB:
        st.subheader("Übersicht & Uploads (Admin)")

        if not all_projects:
            st.info("Erstelle zuerst ein Projekt.")
            st.stop()

        sel_p = st.selectbox("Projekt", all_projects, key="admin_sel_project")

        cX, cY = st.columns(2)

        with cX:
            st.markdown("### 📷 Fotos (Cloud Run)")
            cloudrun_upload_widget(
                project=sel_p,
                upload_type="photo",
                title="Foto(s) hochladen",
                help_text="Mobil stabil (Kamera/Galerie).",
                accept="image/*",
                multiple=True,
                height=240,
            )

        with cY:
            st.markdown("### 📄 Pläne/Dokumente (direkt nach Drive)")
            st.caption("Admin-Upload (PC stabil). Dateien werden mit Projekt-Präfix gespeichert.")

            docs = st.file_uploader("Dokumente auswählen", accept_multiple_files=True, key="adm_docs")
            if st.button("📤 Dokument(e) speichern", type="primary"):
                if not docs:
                    st.warning("Bitte zuerst Dateien auswählen.")
                else:
                    ok_n = 0
                    fail_n = 0
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    for f in docs:
                        try:
                            data = f.getvalue()
                            mime = getattr(f, "type", None) or "application/octet-stream"
                            fname = f"{sel_p}_{ts}_{f.name}"
                            if drive_upload_bytes(data, PLANS_FOLDER_ID, fname, mime):
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

        tabF, tabD = st.tabs(["📷 Fotos – Übersicht", "📄 Pläne/Dokumente – Übersicht"])

        with tabF:
            if st.button("🔄 Fotos aktualisieren", key="adm_ref_ph"):
                st.rerun()
            files = drive_list_files(PHOTOS_FOLDER_ID, mime_contains="image/", limit=800)
            items = [x for x in files if x["name"].startswith(sel_p + "_")][:240]
            if not items:
                st.info("Keine Fotos vorhanden.")
            else:
                cols = st.columns(3)
                for i, f in enumerate(items):
                    with cols[i % 3]:
                        with st.expander(f"🖼️ {f['name']}", expanded=False):
                            safe_image_preview(f["id"])
                            if st.button("🗑 Foto löschen", key=f"del_ph_{f['id']}"):
                                if drive_delete(f["id"]):
                                    st.success("Gelöscht ✅")
                                    sys_time.sleep(0.2)
                                    st.rerun()

        with tabD:
            if st.button("🔄 Pläne aktualisieren", key="adm_ref_docs"):
                st.rerun()
            files = drive_list_files(PLANS_FOLDER_ID, limit=800)
            items = [x for x in files if x["name"].startswith(sel_p + "_")][:500]
            if not items:
                st.info("Keine Dokumente vorhanden.")
            else:
                for f in items:
                    data = drive_download_bytes(f["id"])
                    if not data:
                        continue
                    a1, a2, a3 = st.columns([0.65, 0.2, 0.15])
                    a1.write(f"📄 {f['name']}")
                    a2.download_button("⬇️ Download", data, file_name=f["name"])
                    if a3.button("🗑", key=f"del_doc_{f['id']}"):
                        if drive_delete(f["id"]):
                            st.success("Gelöscht ✅")
                            sys_time.sleep(0.2)
                            st.rerun()

    # -------------------------
    # Rapporte ansehen + editieren
    # -------------------------
    with tabC:
        st.subheader("Rapporte (pro Projekt)")

        if not all_projects:
            st.info("Keine Projekte vorhanden.")
            st.stop()

        sel = st.selectbox("Projekt", all_projects, key="adm_rep_proj")
        df_r, rep_fid = get_reports(sel)

        if df_r.empty:
            st.info("Keine Rapporte vorhanden.")
        else:
            st.caption("Hier kannst du Rapporte kontrollieren und korrigieren. Speichern aktualisiert die CSV in Drive.")
            edited = st.data_editor(df_r, use_container_width=True, num_rows="dynamic", key=f"ed_rep_{sel}")

            c1, c2 = st.columns([0.25, 0.75])
            if c1.button("💾 Speichern", type="primary"):
                if save_reports(sel, edited, rep_fid):
                    st.success("Gespeichert ✅")
                    sys_time.sleep(0.2)
                    st.rerun()
                else:
                    st.error("Speichern fehlgeschlagen (Drive).")

            c2.download_button(
                "⬇️ CSV herunterladen",
                edited.to_csv(index=False).encode("utf-8"),
                file_name=f"{sel}_Reports.csv",
                mime="text/csv",
            )

    # -------------------------
    # Mitarbeiter
    # -------------------------
    with tabD:
        st.subheader("Mitarbeiter verwalten")
        st.caption("PIN wird nur für Wochenabschluss/Signatur verwendet (kein Login).")

        df_emp, emp_fid = get_employees()

        # datentypen glätten
        df_emp = df_emp.copy()
        for c in EMPLOYEES_COLS:
            if c not in df_emp.columns:
                df_emp[c] = ""
        for c in ["EmployeeID", "Name", "Rolle", "PIN", "Status"]:
            df_emp[c] = df_emp[c].astype(str).replace("nan", "").fillna("").str.strip()
        df_emp["Stundenlohn"] = pd.to_numeric(df_emp["Stundenlohn"], errors="coerce").fillna(0.0)
        df_emp["Status"] = df_emp["Status"].apply(lambda x: x if x in ["aktiv", "inaktiv"] else "aktiv")

        edited = st.data_editor(
            df_emp,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "Status": st.column_config.SelectboxColumn("Status", options=["aktiv", "inaktiv"]),
                "Stundenlohn": st.column_config.NumberColumn("Stundenlohn", step=0.05),
            },
        )

        if st.button("💾 Mitarbeiter speichern", type="primary"):
            if save_employees(edited, emp_fid):
                st.success("Gespeichert ✅")
                sys_time.sleep(0.2)
                st.rerun()
            else:
                st.error("Speichern fehlgeschlagen (Drive).")

    # -------------------------
    # Zeitdaten: Closures + AZK
    # -------------------------
    with tabE:
        st.subheader("Zeitdaten (AZK / Wochenabschlüsse)")

        df_cl, cl_fid = get_closures()
        df_azk, azk_fid = get_azk()

        st.markdown("### ✅ Wochenabschlüsse / Signaturen (Closures.csv)")
        if df_cl.empty:
            st.info("Noch keine Wochenabschlüsse vorhanden.")
        else:
            st.dataframe(df_cl.sort_values(["Jahr", "KW"], ascending=False), use_container_width=True)
            st.download_button(
                "⬇️ Closures.csv",
                df_cl.to_csv(index=False).encode("utf-8"),
                file_name="Closures.csv",
                mime="text/csv",
            )

        st.divider()
        st.markdown("### 📊 AZK (AZK.csv) – Summen pro MA/Woche/Projekt")
        if df_azk.empty:
            st.info("AZK ist leer (wird beim Wochenabschluss automatisch upsertet).")
        else:
            st.dataframe(df_azk.sort_values(["Jahr", "KW"], ascending=False), use_container_width=True)
            st.download_button(
                "⬇️ AZK.csv",
                df_azk.to_csv(index=False).encode("utf-8"),
                file_name="AZK.csv",
                mime="text/csv",
            )
