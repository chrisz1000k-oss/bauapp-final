import io
import os
import re
import time as sys_time
import base64
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

# Streamlit UI-Chrome so gut wie möglich ausblenden (für normale User).
# Hinweis: Als Streamlit-Cloud-Owner am Desktop können Owner-Controls trotzdem sichtbar bleiben.
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

# Basis-URL Ihrer App (für QR-Codes) – NUR EINMAL
BASE_APP_URL = "https://8bv6gzagymvrdgnm8wrtrq.streamlit.app"

# Logo – NUR EINMAL
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

        res = (
            drive.files()
            .list(
                q=q,
                pageSize=200,
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
# QR CODE & PRINT TEMPLATE
# =========================
def generate_project_qr(project_name: str) -> bytes:
    """
    Erzeugt einen QR-Code, der direkt auf das Projekt verlinkt.
    WICHTIG: embed=true, damit Mitarbeiter/Normalnutzer keine Cloud-Toolbar sehen.
    """
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


def get_printable_html(project_name, qr_bytes):
    """Erzeugt HTML im Design von R. BAUMGARTNER AG."""
    qr_b64 = base64.b64encode(qr_bytes).decode("utf-8")
    today_str = datetime.now().strftime("%d.%m.%Y")
    empty_rows12 = "\n".join([
        "                <tr>\n                    <td class=\"col-desc\"></td>\n                    <td class=\"col-mat\"></td>\n                </tr>"
        for _ in range(12)
    ])
    empty_rows8 = "\n".join([
        "                <tr>\n                    <td class=\"col-desc\"></td>\n                    <td class=\"col-mat\"></td>\n                </tr>"
        for _ in range(8)
    ])

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, Helvetica, sans-serif; padding: 20px; font-size: 12px; color: black; }}

            .header-top {{ display: flex; justify-content: space-between; align-items: flex-end; margin-bottom: 10px; }}
            .company-name {{ font-size: 24px; font-weight: bold; text-transform: uppercase; letter-spacing: 1px; }}
            .order-info {{ font-weight: bold; font-size: 14px; }}

            .info-grid {{ display: grid; grid-template-columns: 1fr 150px; gap: 20px; border-bottom: 2px solid #000; padding-bottom: 5px; margin-bottom: 10px; }}
            .info-lines {{ display: flex; flex-direction: column; gap: 8px; }}

            .input-line {{ display: flex; align-items: flex-end; }}
            .label {{ width: 100px; font-weight: bold; font-size: 11px; }}
            .value {{ flex-grow: 1; border-bottom: 1px solid #000; padding-left: 5px; min-height: 18px; }}
            .value-filled {{ font-weight: bold; font-size: 13px; }}

            .qr-box {{ border: 1px solid #ccc; text-align: center; padding: 2px; height: 130px; display: flex; flex-direction: column; align-items: center; justify-content: center; }}
            .qr-box img {{ width: 110px; height: 110px; }}
            .qr-header {{ font-size: 10px; font-weight: bold; margin-bottom: 2px; text-transform: uppercase; }}

            .check-section {{ margin-bottom: 15px; border-bottom: 1px solid #000; padding-bottom: 5px; }}
            .check-header {{ text-align: right; font-size: 11px; font-weight: bold; margin-bottom: 2px; }}
            .check-row {{ display: flex; justify-content: space-between; margin-bottom: 4px; border-bottom: 1px solid #eee; }}
            .check-left {{ display: flex; align-items: center; }}
            .check-label {{ width: 80px; font-size: 11px; }}
            .check-box {{ width: 12px; height: 12px; border: 1px solid #000; margin-left: 10px; display: inline-block; }}
            .check-right {{ font-size: 10px; color: #888; text-align: right; }}

            .main-table {{ width: 100%; border-collapse: collapse; margin-top: 10px; border: 1px solid #000; }}
            .main-table th {{ border: 1px solid #000; padding: 5px; text-align: left; background-color: #fff; font-weight: bold; font-size: 12px; }}
            .main-table td {{ border-right: 1px solid #000; height: 24px; vertical-align: bottom; }}

            .col-desc {{ width: 70%; text-align: left; padding-left: 5px; border-bottom: 1px dotted #ccc; }}
            .col-mat {{ width: 30%; border-bottom: 1px solid #000; }}

            .footer-date {{ margin-top: 20px; font-size: 12px; }}

            @media print {{
                body {{ padding: 0; margin: 0; }}
                button {{ display: none; }}
            }}
        </style>
    </head>
    <body>

        <div class="header-top">
            <div class="company-name">R. BAUMGARTNER AG</div>
            <div class="order-info">Auftragsnr. ___________ &nbsp; RBAG ___________</div>
        </div>

        <div class="info-grid">
            <div class="info-lines">
                <div class="input-line">
                    <span class="label">OBJEKT</span>
                    <span class="value value-filled">{project_name}</span>
                </div>
                <div class="input-line">
                    <span class="label">KUNDE</span>
                    <span class="value"></span>
                </div>
                <div class="input-line">
                    <span class="label">TELEFON</span>
                    <span class="value"></span>
                </div>
                <div class="input-line" style="margin-top: 10px;">
                    <span class="label">KONTAKTPER.</span>
                    <span class="value"></span>
                </div>
                <div class="input-line">
                    <span class="label">TELEFON</span>
                    <span class="value"></span>
                </div>
            </div>

            <div class="qr-box">
                <div class="qr-header">STUNDEN / APP</div>
                <img src="data:image/png;base64,{qr_b64}" />
            </div>
        </div>

        <div class="check-section">
            <div class="check-header">FUGENFARBEN</div>

            <div class="check-row">
                <div class="check-left"><span class="check-label">CODE</span> <span class="check-box"></span></div>
                <div class="check-right">ZEM. / SIL.</div>
            </div>
            <div class="check-row">
                <div class="check-left"><span class="check-label">MATERIAL</span> <span class="check-box"></span></div>
                <div class="check-right">ZEM. / SIL.</div>
            </div>
            <div class="check-row">
                <div class="check-left"><span class="check-label">ASBEST</span> <span class="check-box"></span></div>
                <div class="check-right">ZEM. / SIL.</div>
            </div>
        </div>

        <table class="main-table">
            <thead>
                <tr>
                    <th class="col-desc" style="border-bottom: 1px solid #000;">ARBEITSBESCHRIEB & NOTIZEN</th>
                    <th class="col-mat">Material</th>
                </tr>
            </thead>
            <tbody>
                {empty_rows12}

                <tr>
                    <td class="col-desc"></td>
                    <td class="col-mat" style="text-align:center; font-weight:bold; font-size:10px; background-color:#f9f9f9; border-top: 2px solid #000; border-bottom: 1px solid #000;">MATERIAL REGIE</td>
                </tr>

                {empty_rows8}
            </tbody>
        </table>

        <div class="footer-date">
            {today_str}
        </div>

    </body>
    </html>
    """
    return html


# =========================
# PROJECTS + REPORTS
# =========================
PROJECTS_CSV_NAME = "Projects.csv"
PROJECTS_COLS = ["ProjektID", "Projekt", "Status", "Auftragsnr", "Objekt", "Kunde", "Telefon", "Kontaktperson", "Kontakttelefon"]
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
    "Reisezeit_h",
    "Start",
    "Ende",
    "Pause_h",
    "Stunden",
    "Material",
    "Bemerkung",
]

# =========================
# EMPLOYEES + CLOSURES
# =========================
EMPLOYEES_CSV_NAME = "Employees.csv"
EMPLOYEES_COLS = ["EmployeeID", "Name", "Rolle", "Stundenlohn", "PIN", "Status"]

CLOSURES_CSV_NAME = "Closures.csv"
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


def get_projects_df():
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f["name"] == PROJECTS_CSV_NAME), None)
    if csv_file:
        data = download_bytes(csv_file["id"])
        if data:
            df = pd.read_csv(io.BytesIO(data))
            # Backward-compatible: ensure expected columns exist
            for c in PROJECTS_COLS:
                if c not in df.columns:
                    df[c] = ""
            df = df[PROJECTS_COLS]
            # Default status if missing/empty
            if "Status" in df.columns:
                df["Status"] = df["Status"].fillna("aktiv").replace("", "aktiv")
            return df, csv_file["id"]
    return pd.DataFrame(columns=PROJECTS_COLS), None


def ensure_project_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure each project has a stable ProjektID."""
    df = df.copy()
    if "ProjektID" not in df.columns:
        df["ProjektID"] = ""
    for idx, row in df.iterrows():
        name = str(row.get("Projekt", "")).strip()
        pid = str(row.get("ProjektID", "")).strip()
        if name and not pid:
            df.at[idx, "ProjektID"] = str(uuid4())[:8]
    return df


def save_projects_df(df, file_id=None):
    df = ensure_project_ids(df)
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


def get_all_projects():
    df, _ = get_projects_df()
    if df.empty:
        return []
    return df["Projekt"].tolist()


def get_project_record(project_name: str) -> dict:
    df, _ = get_projects_df()
    if df.empty:
        return {}
    hit = df[df["Projekt"].astype(str) == str(project_name)]
    if hit.empty:
        return {}
    row = hit.iloc[0].to_dict()
    # normalize keys
    for k in PROJECTS_COLS:
        row.setdefault(k, "")
    return row


def get_reports_df(project_name: str):
    filename = f"{project_name}_Reports.csv"
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f["name"] == filename), None)
    if csv_file:
        data = download_bytes(csv_file["id"])
        if data:
            df = pd.read_csv(io.BytesIO(data))
            # Backward-compatible: ensure expected columns exist
            for c in RAPPORT_COLS:
                if c not in df.columns:
                    df[c] = "" if c not in ["Pause_h", "Stunden", "Reisezeit_h"] else 0.0
            df = df[RAPPORT_COLS]
            return df, csv_file["id"]
    return pd.DataFrame(columns=RAPPORT_COLS), None


def save_report(project_name: str, row_dict: dict) -> bool:
    df, file_id = get_reports_df(project_name)
    df = pd.concat([df, pd.DataFrame([row_dict])], ignore_index=True)
    csv_data = df.to_csv(index=False).encode("utf-8")
    if file_id:
        return update_file_in_drive(file_id, csv_data, mimetype="text/csv")
    return upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, f"{project_name}_Reports.csv", "text/csv")




def get_employees_df():
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f["name"] == EMPLOYEES_CSV_NAME), None)
    if csv_file:
        data = download_bytes(csv_file["id"])
        if data:
            df = pd.read_csv(io.BytesIO(data))
            for c in EMPLOYEES_COLS:
                if c not in df.columns:
                    df[c] = ""
            df = df[EMPLOYEES_COLS]
            if "Status" in df.columns:
                df["Status"] = df["Status"].fillna("aktiv").replace("", "aktiv")
            return df, csv_file["id"]
    return pd.DataFrame(columns=EMPLOYEES_COLS), None


def save_employees_df(df: pd.DataFrame, file_id: str | None = None) -> bool:
    df2 = df.copy()
    for c in EMPLOYEES_COLS:
        if c not in df2.columns:
            df2[c] = ""
    df2 = df2[EMPLOYEES_COLS]
    csv_data = df2.to_csv(index=False).encode("utf-8")
    if file_id:
        return update_file_in_drive(file_id, csv_data, mimetype="text/csv")
    return upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, EMPLOYEES_CSV_NAME, "text/csv")


def get_closures_df():
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f["name"] == CLOSURES_CSV_NAME), None)
    if csv_file:
        data = download_bytes(csv_file["id"])
        if data:
            df = pd.read_csv(io.BytesIO(data))
            for c in CLOSURES_COLS:
                if c not in df.columns:
                    df[c] = ""
            df = df[CLOSURES_COLS]
            return df, csv_file["id"]
    return pd.DataFrame(columns=CLOSURES_COLS), None


def append_closure(row: dict) -> bool:
    df, fid = get_closures_df()
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    csv_data = df.to_csv(index=False).encode("utf-8")
    if fid:
        return update_file_in_drive(fid, csv_data, mimetype="text/csv")
    return upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, CLOSURES_CSV_NAME, "text/csv")

AZK_CSV_NAME = "AZK.csv"
AZK_COLS = ["Jahr", "KW", "EmployeeID", "Mitarbeiter", "ProjektID", "Projekt", "TotalStunden", "TotalReisezeit_h", "Timestamp"]

def get_azk_df():
    files = list_files(REPORTS_FOLDER_ID)
    csv_file = next((f for f in files if f["name"] == AZK_CSV_NAME), None)
    if csv_file:
        data = download_bytes(csv_file["id"])
        if data:
            df = pd.read_csv(io.BytesIO(data))
            for c in AZK_COLS:
                if c not in df.columns:
                    df[c] = ""
            df = df[AZK_COLS]
            return df, csv_file["id"]
    return pd.DataFrame(columns=AZK_COLS), None


def upsert_azk_row(row: dict) -> bool:
    """Upsert by (Jahr, KW, EmployeeID, ProjektID)."""
    df, fid = get_azk_df()
    df2 = pd.DataFrame([row])
    for c in AZK_COLS:
        if c not in df2.columns:
            df2[c] = ""
    df2 = df2[AZK_COLS]

    if df.empty:
        out = df2
    else:
        key_cols = ["Jahr", "KW", "EmployeeID", "ProjektID"]
        # ensure types
        for k in key_cols:
            df[k] = df[k].astype(str)
            df2[k] = df2[k].astype(str)
        mask = (df["Jahr"] == str(row.get("Jahr"))) & (df["KW"] == str(row.get("KW"))) & (df["EmployeeID"] == str(row.get("EmployeeID"))) & (df["ProjektID"] == str(row.get("ProjektID")))
        if mask.any():
            # replace first match
            idx = df[mask].index[0]
            for c in AZK_COLS:
                df.at[idx, c] = df2.iloc[0][c]
            out = df
        else:
            out = pd.concat([df, df2], ignore_index=True)

    csv_data = out.to_csv(index=False).encode("utf-8")
    if fid:
        return update_file_in_drive(fid, csv_data, mimetype="text/csv")
    return upload_bytes_to_drive(csv_data, REPORTS_FOLDER_ID, AZK_CSV_NAME, "text/csv")


def iso_year_week(d: datetime.date) -> tuple[int, int]:
    iso = d.isocalendar()
    return int(iso[0]), int(iso[1])


def week_date_range(year: int, week: int) -> tuple[datetime, datetime]:
    start = datetime.fromisocalendar(year, week, 1)
    end = datetime.fromisocalendar(year, week, 7)
    return start, end
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
):
    """
    bucket: "photos" oder "uploads" (uploads wird aktuell nur als Typ-Unterscheidung verwendet)
    accept: "image/*" für Fotos
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

          // Backend erwartet "plan" oder "photo"
          const uploadType = (bucket === "uploads") ? "plan" : "photo";
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
# UI & DEEP LINKING LOGIC
# =========================
st.sidebar.title("Menü")
default_mode = 0  # 👷 Mitarbeiter
if st.query_params.get("mode") == "admin":
    default_mode = 1  # 🛠️ Admin

mode = st.sidebar.radio(
    "Bereich",
    ["👷 Mitarbeiter", "🛠️ Admin"],
    index=default_mode
)


# Deep Linking (QR)
target_project_from_qr = None
if "project" in st.query_params:
    target_project_from_qr = st.query_params["project"]


# -------------------------
# 👷 MITARBEITER
# -------------------------
if mode == "👷 Mitarbeiter":
    st.title("👷 Mitarbeiterbereich")

    # 📲 BauApp als Verknüpfung (Handy / Startbildschirm)
    emp_link = f"{BASE_APP_URL}?embed=true"
    st.info(
        "📲 **BauApp als Verknüpfung speichern**\n\n"
        f"👉 Öffne diesen Link: {emp_link}\n\n"
        "• **iPhone (Safari):** Teilen → *Zum Home-Bildschirm*\n"
        "• **Android (Chrome):** ⋮ → *Zum Startbildschirm hinzufügen* / *App installieren*"
    )
    st.link_button("🔗 Mitarbeiter-Link öffnen", emp_link)

    # 🛠️ Admin-Link (ohne embed) – damit der Admin-Bereich/Sidebar verfügbar ist
    admin_link_public = f"{BASE_APP_URL}?mode=admin"
    with st.expander("🛠️ Admin öffnen"):
        st.write("Dieser Link öffnet die App **ohne** Embed-Modus. Danach kann im Admin-Bereich der Admin-PIN eingegeben werden.")
        st.code(admin_link_public)
        st.link_button("🔐 Admin-Link öffnen", admin_link_public)

    active_projects = get_active_projects()
    if not active_projects:
        st.warning("Keine aktiven Projekte.")
        st.stop()

    default_index = 0
    if target_project_from_qr:
        if target_project_from_qr in active_projects:
            default_index = active_projects.index(target_project_from_qr)
            st.success(f"📍 Direkteinstieg via QR-Code: {target_project_from_qr}")
        else:
            st.warning(
                f"Das Projekt '{target_project_from_qr}' aus dem QR-Code ist nicht aktiv oder existiert nicht."
            )

    project = st.selectbox("Projekt wählen", active_projects, index=default_index)

    # 🔎 Projekt-Stammdaten (wie Papier-Rapport) anzeigen
    proj_rec = get_project_record(project)
    if proj_rec:
        lines = []
        if str(proj_rec.get("Auftragsnr","")).strip():
            lines.append(f"**Auftragsnr.:** {proj_rec.get('Auftragsnr')}")
        if str(proj_rec.get("Objekt","")).strip():
            lines.append(f"**Objekt:** {proj_rec.get('Objekt')}")
        if str(proj_rec.get("Kunde","")).strip():
            lines.append(f"**Kunde:** {proj_rec.get('Kunde')}")
        if str(proj_rec.get("Telefon","")).strip():
            lines.append(f"**Telefon:** {proj_rec.get('Telefon')}")
        if str(proj_rec.get("Kontaktperson","")).strip():
            lines.append(f"**Kontaktperson:** {proj_rec.get('Kontaktperson')}")
        if str(proj_rec.get("Kontakttelefon","")).strip():
            lines.append(f"**Kontakt-Tel.:** {proj_rec.get('Kontakttelefon')}")
        if lines:
            st.markdown("#### Projektdaten")
            st.info(" · ".join(lines))

    t1, t2, t3 = st.tabs(["📝 Rapport", "📷 Fotos", "📂 Pläne"])

    # --- RAPPORT ---
    with t1:
        st.subheader("Rapport erfassen")
        c1, c2, c3 = st.columns(3)

        date_val = c1.date_input("Datum", datetime.now())
        df_emp, _emp_fid = get_employees_df()
        df_emp_active = df_emp[df_emp["Status"].astype(str).str.lower().fillna("aktiv") == "aktiv"] if not df_emp.empty else df_emp
        if df_emp_active.empty:
            st.warning("Keine Mitarbeiter erfasst (Employees.csv fehlt/leer). Admin: Mitarbeiter anlegen.")
            ma_sel = None
        else:
            # Display name list, store EmployeeID
            emp_options = df_emp_active.apply(lambda r: f'{r["Name"]} ({r["EmployeeID"]})' if str(r["EmployeeID"]).strip() else str(r["Name"]), axis=1).tolist()
            emp_map = {emp_options[i]: df_emp_active.iloc[i].to_dict() for i in range(len(emp_options))}
            ma_key = c1.selectbox("Mitarbeiter", emp_options)
            ma_sel = emp_map.get(ma_key)
        has_ank = c1.checkbox("Ankunft Magazin erfassen", value=False)
        ank_mag = c1.time_input("Ankunft Magazin (Uhrzeit)", time(0, 0)) if has_ank else None
        has_abd = c2.checkbox("Abfahrt Magazin erfassen", value=False)
        abd_mag = c2.time_input("Abfahrt Magazin (Uhrzeit)", time(0, 0)) if has_abd else None
        reise_min = c3.number_input("Reisezeit direkt (Min, optional)", 0, 600, 0, 5)

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
            if not ma_sel:
                st.error("Mitarbeiter fehlt.")
            else:
                ok = save_report(
                    project,
                    {
                        "Datum": str(date_val),
                        "Projekt": project,
                        "ProjektID": str(proj_rec.get("ProjektID","")).strip(),
                        "Auftragsnr": str(proj_rec.get("Auftragsnr","")).strip(),
                        "Objekt": str(proj_rec.get("Objekt","")).strip(),
                        "Kunde": str(proj_rec.get("Kunde","")).strip(),
                        "Telefon": str(proj_rec.get("Telefon","")).strip(),
                        "Kontaktperson": str(proj_rec.get("Kontaktperson","")).strip(),
                        "Kontakttelefon": str(proj_rec.get("Kontakttelefon","")).strip(),
                        "EmployeeID": str(ma_sel.get("EmployeeID","")).strip(),
                        "Mitarbeiter": str(ma_sel.get("Name","")).strip(),
                        "AnkunftMagazin": str(ank_mag) if ank_mag else "",
                        "AbfahrtMagazin": str(abd_mag) if abd_mag else "",
                        "Reisezeit_h": round(float(reise_min) / 60.0, 2) if reise_min else 0.0,
                        "Start": str(start_val),
                        "Ende": str(end_val),
                        "Pause_h": pause_val,
                        "Stunden": dur,
                        "Material": mat_val,
                        "Bemerkung": rem_val,
                    },
                )
                if ok:
                    st.success("Gespeichert!")
                    sys_time.sleep(0.3)
                    st.rerun()
                else:
                    st.error("Speichern fehlgeschlagen (Drive). Bitte erneut versuchen.")

        st.divider()
        st.subheader("Woche abschliessen (Signatur)")

        if ma_sel:
            # Load reports for this project and employee
            df_r, _ = get_reports_df(project)
            if not df_r.empty:
                df_r2 = df_r.copy()
                df_r2["Datum_dt"] = pd.to_datetime(df_r2["Datum"], errors="coerce").dt.date
                df_r2 = df_r2[df_r2["EmployeeID"].astype(str) == str(ma_sel.get("EmployeeID","")).strip()]

                # Available weeks from data
                weeks = []
                for d in df_r2["Datum_dt"].dropna().unique():
                    y, w = iso_year_week(d)
                    weeks.append((y, w))
                weeks = sorted(set(weeks))
            else:
                df_r2 = pd.DataFrame()
                weeks = []

            # Add current week
            today = datetime.now().date()
            cy, cw = iso_year_week(today)
            if (cy, cw) not in weeks:
                weeks = [(cy, cw)] + weeks

            week_labels = [f"{y}-KW{w:02d}" for (y, w) in weeks] if weeks else [f"{cy}-KW{cw:02d}"]
            chosen = st.selectbox("Kalenderwoche", week_labels, index=0)
            y = int(chosen.split("-KW")[0])
            w = int(chosen.split("-KW")[1])

            w_start, w_end = week_date_range(y, w)
            st.caption(f"Zeitraum: {w_start.date()} – {w_end.date()}")

            if not df_r2.empty:
                mask = (df_r2["Datum_dt"] >= w_start.date()) & (df_r2["Datum_dt"] <= w_end.date())
                df_week = df_r2[mask].copy()
                total_hours = float(pd.to_numeric(df_week["Stunden"], errors="coerce").fillna(0).sum())
                total_travel = float(pd.to_numeric(df_week["Reisezeit_h"], errors="coerce").fillna(0).sum())
            else:
                df_week = pd.DataFrame()
                total_hours = 0.0
                total_travel = 0.0

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
                            "Projekt": project,
                            "ProjektID": str(proj_rec.get("ProjektID","")).strip(),
                            "EmployeeID": str(ma_sel.get("EmployeeID","")).strip(),
                            "Mitarbeiter": str(ma_sel.get("Name","")).strip(),
                            "Jahr": y,
                            "KW": w,
                            "Von": str(w_start.date()),
                            "Bis": str(w_end.date()),
                            "TotalStunden": round(total_hours, 2),
                            "TotalReisezeit_h": round(total_travel, 2),
                            "Timestamp": datetime.now().isoformat(timespec="seconds"),
                            "Signed": True,
                        }
                    )
                    if okc:
                        # AZK updaten (Arbeits- und Reisezeit pro MA/Woche/Projekt)
                        _ = upsert_azk_row({
                            "Jahr": str(y),
                            "KW": str(w),
                            "EmployeeID": str(ma_sel.get("EmployeeID","")).strip(),
                            "Mitarbeiter": str(ma_sel.get("Name","")).strip(),
                            "ProjektID": str(proj_rec.get("ProjektID","")).strip(),
                            "Projekt": project,
                            "TotalStunden": round(total_hours, 2),
                            "TotalReisezeit_h": round(total_travel, 2),
                            "Timestamp": datetime.now().isoformat(timespec="seconds"),
                        })
                        st.success("Woche abgeschlossen / signiert ✅ (AZK aktualisiert)")
                    else:
                        st.error("Konnte Signatur nicht speichern (Drive).")
        else:
            st.info("Mitarbeiter auswählen, um eine Woche abzuschliessen.")


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

    # --- FOTOS ---
    with t2:
        st.subheader("Fotos")

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

        files = list_files(PHOTOS_FOLDER_ID, mime_prefix="image/")
        proj_photos = [x for x in files if x["name"].startswith(project + "_")][:60]

        if not proj_photos:
            st.info("Keine Fotos vorhanden.")
        else:
            for f in proj_photos:
                with st.expander(f"🖼️ {f['name']}", expanded=False):
                    image_preview_from_drive(f["id"])

    # --- PLÄNE / DOKUMENTE (DOWNLOAD) ---
    with t3:
        st.subheader("Pläne & Dokumente")

        if st.button("🔄 Pläne aktualisieren", key="refresh_docs_emp"):
            st.rerun()

        files = list_files(UPLOADS_FOLDER_ID)
        proj_docs = [x for x in files if x["name"].startswith(project + "_")][:200]

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


# -------------------------
# 🛠️ ADMIN
# -------------------------
elif mode == "🛠️ Admin":
    st.title("🛠️ Admin")

    pin = st.text_input("PIN", type="password")
    if pin != ADMIN_PIN:
        st.stop()

    st.success("Angemeldet")

    # 🔐 Admin-Verknüpfung (für Handy / Startbildschirm)
    admin_link = f"{BASE_APP_URL}?mode=admin"
    st.info(
        "🔐 **Adminbereich als Verknüpfung speichern**\n\n"
        f"👉 Öffne diesen Link: {admin_link}\n\n"
        "• **iPhone (Safari):** Teilen → *Zum Home-Bildschirm*\n"
        "• **Android (Chrome):** ⋮ → *Zum Startbildschirm hinzufügen*\n\n"
        "⚠️ Zugriff nur mit Admin-PIN"
    )
    st.link_button("🔗 Admin-Link öffnen", admin_link)

    tabA, tabB, tabC, tabD, tabE = st.tabs(["📌 Projekte", "📂 Uploads & Übersicht", "🧾 Rapporte", "👤 Mitarbeiter", "📤 Exporte"])


    # --- Projekte ---
    with tabA:
        st.subheader("Projekte verwalten")
        df_p, pid = get_projects_df()

        c1, c2 = st.columns([0.7, 0.3])
        new_p = c1.text_input("Neues Projekt")

        if c2.button("➕ Anlegen") and new_p.strip():
            project_name = new_p.strip()
            df_p = pd.concat(
                [df_p, pd.DataFrame([{"Projekt": project_name, "Status": "aktiv"}])],
                ignore_index=True,
            )
            save_projects_df(df_p, pid)

            st.success(f"Projekt '{project_name}' angelegt.")

            st.divider()
            st.subheader(f"QR-Code für {project_name}")

            qr_bytes = generate_project_qr(project_name)

            col_qr1, col_qr2 = st.columns([0.3, 0.7])
            col_qr1.image(qr_bytes, width=200, caption=f"Scan für {project_name}")

            col_qr2.info("Diesen Code können Sie herunterladen und auf dem Rapport-Formular platzieren.")
            col_qr2.download_button(
                label="⬇️ QR-Code (.png) herunterladen",
                data=qr_bytes,
                file_name=f"QR_{project_name}.png",
                mime="image/png",
            )

        if df_p.empty:
            st.info("Noch keine Projekte.")
        else:
            st.markdown("---")
            st.subheader("Projektliste")
            st.dataframe(df_p, use_container_width=True)

            st.divider()
            st.subheader("Projekt Status ändern")
            all_projs = df_p["Projekt"].tolist()
            sel = st.selectbox("Projekt wählen", all_projs, key="admin_status_proj")
            new_status = st.radio(
                "Neuer Status",
                ["aktiv", "archiviert"],
                horizontal=True,
                key="admin_status_radio",
            )
            if st.button("Status speichern", key="admin_save_status"):
                df_p.loc[df_p["Projekt"] == sel, "Status"] = new_status
                save_projects_df(df_p, pid)
                st.success("Status geändert.")
                st.rerun()

        st.divider()
        st.subheader("🖨️ Druckvorlage erstellen (Scan-Design)")

        if not df_p.empty:
            print_proj = st.selectbox("Projekt für Druck wählen", get_active_projects(), key="print_sel")
            if st.button("Vorschau generieren", key="btn_preview"):
                qrb = generate_project_qr(print_proj)
                html_code = get_printable_html(print_proj, qrb)

                st.components.v1.html(html_code, height=600, scrolling=True)
                st.download_button(
                    label="Druckdatei (.html) herunterladen",
                    data=html_code,
                    file_name=f"Rapport_{print_proj}.html",
                    mime="text/html",
                )

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
            st.markdown("### 📷 Fotos (Upload via Cloud Run)")
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
            st.markdown("### 📄 Pläne/Dokumente (Upload direkt nach Drive)")
            st.caption("Dieser Upload ist bewusst im Adminbereich (PC zuverlässig; Handy je nach Browser).")

            admin_docs_files = st.file_uploader(
                "Dokumente auswählen",
                type=None,
                accept_multiple_files=True,
                key="admin_docs_uploader",
            )

            if st.button("📤 Dokument(e) speichern", type="primary", key="admin_docs_upload_btn"):
                if not admin_docs_files:
                    st.warning("Bitte zuerst Dokumente auswählen.")
                else:
                    ok_n = 0
                    fail_n = 0
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    for f in admin_docs_files:
                        try:
                            data = f.getvalue()
                            mime = getattr(f, "type", None) or "application/octet-stream"
                            filename = f"{sel_p}_{ts}_{f.name}"
                            if upload_bytes_to_drive(data, UPLOADS_FOLDER_ID, filename, mime):
                                ok_n += 1
                            else:
                                fail_n += 1
                        except Exception:
                            fail_n += 1

                    if fail_n == 0:
                        st.success(f"✅ {ok_n} Datei(en) in Drive gespeichert.")
                    else:
                        st.warning(f"⚠️ {ok_n} ok, {fail_n} fehlgeschlagen.")

                    sys_time.sleep(0.2)
                    st.rerun()

        st.divider()

        tabF, tabDocs = st.tabs(["📷 Fotos – Übersicht", "📄 Pläne/Dokumente – Übersicht"])

        with tabF:
            c1, c2 = st.columns([0.7, 0.3])
            c1.subheader("📷 Fotos – Vorschau")
            if c2.button("🔄 Aktualisieren", key="admin_refresh_photos"):
                st.rerun()

            files_ph = list_files(PHOTOS_FOLDER_ID, mime_prefix="image/")
            admin_photos = [x for x in files_ph if x["name"].startswith(sel_p + "_")][:180]

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
                                st.success("Gelöscht.")
                                sys_time.sleep(0.2)
                                st.rerun()

        with tabDocs:
            c1, c2 = st.columns([0.7, 0.3])
            c1.subheader("📄 Pläne/Dokumente – Download")
            if c2.button("🔄 Aktualisieren", key="admin_refresh_docs"):
                st.rerun()

            files_docs = list_files(UPLOADS_FOLDER_ID)
            admin_docs = [x for x in files_docs if x["name"].startswith(sel_p + "_")][:400]

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

        projs = get_all_projects()
        if not projs:
            st.info("Keine Projekte vorhanden.")
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
            )


    # --- Mitarbeiter ---
    with tabD:
        st.subheader("Mitarbeiter verwalten")
        st.caption("PIN wird nur für Wochenabschluss/Signatur verwendet (kein Login).")

        df_emp, emp_fid = get_employees_df()

        if df_emp.empty:
            st.info("Noch keine Mitarbeiter vorhanden. Füge unten eine Zeile hinzu.")
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
            key="emp_editor",
        )

        if st.button("💾 Mitarbeiter speichern", type="primary"):
            df_save = edited.copy()
            # Basic cleanup
            df_save["EmployeeID"] = df_save["EmployeeID"].astype(str).str.strip()
            df_save["Name"] = df_save["Name"].astype(str).str.strip()
            df_save["PIN"] = df_save["PIN"].astype(str).str.strip()
            df_save["Status"] = df_save["Status"].fillna("aktiv").replace("", "aktiv")

            # Auto-generate EmployeeID if missing
            for idx, row in df_save.iterrows():
                if not str(row.get("EmployeeID", "")).strip() and str(row.get("Name", "")).strip():
                    base = re.sub(r"[^A-Za-z0-9]+", "", str(row["Name"]).upper())[:6]
                    df_save.at[idx, "EmployeeID"] = base or str(uuid4())[:6].upper()

            ok = save_employees_df(df_save, emp_fid)
            if ok:
                st.success("Mitarbeiter gespeichert ✅")
                sys_time.sleep(0.2)
                st.rerun()
            else:
                st.error("Konnte Employees.csv nicht speichern (Drive).")

        st.divider()
        st.subheader("Wochenabschlüsse / Signaturen (Übersicht)")
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

            st.caption("Reversibel: Admin kann einzelne Einträge entfernen.")
            del_idx = st.number_input("Zeilenindex löschen (0..n-1)", min_value=0, max_value=max(0, len(df_cl) - 1), value=0, step=1)
            if st.button("🗑️ Signatur-Zeile löschen"):
                df2 = df_cl.drop(index=int(del_idx)).reset_index(drop=True)
                ok = update_file_in_drive(cl_fid, df2.to_csv(index=False).encode("utf-8"), mimetype="text/csv") if cl_fid else upload_bytes_to_drive(df2.to_csv(index=False).encode("utf-8"), REPORTS_FOLDER_ID, CLOSURES_CSV_NAME, "text/csv")
                if ok:
                    st.success("Gelöscht ✅")
                    sys_time.sleep(0.2)
                    st.rerun()
                else:
                    st.error("Löschen fehlgeschlagen (Drive).")

    # --- Exporte ---
    with tabE:
        st.subheader("Exporte")
        projs = get_all_projects()
        if not projs:
            st.info("Keine Projekte vorhanden.")
        else:
            sel = st.selectbox("Projekt für Export", projs, key="exp_proj")
            df_r, _ = get_reports_df(sel)
            if df_r.empty:
                st.info("Keine Rapporte für dieses Projekt.")
            else:
                # Normalize types
                df_r2 = df_r.copy()
                df_r2["Datum_dt"] = pd.to_datetime(df_r2["Datum"], errors="coerce").dt.date
                df_r2["Stunden_num"] = pd.to_numeric(df_r2["Stunden"], errors="coerce").fillna(0.0)
                reise_series = df_r2["Reisezeit_h"] if "Reisezeit_h" in df_r2.columns else pd.Series([0.0] * len(df_r2))
                df_r2["Reise_num"] = pd.to_numeric(reise_series, errors="coerce").fillna(0.0)

                # 1) Baustellenrapporte-Layout (CSV)
                st.markdown("### Export: Baustellenrapporte (CSV)")
                export_cols = [
                    "Jahr",
                    "KW",
                    "Auftrag erfasst!!",
                    "Rechnung gestellt",
                    "Zeit Stempel",
                    "Firma",
                    "Baustelle",
                    "MA",
                    "Datum",
                    "Vormittag ab",
                    "Vormittag bis",
                    "Std. Vorm.",
                    "Nachmittag ab",
                    "Nachmittag bis",
                    "Std. Nachm.",
                    "Kürzel",
                    "Notizen/Spezieles",
                    "Total Std. V. & N.",
                ]
                rows = []
                for _, r in df_r2.iterrows():
                    d = r.get("Datum_dt")
                    if pd.isna(d) or d is None:
                        continue
                    y, w = iso_year_week(d)
                    rows.append(
                        {
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
                        }
                    )
                df_exp = pd.DataFrame(rows, columns=export_cols)
                st.download_button(
                    "⬇️ Baustellenrapporte_export.csv",
                    df_exp.to_csv(index=False).encode("utf-8"),
                    file_name=f"{sel}_Baustellenrapporte_export.csv",
                )

                # 2) AZK-Basisexport (CSV)
                st.markdown("### Export: AZK Basis (CSV)")
                df_azk = df_r2.rename(
                    columns={
                        "Projekt": "Projekt",
                        "EmployeeID": "EmployeeID",
                        "Mitarbeiter": "Mitarbeiter",
                        "Stunden_num": "Stunden",
                        "Reise_num": "Reisezeit_h",
                        "Material": "Material",
                        "Bemerkung": "Bemerkung",
                    }
                )
                df_azk_out = pd.DataFrame(
                    {
                        "Datum": df_azk["Datum_dt"].astype(str),
                        "Projekt": sel,
                        "EmployeeID": df_azk.get("EmployeeID", "").astype(str),
                        "Mitarbeiter": df_azk.get("Mitarbeiter", "").astype(str),
                        "Stunden": df_r2["Stunden_num"],
                        "Reisezeit_h": df_r2["Reise_num"],
                        "Material": df_r2.get("Material", "").astype(str),
                        "Bemerkung": df_r2.get("Bemerkung", "").astype(str),
                    }
                )
                st.download_button(
                    "⬇️ AZK_Rapporte_export.csv",
                    df_azk_out.to_csv(index=False).encode("utf-8"),
                    file_name=f"{sel}_AZK_Rapporte_export.csv",
                )
