import hashlib
from datetime import datetime, date
from typing import List

import bcrypt
import pandas as pd


PROJECTS_FILE = "Projects.csv"
EMPLOYEES_FILE = "Employees.csv"
REPORTS_FILE = "Reports.csv"
WEEKLY_SIG_FILE = "WeeklySignatures.csv"


PROJECTS_SCHEMA: List[str] = ["projekt_id", "projekt_name", "status"]

EMPLOYEES_SCHEMA: List[str] = [
    "employee_id", "employee_name", "rolle", "status",
    "contact_email", "contact_phone", "pin_hash_bcrypt"
]

REPORTS_SCHEMA: List[str] = [
    "rapport_id", "version", "status",
    "created_at", "created_by",
    "confirmed_at", "confirmed_by",
    "datum",
    "projekt_id", "projekt_name",
    "mitarbeiter_name", "gast_info",
    "start_time", "end_time", "pause_h",
    "reisezeit_min", "mittag",
    "arbeitsbeschrieb",
    "material", "material_regie",
    "fugenfarbe", "fugen_code",
    "asbest_relevant", "asbest_probe_genommen",
    "hours",
    "correction_reason"
]

WEEKLY_SIG_SCHEMA: List[str] = [
    "week_id", "employee_key", "projekt_id",
    "signed_at", "signed_by_display", "signature_method",
    "signature_image_file_id",
    "pdf_file_id",
    "status",
    "invalidated_at", "invalidated_by", "invalidation_reason",
    "token_hash", "token_expires_at"
]


def now_utc_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def week_id_from_date(d: date) -> str:
    y, w, _ = d.isocalendar()
    return f"{y}-W{int(w):02d}"


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def ensure_schema(df: pd.DataFrame, schema: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=schema)
    out = df.copy()
    for c in schema:
        if c not in out.columns:
            out[c] = None
    return out


def ensure_projects(df: pd.DataFrame) -> pd.DataFrame:
    df = ensure_schema(df, PROJECTS_SCHEMA)

    # backward compat
    if "Projekt" in df.columns and "projekt_name" in df.columns and df["projekt_name"].isna().all():
        df["projekt_name"] = df["Projekt"]
    if "Status" in df.columns and "status" in df.columns and df["status"].isna().all():
        df["status"] = df["Status"]

    df["projekt_name"] = df["projekt_name"].fillna("").astype(str)
    if df["projekt_id"].isna().all():
        df["projekt_id"] = df["projekt_name"].apply(lambda x: x.strip().replace(" ", "_")[:64] if x else None)

    df["status"] = df["status"].fillna("aktiv")
    return df


def ensure_employees(df: pd.DataFrame) -> pd.DataFrame:
    df = ensure_schema(df, EMPLOYEES_SCHEMA)
    df["status"] = df["status"].fillna("aktiv")
    df["rolle"] = df["rolle"].fillna("STAMM")
    return df


def ensure_reports(df: pd.DataFrame) -> pd.DataFrame:
    df = ensure_schema(df, REPORTS_SCHEMA)

    # backward compat mapping (aus alter App)
    if "Datum" in df.columns and df["datum"].isna().all():
        df["datum"] = df["Datum"]
    if "Projekt" in df.columns and df["projekt_name"].isna().all():
        df["projekt_name"] = df["Projekt"]
    if "Mitarbeiter" in df.columns and df["mitarbeiter_name"].isna().all():
        df["mitarbeiter_name"] = df["Mitarbeiter"]
    if "Start" in df.columns and df["start_time"].isna().all():
        df["start_time"] = df["Start"]
    if "Ende" in df.columns and df["end_time"].isna().all():
        df["end_time"] = df["Ende"]
    if "Pause_h" in df.columns and df["pause_h"].isna().all():
        df["pause_h"] = df["Pause_h"]
    if "Bemerkung" in df.columns and df["arbeitsbeschrieb"].isna().all():
        df["arbeitsbeschrieb"] = df["Bemerkung"]
    if "Material" in df.columns and df["material"].isna().all():
        df["material"] = df["Material"]

    df["version"] = pd.to_numeric(df["version"], errors="coerce").fillna(1).astype(int)
    df["status"] = df["status"].fillna("DRAFT")
    df["hours"] = pd.to_numeric(df["hours"], errors="coerce").fillna(0).astype(float)

    if df["created_at"].isna().all():
        df["created_at"] = now_utc_iso()

    return df


def ensure_weekly_signatures(df: pd.DataFrame) -> pd.DataFrame:
    df = ensure_schema(df, WEEKLY_SIG_SCHEMA)
    return df


def bcrypt_hash_pin(pin: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(pin.encode("utf-8"), salt).decode("utf-8")


def bcrypt_check_pin(pin: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(pin.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False

