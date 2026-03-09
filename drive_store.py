import io
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive"
]


def get_drive_service() -> Optional[Resource]:
    """
    Baut den Google-Drive-Service aus st.secrets['gcp_service_account'] auf.
    Erwartet einen [gcp_service_account]-Block in secrets.toml.
    """
    try:
        if "gcp_service_account" not in st.secrets:
            st.error("Service-Account-Konfiguration fehlt in secrets.toml.")
            return None

        service_account_info = dict(st.secrets["gcp_service_account"])

        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=DRIVE_SCOPES,
        )

        service = build("drive", "v3", credentials=credentials)
        return service

    except Exception as e:
        st.error(f"Google-Drive-Service konnte nicht aufgebaut werden: {e}")
        return None


def _safe_query_value(value: str) -> str:
    """
    Escaped einfache Apostrophe für Drive-Queries.
    """
    return value.replace("'", r"\'")


def list_files(
    service: Resource,
    folder_id: str,
    name: Optional[str] = None,
    mime_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Listet Dateien in einem Ordner auf.
    Optional nach Name und/oder MIME-Type filterbar.
    """
    try:
        query_parts = [f"'{folder_id}' in parents", "trashed = false"]

        if name:
            query_parts.append(f"name = '{_safe_query_value(name)}'")
        if mime_type:
            query_parts.append(f"mimeType = '{mime_type}'")

        query = " and ".join(query_parts)

        response = service.files().list(
            q=query,
            fields="files(id, name, mimeType, createdTime, modifiedTime, parents)",
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()

        return response.get("files", [])

    except HttpError as e:
        st.error(f"Fehler beim Auflisten von Dateien: {e}")
        return []


def get_file_id(
    service: Resource,
    folder_id: str,
    filename: str,
) -> Optional[str]:
    """
    Sucht eine Datei mit exaktem Dateinamen in einem Ordner.
    """
    files = list_files(service, folder_id, name=filename)
    if not files:
        return None
    return files[0]["id"]


def get_folder_id(
    service: Resource,
    parent_folder_id: str,
    folder_name: str,
) -> Optional[str]:
    """
    Sucht einen Unterordner in einem Parent-Ordner.
    """
    folders = list_files(
        service,
        parent_folder_id,
        name=folder_name,
        mime_type="application/vnd.google-apps.folder",
    )
    if not folders:
        return None
    return folders[0]["id"]


def ensure_folder(
    service: Resource,
    parent_folder_id: str,
    folder_name: str,
) -> Optional[str]:
    """
    Sucht einen Ordner. Falls nicht vorhanden, wird er erstellt.
    """
    try:
        existing_folder_id = get_folder_id(service, parent_folder_id, folder_name)
        if existing_folder_id:
            return existing_folder_id

        metadata = {
            "name": folder_name,
            "parents": [parent_folder_id],
            "mimeType": "application/vnd.google-apps.folder",
        }

        folder = service.files().create(
            body=metadata,
            fields="id",
            supportsAllDrives=True,
        ).execute()

        return folder.get("id")

    except HttpError as e:
        st.error(f"Fehler beim Erstellen des Ordners '{folder_name}': {e}")
        return None


def read_csv(
    service: Resource,
    folder_id: str,
    filename: str,
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Liest eine CSV aus Google Drive.
    Gibt (DataFrame, file_id) zurück.
    """
    try:
        file_id = get_file_id(service, folder_id, filename)
        if not file_id:
            return pd.DataFrame(), None

        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        buffer.seek(0)
        df = pd.read_csv(buffer)
        return df, file_id

    except pd.errors.EmptyDataError:
        return pd.DataFrame(), file_id if "file_id" in locals() else None
    except HttpError as e:
        st.error(f"Fehler beim Lesen von '{filename}': {e}")
        return pd.DataFrame(), None
    except Exception as e:
        st.error(f"Unerwarteter Fehler beim Lesen von '{filename}': {e}")
        return pd.DataFrame(), None


def save_csv(
    service: Resource,
    folder_id: str,
    filename: str,
    df: pd.DataFrame,
    file_id: Optional[str] = None,
) -> Optional[str]:
    """
    Speichert ein DataFrame als CSV in Google Drive.
    Falls file_id vorhanden ist, wird die Datei aktualisiert.
    Sonst wird sie neu erstellt.
    """
    try:
        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        media = MediaIoBaseUpload(
            csv_buffer,
            mimetype="text/csv",
            resumable=True,
        )

        if file_id:
            updated = service.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True,
                fields="id",
            ).execute()
            return updated.get("id")

        metadata = {
            "name": filename,
            "parents": [folder_id],
        }

        created = service.files().create(
            body=metadata,
            media_body=media,
            supportsAllDrives=True,
            fields="id",
        ).execute()

        return created.get("id")

    except HttpError as e:
        st.error(f"Fehler beim Speichern von '{filename}': {e}")
        return None
    except Exception as e:
        st.error(f"Unerwarteter Fehler beim Speichern von '{filename}': {e}")
        return None


def ensure_csv_exists(
    service: Resource,
    folder_id: str,
    filename: str,
    columns: List[str],
) -> Optional[str]:
    """
    Stellt sicher, dass eine CSV-Datei existiert.
    Falls nicht, wird sie mit den übergebenen Spalten erstellt.
    """
    existing_file_id = get_file_id(service, folder_id, filename)
    if existing_file_id:
        return existing_file_id

    empty_df = pd.DataFrame(columns=columns)
    return save_csv(service, folder_id, filename, empty_df)


def upload_file(
    service: Resource,
    folder_id: str,
    filename: str,
    file_bytes: bytes,
    mime_type: str,
) -> Optional[str]:
    """
    Lädt beliebige Datei-Bytes nach Google Drive hoch.
    """
    try:
        media = MediaIoBaseUpload(
            io.BytesIO(file_bytes),
            mimetype=mime_type,
            resumable=True,
        )

        metadata = {
            "name": filename,
            "parents": [folder_id],
        }

        created = service.files().create(
            body=metadata,
            media_body=media,
            supportsAllDrives=True,
            fields="id",
        ).execute()

        return created.get("id")

    except HttpError as e:
        st.error(f"Fehler beim Hochladen von '{filename}': {e}")
        return None
    except Exception as e:
        st.error(f"Unerwarteter Fehler beim Hochladen von '{filename}': {e}")
        return None


def upload_streamlit_file(
    service: Resource,
    folder_id: str,
    uploaded_file,
) -> Optional[str]:
    """
    Lädt eine von Streamlit hochgeladene Datei nach Google Drive.
    """
    try:
        file_bytes = uploaded_file.getvalue()
        return upload_file(
            service=service,
            folder_id=folder_id,
            filename=uploaded_file.name,
            file_bytes=file_bytes,
            mime_type=uploaded_file.type or "application/octet-stream",
        )
    except Exception as e:
        st.error(f"Fehler beim Verarbeiten der Upload-Datei: {e}")
        return None


def download_file_bytes(
    service: Resource,
    file_id: str,
) -> Optional[bytes]:
    """
    Lädt eine Datei aus Google Drive als Bytes.
    """
    try:
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        return buffer.getvalue()

    except HttpError as e:
        st.error(f"Fehler beim Download der Datei: {e}")
        return None
    except Exception as e:
        st.error(f"Unerwarteter Fehler beim Download der Datei: {e}")
        return None


def copy_file(
    service: Resource,
    source_file_id: str,
    new_name: str,
    target_folder_id: str,
) -> Optional[str]:
    """
    Kopiert eine bestehende Datei in einen Zielordner.
    Wichtig für:
    - Monatsvorlagen
    - AZK-Vorlagen
    - SPV-Dateien pro Mitarbeiter
    """
    try:
        copied = service.files().copy(
            fileId=source_file_id,
            body={
                "name": new_name,
                "parents": [target_folder_id],
            },
            supportsAllDrives=True,
            fields="id",
        ).execute()

        return copied.get("id")

    except HttpError as e:
        st.error(f"Fehler beim Kopieren der Datei '{new_name}': {e}")
        return None
    except Exception as e:
        st.error(f"Unerwarteter Fehler beim Kopieren der Datei '{new_name}': {e}")
        return None


def move_file(
    service: Resource,
    file_id: str,
    old_parent_id: str,
    new_parent_id: str,
) -> bool:
    """
    Verschiebt eine Datei von einem Ordner in einen anderen.
    """
    try:
        service.files().update(
            fileId=file_id,
            addParents=new_parent_id,
            removeParents=old_parent_id,
            supportsAllDrives=True,
        ).execute()
        return True

    except HttpError as e:
        st.error(f"Fehler beim Verschieben der Datei: {e}")
        return False
    except Exception as e:
        st.error(f"Unerwarteter Fehler beim Verschieben der Datei: {e}")
        return False


def get_file_metadata(
    service: Resource,
    file_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Holt Metadaten einer Datei.
    """
    try:
        metadata = service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, createdTime, modifiedTime, parents",
            supportsAllDrives=True,
        ).execute()
        return metadata

    except HttpError as e:
        st.error(f"Fehler beim Lesen der Metadaten: {e}")
        return None
    except Exception as e:
        st.error(f"Unerwarteter Fehler beim Lesen der Metadaten: {e}")
        return None
