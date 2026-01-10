import io
from typing import Optional, Tuple, List, Dict

import pandas as pd
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


def list_files(drive, folder_id: str) -> List[Dict]:
    q = f"'{folder_id}' in parents and trashed=false"
    res = drive.files().list(
        q=q,
        pageSize=500,
        fields="files(id,name,mimeType,createdTime)",
        orderBy="createdTime desc",
    ).execute()
    return res.get("files", [])


def find_file_id(drive, folder_id: str, filename: str) -> Optional[str]:
    files = list_files(drive, folder_id)
    hit = next((f for f in files if f["name"] == filename), None)
    return hit["id"] if hit else None


def download_bytes(drive, file_id: str) -> Optional[bytes]:
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


def upload_bytes(drive, *, data: bytes, folder_id: str, filename: str, mimetype: str) -> str:
    file_metadata = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
    created = drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
    return created["id"]


def update_bytes(drive, *, file_id: str, data: bytes, mimetype: str) -> None:
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mimetype, resumable=True)
    drive.files().update(fileId=file_id, media_body=media).execute()


def load_csv(drive, *, folder_id: str, filename: str) -> Tuple[pd.DataFrame, Optional[str]]:
    file_id = find_file_id(drive, folder_id, filename)
    if not file_id:
        return pd.DataFrame(), None
    b = download_bytes(drive, file_id)
    if not b:
        return pd.DataFrame(), file_id
    try:
        return pd.read_csv(io.BytesIO(b)), file_id
    except Exception:
        return pd.DataFrame(), file_id


def save_csv(drive, *, folder_id: str, filename: str, df: pd.DataFrame, file_id: Optional[str]) -> str:
    data = df.to_csv(index=False).encode("utf-8")
    if file_id:
        update_bytes(drive, file_id=file_id, data=data, mimetype="text/csv")
        return file_id
    return upload_bytes(drive, data=data, folder_id=folder_id, filename=filename, mimetype="text/csv")


def ensure_subfolder(drive, parent_id: str, folder_name: str) -> str:
    files = list_files(drive, parent_id)
    hit = next(
        (f for f in files if f["name"] == folder_name and f["mimeType"] == "application/vnd.google-apps.folder"),
        None
    )
    if hit:
        return hit["id"]

    folder_metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    created = drive.files().create(body=folder_metadata, fields="id").execute()
    return created["id"]

