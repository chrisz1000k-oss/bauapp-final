import streamlit as st
import pandas as pd
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# --- AUTHENTIFIZIERUNG ---
def get_drive_service():
    """Erstellt die Verbindung zu Google Drive über Service Account aus secrets.toml"""
    try:
        if "gcp_service_account" not in st.secrets:
            st.error("Service-Account Konfiguration fehlt in secrets.toml.")
            return None

        sa_info = dict(st.secrets["gcp_service_account"])

        scopes = [
            "https://www.googleapis.com/auth/drive"
        ]

        creds = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=scopes
        )

        return build("drive", "v3", credentials=creds)
    except Exception as e:
        st.error(f"Google Drive Verbindung fehlgeschlagen: {e}")
        return None

# --- DATEI OPERATIONEN ---
def get_file_id(service, folder_id, filename):
    """Sucht eine Datei im Ordner und gibt die ID zurück"""
    try:
        query = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
        results = service.files().list(q=query, fields="files(id)").execute()
        files = results.get('files', [])
        return files[0]['id'] if files else None
    except Exception as e:
        st.error(f"Verbindungsfehler bei Suche nach '{filename}': {e}")
        return None

def read_csv(service, folder_id, filename):
    """Lädt eine CSV-Datei aus Drive in ein Pandas DataFrame"""
    file_id = get_file_id(service, folder_id, filename)
    
    if not file_id:
        return pd.DataFrame(), None # Datei existiert noch nicht

    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        
        fh.seek(0)
        return pd.read_csv(fh), file_id
    except Exception as e:
        # Falls Datei leer oder defekt, leeres DataFrame zurückgeben
        return pd.DataFrame(), file_id

def save_csv(service, folder_id, filename, df, file_id=None):
    """Speichert (überschreibt) ein DataFrame als CSV in Drive"""
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_bytes = csv_buffer.getvalue()
    
    media = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype='text/csv', resumable=True)
    
    if file_id:
        # Update bestehende Datei
        service.files().update(fileId=file_id, media_body=media).execute()
    else:
        # Erstelle neue Datei
        file_metadata = {'name': filename, 'parents': [folder_id]}
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()

def upload_image(service, folder_id, filename, file_obj, mime_type):
    """Lädt ein Bild direkt hoch"""
    media = MediaIoBaseUpload(file_obj, mimetype=mime_type, resumable=True)
    file_metadata = {'name': filename, 'parents': [folder_id]}
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()
