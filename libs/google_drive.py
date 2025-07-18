from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError
import pandas as pd
import time
import os
import threading
from queue import Queue
from pathlib import Path
from google.colab import auth

# Initialize drive_service as None
drive_service = None

def initialize_drive_service():
    """Initialize or reinitialize the Google Drive service after authentication."""
    global drive_service
    try:
        print("Authenticating user for Google Drive API...")
        auth.authenticate_user()  # Force Colab authentication
        drive_service = build('drive', 'v3')
        print("Drive service initialized successfully.")
    except Exception as e:
        print(f"Failed to initialize Drive service: {e}")
        print("Please run the following command in a new cell to authenticate:")
        print("from google.colab import auth; auth.authenticate_user()")
        raise SystemExit

def prompt_for_auth(error_message):
    """Prompt user to run authentication command."""
    print(f"Authentication error: {error_message}")
    print("Please run the following command in a new cell to authenticate:")
    print("from google.colab import auth; auth.authenticate_user()")
    print("After authentication, rerun the function.")
    raise SystemExit

def get_folder_id(parent_folder_id, path):
    """
    Get the folder ID by traversing a relative path from a parent folder ID.
    
    Args:
        parent_folder_id: The ID of the parent folder to start from.
        path: Relative path to the target folder (e.g., 'thinkpad-t480s/run5_5inch_hv1660b/RAW').
    
    Returns:
        The ID of the target folder.
    
    Raises:
        ValueError: If the parent_folder_id or path is invalid.
        Exception: If a folder in the path is not found or other API errors occur.
    """
    global drive_service
    if drive_service is None:
        initialize_drive_service()
    
    if not parent_folder_id or not isinstance(parent_folder_id, str):
        raise ValueError(f"Invalid parent_folder_id: {parent_folder_id}")
    
    if not path or not isinstance(path, str):
        raise ValueError(f"Invalid path: {path}")
    
    current_folder_id = parent_folder_id
    parts = path.strip('/').split('/')
    for part in parts:
        if not part:  # Skip empty path parts
            continue
        print(f"Searching for folder '{part}' in parent folder ID '{current_folder_id}'...")
        try:
            response = drive_service.files().list(
                q=f"'{current_folder_id}' in parents and name = '{part}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
                spaces='drive',
                fields='files(id, name)',
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            files = response.get('files', [])
            if not files:
                raise Exception(f"Folder '{part}' not found in parent folder ID '{current_folder_id}'.")
            if len(files) > 1:
                print(f"Warning: Multiple folders named '{part}' found in parent ID '{current_folder_id}'. Using the first one.")
            current_folder_id = files[0]['id']
            print(f"Found folder '{part}' with ID '{current_folder_id}'.")
        except (HttpError, RefreshError) as e:
            if isinstance(e, HttpError) and e.resp.status in [401, 403]:  # Unauthorized or Forbidden
                prompt_for_auth(f"HTTP Error {e.resp.status}: {e}")
            elif isinstance(e, RefreshError):
                prompt_for_auth(f"Credential refresh failed: {e}")
            else:
                raise Exception(f"Error accessing folder '{part}' in parent ID '{current_folder_id}': {e}")
    return current_folder_id

def get_folder_contents(folder_id, subfolders=False, save_to_csv=False, output_csv='all_files.csv'):
    """
    List files or subfolders inside a Drive folder, with optional CSV saving.
    
    Args:
        folder_id (str): The ID of the folder to list contents from.
        subfolders (bool): If True, list subfolders; if False, list files (default: False).
        save_to_csv (bool): If True, save results to a CSV file (default: False).
        output_csv (str): Path to the output CSV file (default: 'all_files.csv').
    
    Returns:
        list: A list of file or subfolder names.
    """
    global drive_service
    if drive_service is None:
        initialize_drive_service()
    
    content_type = "subfolders" if subfolders else "files"
    mime_type_filter = "mimeType = 'application/vnd.google-apps.folder'" if subfolders else "mimeType != 'application/vnd.google-apps.folder'"
    page_token = None
    batch_count = 0
    total_items = 0
    all_contents = []
    
    print(f"Fetching {content_type}...")
    while True:
        try:
            response = drive_service.files().list(
                q=f"'{folder_id}' in parents and {mime_type_filter} and trashed = false",
                spaces='drive',
                fields='nextPageToken, files(name)',
                pageSize=1000,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            files = response.get('files', [])
            if not files and batch_count == 0 and not page_token:
                break
            batch = [f['name'] for f in files]
            if batch:
                all_contents.extend(batch)
            batch_count += 1
            total_items += len(batch)
            if files:
                print(f"Batch {batch_count}: Got {len(batch)} {content_type} (Total: {total_items})")
            page_token = response.get('nextPageToken')
            if not page_token:
                if total_items > 0:
                    print(f"Found {total_items} {content_type}.")
                break
            time.sleep(0.5)
        except (HttpError, RefreshError) as e:
            if isinstance(e, HttpError) and e.resp.status in [401, 403]:  # Unauthorized or Forbidden
                prompt_for_auth(f"HTTP Error {e.resp.status}: {e}")
            elif isinstance(e, RefreshError):
                prompt_for_auth(f"Credential refresh failed: {e}")
            else:
                raise Exception(f"Error listing {content_type}: {e}")
    
    if total_items == 0:
        print(f"No {content_type} found in the specified folder.")
    
    if save_to_csv and all_contents:
        header = 'foldername' if subfolders else 'filename'
        pd.DataFrame(all_contents, columns=[header]).to_csv(
            output_csv, mode='w', header=True, index=False
        )
        print(f"Saved {total_items} {content_type} to {output_csv}.")
    
    return all_contents

def wait_for_drive_ready(folder_path, timeout=5, retry_interval=30):
    """
    Wait until a Google Drive folder is accessible (i.e., os.listdir() succeeds).
    
    Args:
        folder_path (str): Path to the mounted Drive folder (e.g., '/content/drive/MyDrive/.../RAW').
        timeout (int): Seconds to wait per check attempt (default: 5).
        retry_interval (int): Seconds to wait between retries (default: 30).
    
    Returns:
        None
    """
    try:
        resolved_path = str(Path(folder_path).resolve())
    except Exception as e:
        print(f"Error resolving path '{folder_path}': {e}")
        raise SystemExit
    
    print("Warning: This can take several minutes for folders with tens of thousands of files.")
    print(f"Checking if Google Drive is ready (folder: {folder_path})...")
    
    def can_list_dir(path, timeout):
        result = Queue()
        def try_list():
            try:
                os.listdir(path)
                result.put(True)
            except Exception:
                result.put(False)
        thread = threading.Thread(target=try_list)
        thread.daemon = True
        thread.start()
        thread.join(timeout)
        return False if thread.is_alive() else result.get()
    
    attempt = 1
    start_time = time.time()
    
    while True:
        if can_list_dir(resolved_path, timeout):
            total_time_minutes = (time.time() - start_time) / 60
            print(f"Drive is ready after {attempt} attempts (total wait: {total_time_minutes:.2f} minutes)")
            break
        else:
            total_time_minutes = (time.time() - start_time) / 60
            print(f"Retry {attempt}: Folder not accessible, total wait: {total_time_minutes:.2f} minutes")
            attempt += 1
            time.sleep(retry_interval)