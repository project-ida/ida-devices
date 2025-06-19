from googleapiclient.discovery import build
import pandas as pd
import time
import os
import threading
from queue import Queue
import random

# Initialize drive_service (assumes auth.authenticate_user() was called before importing)
drive_service = build('drive', 'v3')

def get_folder_id(parent_folder_id, path):
    """
    Get the folder ID by traversing a relative path from a parent folder ID.
    
    Args:
        parent_folder_id: The ID of the parent folder to start from.
        path: Relative path to the target folder (e.g., 'thinkpad-t480s/run5_5inch_hv1660b/RAW').
    
    Returns:
        The ID of the target folder.
    
    Raises:
        Exception: If a folder in the path is not found.
    """
    current_folder_id = parent_folder_id
    parts = path.strip('/').split('/')
    for part in parts:
        response = drive_service.files().list(
            q=f"'{current_folder_id}' in parents and name = '{part}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
            spaces='drive',
            fields='files(id)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = response.get('files', [])
        if not files:
            raise Exception(f"Folder '{part}' not found in parent folder ID '{current_folder_id}'.")
        current_folder_id = files[0]['id']
    return current_folder_id

def save_filenames(folder_id, output_csv='all_files.csv'):
    """
    List files inside a Drive folder and save to CSV, showing progress.
    
    Args:
        folder_id: The ID of the folder to list files from.
        output_csv: Path to the output CSV file (default: 'all_files.csv').
    
    Returns:
        None
    """
    page_token = None
    batch_count = 0
    total_files = 0
    with open(output_csv, 'w') as f:
        f.write('filename\n')
    print("Fetching files...")
    while True:
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            spaces='drive',
            fields='nextPageToken, files(name)',
            pageSize=1000,
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        files = response.get('files', [])
        batch = [f['name'] for f in files]
        if batch:
            pd.DataFrame(batch, columns=['filename']).to_csv(
                output_csv, mode='a', header=False, index=False
            )
        batch_count += 1
        total_files += len(batch)
        print(f"Batch {batch_count}: Got {len(batch)} files (Total: {total_files})")
        page_token = response.get('nextPageToken')
        if not page_token:
            break
        time.sleep(0.5)
    print(f"Found {total_files} files.")
    return

def wait_for_drive_ready(csv_path='processed_files.csv', timeout=5, retry_interval=30):
    """
    Check if a random file from CSV exists in Google Drive mount until accessible, indicating Drive is ready.
    
    Args:
        csv_path (str): Path to CSV with at least a 'filename' column containing full file paths.
        timeout (int): Seconds to wait per existence check (default: 5).
        retry_interval (int): Seconds to wait between retries (default: 30).
    """
    # Read CSV and select random file
    try:
        df = pd.read_csv(csv_path)
        if 'filename' not in df.columns:
            print("CSV missing 'filename' column")
            raise SystemExit
        if df.empty:
            print("CSV is empty, no files found")
            raise SystemExit
        file_path = df['filename'].sample(n=1).iloc[0]
        file_name = os.path.basename(file_path)
    except FileNotFoundError:
        print(f"CSV file {csv_path} not found")
        raise SystemExit
    except Exception as e:
        print(f"Error reading CSV: {e}")
        raise SystemExit
    
    print(f"Checking if Google Drive is ready (testing with file: {file_name})...")
    
    def check_file_exists_with_timeout(path, timeout):
        result = Queue()
        def target():
            try:
                exists = os.path.exists(path)
                result.put(exists)
            except Exception:
                result.put(False)
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout)
        return False if thread.is_alive() else result.get()
    
    # Poll with timeout and retry
    attempt = 1
    start_time = time.time()
    
    while True:
        if check_file_exists_with_timeout(file_path, timeout):
            total_time = time.time() - start_time
            print(f"Drive is ready after {attempt} attempts (total wait: {total_time:.2f} seconds)")
            break
        else:
            total_time = time.time() - start_time
            print(f"Retry {attempt}: File not accessible, total wait: {total_time:.2f} seconds")
            attempt += 1
            time.sleep(retry_interval)