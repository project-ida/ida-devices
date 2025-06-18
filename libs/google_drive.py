from googleapiclient.discovery import build
import pandas as pd
import time

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
        f.write('File Name\n')
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
            pd.DataFrame(batch, columns=['File Name']).to_csv(
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