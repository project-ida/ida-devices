# caen-rootpostprocessing.py
#
# Purpose:
#   Processes ROOT files from a nuclear physics experiment to extract event timestamps,
#   energy, and PSP, storing them in a PostgreSQL database. Stores PSP and energy as
#   a double precision array [psp, energy] in the channels column in a single table per channel.
#   Keeps track of processed files in a CSV to avoid reprocessing.
#
# Functionality:
#   - Reads ROOT files from a RAW folder, matching a user-specified channel pattern (e.g., _CH0@).
#   - Extracts timestamps, energy, and energy_short from ROOT trees, computing PSP.
#   - Inserts timestamps with microsecond precision in the time column and stores the sub-second offset with picosecond precision in the ps column, along with [psp, energy] in the channels column, into database tables
#     (e.g., caen8ch_ch0, caen8ch_ch1).
#   - Optionally renames processed files with start and end timestamps and changes extension to .root2.
#   - Inserts metadata into root_files table after processing, using original or renamed filename.
#   - Keeps track of processed files in processed_files.csv and skips already processed files.
#
# Requirements:
#   - Folder structure: Parent folder with a settings.xml file (used to estimate start time from last modified time)
#     and a RAW subfolder with ROOT files.
#   - Files:
#     - psql_credentials.py: Defines PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD for PostgreSQL.
#   - Environment variable COMPUTER_NAME set if post-processing on the data collection computer.
#
# Usage:
#   - Run: python caen-rootpostprocessing.py
#   - Prompts for folder path, channel number (default 0), table prefix (e.g., caen8ch), computer name (if COMPUTER_NAME not set), and whether to rename files.
#   - Creates processed_files.csv to track progress.
#   - Outputs data to PostgreSQL tables with prefix caen8ch (e.g., caen8ch_ch0).
#
# Notes:
#   - Ensure Google Drive folders are marked "Available Offline" if used.
#   - Database tables must exist with schema:
#     - Event tables: time (timestamp(6) for microsecond precision), channels (double precision[]), ps (bigint).
#     - root_files: time (varchar), computer (varchar), daq_folder (varchar), dir (varchar), file (varchar).

import argparse
import os
import pandas as pd
import uproot
import psycopg2
from psycopg2.extras import execute_values
import re
from datetime import datetime
import numpy as np
from pathlib import Path
import glob
import sys
import time

# Detect if running in Colab using environment variable
IS_COLAB = os.getenv("RUNNING_IN_COLAB") == "1"

# PostgreSQL connection details (replace with your credentials)
from psql_credentials import PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

# Dynamically set CSV path with override capability
if IS_COLAB:
    # Default CSV path
    csv_path = '/content/drive/MyDrive/Nucleonics/Analysis/Colab Notebooks/Data/processed_files.csv'
    # Check if csv_path is overridden in the global namespace by looking for processed_files_path
    if 'processed_files_path' in globals():
        csv_path = globals()['processed_files_path']
    print(f"Going to track progress with: {csv_path}")
else:
    # Locally, use the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, 'processed_files.csv')

# Connect to PostgreSQL database
def connect_to_db():
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                dbname=PGDATABASE,
                user=PGUSER,
                password=PGPASSWORD,
                host=PGHOST,
                port=PGPORT,
                connect_timeout=10
            )
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Connection attempt {attempt + 1} failed: {e}. Retrying in 1s...")
                time.sleep(1)
            else:
                print(f"Max retries ({max_retries}) reached. Failed to connect to database: {e}")
                sys.exit(1)  # Terminates the program with an error code

# Function to insert event timestamps with picosecond precision
def insert_timestamps_to_db(conn, table_name, time_value, channels, ps):
    with conn.cursor() as cur:
        query = f"""
            INSERT INTO {table_name} (time, channels, ps)
            VALUES (%s, %s::double precision[], %s)
        """
        cur.execute(query, (time_value, channels, ps))
    conn.commit()

# Batched insert many timestamp events with picosecond precision
def insert_many_timestamps_to_db(conn, table_name, rows, batch_size=1000):
    with conn.cursor() as cur:
        query = f"""
            INSERT INTO {table_name} (time, channels, ps)
            VALUES %s
        """
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            execute_values(cur, query, batch)
    conn.commit()

# Function to insert root file metadata into the database
def insert_root_file_to_db(conn, time_value, computer, daq_folder, rel_dir, file):
    with conn.cursor() as cur:
        query = f"""
            INSERT INTO root_files (time, computer, daq_folder, dir, file)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(query, (time_value, computer, daq_folder, rel_dir, file))
    conn.commit()


# Function to get the table name based on channel number
def get_table_name_from_channel(channel_number, table_prefix):
    return f"{table_prefix}_ch{channel_number}"

# Function to get channel number from file name
def get_channel_number_from_filename(file_path):
    match = re.search(r'CH(\d)', file_path)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Could not extract channel number from file name: {file_path}")

# Function to extract the number before .root for sorting
def get_file_number(filename):
    match = re.search(r'_(\d+)\.root', filename)
    return int(match.group(1)) if match else 0

def get_acquisition_start(df):
    file_path = df.iloc[0]['filename']
    if not os.path.exists(file_path):
        print(f"Error: The file {file_path} does not exist. Please check the path.")
        sys.exit(1)
    parent_folder = os.path.dirname(os.path.dirname(file_path))
    try:
        acquisition_start_timestamp = get_acquisition_start_from_settings(parent_folder)
        return acquisition_start_timestamp
    except Exception as e:
        print(f"Error retrieving experiment start time: {e}")
        sys.exit(1)

# Function to extract acquisition start time from settings.xml last modified time or user input
def get_acquisition_start_from_settings(parent_folder):
    try:
        settings_file = os.path.join(parent_folder, "settings.xml")
        
        if not os.path.exists(settings_file):
            print(f"settings.xml not found in {parent_folder}")
            # Prompt user for start time
            while True:
                start_time_str = input("Enter experiment start time (YYYY-MM-DD HH:MM:SS): ").strip()
                try:
                    # Parse user input to datetime
                    start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                    # Convert to Unix timestamp
                    acquisition_start_timestamp = start_time.timestamp()
                    print(f"User-provided start time: {start_time}")
                    return acquisition_start_timestamp
                except ValueError as e:
                    print(f"Invalid date format: {e}. Please use YYYY-MM-DD HH:MM:SS (e.g., 2025-05-19 17:06:07)")
        
        # Get the last modified time of settings.xml
        settings_mtime = os.path.getmtime(settings_file)
        acquisition_start_timestamp = settings_mtime
        print(f"Last modified time of {settings_file}: {datetime.fromtimestamp(settings_mtime)}")
        return acquisition_start_timestamp
    
    except Exception as e:
        print(f"Error accessing settings.xml or processing start time in {parent_folder}: {e}")
        raise

def process_root_file(file_path, table_prefix, channel_number, acquisition_start_timestamp, conn):
    try:
        branches_to_import = ["Timestamp", "Energy", "EnergyShort"]
        table_name = get_table_name_from_channel(channel_number, table_prefix)
        total_events = 0
        chunk_number = 0

        print(f"🔄 Streaming events from {os.path.basename(file_path)}")

        with uproot.open(file_path) as f:
            tree = next(obj for k, obj in f.items() if isinstance(obj, uproot.behaviors.TTree.TTree))

            total_events = 0
            chunk_number = 0

            for arrays in tree.iterate(
                branches_to_import,
                library="np",
                step_size="100 MB"
            ):
                chunk_number += 1

                if len(arrays["Timestamp"]) == 0:
                    print(f"⚠️  Chunk {chunk_number} is empty. Skipping.")
                    continue

                timestamps = arrays["Timestamp"] / 1e12
                energy = arrays["Energy"].astype(np.float64)
                energy_short = arrays["EnergyShort"].astype(np.float64)

                # PSP calculation with divide-by-zero protection
                with np.errstate(divide='ignore', invalid='ignore'):
                    psp = np.where(energy != 0, (energy - energy_short) / energy, 0.0)

                abs_times = timestamps + acquisition_start_timestamp

                event_rows = []
                for abs_time, e, p in zip(abs_times, energy, psp):
                    time_value = datetime.fromtimestamp(abs_time)
                    time_floor = np.floor(abs_time)
                    subsecond_ps = int((abs_time - time_floor) * 1e12)
                    event_rows.append((time_value, [float(p), float(e)], subsecond_ps))

                insert_many_timestamps_to_db(conn, table_name, event_rows, batch_size=1000)
                total_events += len(event_rows)

                print(f"✅ Chunk {chunk_number}: inserted {len(event_rows)} events (total: {total_events})")

            if total_events == 0:
                print(f"⚠️  No events found in {file_path}")
                return False, None, None

            # Calculate start and end times for renaming
            start_time = min(abs_times)
            end_time = max(abs_times)
            start_time_str = datetime.fromtimestamp(start_time).strftime('%Y%m%d_%H%M%S')
            end_time_str = datetime.fromtimestamp(end_time).strftime('%Y%m%d_%H%M%S')

            print(f"🎉 Done: inserted {total_events} events from {os.path.basename(file_path)}")
            print(f"current file start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"current file end time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}")
            return True, start_time_str, end_time_str

    except Exception as e:
        print(f"❌ Failed to process {file_path}: {e}")
        return False, None, None

# Main function
def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Process ROOT files for all events")
    args = parser.parse_args()

    # Prompt for whether this is the data collection computer
    while True:
        is_collection_computer = input("Is this the computer where the data was collected? (y/n): ").strip().lower()
        if is_collection_computer in ['y', 'n']:
            is_collection_computer = is_collection_computer == 'y'
            break
        print("Invalid input. Please enter 'y' or 'n'.")

    # Get computer name based on user response
    global computer_name
    if is_collection_computer:
        computer_name = os.getenv("COMPUTER_NAME")
        if not computer_name:
            print("Error: COMPUTER_NAME environment variable not set.")
            print("You must run 'bash ida-devices/scripts/set-computer-name.sh' to set it.")
            sys.exit(1)
    else:
        while True:
            computer_name = input("Enter the computer name where data was collected: ").strip()
            if not computer_name:
                print("Error: Computer name cannot be empty.")
            else:
                break

    # Prompt for table prefix
    while True:
        table_prefix = input("Enter table prefix (e.g., caen8ch): ").strip()
        if table_prefix:
            break
        print("Invalid input. Please enter a non-empty table prefix.")

    # Prompt for whether to rename files
    while True:
        rename_files = input("Rename processed files to include start and end timestamps and .root2? (y/n): ").strip().lower()
        if rename_files in ['y', 'n']:
            rename_files = rename_files == 'y'
            break
        print("Invalid input. Please enter 'y' or 'n'.")

    # Prompt for file types to process
    while True:
        file_types = input("Process which file types? (1: .root only, 2: .root2 only, 3: both .root and .root2): ").strip()
        if file_types in ['1', '2', '3']:
            break
        print("Invalid input. Please enter '1', '2', or '3'.")

    default_channel = 0  # Default channel number

    # Check if CSV exists
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        total_files = len(df)
        unprocessed_files = len(df[df['processed'] == False])
        failed_files = len(df[df['processed'] == 'Failed'])
        try:
            channel_input = get_channel_number_from_filename(df.iloc[0]['filename'])
        except ValueError as e:
            print(f"Error with CSV file: {e}")
            sys.exit(1)
        print(f"Found {total_files} files in {csv_path}, {unprocessed_files} remain to be processed, {failed_files} failed.")
        print()

        if unprocessed_files == 0:
            print("✅ All valid files in processed_files.csv have been processed or failed.")
            print("🗑️  If you want to start a new processing run, please delete the CSV file:")
            print(f"    {csv_path}")
            print("Then re-run this script to select a new folder and channel.")
            return
    
    else:
        # Prompt for folder and channel numbers
        folder_path = input("Enter the folder path containing the Compass settings.xml file (ROOT files in RAW subfolder): ")
        channel_input = input(f"Enter channel numbers (comma-separated, e.g., 0,1,2, default '{default_channel}'): ") or str(default_channel)
        
        # Parse and validate channel numbers
        try:
            channels = [ch.strip() for ch in channel_input.split(",")]
            for ch in channels:
                if not ch.isdigit():
                    raise ValueError(f"Invalid channel number: {ch}")
            print(f"Processing channels: {channels}")
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)

        # Resolve the folder path to handle virtual file systems (e.g., Google Drive)
        try:
            folder_path = str(Path(folder_path).resolve())
            print(f"Resolved folder path: {folder_path}")
        except Exception as e:
            print(f"Error resolving path '{folder_path}': {e}")
            print("If using Google Drive, ensure the folder is marked 'Available Offline' or try using the path under ~/Library/CloudStorage/GoogleDrive-<your_email>/My Drive")
            return

        # Validate folder
        if not os.path.isdir(folder_path):
            print(f"Error: {folder_path} is not a valid directory")
            print("If using Google Drive, ensure the folder is marked 'Available Offline' or try using the path under ~/Library/CloudStorage/GoogleDrive-<your_email>/My Drive")
            return

        # Validate RAW subfolder
        raw_folder = os.path.join(folder_path, "RAW")
        if not os.path.isdir(raw_folder):
            print(f"Error: {raw_folder} subfolder does not exist")
            return
        
        # Build glob pattern based on user selection
        if file_types == '1':
            patterns = [os.path.join(raw_folder, f"*_CH{ch}@*.root") for ch in channels]
        elif file_types == '2':
            patterns = [os.path.join(raw_folder, f"*_CH{ch}@*.root2") for ch in channels]
        else:  # file_types == '3'
            patterns = [os.path.join(raw_folder, f"*_CH{ch}@*.root*") for ch in channels]

        files = []
        for pattern in patterns:
            files.extend(glob.glob(pattern))

        if not files:
            print(f"No files with channel numbers {channels} and matching selected file types found in {raw_folder}")
            return

        # Sort files by number before .root
        files.sort(key=get_file_number)

        # Create DataFrame with full paths
        df = pd.DataFrame({
            'filename': files,  # Use full paths directly from glob
            'processed': [False] * len(files)
        })
        df.to_csv(csv_path, index=False)
        total_files = len(df)
        print(f"Found {total_files} files to process. Created CSV: {csv_path}")
        print()

    # Get the conn
    conn = connect_to_db()
    print("Connection to db established")

    try:
        # Get experiment start time (needed because ROOT timestamps are relative to the start of the experiment) 
        acquisition_start_timestamp = get_acquisition_start(df)

        # Process unprocessed files
        total_files = len(df)
        for index, row in df.iterrows():
            if not row['processed']:
                file_path = row['filename']
                current_file_number = index + 1
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1;")  # Simple heartbeat
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    print(f"Database connection lost: {e}. Retrying in 5 seconds...")
                    time.sleep(5)
                    conn = connect_to_db()
        
                if os.path.exists(file_path):
                    print(f"Processing file {current_file_number} out of {total_files}: {os.path.basename(file_path)}")
                    print(f"Experiment start time: {datetime.fromtimestamp(acquisition_start_timestamp).strftime('%Y-%m-%d %H:%M:%S')}")
                    channel_number = get_channel_number_from_filename(file_path)
                    success, start_time_str, end_time_str = process_root_file(file_path, table_prefix, channel_number, acquisition_start_timestamp, conn)
                    if success:
                        # Determine filename and path for metadata
                        filename = os.path.basename(file_path)
                        new_file_path = file_path
                        if rename_files:
                            # Rename the file with start and end times and change from .root to .root2
                            new_filename = f"{start_time_str}-{end_time_str}_{filename[:-5]}.root2"
                            new_file_path = os.path.join(os.path.dirname(file_path), new_filename)
                            try:
                                os.rename(file_path, new_file_path)
                                print(f"File renamed to: {new_file_path}")
                                filename = new_filename
                            except OSError as e:
                                print(f"Failed to rename {file_path} to {new_file_path}: {e}")
                                sys.exit(1)

                        # Insert root file metadata into the database
                        directory = os.path.dirname(new_file_path)
                        dir_components = directory.split(os.sep)
                        # Strip up to and including the username (assumed to be at index 2)
                        dir_components = dir_components[3:]
                        # If computer_name exists in remaining components, strip up to and including it
                        if computer_name in dir_components:
                            computer_idx = dir_components.index(computer_name)
                            dir_components = dir_components[computer_idx + 1:]
                        # Join remaining components for rel_dir
                        rel_dir = os.path.join(*dir_components)
                        daq_folder = os.path.basename(os.path.dirname(os.path.dirname(new_file_path)))
                        insert_root_file_to_db(conn, end_time_str, computer_name, daq_folder, rel_dir, filename)
                        print(f"Inserted root file metadata into the database")

                        # Update DataFrame with new file path (or original if not renamed)
                        df.at[index, 'filename'] = new_file_path
                        df.at[index, 'processed'] = True
                        df.to_csv(csv_path, index=False)
                        print("Updated processed status in CSV")
                        print()
                    else:
                        df.at[index, 'processed'] = 'Failed'
                        df.to_csv(csv_path, index=False)
                        continue
                else:
                    print(f"File not found: {file_path}")
                    df.at[index, 'processed'] = True
                    df.to_csv(csv_path, index=False)
    finally:
        conn.close()
        print("Connection closed")
        # Check for unprocessed and failed files in the CSV
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            unprocessed_files = df[df['processed'] == False]['filename'].tolist()
            failed_files = df[df['processed'] == 'Failed']['filename'].tolist()
            if unprocessed_files or failed_files:
                if unprocessed_files:
                    print("\n⚠️ The following files in processed_files.csv remain unprocessed:")
                    for file in unprocessed_files:
                        print(f"  - {os.path.basename(file)}")
                    print(f"Total unprocessed files: {len(unprocessed_files)}")
                if failed_files:
                    print("\n⚠️ The following files in processed_files.csv failed processing (possibly incomplete or corrupted):")
                    for file in failed_files:
                        print(f"  - {os.path.basename(file)}")
                    print(f"Total failed files: {len(failed_files)}")
            else:
                print("\n✅ All files in processed_files.csv have been processed or failed.")
            if not unprocessed_files:
                print("\n🗑️ All valid files have been processed or failed. If you want to start a new processing run, please delete the CSV file:")
                print(f"    {csv_path}")
        else:
            print(f"\n⚠️ No processed_files.csv found at {csv_path}")

if __name__ == "__main__":
    main()