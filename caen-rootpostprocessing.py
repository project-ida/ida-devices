# caen-rootpostprocessing.py
#
# Purpose:
#   Processes ROOT files from a nuclear physics experiment to extract per-event timestamps,
#   energy, and PSP values, and inserts them into a PostgreSQL database.
#
# Summary:
#   - Reads ROOT files in a RAW subdirectory based on a user-specified digitizer channel (e.g., CH0).
#   - Uses the Compass acquisition "Start time" from a companion .txt file to calculate absolute event timestamps.
#   - Computes PSP = (Energy - EnergyShort) / Energy for each event.
#   - Writes each event into a PostgreSQL table named like `caen8ch_ch0`, storing:
#       - `time` (timestamp with microsecond precision),
#       - `channels` (double precision array of [PSP, Energy]),
#       - `ps` (picosecond offset from floored second).
#   - Tracks processed ROOT files using a CSV (`processed_files.csv`) to avoid duplicates.
#
# Folder Structure Required:
#   Parent folder containing:
#     - A Compass .txt metadata file with a "Start time = ..." line on the second line.
#     - A `RAW/` subfolder with ROOT files (e.g., *_CH0@*.root).
#
# Files Required:
#   - `psql_credentials.py`: Defines PostgreSQL access variables: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
#
# Usage:
#   - Run: python caen-rootpostprocessing.py
#   - Prompts for table prefix (caen4ch or caen8ch) and ROOT channel number.
#   - If `processed_files.csv` already exists, resumes from where it left off.
#   - Otherwise, creates a new CSV list of matching ROOT files in the RAW folder.
#
# Notes:
#   - All event times are stored with microsecond accuracy and separate picosecond offsets.
#   - Designed for use both locally and in Google Colab (when RUNNING_IN_COLAB=1 is set).
#   - If using Google Drive, ensure files are marked "Available Offline" in Finder for access.
#   - Database tables must already exist with schema:
#       - `time` (timestamp(6)),
#       - `channels` (double precision[]),
#       - `ps` (bigint).


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
        acquisition_start_timestamp = get_acquisition_start_from_txt(parent_folder)
        return acquisition_start_timestamp
    except Exception as e:
        print(f"Error retrieving experiment start time: {e}")
        sys.exit(1)

# Function to extract acquisition start time from .txt file
def get_acquisition_start_from_txt(folder_path):
    try:
        # Find the first .txt file in the folder
        txt_files = [f for f in os.listdir(folder_path) if f.endswith('.txt')]
        if not txt_files:
            raise FileNotFoundError(f"No .txt file found in {folder_path}")
        
        txt_file = os.path.join(folder_path, txt_files[0])
        
        # Read the second line
        with open(txt_file, 'r') as f:
            lines = f.readlines()
            if len(lines) < 2:
                raise ValueError(f"{txt_file} does not have enough lines")
            
            second_line = lines[1].strip()
            # Expect format: "Start time = Tue Feb 25 19:54:42 2025"
            if not second_line.startswith("Start time = "):
                raise ValueError(f"Second line in {txt_file} does not start with 'Start time = '")
            
            # Extract datetime string
            datetime_str = second_line.replace("Start time = ", "")
            # Parse datetime (format: Tue Feb 25 19:54:42 2025)
            dt = datetime.strptime(datetime_str, "%a %b %d %H:%M:%S %Y")
            # Convert to Unix timestamp
            return dt.timestamp()
    
    except Exception as e:
        print(f"Error reading acquisition start time from {txt_file}: {e}")
        raise

# Function to process a single ROOT file
def process_root_file(file_path, table_prefix, channel_number, acquisition_start_timestamp, conn):
    try:
        # Open ROOT file
        with uproot.open(file_path) as file:
            tree = file["Data_R"]
            timetag = tree["Timestamp"].array(library="np") * 1e-12
            if len(timetag) == 0:
                print(f"No data found in {file_path}")
                return False

            # Load data
            branches_to_import = ["Timestamp", "Energy", "EnergyShort"]
            df = tree.arrays(branches_to_import, library="pd")
            df["PSP"] = (df['Energy'] - df['EnergyShort']) / df['Energy']
            df["Timestamp"] = df["Timestamp"] / 1e12

            # Process all events
            abs_times = df["Timestamp"] + acquisition_start_timestamp
            print(f"Processing {len(abs_times)} events")

            table_name = get_table_name_from_channel(channel_number, table_prefix)
            event_rows = []
            for abs_time, energy, psp in zip(abs_times, df["Energy"], df["PSP"]):
                # Convert abs_time to datetime with microsecond precision
                time_value = datetime.fromtimestamp(abs_time)
                # Calculate picosecond offset from the floored second
                time_floor = np.floor(abs_time)
                subsecond_ps = int((abs_time - time_floor) * 1e12)
                event_rows.append((time_value, [float(psp), float(energy)], subsecond_ps))

            print("Begin database insertion")
            insert_many_timestamps_to_db(conn, table_name, event_rows)
            print("Finished database insertion")

            print(f"Done")
            return True

    except Exception as e:
        print(f"Failed to process {file_path}: {e}")
        return False

# Main function
def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Process ROOT files for all events")
    args = parser.parse_args()

    # Prompt for table prefix
    while True:
        channel_count = input("Enter digitizer channel capacity (4 or 8) for table prefix (caen4ch or caen8ch): ").strip()
        if channel_count in ['4', '8']:
            table_prefix = f"caen{channel_count}ch"
            break
        print("Invalid input. Please enter '4' or '8'.")

    default_channel = 0  # Default channel number

    # Check if CSV exists
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        total_files = len(df)
        unprocessed_files = len(df[~df['processed']])
        channel_input = get_channel_number_from_filename(df.iloc[0]['filename']) # use the first file in the CSV to determine channel number
        print(f"Found {total_files} files in {csv_path}, {unprocessed_files} remain to be processed.")
        print()
    else:
        # Prompt for folder and channel number
        folder_path = input("Enter the folder path containing the Compass .txt file (ROOT files in RAW subfolder): ")
        channel_input = input(f"Enter channel number (default '{default_channel}'): ") or str(default_channel)
        file_pattern = f"_CH{channel_input}@"

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
        
        # Build glob pattern to match files like *_CH0@*.root or .root2
        pattern = os.path.join(raw_folder, f"*{file_pattern}*.root*")

        # Get list of ROOT files matching pattern in RAW subfolder
        files = glob.glob(pattern)

        if not files:
            print(f"No files with channel number '{channel_input}' and containing '.root' found in {raw_folder}")
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
                    success = process_root_file(file_path, table_prefix, channel_input, acquisition_start_timestamp, conn)
                    if success:
                        df.at[index, 'processed'] = True
                        df.to_csv(csv_path, index=False)
                        print("Updated processed status in CSV")
                        print()
                else:
                    print(f"File not found: {file_path}")
                    df.at[index, 'processed'] = True
                    df.to_csv(csv_path, index=False)
    finally:
        conn.close()
        print("Connection closed")

if __name__ == "__main__":
    main()