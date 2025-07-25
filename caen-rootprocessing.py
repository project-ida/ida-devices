import os
import sys
import re
import numpy as np
import uproot
import psycopg2
import pandas as pd
import time
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from psycopg2.extras import execute_values
from libs.telegram_notifier import send_telegram_alert
from libs.network import internet_available, reset_wifi
from libs.heartbeat import send_heartbeat

# Add the parent directory (../) to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Get computer name from environment variable or user input
computer_name = os.getenv("COMPUTER_NAME")
if not computer_name:
    print("COMPUTER_NAME environment variable not set.")
    print("You must run 'bash ida-devices/scripts/set-computer-name.sh' to set it.")
    exit(1)

# PostgreSQL connection details
from psql_credentials import PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

# Connect to PostgreSQL database
def connect_to_db():
    alert_after_retries = 10
    max_retries = 1_000_000

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
            if not internet_available():
                print("No internet detected. Attempting to reset Wi-Fi...")
                reset_wifi()

            if attempt == alert_after_retries:
                print(f"Sending alert after failing to connect to database {alert_after_retries} times: {e}")
                send_telegram_alert(
                    f"*caen-rootprocessing failed:*\n"
                    f"Cannot connect to the database after `{alert_after_retries}` attempts. Continuing to retry.\n\n"
                    f"⚠️ *Do not restart caen-rootprocessing.* It will automatically resume processing any queued files once the connection is restored."
                    f"Try manually resetting the WiFi connection to speed up recovery.\n"
                )

            print(f"Connection attempt {attempt + 1} failed: {e}. Retrying in 5s...")
            time.sleep(5)

    print("Exceeded maximum number of retries. Exiting.")
    sys.exit(1)


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

# Function to insert event timestamps with picosecond precision
def insert_root_file_to_db(conn, time_value, computer, daq_folder, rel_dir, file):
    with conn.cursor() as cur:
        query = f"""
            INSERT INTO root_files (time, computer, daq_folder, dir, file)
            VALUES (%s, %s, %s, %s, %s)
        """
        cur.execute(query, (time_value, computer, daq_folder, rel_dir, file))
    conn.commit()

# Function to estimate the acquisition start time from settings.xml last modified time
def estimate_acquisition_start(file_path):
    try:
        # Get the parent directory of the file's folder (one folder up)
        folder = os.path.dirname(file_path)
        parent_folder = os.path.dirname(folder)
        settings_file = os.path.join(parent_folder, "settings.xml")
        
        if not os.path.exists(settings_file):
            print(f"Error: settings.xml not found in {parent_folder}")
            return None, None
        
        # Get the last modified time of settings.xml
        settings_mtime = os.path.getmtime(settings_file)
        acquisition_start_datetime = datetime.fromtimestamp(settings_mtime)
        acquisition_start_timestamp = settings_mtime
        print(f"Last modified time of {settings_file}: {acquisition_start_datetime}")
        return acquisition_start_datetime, acquisition_start_timestamp
    except Exception as e:
        print(f"Error accessing settings.xml for {file_path}: {e}")
        return None, None

# Function to check if the ROOT file is ready
def is_root_file_ready(file_path, tree_name='Data_R'):
    try:
        with uproot.open(file_path) as file:
            return tree_name in file
    except Exception as e:
        print(f"File {file_path} not ready: {e}")
        return False

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

# Function to process the ROOT file
def process_root_file(file_path, table_prefix):
    if not is_root_file_ready(file_path):
        return False, None, None
    
    # Extract the channel number
    channel_number = get_channel_number_from_filename(file_path)
    table_name = get_table_name_from_channel(channel_number, table_prefix)
    
    try:
        print("----------START-------------")
        
        acquisition_start_datetime, acquisition_start_timestamp = estimate_acquisition_start(file_path)
        print(f"--> acquisition_start_datetime: {acquisition_start_datetime}")
        if acquisition_start_timestamp is None:
            print(f"Skipping file {file_path} due to missing settings.xml to extract experiment start time.")
            return False, None, None

        with uproot.open(file_path) as file:
            tree = file["Data_R"]
            branches_to_import = ["Timestamp", "Energy", "EnergyShort"]
            df = tree.arrays(branches_to_import, library="pd")
            
            if len(df["Timestamp"]) == 0:
                print(f"No data found in {file_path}")
                return False, None, None

            # Convert timestamps to seconds
            df["Timestamp"] = df["Timestamp"] / 1e12
            # Calculate PSP with divide-by-zero protection
            with np.errstate(divide='ignore', invalid='ignore'):
                df["PSP"] = np.where(df["Energy"] != 0, (df["Energy"] - df["EnergyShort"]) / df["Energy"], 0.0)
            
            # Calculate absolute times
            abs_times = df["Timestamp"] + acquisition_start_timestamp

            # Calculate start and end times for filename
            start_time = min(abs_times)
            end_time = max(abs_times)
            start_time_str = datetime.fromtimestamp(start_time).strftime('%Y%m%d_%H%M%S')
            end_time_str = datetime.fromtimestamp(end_time).strftime('%Y%m%d_%H%M%S')

            total_events = 0

            # Collect events for batch insertion
            event_rows = []
            for abs_time, psp, energy in zip(abs_times, df["PSP"], df["Energy"]):
                time_value = datetime.fromtimestamp(abs_time)
                time_floor = np.floor(abs_time)
                subsecond_ps = int((abs_time - time_floor) * 1e12)
                event_rows.append((time_value, [float(psp), float(energy)], subsecond_ps))

            # Create new connection for event insertion
            conn = connect_to_db()
            try:
                # Insert events in batches
                insert_many_timestamps_to_db(conn, table_name, event_rows, batch_size=1000)
                total_events += len(event_rows)
                
                print(f"🎉 Done: inserted {total_events} events from {os.path.basename(file_path)}")
                print(f"current file start time: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"current file end time: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}")
                return True, start_time_str, end_time_str
            finally:
                conn.close()
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")
        return False, None, None

# Monitor folder for modified ROOT files
class ModifiedFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith(".root"):
            file_path = event.src_path  # File path for the current event
            if file_path in processed_files:
                print(f"Skipping {file_path} because it has already been processed.")
                return  # Skip processing this file
            try:
                # Attempt to process the file
                file_processed, start_time_str, end_time_str = process_root_file(file_path, table_prefix)
                if file_processed:
                    # send heartbeat to healthchecks.io to signal that the data collection process is still alive
                    send_heartbeat()
                    # Mark the file as processed
                    processed_files[file_path] = True
                    # Rename the file with start and end times and changed from root to root2
                    original_filename = os.path.basename(file_path)
                    new_filename = f"{start_time_str}-{end_time_str}_{original_filename[:-5]}.root2"
                    new_file_path = os.path.join(os.path.dirname(file_path), new_filename)
                    os.rename(file_path, new_file_path)
                    print(f"File renamed to: {new_file_path}")

                    # Create new connection for metadata insertion
                    conn = connect_to_db()
                    try:
                        # Insert root file metadata into the database
                        filename = os.path.basename(new_file_path)
                        directory = os.path.dirname(new_file_path) # /home/cf/caen/daq/test/RAW
                        dir_components = directory.split(os.sep) # ['', 'home', 'cf', 'caen', 'daq', 'test', 'RAW']
                        rel_dir = os.path.join(*dir_components[3:])
                        daq_folder = os.path.basename(os.path.dirname(os.path.dirname(new_file_path))) # test
                        insert_root_file_to_db(conn, end_time_str, computer_name, daq_folder, rel_dir, filename)
                        print(f"Inserted root file meta data into the database")
                    finally:
                        conn.close()
                    
                    print("-----------END------------")
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

# Dictionary to track processed files
processed_files = {}

if __name__ == "__main__":
    # Prompt for folder path and table prefix
    data_folder = input("Enter the folder path containing ROOT files: ").strip()
    if not os.path.isdir(data_folder):
        print(f"Error: {data_folder} is not a valid directory")
        sys.exit(1)
    
    table_prefix = input("Enter the table prefix for database tables (e.g., caen8ch): ").strip()
    if not table_prefix:
        print("Error: Table prefix cannot be empty")
        sys.exit(1)

    event_handler = ModifiedFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=data_folder, recursive=True)

    print(f"Monitoring directory: {data_folder}")
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()