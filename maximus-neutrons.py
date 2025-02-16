import time
import os
import logging
import argparse
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from dateutil import parser

# Add the parent directory (../) to the Python path
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

def init_db():
    """
    Initialize and return a PostgreSQL database connection.
    """
    from mitcf import pglogger
    import psql_credentials as creds
    try:
        db_cloud = pglogger(creds)
        logging.info("Database connection initialized.")
        return db_cloud
    except Exception as e:
        logging.error(f"Failed to initialize database connection: {e}")
        return None

def reconnect_db():
    """
    Attempt to reconnect to the database.
    """
    logging.warning("Attempting to reconnect to the database...")
    return init_db()

def process_file(event, table_prefix, db_cloud, last_filename):
    """
    Process new or modified CSV files and insert data into the PostgreSQL database.
    """
    if event.is_directory:
        return

    file_path = event.src_path
    file_mod_time = os.path.getmtime(file_path)
    current_time = time.time()

    # Ignore old files (older than 5 minutes)
    if current_time - file_mod_time > 300:
        logging.info(f"Ignoring old file: {file_path}")
        return

    new_file = os.path.basename(file_path)
    if new_file == last_filename:
        logging.info("Duplicate file detected. Skipping.")
        return

    logging.info(f"Processing: {file_path}")
    last_filename = new_file  # Update last processed filename
    time.sleep(1)  # Allow file to fully write

    if "History" in new_file:
        process_history_file(file_path, table_prefix, db_cloud)
    elif "Spectrum" in new_file:
        process_spectrum_file(file_path, table_prefix, db_cloud)

def process_history_file(file_path, table_prefix, db_cloud):
    """
    Process and log a history file into the database.
    """
    channel_match = re.search(r"History(\d+)-", file_path)
    channel = int(channel_match.group(1)) if channel_match else None

    with open(file_path) as f:
        lines = f.readlines()

    started_datetime = datetime.fromtimestamp(os.path.getctime(file_path) - 60)
    timestamps, values = [], []
    for entry in lines:
        try:
            seconds, value = map(int, entry.split(', '))
            timestamp = started_datetime + timedelta(seconds=seconds)
            timestamps.append(timestamp.strftime('%Y-%m-%d %H:%M:%S'))
            values.append(value)
        except ValueError:
            logging.warning(f"Skipping malformed line: {entry.strip()}")

    table_name = f"{table_prefix}{channel}_history"
    for timestamp, value in zip(timestamps, values):
        success = db_cloud.log(table=table_name, channels=np.array([value]))
        if not success:
            logging.warning(f"Failed to log data from {file_path}")
            db_cloud = reconnect_db()

    logging.info(f"History data from {file_path} inserted into database.")

def process_spectrum_file(file_path, table_prefix, db_cloud):
    """
    Process and log a spectrum file into the database.
    """
    channel_match = re.search(r"Spectrum(\d+)-", file_path)
    channel = int(channel_match.group(1)) if channel_match else None

    with open(file_path) as f:
        lines = [line.strip() for line in f.readlines()]

    try:
        channels, counts = zip(*[line.split(', ') for line in lines])
        df = pd.DataFrame({'counts': counts, 'channel': channels}).astype({'counts': float, 'channel': int})
    except ValueError:
        logging.warning(f"Skipping malformed spectrum file: {file_path}")
        return

    timestamp = (datetime.fromtimestamp(os.path.getctime(file_path) - 60) + timedelta(seconds=60)).isoformat(sep=' ')

    spectrum_values = ",".join(df['counts'].astype(str))
    table_name = f"{table_prefix}{channel}_spectrum"

    success = db_cloud.log(table=table_name, channels=np.array([spectrum_values]))
    if not success:
        logging.warning(f"Failed to log spectrum data from {file_path}")
        db_cloud = reconnect_db()

    logging.info(f"Spectrum data from {file_path} inserted into database.")

def on_created(event, table_prefix, db_cloud, last_filename):
    """
    Handle new file creation event.
    """
    process_file(event, table_prefix, db_cloud, last_filename)

def on_modified(event):
    """
    Ignore modified files to prevent duplicate processing.
    """
    pass

def start_observer(folder_path, table_prefix):
    """
    Start the watchdog observer to monitor new files.
    """
    db_cloud = init_db()
    if db_cloud is None:
        logging.error("Could not initialize database connection. Exiting.")
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("pulse_counter.log"),
            logging.StreamHandler()
        ]
    )

    last_filename = ""
    observer = Observer()
    event_handler = PatternMatchingEventHandler(patterns=["*.csv"], ignore_directories=True)
    
    event_handler.on_created = lambda event: on_created(event, table_prefix, db_cloud, last_filename)
    event_handler.on_modified = on_modified  # Prevent re-processing files

    observer.schedule(event_handler, folder_path, recursive=False)
    observer.start()

    logging.info(f"Started observer for path: {folder_path}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping observer...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pulse Counter File Watcher")
    parser.add_argument('--table-prefix', required=True, help="Database table name prefix")
    parser.add_argument('--folder', required=True, help="Folder path to watch for .csv files")
    args = parser.parse_args()

    start_observer(args.folder, args.table_prefix)
