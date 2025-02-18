import time
import os
import logging
import argparse
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from dateutil import parser
from datetime import datetime, timedelta

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

def process_spe_file(filepath, table_name, db_cloud):
    """
    Process new .spe files and insert data into the PostgreSQL database.
    """
    logging.info(f"Processing new .spe file: {filepath}")
    time.sleep(1)  # Allow file to fully write

    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()
        
        lines = list(map(str.strip, lines))
        
        # Extract measurement time
        seconds = float(lines[lines.index('$MEAS_TIM:') + 1].split(" ")[0])
        
        # Extract start time
        started_string = lines[lines.index('$DATE_MEA:') + 1]
        started_dt = parser.parse(started_string)
        ended_dt = started_dt + timedelta(seconds=seconds)
        timestamp = ended_dt.isoformat(sep=' ')

        # Extract spectrum data
        data_index = lines.index("$DATA:")
        end_index = lines.index("$ROI:")
        data_lines = lines[(data_index + 2):end_index]
        df = pd.DataFrame(data_lines, columns=['counts'], dtype='float')
        df['channel'] = df.index
        df['counts'] = df['counts'].astype(int)

        # Convert data to PostgreSQL format
        spectrum_values = ",".join(df['counts'].astype(str))

        # Insert into database
        success = db_cloud.log(table=table_name, channels=spectrum_values)
        if not success:
            logging.warning(f"Failed to log data from {filepath}")
            db_cloud = reconnect_db()

        logging.info(f"Data from {filepath} inserted into database.")

    except Exception as e:
        logging.error(f"Error processing {filepath}: {e}")

def on_created(event, table_name, db_cloud):
    """
    Handle new file creation event.
    """
    if not event.is_directory:
        process_spe_file(event.src_path, table_name, db_cloud)

def on_modified(event):
    """
    Ignore modified files to prevent duplicate processing.
    """
    pass

def start_observer(folder_path, table_name):
    """
    Start the watchdog observer to monitor new .spe files.
    """
    db_cloud = init_db()
    if db_cloud is None:
        logging.error("Could not initialize database connection. Exiting.")
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("spe_watcher.log"),
            logging.StreamHandler()
        ]
    )

    observer = Observer()
    event_handler = PatternMatchingEventHandler(patterns=["*.spe"], ignore_directories=True)
    
    event_handler.on_created = lambda event: on_created(event, table_name, db_cloud)
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
    parser = argparse.ArgumentParser(description="SPE File Watcher and Database Logger")
    parser.add_argument('--table', required=True, help="Database table name for logging data")
    parser.add_argument('--folder', required=True, help="Folder path to watch for .spe files")
    args = parser.parse_args()

    start_observer(args.folder, args.table)
