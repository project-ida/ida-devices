import time
import os
import logging
import argparse
import psycopg2
import numpy as np
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

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
    try:
        return init_db()
    except Exception as e:
        logging.error(f"Reconnection failed: {e}")
        return None

def process_file(filepath, table_name, db_cloud):
    """
    Process new or modified .mca files and insert data into the PostgreSQL database.
    """
    global lastfilename
    if os.path.basename(filepath) == os.path.basename(lastfilename):
        return  # Prevent duplicate processing

    logging.info(f"Processing new .mca file: {filepath}")
    time.sleep(1)  # Allow file to fully write

    try:
        with open(filepath, 'r') as file:
            data = file.readlines()

        extracted_data = [float(line.strip()) for line in data if line.strip().isdigit()]
        logging.info(f"Extracted {len(extracted_data)} values from {filepath}.")

        # Log data to the database
        if db_cloud:
            for value in extracted_data:
                success = db_cloud.log(table=table_name, channels=np.array([value]))
                if not success:
                    logging.warning(f"Failed to log value {value} from {filepath}")
                    db_cloud = reconnect_db()
        else:
            logging.error("Database connection lost. Reconnecting...")

        logging.info(f"Data from {filepath} inserted into database.")
        global lastfilename
        lastfilename = filepath

    except Exception as e:
        logging.error(f"Error processing {filepath}: {e}")

def on_modified(event, table_name, db_cloud):
    """
    Handler function for modified files.
    """
    if not event.is_directory:
        process_file(event.src_path, table_name, db_cloud)

def main():
    """
    Main function to set up file watching and database logging.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="MCA File Watcher and Database Logger")
    parser.add_argument('--table', required=True, help="Name of the database table to log data to.")
    parser.add_argument('--folder', required=True, help="Folder path to watch for .mca files.")
    args = parser.parse_args()

    table_name = args.table
    watch_path = args.folder

    # Initialize database connection
    db_cloud = init_db()
    if db_cloud is None:
        logging.error("Could not initialize database connection. Exiting.")
        sys.exit(1)

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("mca_watcher.log"),
            logging.StreamHandler()
        ]
    )

    # Watchdog observer setup
    observer = Observer()
    event_handler = PatternMatchingEventHandler(patterns=["*.mca"], ignore_directories=True)

    # Wrap on_modified to include arguments
    event_handler.on_modified = lambda event: on_modified(event, table_name, db_cloud)

    observer.schedule(event_handler, watch_path, recursive=False)
    observer.start()

    logging.info(f"Watching folder: {watch_path} for new .mca files.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logging.info("Shutting down observer.")

    observer.join()
    logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
