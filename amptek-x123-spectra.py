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

# Set to track processed files
processed_files = set()

def init_db():
    """
    Initialize and return a PostgreSQL database connection.
    """
    from ida_db import pglogger
    import psql_credentials as creds
    try:
        db_cloud = pglogger(creds)
        logging.info("Database connection initialized.")
        return db_cloud
    except Exception as e:
        logging.error("Failed to initialize database connection: {}".format(e))
        return None

def reconnect_db():
    """
    Attempt to reconnect to the database.
    """
    logging.warning("Attempting to reconnect to the database...")
    try:
        return init_db()
    except Exception as e:
        logging.error("Reconnection failed: {}".format(e))
        return None

def process_file(filepath, table_name, db_cloud):
    """
    Process new or modified .mca files and insert data into the PostgreSQL database.
    """
    filename = os.path.basename(filepath)
    
    if filename in processed_files:
        logging.info(f"Skipping already processed file: {filename}")
        return  # Prevent duplicate processing
    
    logging.info(f"Processing new .mca file: {filepath}")
    time.sleep(1)  # Allow file to fully write

    try:
        with open(filepath, 'r') as file:
            data = file.readlines()

        # Extract START_TIME
        start_time = None
        extracted_data = []
        
        for line in data:
            line = line.strip()
            
            if line.startswith("START_TIME -"):
                start_time_str = line.split(" - ")[1]
                start_time = pd.to_datetime(start_time_str, format="%m/%d/%Y %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')
            
            elif line.isdigit():  # Ensure it’s an integer value
                extracted_data.append(int(line))  # Convert to integer
        
        if start_time is None:
            logging.warning(f"No START_TIME found in {filepath}, skipping file.")
            return
        
        if not extracted_data:
            logging.warning(f"No valid data found in {filepath}, skipping file.")
            return

        logging.info(f"Extracted {len(extracted_data)} integer values from {filepath} with START_TIME: {start_time}")

        # Convert list to a PostgreSQL-compatible array format (WITHOUT {})
        channels_str = ",".join(map(str, extracted_data))  # Convert integers to comma-separated string

        # Log data to the database using correct format
        if db_cloud:
            success = db_cloud.log(table_name, channels_str, start_time)  # Pass the string without extra {}
            if not success:
                logging.warning(f"Failed to log data from {filepath}")
                db_cloud = reconnect_db()
        else:
            logging.error("Database connection lost. Reconnecting...")

        logging.info(f"Data from {filepath} inserted into database.")
        processed_files.add(filename)  # Add processed file to the set

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
