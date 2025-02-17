import pandas as pd
import numpy as np
import logging
import argparse
import csv
import time
import datetime
import sys
import os
from serial import SerialException
from collections import namedtuple
from ut61e import UT61E

# Add the parent directory (../) to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

def init_db():
    """
    Initialize and return a PostgreSQL database connection.
    """
    from mitcf import pglogger
    import psql_credentials as creds_cloud
    try:
        db_cloud = pglogger(creds_cloud)
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

def setup_csv():
    """
    Setup and return a CSV writer and its associated file handle in a named tuple.
    """
    CsvHandle = namedtuple('CsvHandle', ['writer', 'file', 'filename'])
    start_time = datetime.datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_multimeter_data.csv"
    
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Timestamp", "Value"])  # Write header

    logging.info(f"CSV logging started. File: {filename}")
    return CsvHandle(writer=csv_writer, file=csv_file, filename=filename)


def read_multimeter(dmm):
    """
    Reads data from the UT61E multimeter and processes it.
    """
    try:
        response = dmm.get_readable(disp_norm_val=True)
        logging.info(f"Read data: {response}")

        response_tail = response[-11:-2]
        response_final = response_tail.split("=")
        
        try:
            value_str = response_final[1]
        except IndexError:
            logging.warning("Could not extract value, using first element.")
            value_str = response_final[0]

        value_str_clean = value_str.replace(" ", "")
        value = float(value_str_clean)
        logging.info(f"Extracted value: {value}")
        return value

    except (IndexError, ValueError, SerialException) as e:
        logging.error(f"Error reading multimeter: {e}")
        return None

def main():
    """
    Main function to continuously read multimeter data and log it.
    """
    parser = argparse.ArgumentParser(description="Multimeter Logger")
    parser.add_argument('--table', required=True, help="Database table name for logging data")
    parser.add_argument('--port', required=True, help="Serial COM port for the multimeter (e.g., COM5)")
    args = parser.parse_args()

    # Initialize database connection
    db_cloud = init_db()

    # Setup CSV logging
    csv_handle = setup_csv()

    # Initialize multimeter
    try:
        dmm = UT61E(args.port)
    except SerialException as e:
        logging.error(f"Could not open port {args.port}: {e}")
        sys.exit(1)

    try:
        while True:
            value = read_multimeter(dmm)
            if value is None:
                continue

            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logging.info(f"Logging Value: {value}")

            # Write to CSV
            csv_handle.writer.writerow([timestamp, value])  # Append the new row
            csv_handle.file.flush()  # Flush to ensure data is saved immediately

            # Log data to the database
            success_cloud = db_cloud.log(table=args.table, channels=np.array([value]))
            if not success_cloud:
                logging.warning("Failed to log data to the cloud database.")
                db_cloud = reconnect_db()

            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Stopping data acquisition.")
    finally:
        del dmm
        logging.info("Multimeter disconnected.")
        csv_handle.file.close()

if __name__ == "__main__":
    main()
