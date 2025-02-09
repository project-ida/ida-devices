#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    MCC 134 Functions Demonstrated:
        mcc134.t_in_read

    Purpose:
        Read a single data value for each channel in a loop.

    Updates:
        - Added CSV logging.
        - Added database reconnection.
        - Made the table name configurable via a command-line parameter.
        - Improved logging for robustness.
        - Ensured consistency with other DAQ scripts.
"""

from __future__ import print_function
from time import sleep
import logging
import argparse
import numpy as np
import csv
import sys
import os
from collections import namedtuple
from datetime import datetime
from daqhats import mcc134, HatIDs, HatError, TcTypes
from daqhats_utils import select_hat_device

# Add the parent directory (../) to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Add the helper library path if needed
sys.path.append(os.path.expanduser("~/daqhats/examples/python/mcc134"))

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("mcc134_log.log"),
        logging.StreamHandler()
    ]
)

# Constants
CURSOR_BACK_2 = '\x1b[2D'
ERASE_TO_END_OF_LINE = '\x1b[0K'
TEMPERATURE_CHANNELS = [0, 1, 2, 3]
DELAY_BETWEEN_READS = 1  # Seconds

def init_db():
    """
    Initialize and return the database connection.
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
    try:
        return init_db()
    except Exception as e:
        logging.error(f"Reconnection failed: {e}")
        return None

def setup_csv(channels):
    """
    Setup and return a CSV writer and its associated file handle in a named tuple.
    """
    CsvHandle = namedtuple('CsvHandle', ['writer', 'file'])
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_thermocouple_data.csv"
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Timestamp"] + [f"TC_Ch{i} (°C)" for i in channels])
    logging.info(f"CSV logging started. File: {filename}")
    return CsvHandle(writer=csv_writer, file=csv_file)

def main():
    """
    Main function to run the MCC 134 thermocouple data acquisition.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="MCC 134 Thermocouple DAQ Logging Script")
    parser.add_argument('--table', required=True, help="Name of the database table to log data to.")
    args = parser.parse_args()
    table_name = args.table  # Assign the passed table name

    # Initialize database connection
    db_cloud = init_db()

    # Setup CSV logging
    csv_handle = setup_csv(TEMPERATURE_CHANNELS)

    try:
        # Get an instance of the selected MCC 134 device
        address = select_hat_device(HatIDs.MCC_134)
        hat = mcc134(address)

        # Assign thermocouple types to each channel
        hat.tc_type_write(0, TcTypes.TYPE_K)
        hat.tc_type_write(1, TcTypes.TYPE_K)
        hat.tc_type_write(2, TcTypes.TYPE_T)
        hat.tc_type_write(3, TcTypes.TYPE_K)

        logging.info("MCC 134 Thermocouple DAQ initialized.")
        print('\nAcquiring data ... Press Ctrl-C to abort')

        sample_count = 0

        while True:
            try:
                # Increment sample count
                sample_count += 1

                # Read temperature data
                temperature_data = []
                for channel in TEMPERATURE_CHANNELS:
                    value = hat.t_in_read(channel)
                    if value == mcc134.OPEN_TC_VALUE:
                        logging.warning(f"Channel {channel}: Open thermocouple detected.")
                        temperature_data.append(None)
                    elif value == mcc134.OVERRANGE_TC_VALUE:
                        logging.warning(f"Channel {channel}: Over-range condition.")
                        temperature_data.append(None)
                    elif value == mcc134.COMMON_MODE_TC_VALUE:
                        logging.warning(f"Channel {channel}: Common mode error.")
                        temperature_data.append(None)
                    else:
                        temperature_data.append(value)

                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Print collected data
                print(f"{timestamp} Sample {sample_count}")
                for i, temp in enumerate(temperature_data):
                    print(f"  Channel {i}: {'N/A' if temp is None else f'{temp:.2f} °C'}")

                # Write data to CSV
                csv_handle.writer.writerow([timestamp] + temperature_data)
                csv_handle.file.flush()

                # Log data to the database
                temp_array = np.array([t if t is not None else np.nan for t in temperature_data])  # Handle None values
                success_cloud = db_cloud.log(table=table_name, channels=temp_array)
                if not success_cloud:
                    logging.warning(f"Failed to log temperature data to table '{table_name}'.")
                    db_cloud = reconnect_db()

            except Exception as e:
                logging.error(f"Error during data acquisition: {e}")
                db_cloud = reconnect_db()

            # Wait the specified interval between reads
            sleep(DELAY_BETWEEN_READS)

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Stopping data acquisition.")
    finally:
        csv_handle.file.close()
        if db_cloud:
            db_cloud.close()
        logging.info("Resources cleaned up and program terminated.")

if __name__ == '__main__':
    main()
