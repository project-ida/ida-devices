from __future__ import absolute_import, division, print_function
from builtins import *
import time
import logging
import numpy as np
import argparse
from uldaq import TempScale, DaqDeviceInfo, get_daq_device_inventory, InterfaceType, DaqDevice
from collections import namedtuple
from datetime import datetime
import csv
import sys
import os

# Add the parent directory (../) to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("usb_temp_log.log"),
        logging.StreamHandler()
    ]
)

TEMPERATURE_CHANNELS = [0, 1, 2, 3, 4, 5]

def init_db():
    """
    Initialize and return the database connection and cursor.
    """
    from ida_db import pglogger
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
    filename = f"{timestamp}_temperature_data.csv"
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Timestamp"] + [f"Temp_Ch{i} (°C)" for i in channels])
    logging.info(f"CSV logging started. File: {filename}")
    return CsvHandle(writer=csv_writer, file=csv_file)

def prompt_for_temp_device():
    """
    Automatically selects a temperature device if only one is available.
    Prompts the user to select a device if multiple are detected.
    """
    devices = get_daq_device_inventory(InterfaceType.USB)
    num_devices = len(devices)

    if num_devices == 0:
        logging.error("No USB-TEMP devices found. Exiting.")
        raise RuntimeError("No USB-TEMP devices found.")

    if num_devices == 1:
        logging.info(f"Automatically selecting the only available device: {devices[0].product_name}")
        return DaqDevice(devices[0])

    print("Please enter a board number from the following:")
    for i, d in enumerate(devices):
        name = DaqDevice(d).get_descriptor().product_name
        id = DaqDevice(d).get_descriptor().unique_id
        print(f"{i}) {name} ({id})")
    temp_index = int(input("Index of USB-TEMP Device: "))
    return DaqDevice(devices[temp_index])

def read_temperatures(device: DaqDevice):
    """
    Read temperatures from the specified device.
    """
    output = {}
    for c in TEMPERATURE_CHANNELS:
        output[c] = device.get_ai_device().t_in(c, TempScale.CELSIUS)
    return output

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="USB-TEMP DAQ Logging Script")
    parser.add_argument('--table', required=True, help="Name of the database table to log data to.")
    args = parser.parse_args()

    table_name = args.table  # Assign the passed table name

    # Initialize database connection
    db_cloud = init_db()

    # Setup CSV logging
    csv_handle = setup_csv(TEMPERATURE_CHANNELS)

    # Prompt user for temperature device or auto-select if only one is available
    usb_temp = prompt_for_temp_device()
    usb_temp.connect()

    try:
        while True:
            try:
                # Read temperature data
                temperature_data = read_temperatures(usb_temp)
                logging.info(f"Temperature Data: {temperature_data}")

                # Get current timestamp
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Write data to CSV
                temperature_values = list(temperature_data.values())
                csv_handle.writer.writerow([timestamp] + temperature_values)
                csv_handle.file.flush()

                # Log data to the database
                temperature_array = np.array(temperature_values)
                success_cloud = db_cloud.log(table=table_name, channels=temperature_array)
                if not success_cloud:
                    logging.warning(f"Failed to log temperature data to table '{table_name}'.")
                    db_cloud = reconnect_db()

            except Exception as e:
                logging.error(f"Error during data acquisition: {e}")
                db_cloud = reconnect_db()

            # Wait for 1 second before the next reading
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Stopping data acquisition.")
    finally:
        # Cleanup resources
        usb_temp.disconnect()
        csv_handle.file.close()
        if db_cloud:
            db_cloud.close()
        logging.info("Resources cleaned up and program terminated.")

if __name__ == "__main__":
    main()
