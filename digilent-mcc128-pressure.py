#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from time import sleep
from sys import argv
from datetime import datetime
import csv
import argparse
import numpy as np
import logging
from daqhats import mcc128, OptionFlags, HatIDs, HatError, AnalogInputMode, \
    AnalogInputRange
from daqhats_utils import select_hat_device, chan_list_to_mask

READ_ALL_AVAILABLE = -1

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("daq_log.log"),
        logging.StreamHandler()
    ]
)

def init_db():
    """
    Initialize and return the database connection and cursor.
    """
    from mitcf import pglogger
    import psql_credentials_cloud as creds_cloud
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

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="DAQ Logging Script")
    parser.add_argument('--table', required=True, help="Name of the database table to log data to.")
    parser.add_argument('--resistor', type=float, required=True, help="Value of the resistor in Ohms (e.g., 458).")
    parser.add_argument('--pressure-lowest', type=float, required=True, help="Pressure corresponding to 4 mA (e.g., 0 bar).")
    parser.add_argument('--pressure-highest', type=float, required=True, help="Pressure corresponding to 20 mA (e.g., 7 bar).")
    args = parser.parse_args()

    table_name = args.table
    resistor_value = args.resistor
    pressure_lowest = args.pressure_lowest
    pressure_highest = args.pressure_highest

    # Initial configurations
    channels = [0, 1, 2, 3]
    channel_mask = chan_list_to_mask(channels)
    num_channels = len(channels)
    input_mode = AnalogInputMode.DIFF
    input_range = AnalogInputRange.BIP_10V
    scan_rate = 100.0  # Adjusted to approximately match the original behavior (1 update/second)
    options = OptionFlags.CONTINUOUS

    # Setup for writing to a CSV file
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}.csv"
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Timestamp"] + [f"Voltage_Ch{i}" for i in channels] +
                         [f"Current_Ch{i} (mA)" for i in channels] +
                         [f"Pressure_Ch{i} (bar)" for i in channels])
    logging.info(f"CSV logging started. File: {filename}")

    # Initialize database logging
    db_cloud = init_db()

    try:
        address = select_hat_device(HatIDs.MCC_128)
        hat = mcc128(address)
        hat.a_in_mode_write(input_mode)
        hat.a_in_range_write(input_range)
        input('\nPress ENTER to continue ...')
        start_acquisition(hat, channel_mask, 0, scan_rate, options)
        logging.info("DAQ acquisition started. Press Ctrl-C to stop.")
        read_and_display_data(hat, num_channels, csv_writer, csv_file, db_cloud, table_name, resistor_value, pressure_lowest, pressure_highest)
    except (HatError, ValueError) as err:
        logging.error(f"Hardware error: {err}")
    finally:
        csv_file.close()
        if db_cloud:
            db_cloud.close()
        logging.info("DAQ acquisition stopped. Resources cleaned up.")

def start_acquisition(hat, channel_mask, samples_per_channel, scan_rate, options):
    """
    Starts the DAQ process with the specified configuration.
    """
    hat.a_in_scan_start(channel_mask, samples_per_channel, scan_rate, options)

def stop_and_cleanup(hat):
    """
    Stops the DAQ and cleans up resources.
    """
    hat.a_in_scan_stop()
    hat.a_in_scan_cleanup()

def read_and_display_data(hat, num_channels, csv_writer, csv_file, db_cloud, table_name, resistor_value, pressure_lowest, pressure_highest):
    """
    Reads and processes data from the DAQ.
    """
    timeout = 5.0

    while True:
        try:
            read_result = hat.a_in_scan_read(READ_ALL_AVAILABLE, timeout)
            if read_result.hardware_overrun:
                logging.error("Hardware overrun detected.")
                sleep(1.0)
                continue
            if read_result.buffer_overrun:
                logging.warning("Buffer overrun detected. Restarting acquisition.")
                stop_and_cleanup(hat)
                start_acquisition(hat, chan_list_to_mask(range(num_channels)), 0, 100.0, OptionFlags.CONTINUOUS)
                continue

            # Process data
            new_samples = np.array(read_result.data).reshape(-1, num_channels)
            aggregated_values = np.mean(new_samples, axis=0)
            currents = []
            pressures = []

            for value in aggregated_values:
                # Step 1: Convert voltage to current in mA
                current_mA = (value / resistor_value) * 1000
                currents.append(current_mA)

                # Step 2: Map current to pressure range
                pressure = ((current_mA - 4) * (pressure_highest - pressure_lowest) / 16) + pressure_lowest
                pressures.append(pressure)

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Display output for each channel on a separate line
            print(f"{timestamp}")
            for i, (voltage, current, pressure) in enumerate(zip(aggregated_values, currents, pressures)):
                print(f"  Channel {i+1}: Voltage={voltage:.2f} V, Current={current:.2f} mA, Pressure={pressure:.2f} bar")

            # Write to CSV
            csv_writer.writerow([timestamp] + aggregated_values.tolist() + currents + pressures)
            csv_file.flush()

            # Log to database
            if db_cloud:
                try:
                    channels_array = np.array(aggregated_values.tolist() + currents + pressures)  # Convert to NumPy array
                    success = db_cloud.log(table=table_name, channels=channels_array)
                    if not success:
                        logging.warning(f"Failed to log data to table '{table_name}'.")
                        db_cloud = reconnect_db()
                except Exception as e:
                    logging.error(f"Database logging failed: {e}")
            else:
                db_cloud = reconnect_db()

            sleep(1.0)  # Ensure updates are approximately 1 second apart

        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received. Stopping DAQ.")
            stop_and_cleanup(hat)
            break

if __name__ == '__main__':
    main()
