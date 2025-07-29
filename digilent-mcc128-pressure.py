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
from collections import namedtuple
from daqhats import mcc128, OptionFlags, HatIDs, HatError, AnalogInputMode, \
    AnalogInputRange
import sys
import os

# Add the parent directory (../) to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Add the helper library path
sys.path.append(os.path.expanduser("~/daqhats/examples/python/mcc134"))
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
    filename = f"{timestamp}.csv"
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Timestamp"] + [f"Voltage_Ch{i}" for i in channels] +
                         [f"Current_Ch{i} (mA)" for i in channels] +
                         [f"Pressure_Ch{i} (bar)" for i in channels])
    logging.info(f"CSV logging started. File: {filename}")
    return CsvHandle(writer=csv_writer, file=csv_file)

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
    scan_rate = 1000.0  # Matches the old code's scan rate
    options = OptionFlags.CONTINUOUS

    # Setup CSV logging
    csv_handle = setup_csv(channels)

    # Initialize database logging
    db_cloud = init_db()

    try:
        address = select_hat_device(HatIDs.MCC_128)
        hat = mcc128(address)
        hat.a_in_mode_write(input_mode)
        hat.a_in_range_write(input_range)
        # save the correct mask for later restarts
        hat.channel_mask = channel_mask
        input('\nPress ENTER to continue ...')
        start_acquisition(hat, channel_mask, 0, scan_rate, options)
        logging.info("DAQ acquisition started. Press Ctrl-C to stop.")
        read_and_display_data(hat, num_channels, csv_handle, db_cloud, table_name, resistor_value, pressure_lowest, pressure_highest)
    except (HatError, ValueError) as err:
        logging.error(f"Hardware error: {err}")
    finally:
        csv_handle.file.close()  # Only reference the file here for cleanup
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

def read_and_display_data(hat, num_channels, csv_handle, db_cloud, table_name, resistor_value, pressure_lowest, pressure_highest):
    """
    Reads and processes data from the DAQ with aggregation.
    """
    timeout = 5.0
    buffer_size_per_channel = 1000  # Matches the old code's buffer size
    aggregation_buffer = np.empty((buffer_size_per_channel, num_channels))
    samples_collected = 0

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
                # reuse the original mask we saved on hat
                start_acquisition(hat, hat.channel_mask, 0, 1000.0, OptionFlags.CONTINUOUS)
                continue

            new_samples = np.array(read_result.data).reshape(-1, num_channels)
            samples_to_copy = min(buffer_size_per_channel - samples_collected, new_samples.shape[0])
            aggregation_buffer[samples_collected:samples_collected+samples_to_copy, :] = new_samples[:samples_to_copy, :]
            samples_collected += samples_to_copy

            if samples_collected >= buffer_size_per_channel:
                aggregated_values = np.mean(aggregation_buffer, axis=0)
                samples_collected = 0  # Reset the sample count

                # Calculate currents and pressures
                if resistor_value == 0:
                    # Direct 0-5V to pressure mapping
                    pressures = [
                        ((voltage - 0) * (pressure_highest - pressure_lowest) / 5) + pressure_lowest
                        for voltage in aggregated_values
                    ]
                else:
                    # 4-20 mA conversion method
                    currents = [(value / resistor_value) * 1000 for value in aggregated_values]
                    pressures = [
                        ((current - 4) * (pressure_highest - pressure_lowest) / 16) + pressure_lowest
                        for current in currents
                    ]
                    
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Display output for each channel
                print(f"{timestamp}")
                for i, (voltage, current, pressure) in enumerate(zip(aggregated_values, currents, pressures)):
                    print(f"  Channel {i+1}: Voltage={voltage:.2f} V, Current={current:.2f} mA, Pressure={pressure:.2f} bar")

                # Write to CSV
                csv_handle.writer.writerow([timestamp] + aggregated_values.tolist() + currents + pressures)
                csv_handle.file.flush()

                # Log to database
                if db_cloud:
                    try:
                        channels_array = np.array(aggregated_values.tolist() + currents + pressures)
                        success = db_cloud.log(table=table_name, channels=channels_array)
                        if not success:
                            logging.warning(f"Failed to log data to table '{table_name}'.")
                            db_cloud = reconnect_db()
                    except Exception as e:
                        logging.error(f"Database logging failed: {e}")
                else:
                    db_cloud = reconnect_db()

        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received. Stopping DAQ.")
            stop_and_cleanup(hat)
            break

if __name__ == '__main__':
    main()
