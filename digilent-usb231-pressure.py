#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from time import sleep
from datetime import datetime
import csv
import argparse
import numpy as np
import logging
from collections import namedtuple
import sys
import os

# --- USB-231-specific imports ---
from mcculw import ul
from mcculw.enums import ScanOptions, FunctionType, Status, Range
from mcculw.device_info import DaqDeviceInfo
from ctypes import cast, POINTER, c_double

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
    logging.warning("Attempting to reconnect to the database...")
    try:
        return init_db()
    except Exception as e:
        logging.error(f"Reconnection failed: {e}")
        return None

def setup_csv(channels):
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

def start_acquisition(board_num, low_chan, high_chan, total_count, scan_rate, ai_range, memhandle):
    scan_options = ScanOptions.BACKGROUND | ScanOptions.SCALEDATA
    ul.a_in_scan(board_num, low_chan, high_chan, total_count, scan_rate, ai_range, memhandle, scan_options)

def stop_and_cleanup(board_num):
    ul.stop_background(board_num, FunctionType.AIFUNCTION)

def read_and_display_data(board_num, num_channels, csv_handle, db_cloud, table_name, resistor_value, pressure_lowest, pressure_highest, memhandle, total_count):
    buffer_size_per_channel = 1000
    aggregation_buffer = np.empty((buffer_size_per_channel, num_channels))
    samples_collected = 0
    prev_index = 0
    ctypes_array = cast(memhandle, POINTER(c_double))

    while True:
        try:
            status, curr_count, curr_index = ul.get_status(board_num, FunctionType.AIFUNCTION)
            if curr_count - samples_collected * num_channels < buffer_size_per_channel * num_channels:
                sleep(0.1)
                continue

            for i in range(buffer_size_per_channel):
                for ch in range(num_channels):
                    index = (curr_index + i * num_channels + ch) % total_count
                    aggregation_buffer[i, ch] = ctypes_array[index]

            samples_collected = 0  # reset

            aggregated_values = np.mean(aggregation_buffer, axis=0)

            currents = [(value / resistor_value) * 1000 for value in aggregated_values]
            pressures = [
                ((current - 4) * (pressure_highest - pressure_lowest) / 16) + pressure_lowest
                for current in currents
            ]

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            print(f"{timestamp}")
            for i, (voltage, current, pressure) in enumerate(zip(aggregated_values, currents, pressures)):
                print(f"  Channel {i+1}: Voltage={voltage:.2f} V, Current={current:.2f} mA, Pressure={pressure:.2f} bar")

            csv_handle.writer.writerow([timestamp] + aggregated_values.tolist() + currents + pressures)
            csv_handle.file.flush()

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
            stop_and_cleanup(board_num)
            break

def main():
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

    channels = [0, 1, 2, 3]
    num_channels = len(channels)
    low_chan = channels[0]
    high_chan = channels[-1]
    scan_rate = 1000
    points_per_channel = 1000
    total_count = points_per_channel * num_channels
    ai_range = Range.BIP10VOLTS
    board_num = 0

    csv_handle = setup_csv(channels)
    db_cloud = init_db()

    try:
        daq_dev_info = DaqDeviceInfo(board_num)
        if not daq_dev_info.supports_analog_input:
            raise Exception("DAQ device does not support analog input.")

        logging.info(f"Detected device: {daq_dev_info.product_name} ({daq_dev_info.unique_id})")
        memhandle = ul.scaled_win_buf_alloc(total_count)
        if not memhandle:
            raise RuntimeError("Failed to allocate memory for scan buffer.")

        input("\nPress ENTER to continue ...")
        start_acquisition(board_num, low_chan, high_chan, total_count, scan_rate, ai_range, memhandle)
        logging.info("DAQ acquisition started. Press Ctrl-C to stop.")
        read_and_display_data(board_num, num_channels, csv_handle, db_cloud, table_name, resistor_value, pressure_lowest, pressure_highest, memhandle, total_count)

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        csv_handle.file.close()
        if db_cloud:
            db_cloud.close()
        logging.info("DAQ acquisition stopped. Resources cleaned up.")

if __name__ == '__main__':
    main()
