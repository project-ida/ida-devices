#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Power supply serial logger.

This replaces the MCC128 pressure-specific acquisition code with serial polling
of the high-voltage power supply, while keeping the same general pattern:

  - command-line table name
  - CSV logging
  - database connection through ida_db.pglogger + psql_credentials
  - reconnect attempts if database logging fails
  - Ctrl-C cleanup

Database channel order:
  0: voltage_kv
  1: current_ma
  2: voltage_counts_raw
  3: current_counts_raw
  4: mode_current_flag     (1=current mode, 0=voltage mode)
  5: hv_on_flag            (1=HV on, 0=HV off)
  6: fault_flag            (1=fault, 0=no fault)
"""

from __future__ import print_function

import argparse
import csv
import logging
import os
import re
import sys
import time
from collections import namedtuple
from datetime import datetime

import numpy as np
import serial
import serial.tools.list_ports


QUERY_CMD = b"\x01Q51\r"      # SOH Q checksum CR
VERSION_CMD = b"\x01V56\r"    # SOH V checksum CR


# Add the parent directory (../) to the Python path, matching the old DAQ script.
# This is useful if ida_db.py and psql_credentials.py live one directory above.
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("power_supply_log.log"),
        logging.StreamHandler()
    ]
)


def init_db():
    """
    Initialize and return the database logger.

    Expects ida_db.py and psql_credentials.py to be importable, as in the
    original MCC128 pressure logger.
    """
    try:
        from ida_db import pglogger
        import psql_credentials as creds_cloud

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


def setup_csv():
    """
    Setup and return a CSV writer and its associated file handle.
    """
    CsvHandle = namedtuple("CsvHandle", ["writer", "file"])

    start_time = datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_power_supply.csv"

    csv_file = open(filename, "w", newline="")
    csv_writer = csv.writer(csv_file)

    csv_writer.writerow([
        "Timestamp",
        "Voltage_kV",
        "Current_mA",
        "Voltage_Counts_Raw",
        "Current_Counts_Raw",
        "Mode",
        "Mode_Current_Flag",
        "HV_On_Flag",
        "Fault_Flag",
        "Raw_Response_ASCII",
        "Raw_Response_Hex",
    ])

    logging.info(f"CSV logging started. File: {filename}")
    return CsvHandle(writer=csv_writer, file=csv_file)


def list_ports():
    """
    Print available serial ports.
    """
    ports = list(serial.tools.list_ports.comports())

    if not ports:
        print("No serial ports found.")
        return

    for p in ports:
        print(f"{p.device}: {p.description} [{p.hwid}]")


def checksum_ascii(data):
    """
    Calculate the two-character ASCII hex checksum used by the supply.
    """
    return f"{sum(data) & 0xFF:02X}"


def read_response(ser, max_bytes=64):
    """
    Read until carriage return or timeout.
    """
    return ser.read_until(b"\r", size=max_bytes)


def parse_query_response(resp, v_max_kv, i_max_ma):
    """
    Parse a query response packet from the supply.

    Expected response format:
        R + 12 ASCII payload characters + 2 ASCII checksum characters + CR

    Example:
        b'R00000000000040\\r'
    """
    if not resp:
        raise RuntimeError("No response from supply")

    if resp.startswith(b"E"):
        raise RuntimeError(f"Supply returned error packet: {resp!r}")

    if len(resp) != 16 or resp[0:1] != b"R" or resp[-1:] != b"\r":
        raise RuntimeError(f"Unexpected query response: {resp!r}")

    transmitted_checksum = resp[13:15].decode("ascii")
    calculated_checksum = checksum_ascii(resp[1:13])

    if transmitted_checksum != calculated_checksum:
        raise RuntimeError(
            f"Checksum mismatch: got {transmitted_checksum}, "
            f"expected {calculated_checksum}, response={resp!r}"
        )

    v_counts = int(resp[1:4].decode("ascii"), 16)
    i_counts = int(resp[4:7].decode("ascii"), 16)

    voltage_kv = v_counts / 0x3FF * v_max_kv
    current_ma = i_counts / 0x3FF * i_max_ma

    # This status-byte mapping follows the earlier working script.
    # It should be verified against your supply's manual / command set.
    status1 = int(chr(resp[10]), 16)

    current_mode = bool(status1 & 0b0001)
    fault = bool(status1 & 0b0010)
    hv_on = bool(status1 & 0b0100)

    return {
        "raw": resp,
        "raw_ascii": resp.decode("ascii", errors="replace").rstrip("\r"),
        "raw_hex": resp.hex(" "),
        "voltage_kv": voltage_kv,
        "current_ma": current_ma,
        "voltage_counts_raw": v_counts,
        "current_counts_raw": i_counts,
        "mode": "current" if current_mode else "voltage",
        "mode_current_flag": 1 if current_mode else 0,
        "fault_flag": 1 if fault else 0,
        "hv_on_flag": 1 if hv_on else 0,
    }


def open_serial(port, baudrate, timeout):
    """
    Open the serial connection to the supply.
    """
    ser = serial.Serial(
        port=port,
        baudrate=baudrate,
        bytesize=8,
        parity="N",
        stopbits=1,
        timeout=timeout,
        xonxoff=False,
        rtscts=False,
        dsrdtr=False,
    )

    # Some instruments require modem-control lines to be asserted.
    # This was harmless in the initial successful test.
    ser.setDTR(True)
    ser.setRTS(True)

    ser.reset_input_buffer()
    ser.reset_output_buffer()

    return ser


def query_version(ser):
    """
    Ask the supply for its version string.
    """
    ser.reset_input_buffer()
    ser.write(VERSION_CMD)
    ser.flush()
    return read_response(ser, max_bytes=16)


def query_supply(ser, v_max_kv, i_max_ma):
    """
    Query the supply once and return parsed data.
    """
    ser.reset_input_buffer()

    ser.write(QUERY_CMD)
    ser.flush()

    resp = read_response(ser, max_bytes=16)
    return parse_query_response(resp, v_max_kv=v_max_kv, i_max_ma=i_max_ma)


def log_to_database(db_cloud, table_name, data):
    """
    Log one supply reading to the database.

    Uses the same pglogger.log(table=..., channels=np.array(...)) pattern
    as the original MCC128 pressure logger.
    """
    channels_array = np.array([
        data["voltage_kv"],
        data["current_ma"],
        data["voltage_counts_raw"],
        data["current_counts_raw"],
        data["mode_current_flag"],
        data["hv_on_flag"],
        data["fault_flag"],
    ], dtype=float)

    return db_cloud.log(table=table_name, channels=channels_array)


def write_csv_row(csv_handle, timestamp, data):
    """
    Write one supply reading to CSV.
    """
    csv_handle.writer.writerow([
        timestamp,
        data["voltage_kv"],
        data["current_ma"],
        data["voltage_counts_raw"],
        data["current_counts_raw"],
        data["mode"],
        data["mode_current_flag"],
        data["hv_on_flag"],
        data["fault_flag"],
        data["raw_ascii"],
        data["raw_hex"],
    ])
    csv_handle.file.flush()


def validate_table_name(table_name):
    """
    Basic safety check for table names.

    Allows:
      table_name
      schema.table_name

    with letters, digits, and underscores. This avoids accidentally passing
    shell-like or SQL-like strings as a table name.
    """
    pattern = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"
    if not re.match(pattern, table_name):
        raise ValueError(
            f"Unsafe table name: {table_name!r}. "
            "Use only letters, digits, underscores, and optionally one schema dot."
        )


def logging_loop(ser, csv_handle, db_cloud, table_name, args):
    """
    Poll the supply, print readings, write CSV rows, and push data to DB.
    """
    while True:
        try:
            data = query_supply(ser, v_max_kv=args.vmax_kv, i_max_ma=args.imax_ma)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            print(
                f"{timestamp} | "
                f"{data['voltage_kv']:.3f} kV, "
                f"{data['current_ma']:.3f} mA, "
                f"mode={data['mode']}, "
                f"HV_ON={bool(data['hv_on_flag'])}, "
                f"FAULT={bool(data['fault_flag'])}"
            )

            if args.raw_debug:
                print(f"  raw={data['raw']!r} hex={data['raw_hex']}")

            if csv_handle is not None:
                write_csv_row(csv_handle, timestamp, data)

            if not args.no_db:
                if db_cloud:
                    try:
                        success = log_to_database(db_cloud, table_name, data)
                        if not success:
                            logging.warning(
                                f"Failed to log data to table {table_name!r}."
                            )
                            db_cloud = reconnect_db()
                    except Exception as e:
                        logging.error(f"Database logging failed: {e}")
                        db_cloud = reconnect_db()
                else:
                    db_cloud = reconnect_db()

            time.sleep(args.interval)

        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received. Stopping serial logger.")
            break

        except (RuntimeError, serial.SerialException, serial.SerialTimeoutException) as e:
            logging.error(f"Read/serial error: {e}")
            time.sleep(args.error_sleep)


def main():
    parser = argparse.ArgumentParser(
        description="Serial power-supply logger with CSV and PostgreSQL database logging."
    )

    parser.add_argument(
        "--table",
        required=False,
        help="Name of the database table to log data to. Required unless --no-db is used."
    )
    parser.add_argument(
        "--port",
        default="COM3",
        help="Serial port for the power supply, e.g. COM3 on Windows or /dev/ttyUSB0 on Linux."
    )
    parser.add_argument(
        "--baudrate",
        type=int,
        default=9600,
        help="Serial baud rate. The working value from testing was 9600."
    )
    parser.add_argument(
        "--vmax-kv",
        type=float,
        default=10.0,
        help="Supply full-scale voltage in kV."
    )
    parser.add_argument(
        "--imax-ma",
        type=float,
        default=60.0,
        help="Supply full-scale current in mA."
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Polling interval in seconds."
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=1.0,
        help="Serial read timeout in seconds."
    )
    parser.add_argument(
        "--error-sleep",
        type=float,
        default=1.0,
        help="Delay after a read/serial error before trying again."
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Do not connect to or log to the database. Useful for serial/CSV testing."
    )
    parser.add_argument(
        "--no-csv",
        action="store_true",
        help="Do not write a CSV file."
    )
    parser.add_argument(
        "--raw-debug",
        action="store_true",
        help="Print raw response bytes and hex."
    )
    parser.add_argument(
        "--list-ports",
        action="store_true",
        help="List available serial ports and exit."
    )
    parser.add_argument(
        "--skip-version",
        action="store_true",
        help="Skip the initial firmware/version query."
    )

    args = parser.parse_args()

    if args.list_ports:
        list_ports()
        return

    if not args.no_db:
        if not args.table:
            parser.error("--table is required unless --no-db is used.")
        validate_table_name(args.table)

    csv_handle = None
    db_cloud = None
    ser = None

    try:
        if not args.no_csv:
            csv_handle = setup_csv()

        if not args.no_db:
            db_cloud = init_db()

        logging.info(
            f"Opening serial port {args.port} at {args.baudrate} baud. "
            "Use a null-modem/crossover connection if using DB9 RS-232."
        )

        ser = open_serial(
            port=args.port,
            baudrate=args.baudrate,
            timeout=args.timeout,
        )

        if not args.skip_version:
            version_resp = query_version(ser)
            logging.info(
                f"Version response: {version_resp!r} "
                f"hex={version_resp.hex(' ') if version_resp else ''}"
            )

        logging.info("Power supply logging started. Press Ctrl-C to stop.")
        logging_loop(
            ser=ser,
            csv_handle=csv_handle,
            db_cloud=db_cloud,
            table_name=args.table,
            args=args,
        )

    except serial.SerialException as e:
        logging.error(f"Could not open/use serial port {args.port!r}: {e}")

    finally:
        if ser is not None and ser.is_open:
            ser.close()

        if csv_handle is not None:
            csv_handle.file.close()

        if db_cloud:
            try:
                db_cloud.close()
            except Exception as e:
                logging.error(f"Error closing database connection: {e}")

        logging.info("Resources cleaned up.")


if __name__ == "__main__":
    main()
