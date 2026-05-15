#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Glassman EK high-voltage power-supply logger.

This version keeps the pressure-logger style of passing run-specific settings
from the command line:

  - --table       database table to log to
  - --port        serial COM port, e.g. COM3
  - --poll-delay  delay after each successful poll, in seconds

It also:

  - connects to database through ida_db.pglogger + psql_credentials
  - writes a local CSV backup
  - polls the Glassman supply over RS-232 serial
  - logs voltage/current/status to the database table named by --table
  - reconnects to database automatically after DB logging errors
  - retries after serial/read errors
  - always asks for the version string at startup
  - exits cleanly on Ctrl-C

Expected hardware setup from testing:
  - 9600 baud, 8N1
  - null-modem / TX-RX crossover adapter
  - working commands:
      version: 01 56 35 36 0d
      query:   01 51 35 31 0d

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
import sys
import time
from collections import namedtuple
from datetime import datetime

import numpy as np
import serial


# ---------------------------------------------------------------------------
# Hardwired instrument settings
# ---------------------------------------------------------------------------

BAUDRATE = 9600
SERIAL_TIMEOUT_SEC = 1.0

# Full-scale ratings are needed to convert the supply's 10-bit hex counts
# into engineering units. Change these constants if you use a different supply.
V_MAX_KV = 10.0
I_MAX_MA = 60.0

QUERY_CMD = b"\x01Q51\r"      # SOH Q checksum CR
VERSION_CMD = b"\x01V56\r"    # SOH V checksum CR

# Standard retry delay after a serial/read/database error.
ERROR_SLEEP_SEC = 1.0


# Add the parent directory (../) to the Python path, matching the old DAQ script.
# This is useful if ida_db.py and psql_credentials.py live one directory above.
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("glassman_ek_voltage_log.log"),
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
    filename = f"{timestamp}_glassman_ek_voltage.csv"

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


def parse_query_response(resp):
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

    voltage_kv = v_counts / 0x3FF * V_MAX_KV
    current_ma = i_counts / 0x3FF * I_MAX_MA

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


def open_serial(port):
    """
    Open the serial connection to the supply.
    """
    ser = serial.Serial(
        port=port,
        baudrate=BAUDRATE,
        bytesize=8,
        parity="N",
        stopbits=1,
        timeout=SERIAL_TIMEOUT_SEC,
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


def query_supply(ser):
    """
    Query the supply once and return parsed data.
    """
    ser.reset_input_buffer()

    ser.write(QUERY_CMD)
    ser.flush()

    resp = read_response(ser, max_bytes=16)
    return parse_query_response(resp)


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


def logging_loop(ser, csv_handle, db_cloud, table_name, poll_delay):
    """
    Poll the supply, print readings, write CSV rows, and push data to DB.
    """
    while True:
        try:
            data = query_supply(ser)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            print(
                f"{timestamp} | "
                f"{data['voltage_kv']:.3f} kV, "
                f"{data['current_ma']:.3f} mA, "
                f"mode={data['mode']}, "
                f"HV_ON={bool(data['hv_on_flag'])}, "
                f"FAULT={bool(data['fault_flag'])}"
            )

            write_csv_row(csv_handle, timestamp, data)

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

            if poll_delay > 0:
                time.sleep(poll_delay)

        except KeyboardInterrupt:
            logging.info("Keyboard interrupt received. Stopping serial logger.")
            break

        except (RuntimeError, serial.SerialException, serial.SerialTimeoutException) as e:
            logging.error(f"Read/serial error: {e}")
            time.sleep(ERROR_SLEEP_SEC)


def main():
    parser = argparse.ArgumentParser(
        description="Glassman EK serial power-supply logger with CSV and PostgreSQL database logging."
    )

    parser.add_argument(
        "--table",
        required=True,
        help="Name of the database table to log data to."
    )
    parser.add_argument(
        "--port",
        required=True,
        help="Serial port for the power supply, e.g. COM3 on Windows or /dev/ttyUSB0 on Linux."
    )
    parser.add_argument(
        "--poll-delay",
        type=float,
        default=0.0,
        help="Delay in seconds after each successful poll. Use 0 for fastest polling."
    )

    args = parser.parse_args()

    csv_handle = None
    db_cloud = None
    ser = None

    try:
        csv_handle = setup_csv()
        db_cloud = init_db()

        logging.info(
            f"Opening serial port {args.port} at {BAUDRATE} baud. "
            "Use a null-modem/crossover connection if using DB9 RS-232."
        )

        ser = open_serial(args.port)

        version_resp = query_version(ser)
        logging.info(
            f"Version response: {version_resp!r} "
            f"hex={version_resp.hex(' ') if version_resp else ''}"
        )

        logging.info(
            f"Power supply logging started with poll_delay={args.poll_delay} s. "
            "Press Ctrl-C to stop."
        )
        logging_loop(
            ser=ser,
            csv_handle=csv_handle,
            db_cloud=db_cloud,
            table_name=args.table,
            poll_delay=args.poll_delay,
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
