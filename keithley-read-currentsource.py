import numpy as np
import datetime
import argparse
import time
import socket
import serial
import logging
import csv
import sys
import os
import threading
from collections import namedtuple
from gpib_power_supply import GPIBPowerSupply  # Use GPIB power supply instead of KoradSerial

# Add the parent directory (../) to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("gpib_power_log.log"),
        logging.StreamHandler()
    ]
)

def init_db():
    """ Initialize and return the database connection. """
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
    """ Attempt to reconnect to the database. """
    logging.warning("Attempting to reconnect to the database...")
    return init_db()

def setup_csv():
    """ Setup and return a CSV writer and its associated file handle in a named tuple. """
    CsvHandle = namedtuple('CsvHandle', ['writer', 'file'])
    start_time = datetime.datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_gpib_power_data.csv"
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Timestamp", "Actual Voltage (V)", "Actual Current (A)", "Power (W)", "Set Voltage (V)", "Set Current (A)"])
    logging.info(f"CSV logging started. File: {filename}")
    return CsvHandle(writer=csv_writer, file=csv_file)

def try_open_port(port, gpib_address, retries=5, delay=2):
    """ Attempts to initialize the GPIB power supply with retries. """
    for attempt in range(retries):
        try:
            device = GPIBPowerSupply(port=port, gpib_address=gpib_address)
            logging.info(f"Connected to GPIB Power Supply at {port}, GPIB Address {gpib_address}")
            return device
        except serial.SerialException as e:
            if attempt < retries - 1:
                logging.warning(f"Port busy, retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                logging.error(f"Failed to open GPIB port after {retries} attempts: {e}")
                raise e

def handle_commands(device, host='localhost', port=12345):
    """Handles incoming current commands from clients over a socket connection."""
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((host, port))
                s.listen()
                logging.info(f"Server listening for commands on {host}:{port}")
                
                while True:
                    conn, addr = s.accept()
                    logging.info(f"Client connected: {addr}")
                    with conn:
                        conn.settimeout(10)  # Timeout for client socket
                        while True:
                            try:
                                data = conn.recv(1024)
                                if not data:
                                    logging.info(f"Client {addr} disconnected.")
                                    break
                                try:
                                    current = float(data.decode())  # Interpret received data as current in mA
                                    device.set_current(current)
                                    logging.info(f"Set current to: {current:.2f} mA")
                                except ValueError:
                                    logging.warning(f"Invalid current data received from {addr}.")
                            except socket.timeout:
                                logging.warning(f"Socket timeout for client {addr}. Closing connection.")
                                break
        except Exception as e:
            logging.error(f"Error in handle_commands: {e}. Restarting server in 5 seconds...")
            time.sleep(5)

def start_server(device, host, port):
    """ Starts the server in a separate thread. """
    def server_thread():
        try:
            logging.info("Server thread starting...")
            handle_commands(device, host, port)
        except Exception as e:
            logging.error(f"Server thread error: {e}")
            return  

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    return thread

def main():
    parser = argparse.ArgumentParser(description="Control and monitor GPIB power supply.")
    parser.add_argument("--com", required=True, help="Serial COM port for the GPIB controller")
    parser.add_argument("--port", type=int, required=True, help="Port for listening to voltage commands")
    parser.add_argument("--device-id", type=int, required=True, help="GPIB device ID")
    parser.add_argument("--table", required=True, help="Database table name for logging data")
    parser.add_argument("--uset", type=float, help="Set voltage limit (optional)")
    parser.add_argument("--iset", type=float, help="Set current limit (optional)")
    args = parser.parse_args()

    # Initialize database and device
    db_cloud = init_db()
    csv_handle = setup_csv()
    device = try_open_port(args.com, args.device_id)

    with device:
        # Set initial voltage and current limits
        if args.uset:
            device.set_voltage_limit(args.uset)

        if args.iset:
            device.set_current(args.iset)

        # Turn on the device output
        device.turn_on()

        try:
            # Start the server thread
            server_thread = start_server(device, 'localhost', args.port)

            # Main loop: log data
            while True:
                current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                try:
                    actual_current, actual_voltage = device.get_status()
                    set_voltage = args.uset if args.uset else actual_voltage
                    set_current = args.iset if args.iset else actual_current
                    power = actual_voltage * actual_current if actual_voltage and actual_current else None

                    logging.info(f"Voltage: {actual_voltage:.2f} V, Current: {actual_current:.3f} A, Power: {power:.3f} W")

                    # Write to CSV
                    csv_handle.writer.writerow([current_time, actual_voltage, actual_current, power, set_voltage, set_current])
                    csv_handle.file.flush()

                    # Log data to the database
                    ps_array = np.array([actual_voltage, actual_current, power, set_voltage, set_current])
                    success_cloud = db_cloud.log(args.table, channels=ps_array)
                    if not success_cloud:
                        logging.warning("Failed to log power supply values to the cloud database.")
                        db_cloud = reconnect_db()

                except Exception as e:
                    logging.error(f"An unexpected error occurred: {e}")

                time.sleep(1)
        finally:
            device.turn_off()
            logging.info("Device output turned off.")
            csv_handle.file.close()

if __name__ == "__main__":
    main()
