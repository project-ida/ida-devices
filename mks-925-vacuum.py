import time
import serial
import logging
import argparse
import numpy as np
import csv
import sys
import os
from collections import namedtuple
from datetime import datetime

# Add the parent directory (../) to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("mks925_log.log"),
        logging.StreamHandler()
    ]
)

class Mks925:
    """ Driver for MKS 925 micro pirani vacuum gauge """

    def __init__(self, port):
        try:
            self.ser = serial.Serial(port, 9600, timeout=2)
            time.sleep(0.1)
            logging.info(f"Connected to MKS 925 on {port}")
        except serial.SerialException as e:
            logging.error(f"Failed to connect to MKS 925 on {port}: {e}")
            raise

    def comm(self, command):
        """ Implement communication protocol """
        prestring = b'@254'
        endstring = b';FF'
        self.ser.write(prestring + command.encode('ascii') + endstring)
        time.sleep(0.3)
        return_string = self.ser.read(self.ser.inWaiting()).decode()
        return return_string

    def read_pressure(self):
        """ Read the pressure from the device """
        command = 'PR1?'
        error = 1
        while (error > 0) and (error < 10):
            signal = self.comm(command)
            signal = signal[7:-3]
            try:
                value = float(signal)
                error = 0
            except ValueError:
                error += 1
                value = -1.0
                logging.warning("Invalid pressure reading received, retrying...")

        return value

    def change_unit(self, unit):
        """ Change the unit of the return value (e.g., TORR, PASCAL, MBAR) """
        command = 'U!' + unit
        signal = self.comm(command)
        return signal

    def read_serial(self):
        """ Read the serial number of the device """
        command = 'SN?'
        signal = self.comm(command)
        signal = signal[7:-3]
        return signal

def init_db():
    """
    Initialize and return the database connection.
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

def setup_csv():
    """
    Setup and return a CSV writer and its associated file handle in a named tuple.
    """
    CsvHandle = namedtuple('CsvHandle', ['writer', 'file'])
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_vacuumgauge_data.csv"
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Timestamp", "Pressure (mbar)"])
    logging.info(f"CSV logging started. File: {filename}")
    return CsvHandle(writer=csv_writer, file=csv_file)

def main():
    """
    Main function for vacuum gauge logging.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="MKS 925 Vacuum Gauge Logging Script")
    parser.add_argument('--table', required=True, help="Name of the database table to log data to.")
    parser.add_argument('--com', required=True, help="Serial port for the vacuum gauge (e.g., /dev/ttyUSB0).")
    args = parser.parse_args()

    table_name = args.table
    com_port = args.com

    # Initialize database connection
    db_cloud = init_db()

    # Setup CSV logging
    csv_handle = setup_csv()

    # Initialize MKS 925 vacuum gauge
    try:
        MKS = Mks925(com_port)
        logging.info(f"Using MKS 925 vacuum gauge on {com_port}")
    except Exception as e:
        logging.error("Failed to initialize vacuum gauge. Exiting.")
        sys.exit(1)

    # Set pressure unit
    logging.info(f"Setting pressure unit to MBAR: {MKS.change_unit('MBAR')}")
    logging.info(f"Gauge Serial Number: {MKS.read_serial()}")

    try:
        while True:
            try:
                # Read pressure
                this_pressure = MKS.read_pressure()
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                logging.info(f"Pressure: {this_pressure} mbar")

                # Write to CSV
                csv_handle.writer.writerow([timestamp, this_pressure])
                csv_handle.file.flush()

                # Log data to the database
                pressure_array = np.array([this_pressure])
                success_cloud = db_cloud.log(table=table_name, channels=pressure_array)
                if not success_cloud:
                    logging.warning(f"Failed to log pressure data to table '{table_name}'.")
                    db_cloud = reconnect_db()

            except Exception as e:
                logging.error(f"Error during data acquisition: {e}")
                db_cloud = reconnect_db()

            # Wait for 1 second before next reading
            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Stopping data acquisition.")
    finally:
        csv_handle.file.close()
        if db_cloud:
            db_cloud.close()
        logging.info("Resources cleaned up and program terminated.")

if __name__ == "__main__":
    main()
