import numpy as np
import serial
import logging
import argparse
import csv
import sys
import os
import time
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
        logging.FileHandler("detector_log.log"),
        logging.StreamHandler()
    ]
)

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

def setup_csv():
    """
    Setup and return a CSV writer and its associated file handle in a named tuple.
    """
    CsvHandle = namedtuple('CsvHandle', ['writer', 'file'])
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_detector_data.csv"
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(["Timestamp", "Detector Value"])
    logging.info(f"CSV logging started. File: {filename}")
    return CsvHandle(writer=csv_writer, file=csv_file)

def init_serial(com_port):
    """
    Initialize serial connection.
    """
    try:
        ser = serial.Serial(
            port=com_port,
            baudrate=9600,
            parity=serial.PARITY_EVEN,
            stopbits=serial.STOPBITS_TWO,
            bytesize=serial.SEVENBITS,
            timeout=0.5  # Allow time for response
        )
        logging.info(f"Connected to serial port {com_port}")
        return ser
    except serial.SerialException as e:
        logging.error(f"Failed to connect to serial port {com_port}: {e}")
        sys.exit(1)

def read_detector(ser):
    """
    Read data from the detector with retries.
    """
    finalresponse = ""

    ser.flushInput()
    ser.flushOutput()
    time.sleep(0.1)

    for attempt in range(3):  # Retry up to 3 times
        ser.write(b'x')  # Initial command to read
        time.sleep(0.010)
        ser.write(b'Rx\r\n')  # Request data (using \r\n)

        counter = 0
        while len(finalresponse) < 25 and counter < 10:
            time.sleep(0.020)
            response = ser.readline().decode(errors='ignore').strip()  # Read and decode

            if response:  # Stop if valid data is received
                logging.info(f"Attempt {attempt + 1}: Response received: {response}")
                finalresponse += response
                break

            counter += 1

        if finalresponse:
            break  # Exit retry loop if we got a valid response

    logging.info(f"Final detector response: {finalresponse}")
    return finalresponse

def main():
    """
    Main function to run the detector data acquisition.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Detector Logging Script")
    parser.add_argument('--table', required=True, help="Name of the database table to log data to.")
    parser.add_argument('--com', required=True, help="Serial port for the detector (e.g., /dev/ttyUSB0).")
    args = parser.parse_args()

    table_name = args.table
    com_port = args.com

    # Initialize database connection
    db_cloud = init_db()

    # Setup CSV logging
    csv_handle = setup_csv()

    # Initialize serial connection
    ser = init_serial(com_port)

    try:
        while True:
            try:
                # Read detector response
                line_str = read_detector(ser)

                if ">#" in line_str and len(line_str) > 20:
                    line_list = line_str.split(" ")

                    try:
                        ext_value = line_list[3]
                        value = float(ext_value)
                    except (IndexError, ValueError):
                        value = -1  # Default if extraction fails
                        logging.warning("Invalid detector response format, using default value -1.")

                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    logging.info(f"Detector Value: {value}")

                    # Write to CSV
                    csv_handle.writer.writerow([timestamp, value])
                    csv_handle.file.flush()

                    # Log data to the database
                    data_array = np.array([value])
                    success_cloud = db_cloud.log(table=table_name, channels=data_array)
                    if not success_cloud:
                        logging.warning(f"Failed to log detector data to table '{table_name}'.")
                        db_cloud = reconnect_db()

            except Exception as e:
                logging.error(f"Error during data acquisition: {e}")
                db_cloud = reconnect_db()

            # Wait for 5 seconds before next reading
            time.sleep(5)

    except KeyboardInterrupt:
        logging.info("Keyboard interrupt received. Stopping data acquisition.")
    finally:
        csv_handle.file.close()
        ser.close()
        if db_cloud:
            db_cloud.close()
        logging.info("Resources cleaned up and program terminated.")

if __name__ == "__main__":
    main()
