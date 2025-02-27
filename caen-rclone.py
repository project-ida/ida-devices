import os
import time
import argparse
from datetime import datetime

# Argument parser to take command-line inputs
parser = argparse.ArgumentParser(description="Automate Rclone sync between source and destination.")
parser.add_argument("--source", required=True, help="Path to the source folder")
parser.add_argument("--destination", required=True, help="Destination in Rclone format (e.g., googledrive:folder)")
parser.add_argument("--interval", type=int, default=60, help="Sync interval in seconds (default: 60)")

args = parser.parse_args()

# Paths and Rclone command template
source_folder = args.source
destination_folder = args.destination
check_interval = args.interval

while True:
    # Print the current time in a human-readable format
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{current_time}] Running Rclone sync from {source_folder} to {destination_folder}...")

    # Run the Rclone command and print the output
    rclone_command = f"rclone sync -v --progress {source_folder} {destination_folder} --exclude '*.root'"
    os.system(rclone_command)
    
    # Print a message indicating the sync is complete
    print(f"[{current_time}] Rclone sync completed. Waiting {check_interval} seconds before next run...\n")

    # Wait for the specified interval before running again
    time.sleep(check_interval)
