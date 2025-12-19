import argparse
import os
import time
from datetime import datetime
import subprocess

def _parse_args():
    parser = argparse.ArgumentParser(description="Periodically upload a CAEN folder to Google Drive using rclone.")
    parser.add_argument(
        "source_folder",
        nargs="?",
        help="Source folder path (e.g., /home/cf/caen-master-project-1). If omitted, you'll be prompted.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Copy interval in seconds (default: 60). If omitted and source_folder is provided, starts immediately.",
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    if args.source_folder:
        source_folder = args.source_folder.strip()
    else:
        source_folder = input("Enter the source folder path (e.g., /home/cf/caen-master-project-1): ").strip()

    if not source_folder:
        print("Source folder is required. Exiting...")
        raise SystemExit(1)

    if args.interval is not None:
        check_interval = args.interval
        if check_interval <= 0:
            print("--interval must be a positive integer. Exiting...")
            raise SystemExit(1)
    else:
        if args.source_folder:
            check_interval = 60
        else:
            interval_input = input("Enter the copy interval in seconds (default is 60): ").strip()
            check_interval = int(interval_input) if interval_input and interval_input.isdigit() else 60

    # Get computer name from environment variable or user input
    computer_name = os.getenv("COMPUTER_NAME")
    if not computer_name:
        print("COMPUTER_NAME environment variable not set.")
        print("Consider running 'bash ida-devices/scripts/set-computer-name.sh' to set it.")
        computer_name = input("Enter the computer name: ").strip()
        if not computer_name:
            print("Computer name is required. Exiting...")
            raise SystemExit(1)

    # Extract the last part of the source folder path
    last_bit = os.path.basename(os.path.normpath(source_folder))

    # Construct the destination folder path
    destination_folder = f"googledrive:Computers/{computer_name}/{last_bit}"

    while True:
        # Print the current time in a human-readable format
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] Running Rclone copy from {source_folder} to {destination_folder}...")

        # Run the Rclone command and capture the output
        rclone_command = f'rclone copy -v --progress "{source_folder}" "{destination_folder}" --exclude "*.root"'
        try:
            result = subprocess.run(rclone_command, shell=True, capture_output=False, text=True)
            if result.returncode != 0:
                print(
                    f"[{current_time}] Rclone copy failed. Please ensure the 'googledrive' Rclone config is set up correctly."
                )
                print(f"Error output: {result.stderr}")
            else:
                print(f"[{current_time}] Rclone copy completed. Waiting {check_interval} seconds before next run...\n")
        except Exception as e:
            print(
                f"[{current_time}] Rclone copy failed. Please ensure the 'googledrive' Rclone config is set up correctly."
            )
            print(f"Exception: {str(e)}")

        # Wait for the specified interval before running again
        time.sleep(check_interval)


if __name__ == "__main__":
    main()
