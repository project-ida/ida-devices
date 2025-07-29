#!/usr/bin/env python3
"""
watch_acq_times.py

Monitors a DAQ directory (and subfolders) to detect acquisition runs by:
  - Printing the acquisition START time when a new settings.xml appears
  - Printing the acquisition END time when a dedicated .txt file appears
  - On startup, scans existing run folders and syncs them into Google Sheets
  - After initial sync, listens for new START/STOP events and updates the sheet
"""

import os
import sys
import time
import logging
import argparse
import socket
from datetime import datetime
from typing import Optional

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Determine local host name (or from env var)
COMPUTER_NAME = os.getenv("COMPUTER_NAME") or socket.gethostname()

# Ensure libs/ is on the path
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import our sheet utilities from libs/
from libs.google_sheet_utils import GoogleSheet

# Import our settings_extras from libs/
from libs.settings_extras import extract_digitizer_info, find_matching_config_files

# Import our settings_validator from libs/
from libs.settings_validator import report_parameter_diffs
from pathlib import Path

# Name of the folder (under your watch root) that holds reference XMLs:
CONFIG_REF_DIR_NAME = 'CONFIG'

# -------------------------------------------------------------------
# Helper functions for scanning and processing run folders
# -------------------------------------------------------------------

def clear_line():
    """Clears the current terminal line."""
    sys.stdout.write('\r' + ' ' * 80 + '\r')
    sys.stdout.flush()

def is_settings_file(path: str) -> bool:
    """Returns True if the given path is a settings.xml file."""
    return os.path.basename(path).lower() == 'settings.xml'

def is_end_file(path: str) -> bool:
    """
    Returns True if the given path is the expected .txt end file for a run.
    We assume the .txt file is named <run_name>.txt
    """
    folder = os.path.dirname(path)
    name = os.path.basename(folder)
    return os.path.basename(path).lower() == f'{name.lower()}.txt'

def estimate_start(path: str) -> datetime | None:
    """Estimates the start time from the file's modification time."""
    try:
        return datetime.fromtimestamp(os.path.getmtime(path))
    except Exception:
        return None

def estimate_end(path: str) -> datetime | None:
    """Estimates the end time from the file's modification time."""
    try:
        return datetime.fromtimestamp(os.path.getmtime(path))
    except Exception:
        return None

def initial_scan(root_folder: str):
    """
    Scans the root_folder for run directories and returns a list of runs.
    Each run is a tuple: (start_dt, stop_dt, run_name, run_folder)
    """
    runs = []
    for dirpath, dirnames, filenames in os.walk(root_folder):
        # 1a) Prevent recursing into hidden subfolders
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        # 1b) Skip this folder if it’s hidden
        if os.path.basename(dirpath).startswith('.'):
            continue

        if 'settings.xml' not in filenames:
            continue

        run_name     = os.path.basename(dirpath)
        settings_pth = os.path.join(dirpath, 'settings.xml')
        txt_pth      = os.path.join(dirpath, f'{run_name}.txt')
        start_dt     = estimate_start(settings_pth)
        stop_dt      = estimate_end(txt_pth) if os.path.exists(txt_pth) else None

        if start_dt:
            runs.append((start_dt, stop_dt, run_name, dirpath))

    runs.sort(key=lambda t: t[0])
    return runs

# -------------------------------------------------------------------
# Event handler
# -------------------------------------------------------------------

class DAQHandler(FileSystemEventHandler):
    """
    Handles file system events for the DAQ directory, updating the Google Sheet as needed.
    """
    def __init__(self, watch_folder: str, sheet):
        super().__init__()
        self.watch_folder = watch_folder
        self.sheet = sheet

    def on_created(self, event):
        """
        Handles the creation of new files in the watched directory.
        Updates the Google Sheet for new run start/end events.
        """
        if event.is_directory:
            return
        # Skip hidden files/folders if needed…
        name = os.path.basename(event.src_path)
        if name.startswith('.'):
            return
        path = event.src_path
        run_folder = os.path.dirname(path)
        run_name = os.path.basename(run_folder)

        if is_settings_file(path):
            start_dt = estimate_start(path)
            if start_dt:
                clear_line()
                logging.info(f"START {run_name}: {start_dt:%Y-%m-%d %H:%M:%S}")
                config_dir = Path(self.watch_folder) / CONFIG_REF_DIR_NAME
                process_run_folder(
                    run_name=run_name,
                    run_folder=run_folder,
                    sheet=self.sheet,
                    config_dir=config_dir,
                    start_dt=start_dt
                )

        elif is_end_file(path):
            stop_dt = estimate_end(path)
            if stop_dt:
                clear_line()
                logging.info(f"STOP  {run_name}: {stop_dt:%Y-%m-%d %H:%M:%S}")
                row = self.sheet.find_run_row(run_name)
                if row is None:
                    # In case we missed START
                    self.sheet.append_run(run_name, None, stop_dt)
                else:
                    self.sheet.update_field_if_blank(run_name, stop_dt, self.sheet.COL_END)
                self.sheet.update_field_if_blank(run_name, COMPUTER_NAME, self.sheet.COL_DAQ_PC)
                digitizer = extract_digitizer_info(path)
                if digitizer:
                    self.sheet.update_field_if_blank(run_name, digitizer, self.sheet.COL_DIGITIZER)

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    """
    Main entry point for the DAQ directory watcher and Google Sheets sync tool.
    Parses arguments, performs initial scan, syncs to sheet, and starts live monitoring.
    """
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    parser = argparse.ArgumentParser(
        description="Watch a DAQ directory and mirror START/STOP times into Google Sheets"
    )
    # make the watch_folder argument optional
    parser.add_argument('watch_folder', nargs='?', help='Root DAQ directory to monitor')
    args = parser.parse_args()

    # If not given on the CLI, prompt the user until a valid directory is entered
    watch_folder = args.watch_folder
    if not watch_folder:
        try:
            while True:
                watch_folder = input("Enter the DAQ folder to monitor: ").strip()
                if os.path.isdir(watch_folder):
                    break
                print(f"❌  '{watch_folder}' is not a valid directory. Please try again.")
        except (EOFError, KeyboardInterrupt):
            print("\nNo folder provided—exiting.")
            sys.exit(1)

    # final check
    if not os.path.isdir(watch_folder):
        logging.error("Invalid directory: %s", watch_folder)
        sys.exit(1)

    sheet = GoogleSheet()

    # Initial directory scan
    runs = initial_scan(watch_folder)

    # Sync initial scan into the sheet
    logging.info("=== Initial Scan & Sheet Sync ===")
    config_dir = Path(watch_folder) / CONFIG_REF_DIR_NAME
    for start_dt, stop_dt, run_name, run_folder in runs:
        logging.info(
            f"SYNC  {run_name}: START={start_dt:%Y-%m-%d %H:%M:%S}  STOP={stop_dt or '(none)'}"
        )
        process_run_folder(
            run_name=run_name,
            run_folder=run_folder,
            sheet=sheet,
            config_dir=config_dir,
            start_dt=start_dt,
            stop_dt=stop_dt
        )

    # Start live monitoring
    handler  = DAQHandler(watch_folder, sheet)
    observer = Observer()
    observer.schedule(handler, path=watch_folder, recursive=True)
    observer.start()
    print(f"\nMonitoring '{watch_folder}' for new START/STOP events...")

    # Simple spinner to show liveness
    spinner = ['|', '/', '-', '\\']
    idx     = 0
    try:
        while True:
            sys.stdout.write(f"\r{spinner[idx % len(spinner)]} watching…")
            sys.stdout.flush()
            idx += 1
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("\nStopping monitor.")
        observer.stop()
    observer.join()

def process_run_folder(
    run_name: str,
    run_folder: str,
    sheet: GoogleSheet,
    config_dir: Path,
    start_dt: Optional[datetime] = None,
    stop_dt: Optional[datetime] = None
) -> None:
    """
    Process a run folder: update the Google Sheet with run info, digitizer info,
    config matches, and parameter diffs.

    Parameters:
    run_name (str): Name of the run.
    run_folder (str): Path to the run folder.
    sheet (GoogleSheet): GoogleSheet instance for updating the sheet.
    config_dir (Path): Path to the config directory.
    start_dt (Optional[datetime]): Start time of the run.
    stop_dt (Optional[datetime]): Stop time of the run.
    """
    settings_path = os.path.join(run_folder, 'settings.xml')
    row = sheet.find_run_row(run_name)
    if row is None:
        sheet.append_run(run_name, start_dt, stop_dt)
    else:
        if start_dt:
            sheet.update_field_if_blank(run_name, start_dt, sheet.COL_SETUP)
        if stop_dt:
            sheet.update_field_if_blank(run_name, stop_dt, sheet.COL_END)
    sheet.update_field_if_blank(run_name, COMPUTER_NAME, sheet.COL_DAQ_PC)
    digitizer = extract_digitizer_info(settings_path)
    if digitizer:
        sheet.update_field_if_blank(run_name, digitizer, sheet.COL_DIGITIZER)
    report_parameter_diffs(settings_path, str(config_dir))
    matches = find_matching_config_files(settings_path, str(config_dir))
    config_files = ','.join(matches)
    if matches:
        sheet.update_field_if_blank(run_name, config_files, sheet.COL_CONFIG)
    else:
        logging.warning(f"⚠️  No matching config files found for {run_name}")

if __name__ == '__main__':
    main()
