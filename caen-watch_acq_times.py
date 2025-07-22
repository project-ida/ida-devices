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

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Determine local host name (or from env var)
COMPUTER_NAME = os.getenv("COMPUTER_NAME") or socket.gethostname()

# Ensure libs/ is on the path
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import our sheet utilities from libs/
from libs.google_sheet_utils import (
    load_run_names,
    find_run_row,
    append_run,
    update_setup_time,
    update_end_time,
    update_pc_name,
    update_digitizer,
)
# Import our settings_extras from libs/
from libs.settings_extras import extract_digitizer_info

# Import our settings_validator from libs/
from libs.settings_validator import report_parameter_diffs
from pathlib import Path

# Name of the folder (under your watch root) that holds reference XMLs:
CONFIG_REF_DIR_NAME = 'CONFIG'

# -------------------------------------------------------------------
# Helper functions for scanning and processing run folders
# -------------------------------------------------------------------

def clear_line():
    sys.stdout.write('\r' + ' ' * 80 + '\r')
    sys.stdout.flush()

def is_settings_file(path: str) -> bool:
    return os.path.basename(path).lower() == 'settings.xml'

def is_end_file(path: str) -> bool:
    """
    We assume the .txt file is named <run_name>.txt
    """
    folder = os.path.dirname(path)
    name = os.path.basename(folder)
    return os.path.basename(path).lower() == f'{name.lower()}.txt'

def estimate_start(path: str) -> datetime | None:
    try:
        return datetime.fromtimestamp(os.path.getmtime(path))
    except Exception:
        return None

def estimate_end(path: str) -> datetime | None:
    try:
        return datetime.fromtimestamp(os.path.getmtime(path))
    except Exception:
        return None

def initial_scan(root_folder: str):
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
    def __init__(self, watch_folder: str):
        super().__init__()
        self.watch_folder = watch_folder

    def on_created(self, event):
        if event.is_directory:
            return

        # Skip hidden files/folders if needed…
        name = os.path.basename(event.src_path)
        if name.startswith('.'):
            return

        path       = event.src_path
        run_folder = os.path.dirname(path)
        run_name   = os.path.basename(run_folder)

        # START event
        if is_settings_file(path):
            start_dt = estimate_start(path)
            if start_dt:
                clear_line()
                print(f"START {run_name}: {start_dt:%Y-%m-%d %H:%M:%S}")
                row = find_run_row(run_name)
                if row is None:
                    append_run(run_name, start_dt, None)
                else:
                    update_setup_time(run_name, start_dt)
                update_pc_name(run_name, COMPUTER_NAME)
                digitizer = extract_digitizer_info(path)
                if digitizer:
                    update_digitizer(run_name, digitizer)
                config_dir = Path(self.watch_folder) / CONFIG_REF_DIR_NAME
                report_parameter_diffs(path, str(config_dir))

        # STOP event
        elif is_end_file(path):
            stop_dt = estimate_end(path)
            if stop_dt:
                clear_line()
                print(f"STOP  {run_name}: {stop_dt:%Y-%m-%d %H:%M:%S}")
                row = find_run_row(run_name)
                if row is None:
                    # In case we missed START
                    append_run(run_name, None, stop_dt)
                else:
                    update_end_time(run_name, stop_dt)
                update_pc_name(run_name, COMPUTER_NAME)
                digitizer = extract_digitizer_info(path)
                if digitizer:
                    update_digitizer(run_name, digitizer)

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    # configure logging early
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

    # 1) Initial directory scan
    runs = initial_scan(watch_folder)

    # 2) Sync initial scan into the sheet
    print("\n=== Initial Scan & Sheet Sync ===")
    for start_dt, stop_dt, run_name, run_folder in runs:
        print(f"\nSYNC  {run_name}: START={start_dt:%Y-%m-%d %H:%M:%S}  STOP={stop_dt or '(none)'}")
        row = find_run_row(run_name)

        if row is None:
            append_run(run_name, start_dt, stop_dt)
        else:
            update_setup_time(run_name, start_dt)
            if stop_dt:
                update_end_time(run_name, stop_dt)

        # populate DAQ_PC column if blank
        update_pc_name(run_name, COMPUTER_NAME)
        settings_path = os.path.join(run_folder, 'settings.xml')
        config_dir    = Path(watch_folder) / CONFIG_REF_DIR_NAME
        report_parameter_diffs(str(settings_path), str(config_dir))
        digitizer = extract_digitizer_info(settings_path)
        if digitizer:
            update_digitizer(run_name, digitizer)

    # 3) Start live monitoring
    handler  = DAQHandler(watch_folder)
    observer = Observer()
    observer.schedule(handler, path=watch_folder, recursive=True)
    observer.start()
    print(f"\nMonitoring '{watch_folder}' for new START/STOP events...")

    # 4) Simple spinner to show liveness
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

if __name__ == '__main__':
    main()
