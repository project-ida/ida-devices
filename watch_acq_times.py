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
from datetime import datetime

# Watchdog for filesystem events
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Make sure our `libs/` folder is on the import path:
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import our sheet utilities from libs/
from libs.sheet_utils import (
    load_run_names,
    find_run_row,
    append_run,
    update_setup_time,
    update_end_time,
)

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

def initial_scan(root_folder):
    runs = []
    for dirpath, dirnames, filenames in os.walk(root_folder):
        # 1a) Prevent os.walk from even recursing into hidden subfolders:
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]

        # 1b) Skip this folder entirely if it *is* hidden:
        run_name = os.path.basename(dirpath)
        if run_name.startswith('.'):
            continue

        if 'settings.xml' not in filenames:
            continue
        run_name = os.path.basename(dirpath)
        settings_path = os.path.join(dirpath, 'settings.xml')
        txt_path = os.path.join(dirpath, f'{run_name}.txt')
        start_dt = estimate_start(settings_path)
        stop_dt  = estimate_end(txt_path) if os.path.exists(txt_path) else None
        if start_dt:
            runs.append((start_dt, stop_dt, run_name, dirpath))

    runs.sort(key=lambda t: t[0])
    return runs

# -------------------------------------------------------------------
# Event handler
# -------------------------------------------------------------------

class DAQHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        # Skip hidden files/folders if needed…
        name = os.path.basename(event.src_path)
        if name.startswith('.'):
            return
            
        path = event.src_path
        run_folder = os.path.dirname(path)
        run_name = os.path.basename(run_folder)

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

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Watch a DAQ directory and mirror START/STOP times into Google Sheets"
    )
    parser.add_argument('watch_folder', help='Root DAQ directory to monitor')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    if not os.path.isdir(args.watch_folder):
        logging.error("Invalid directory: %s", args.watch_folder)
        sys.exit(1)

    # 1) Initial directory scan
    runs = initial_scan(args.watch_folder)

    # 2) Sync initial scan into the sheet
    print("\n=== Initial Scan & Sheet Sync ===")
    for start_dt, stop_dt, run_name, _ in runs:
        print(f"SYNC  {run_name}: START={start_dt:%Y-%m-%d %H:%M:%S}  STOP={stop_dt or '(none)'}")
        row = find_run_row(run_name)
        if row is None:
            append_run(run_name, start_dt, stop_dt)
        else:
            update_setup_time(run_name, start_dt)
            if stop_dt:
                update_end_time(run_name, stop_dt)

    # 3) Start live monitoring
    handler = DAQHandler()
    observer = Observer()
    observer.schedule(handler, path=args.watch_folder, recursive=True)
    observer.start()
    print(f"\nMonitoring '{args.watch_folder}' for new START/STOP events...")

    # 4) Simple spinner to show liveness
    spinner = ['|','/','-','\\']
    idx = 0
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
