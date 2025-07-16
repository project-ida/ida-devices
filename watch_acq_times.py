#!/usr/bin/env python3
"""
watch_acq_times.py

Monitors a DAQ directory (and subfolders) to detect acquisition runs by:
- Printing the acquisition START time when the first .root file appears in a run folder (based on settings.xml mtime)
- Printing the acquisition END time when a .txt file appears in that run folder (based on its ctime or mtime)
- On startup, scans existing run folders so you still see START and STOP for runs in progress or completed before the script began.
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Track which runs we've reported start/stop for
reported = {}  # {run_folder: {'start': bool, 'stop': bool}}


def get_settings_mtime(run_folder: str) -> datetime | None:
    """
    Return the last-modified time of settings.xml in the run_folder.
    """
    settings_path = os.path.join(run_folder, 'settings.xml')
    if not os.path.isfile(settings_path):
        logging.warning("settings.xml not found in %s", run_folder)
        return None
    try:
        return datetime.fromtimestamp(os.path.getmtime(settings_path))
    except Exception:
        logging.exception("Error reading settings.xml in %s", run_folder)
        return None


def get_txt_time(run_folder: str) -> datetime | None:
    """
    Find any .txt file in run_folder, return its newest ctime (or mtime) as the stop time.
    """
    try:
        times = []
        for fname in os.listdir(run_folder):
            if fname.lower().endswith('.txt'):
                path = os.path.join(run_folder, fname)
                try:
                    t = os.path.getctime(path)
                except Exception:
                    t = os.path.getmtime(path)
                times.append(t)
        if not times:
            return None
        return datetime.fromtimestamp(max(times))
    except Exception:
        logging.exception("Error scanning .txt files in %s", run_folder)
        return None


class DAQHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        path = event.src_path
        # Determine run folder as parent of folder containing the file
        folder = os.path.dirname(path)         # e.g. /.../DAQ/SOME_NAME/RAW
        run_folder = os.path.dirname(folder)   # e.g. /.../DAQ/SOME_NAME

        # Ensure we have an entry
        if run_folder not in reported:
            reported[run_folder] = {'start': False, 'stop': False}

        # First .root => START
        if path.lower().endswith('.root') and not reported[run_folder]['start']:
            start_dt = get_settings_mtime(run_folder)
            if start_dt:
                print(f"START {os.path.basename(run_folder)}: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            reported[run_folder]['start'] = True

        # First .txt => STOP
        if path.lower().endswith('.txt') and not reported[run_folder]['stop']:
            stop_dt = get_txt_time(run_folder)
            if stop_dt:
                print(f"STOP  {os.path.basename(run_folder)}: {stop_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            reported[run_folder]['stop'] = True


def initial_scan(root_folder: str):
    """
    On startup, collect all runs (with settings.xml) and their
    START/STOP times, then print them sorted by START time.
    """
    runs = []  # will hold tuples: (start_dt, stop_dt, run_name)

    for dirpath, dirnames, filenames in os.walk(root_folder):
        if 'settings.xml' not in filenames:
            continue
        run_folder = dirpath
        raw_folder = os.path.join(run_folder, 'RAW')
        if not (os.path.isdir(raw_folder) and 
                any(f.lower().endswith('.root') for f in os.listdir(raw_folder))):
            continue

        # get start and stop
        start_dt = get_settings_mtime(run_folder)
        stop_dt  = get_txt_time(run_folder)

        # only record if we at least have a start time
        if start_dt:
            run_name = os.path.basename(run_folder)
            runs.append((start_dt, stop_dt, run_name))
            # mark as reported so live events won't duplicate
            reported[run_folder] = {'start': True, 'stop': stop_dt is not None}

    # sort by start time
    runs.sort(key=lambda t: t[0])

    # print in order
    for start_dt, stop_dt, run_name in runs:
        print(f"START {run_name}: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        if stop_dt:
            print(f"STOP  {run_name}: {stop_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"STOP  {run_name}: (no .txt found)")



def main():
    parser = argparse.ArgumentParser(
        description="Watch DAQ folders and print acquisition start/end times"
    )
    parser.add_argument('watch_folder', help='Root DAQ directory to monitor')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s')

    if not os.path.isdir(args.watch_folder):
        logging.error("Invalid directory: %s", args.watch_folder)
        sys.exit(1)

    # Perform initial scan so we capture runs already started or finished
    initial_scan(args.watch_folder)

    handler = DAQHandler()
    observer = Observer()
    observer.schedule(handler, path=args.watch_folder, recursive=True)
    observer.start()
    print(f"Monitoring '{args.watch_folder}' for acquisition runs...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping monitor.")
        observer.stop()
    observer.join()


if __name__ == '__main__':
    main()
