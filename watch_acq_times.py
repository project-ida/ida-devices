#!/usr/bin/env python3
"""
watch_acq_times.py

Monitors a DAQ directory (and subfolders) to detect acquisition runs by:
- Printing the acquisition START time when a new settings.xml appears in a run folder (based on mtime)
- Printing the acquisition END time when a dedicated .txt file (named after the run folder) appears in that same run folder (based on mtime)
- On startup, scans existing run folders so you still see START and STOP for runs in progress or completed before the script began.
- Displays a live spinner in the terminal to indicate the watcher is running.
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


def get_file_mtime(path: str) -> datetime | None:
    """
    Return the last-modified time (mtime) of the file at path as a datetime,
    or None if the file does not exist or an error occurs.
    """
    if not os.path.isfile(path):
        return None
    try:
        return datetime.fromtimestamp(os.path.getmtime(path))
    except Exception:
        logging.exception("Error getting mtime for %s", path)
        return None


def get_settings_mtime(run_folder: str) -> datetime | None:
    """
    Return the mtime of settings.xml in the run_folder.
    """
    return get_file_mtime(os.path.join(run_folder, 'settings.xml'))


def get_txt_mtime(run_folder: str, run_name: str) -> datetime | None:
    """
    Return the mtime of the single .txt file named after the run (run_name + '.txt') in run_folder,
    or None if it does not exist or an error occurs.
    """
    return get_file_mtime(os.path.join(run_folder, f"{run_name}.txt"))


class DAQHandler(FileSystemEventHandler):
    def on_created(self, event):
        # ignore directory events
        if event.is_directory:
            return

        path = event.src_path
        filename = os.path.basename(path)
        # Determine run folder as directory containing the file
        run_folder = os.path.dirname(path)
        run_name = os.path.basename(run_folder)

        # Initialize status for this run if needed
        if run_folder not in reported:
            reported[run_folder] = {'start': False, 'stop': False}

        # settings.xml => START
        if filename.lower() == 'settings.xml' and not reported[run_folder]['start']:
            start_dt = get_settings_mtime(run_folder)
            if start_dt:
                print(f"\nSTART {run_name}: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            reported[run_folder]['start'] = True

        # dedicated .txt => STOP
        if filename.lower() == f'{run_name.lower()}.txt' and not reported[run_folder]['stop']:
            stop_dt = get_txt_mtime(run_folder, run_name)
            if stop_dt:
                print(f"\nSTOP  {run_name}: {stop_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            reported[run_folder]['stop'] = True


def initial_scan(root_folder: str):
    """
    On startup, collect all runs (directories containing settings.xml) and their
    START/STOP times, then print them sorted by START time.
    """
    runs = []  # list of (start_dt, stop_dt, run_name, run_folder)

    for dirpath, dirnames, filenames in os.walk(root_folder):
        if 'settings.xml' not in filenames:
            continue
        run_folder = dirpath
        run_name = os.path.basename(run_folder)
        start_dt = get_settings_mtime(run_folder)
        stop_dt = get_txt_mtime(run_folder, run_name)
        if start_dt:
            runs.append((start_dt, stop_dt, run_name, run_folder))

    # sort by start time
    runs.sort(key=lambda t: t[0])

    # print and mark reported
    for start_dt, stop_dt, run_name, run_folder in runs:
        print(f"START {run_name}: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        if stop_dt:
            print(f"STOP  {run_name}: {stop_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"STOP  {run_name}: (no {run_name}.txt found)")
        reported[run_folder] = {'start': True, 'stop': stop_dt is not None}


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

    # Initial scan to capture prior runs
    initial_scan(args.watch_folder)

    handler = DAQHandler()
    observer = Observer()
    observer.schedule(handler, path=args.watch_folder, recursive=True)
    observer.start()

    # Spinner setup
    spinner = ['|', '/', '-', '\\']
    idx = 0
    try:
        while True:
            sys.stdout.write(f"\r{spinner[idx % len(spinner)]} Watching {args.watch_folder}")
            sys.stdout.flush()
            idx += 1
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("\nStopping monitor.")
        observer.stop()
    observer.join()


if __name__ == '__main__':
    main()
