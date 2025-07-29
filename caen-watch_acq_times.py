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
from typing import Optional, List, Tuple, Dict, Any

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path

# Determine local host name (or from env var)
COMPUTER_NAME = os.getenv("COMPUTER_NAME") or socket.gethostname()

# Ensure libs/ is on the path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import our sheet utilities from libs/
from libs.google_sheet_utils import GoogleSheet

# Import our settings_extras from libs/
from libs.settings_extras import extract_digitizer_info, find_matching_config_files

# Import our settings_validator from libs/
from libs.settings_validator import report_parameter_diffs

# Magic string constants
SETTINGS_FILENAME = 'settings.xml'
END_FILE_SUFFIX = '.txt'
CONFIG_REF_DIR_NAME = 'CONFIG'

def clear_line() -> None:
    """
    Clear the current terminal line in the console.
    Used for updating status output in place.
    """
    sys.stdout.write('\r' + ' ' * 80 + '\r')
    sys.stdout.flush()

def is_settings_file(path: Path) -> bool:
    """
    Check if the given path is a settings.xml file.

    Parameters:
    path (Path): The file path to check.

    Returns:
    bool: True if the file is named 'settings.xml', False otherwise.
    """
    return path.name.lower() == SETTINGS_FILENAME

def is_end_file(path: Path) -> bool:
    """
    Check if the given path is the expected END_FILE_SUFFIX end file for a run.
    The END_FILE_SUFFIX file must be named <run_folder>.txt (case-insensitive).

    Parameters:
    path (Path): The file path to check.

    Returns:
    bool: True if the file is the expected end file, False otherwise.
    """
    return (
        path.suffix.lower() == END_FILE_SUFFIX
        and path.stem.lower() == path.parent.name.lower()
    )

def estimate_start(path: Path) -> Optional[datetime]:
    """
    Estimate the start time from the file's modification time.

    Parameters:
    path (Path): The file path.

    Returns:
    Optional[datetime]: The modification time as a datetime object, or None if unavailable.
    """
    try:
        return datetime.fromtimestamp(path.stat().st_mtime)
    except Exception:
        return None

def estimate_end(path: Path) -> Optional[datetime]:
    """
    Estimate the end time from the file's modification time.

    Parameters:
    path (Path): The file path.

    Returns:
    Optional[datetime]: The modification time as a datetime object, or None if unavailable.
    """
    try:
        return datetime.fromtimestamp(path.stat().st_mtime)
    except Exception:
        return None


def ensure_run_row_exists(
    sheet: 'GoogleSheet',
    run_name: str,
    start_dt: Optional[datetime],
    stop_dt: Optional[datetime]
) -> None:
    """
    Ensure a row for the run exists in the sheet, appending if necessary.

    Parameters:
    sheet (GoogleSheet): The GoogleSheet instance.
    run_name (str): Name of the run.
    start_dt (Optional[datetime]): Start time of the run.
    stop_dt (Optional[datetime]): Stop time of the run.
    """
    row = sheet.find_run_row(run_name)
    if row is None:
        sheet.append_run(run_name, start_dt, stop_dt)

def format_config_files(matches: List[str]) -> str:
    """
    Format the list of matching config files as a comma-separated string.

    Parameters:
    matches (List[str]): List of matching config filenames.

    Returns:
    str: Comma-separated config filenames, or empty string if none.
    """
    return ','.join(matches) if matches else ''

def prepare_update_values(
    sheet: 'GoogleSheet',
    start_dt: Optional[datetime],
    stop_dt: Optional[datetime],
    digitizer: Optional[str],
    config_files: str
) -> Dict[int, Any]:
    """
    Prepare the dictionary of values to update in the sheet.

    Parameters:
    sheet (GoogleSheet): The GoogleSheet instance.
    start_dt (Optional[datetime]): Start time of the run.
    stop_dt (Optional[datetime]): Stop time of the run.
    digitizer (Optional[str]): Digitizer info string.
    config_files (str): Comma-separated config filenames.

    Returns:
    Dict[int, Any]: Mapping of column indices to values.
    """
    return {
        sheet.COL_SETUP: start_dt,
        sheet.COL_END: stop_dt,
        sheet.COL_DAQ_PC: COMPUTER_NAME,
        sheet.COL_DIGITIZER: digitizer,
        sheet.COL_CONFIG: config_files
    }

def warn_if_no_config_matches(matches: List[str], run_name: str) -> None:
    """
    Log a warning if no matching config files are found.

    Parameters:
    matches (List[str]): List of matching config filenames.
    run_name (str): Name of the run.
    """
    if not matches:
        logging.warning(f"⚠️  No matching config files found for {run_name}")

def process_run_folder(
    run_name: str,
    run_folder: Path,
    sheet: 'GoogleSheet',
    config_dir: Path,
    start_dt: Optional[datetime] = None,
    stop_dt: Optional[datetime] = None
) -> None:
    """
    Process a run folder and atomically update the Google Sheet with all run info.

    Parameters:
    run_name (str): Name of the run.
    run_folder (Path): Path to the run folder.
    sheet (GoogleSheet): GoogleSheet instance for updating the sheet.
    config_dir (Path): Path to the config directory.
    start_dt (Optional[datetime]): Start time of the run.
    stop_dt (Optional[datetime]): Stop time of the run.
    """
    settings_path = run_folder / SETTINGS_FILENAME
    ensure_run_row_exists(sheet, run_name, start_dt, stop_dt)
    digitizer = extract_digitizer_info(str(settings_path))
    matches = find_matching_config_files(str(settings_path), str(config_dir))
    config_files = format_config_files(matches)
    values = prepare_update_values(sheet, start_dt, stop_dt, digitizer, config_files)
    sheet.update_run_row(run_name, values)
    report_parameter_diffs(str(settings_path), str(config_dir))
    warn_if_no_config_matches(matches, run_name)

def initial_scan(root_folder: Path) -> List[Tuple[datetime, Optional[datetime], str, Path]]:
    """
    Scan the root_folder for run directories and return a list of runs.
    Each run is a tuple: (start_dt, stop_dt, run_name, run_folder).

    Parameters:
    root_folder (Path): The root directory to scan.

    Returns:
    List[Tuple[datetime, Optional[datetime], str, Path]]: List of detected runs.
    """
    runs = []
    for dirpath, dirnames, filenames in os.walk(root_folder):
        dirpath = Path(dirpath)
        # Prevent recursing into hidden subfolders
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        # Skip this folder if it’s hidden
        if dirpath.name.startswith('.'):
            continue

        if SETTINGS_FILENAME not in filenames:
            continue

        run_name = dirpath.name
        settings_pth = dirpath / SETTINGS_FILENAME
        txt_pth = dirpath / f'{run_name}{END_FILE_SUFFIX}'
        start_dt = estimate_start(settings_pth)
        stop_dt = estimate_end(txt_pth) if txt_pth.exists() else None

        if start_dt:
            runs.append((start_dt, stop_dt, run_name, dirpath))

    runs.sort(key=lambda t: t[0])
    return runs

# -------------------------------------------------------------------
# Event handler
# -------------------------------------------------------------------

class DAQHandler(FileSystemEventHandler):
    """
    File system event handler for the DAQ directory.
    Updates the Google Sheet when new run start/end events are detected.
    """
    def __init__(self, watch_folder: Path, sheet: 'GoogleSheet', config_dir: Path) -> None:
        """
        Initialize the DAQHandler.

        Parameters:
        watch_folder (Path): The root folder being watched.
        sheet (GoogleSheet): The GoogleSheet instance.
        config_dir (Path): Path to the config directory.
        """
        super().__init__()
        self.watch_folder = watch_folder
        self.sheet = sheet
        self.config_dir = config_dir

    def on_created(self, event: Any) -> None:
        """
        Handles the creation of new files in the watched directory.
        Updates the Google Sheet for new run start/end events.

        Parameters:
        event: The file system event.
        """
        if event.is_directory:
            return
        path = Path(event.src_path)
        name = path.name
        if name.startswith('.'):
            return
        run_folder = path.parent
        run_name = run_folder.name

        if is_settings_file(path):
            start_dt = estimate_start(path)
            if start_dt:
                clear_line()
                logging.info(f"START {run_name}: {start_dt:%Y-%m-%d %H:%M:%S}")
                process_run_folder(
                    run_name=run_name,
                    run_folder=run_folder,
                    sheet=self.sheet,
                    config_dir=self.config_dir,
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
                    # Correct: get digitizer info from settings.xml, not the .txt file
                    settings_path = run_folder / SETTINGS_FILENAME
                    digitizer = extract_digitizer_info(str(settings_path))
                    values = {
                        self.sheet.COL_END: stop_dt,
                        self.sheet.COL_DAQ_PC: COMPUTER_NAME,
                        self.sheet.COL_DIGITIZER: digitizer
                    }
                    self.sheet.update_run_row(run_name, values)

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------

def main() -> None:
    """
    Main entry point for the DAQ directory watcher and Google Sheets sync tool.
    Parses arguments, performs initial scan, syncs to sheet, and starts live monitoring.
    """
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    parser = argparse.ArgumentParser(
        description="Watch a DAQ directory and mirror START/STOP times into Google Sheets"
    )
    parser.add_argument('watch_folder', nargs='?', help='Root DAQ directory to monitor')
    args = parser.parse_args()

    # If not given on the CLI, prompt the user until a valid directory is entered
    watch_folder = args.watch_folder
    if not watch_folder:
        try:
            while True:
                watch_folder = input("Enter the DAQ folder to monitor: ").strip()
                if Path(watch_folder).is_dir():
                    break
                logging.error(f"❌  '{watch_folder}' is not a valid directory. Please try again.")
        except (EOFError, KeyboardInterrupt):
            logging.info("No folder provided—exiting.")
            sys.exit(1)

    # final check
    watch_folder_path = Path(watch_folder)
    if not watch_folder_path.is_dir():
        logging.error("Invalid directory: %s", watch_folder)
        sys.exit(1)

    sheet = GoogleSheet()

    # Construct config_dir once here
    config_dir = watch_folder_path / CONFIG_REF_DIR_NAME

    # Initial directory scan
    runs = initial_scan(watch_folder_path)

    # Sync initial scan into the sheet
    logging.info("=== Initial Scan & Sheet Sync ===")
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
    handler = DAQHandler(watch_folder_path, sheet, config_dir)
    observer = Observer()
    observer.schedule(handler, path=str(watch_folder_path), recursive=True)
    observer.start()
    logging.info(f"\nMonitoring '{watch_folder_path}' for new START/STOP events...")

    # Simple spinner to show liveness (keep using print for spinner)
    spinner = ['|', '/', '-', '\\']
    idx = 0
    try:
        while True:
            sys.stdout.write(f"\r{spinner[idx % len(spinner)]} watching…")
            sys.stdout.flush()
            idx += 1
            time.sleep(0.2)
    except KeyboardInterrupt:
        logging.info("Stopping monitor.")
        observer.stop()
    observer.join()

if __name__ == '__main__':
    main()
