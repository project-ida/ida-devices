#!/usr/bin/env python3
"""
libs/google_sheet_utils.py

Google Sheets utilities for run monitoring using gspread.
This version does one initial fetch of the entire sheet into memory,
then services all read operations locally. Only append/update calls
hit the API thereafter.

Requirements:
  - A service‐account JSON key at GOOGLE_CREDS (default "credentials.json")
  - A config file at SHEET_CONFIG_PATH (default "config/google_sheet_config.json")
    defining:
      * spreadsheet_id
      * sheet_name
      * header_row
      * columns.id_header
      * columns.google_drive_data_folders_header
      * columns.setup_header
      * columns.end_header
      * columns.daq_laptop_name_header
"""

import os
import json
import time
from typing import List, Optional
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ———————————————————————————————
# Load JSON config
# ———————————————————————————————
CONFIG_FILE = os.getenv('SHEET_CONFIG_PATH', 'config/google_sheet_config.json')
try:
    with open(CONFIG_FILE, 'r') as cf:
        cfg = json.load(cf)
        SPREADSHEET_ID   = cfg['spreadsheet_id']
        SHEET_NAME       = cfg['sheet_name']
        HEADER_ROW       = cfg.get('header_row', 1)
        cols             = cfg['columns']
        ID_HEADER        = cols['id_header']
        RUN_HEADER       = cols['google_drive_data_folders_header']
        SETUP_HEADER     =  cols['setup_header']
        END_HEADER       = cols['end_header']
        DAQ_PC_HEADER    = cols['daq_laptop_name_header']
        DIGITIZER_HEADER = cols['digitizer_header']
        CONFIG_HEADER    = cols['compas_config_file_header']

except Exception as e:
    raise RuntimeError(f"Failed to load sheet config from {CONFIG_FILE}: {e}")

# ———————————————————————————————
# Authenticate & fetch entire sheet once
# ———————————————————————————————
CREDS_FILE = os.getenv('GOOGLE_CREDS', 'credentials.json')
SCOPES     = ['https://www.googleapis.com/auth/spreadsheets']
creds      = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPES)
gc         = gspread.authorize(creds)
ws         = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
all_rows   = ws.get_all_values()  # ONE HTTP call

# ———————————————————————————————
# Build header→column map and in-memory rows
# ———————————————————————————————
headers       = all_rows[HEADER_ROW - 1]
header_to_col = {name: idx + 1 for idx, name in enumerate(headers)}

# verify required headers exist
for hdr in (ID_HEADER, RUN_HEADER, SETUP_HEADER, END_HEADER, DAQ_PC_HEADER, DIGITIZER_HEADER):
    if hdr not in header_to_col:
        raise RuntimeError(f"Missing required header '{hdr}' in row {HEADER_ROW}")

COL_ID        = header_to_col[ID_HEADER]
COL_RUN_NAME  = header_to_col[RUN_HEADER]
COL_SETUP     = header_to_col[SETUP_HEADER]
COL_END       = header_to_col[END_HEADER]
COL_DAQ_PC    = header_to_col[DAQ_PC_HEADER]
COL_DIGITIZER = header_to_col[DIGITIZER_HEADER]
COL_CONFIG    = header_to_col[CONFIG_HEADER]

# rows below the header, zero-indexed
data_rows = all_rows[HEADER_ROW:]

# ———————————————————————————————
# Internal retry helper for API calls
# ———————————————————————————————
def _retry_api_call(fn, *args, retries: int = 3, delay: float = 1.0, **kwargs):
    last_exc = None
    for _ in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            time.sleep(delay)
    raise last_exc

# ———————————————————————————————
# In-memory lookups
# ———————————————————————————————
def load_run_names() -> List[str]:
    """
    Return all non-blank run names from the in-memory sheet copy,
    in the order they appear.
    """
    names: List[str] = []
    for row in data_rows:
        val = row[COL_RUN_NAME - 1].strip()
        if val:
            names.append(val)
    return names

def find_run_row(run_name: str) -> Optional[int]:
    """
    Return the 1-based sheet row index of the master row for `run_name`, or None.
    A master row is where:
      - the 'Run Name' cell == run_name
      - the 'ID' cell is non-blank
    """
    if not isinstance(run_name, str) or not run_name.strip():
        raise ValueError("run_name must be a non-empty string")
    for sheet_row, row in enumerate(data_rows, start=HEADER_ROW + 1):
        if (row[COL_RUN_NAME - 1].strip() == run_name
                and row[COL_ID - 1].strip()):
            return sheet_row
    return None


def find_run_rows(run_name: str) -> List[int]:
    """
    Return all 1-based sheet row indices where:
      • the 'Run Name' column equals `run_name`, and
      • the 'ID' column is non-blank.

    Raises:
        ValueError: if `run_name` is not a non-empty string.
    """
    if not isinstance(run_name, str) or not run_name.strip():
        raise ValueError("run_name must be a non-empty string")

    rows: List[int] = []
    for sheet_row, row in enumerate(data_rows, start=HEADER_ROW + 1):
        has_id   = row[COL_ID       - 1].strip()
        has_name = row[COL_RUN_NAME - 1].strip() == run_name
        if has_id and has_name:
            rows.append(sheet_row)
    return rows


def get_last_row() -> int:
    """
    Return the last sheet row number that has a non-blank ID.
    """
    last = HEADER_ROW
    for sheet_row, row in enumerate(data_rows, start=HEADER_ROW + 1):
        if row[COL_ID - 1].strip():
            last = sheet_row
    return last

# ———————————————————————————————
# Write operations (hit the API, then sync memory)
# ———————————————————————————————
def append_run(run_name: str, setup_dt: datetime, end_dt: Optional[datetime] = None) -> None:
    """
    Append a new row with auto-incremented ID, run_name, setup_dt, end_dt.
    Also update in-memory data_rows so future reads see it.
    """
    if not isinstance(run_name, str) or not run_name.strip():
        raise ValueError("run_name must be a non-empty string")
    if not isinstance(setup_dt, datetime):
        raise TypeError("setup_dt must be a datetime instance")

    # compute next ID from memory
    existing_ids = [
        int(r[COL_ID - 1]) for r in data_rows
        if r[COL_ID - 1].isdigit()
    ]
    next_id = max(existing_ids) + 1 if existing_ids else 1

    # build the new row up through COL_END
    new_row = [''] * len(headers)
    new_row[COL_ID - 1]       = str(next_id)
    new_row[COL_RUN_NAME - 1] = run_name
    new_row[COL_SETUP    - 1] = setup_dt.strftime('%Y-%m-%d %H:%M:%S')
    if end_dt:
        if not isinstance(end_dt, datetime):
            raise TypeError("end_dt must be a datetime instance or None")
        new_row[COL_END - 1] = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    _retry_api_call(ws.append_row, new_row, value_input_option='RAW')
    data_rows.append(new_row)

def update_setup_time(run_name: str, setup_dt: datetime) -> None:
    """
    Overwrite the 'Setup' cell for the given run, in both sheet and memory,
    but only if it’s currently blank.
    """
    row_idx = find_run_row(run_name)
    if row_idx is None:
        raise ValueError(f"Run '{run_name}' not found")

    # DON’T overwrite if already present
    current = data_rows[row_idx - HEADER_ROW - 1][COL_SETUP - 1].strip()
    if current:
        return

    val = setup_dt.strftime('%Y-%m-%d %H:%M:%S')
    _retry_api_call(ws.update_cell, row_idx, COL_SETUP, val)
    data_rows[row_idx - HEADER_ROW - 1][COL_SETUP - 1] = val

def update_end_time(run_name: str, end_dt: datetime) -> None:
    """
    Write the End timestamp into only the *last* master row matching `run_name`,
    but only if that cell is currently blank. Uses `find_run_rows()` to locate
    all master rows, picks the final one, retries transient API errors, and
    syncs the in-memory cache.

    Args:
        run_name: the run folder name to match in the sheet.
        end_dt:   the datetime to write (must be a datetime instance).

    Raises:
        ValueError: if `run_name` is invalid.
        TypeError: if `end_dt` is not a datetime.
    """
    if not isinstance(end_dt, datetime):
        raise TypeError("end_dt must be a datetime instance")

    rows = find_run_rows(run_name)
    if not rows:
        # no master rows to update
        return

    # target only the last master row
    sheet_row = rows[-1]
    mem_idx   = sheet_row - HEADER_ROW - 1
    col       = COL_END

    # skip if already populated
    if data_rows[mem_idx][col - 1].strip():
        return

    val = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    # write with retry, then update memory
    _retry_api_call(ws.update_cell, sheet_row, col, val)
    data_rows[mem_idx][col - 1] = val


def update_pc_name(run_name: str, pc_name: str) -> None:
    """
    If the 'DAQ PC' cell is blank, write `pc_name`.
    Never overwrite an existing value.
    """
    row_idx = find_run_row(run_name)
    if row_idx is None:
        return

    current = data_rows[row_idx - HEADER_ROW - 1][COL_DAQ_PC - 1].strip()
    if current:
        return

    _retry_api_call(ws.update_cell, row_idx, COL_DAQ_PC, pc_name)
    data_rows[row_idx - HEADER_ROW - 1][COL_DAQ_PC - 1] = pc_name

def update_digitizer(run_name: str, digitizer: str) -> None:
    """
    If the 'Digitizer' cell is blank, write `digitizer`.
    Never overwrite an existing value.
    """
    if not isinstance(digitizer, str) or not digitizer.strip():
        return

    row_idx = find_run_row(run_name)
    if row_idx is None:
        return

    current = data_rows[row_idx - HEADER_ROW - 1][COL_DIGITIZER - 1].strip()
    if current:
        return

    _retry_api_call(ws.update_cell, row_idx, COL_DIGITIZER, digitizer)
    data_rows[row_idx - HEADER_ROW - 1][COL_DIGITIZER - 1] = digitizer

def update_config_files(run_name: str, config_files: str) -> None:
    """
    If the 'Compas configuration file (digitizer settings)' cell is blank,
    write `config_files` (comma-separated filenames). Never overwrite existing.
    """
    if not isinstance(config_files, str) or not config_files.strip():
        return

    row_idx = find_run_row(run_name)
    if row_idx is None:
        return

    # Use the same column as DIGITIZER_HEADER
    current = data_rows[row_idx - HEADER_ROW - 1][COL_CONFIG - 1].strip()
    if current:
        return

    _retry_api_call(ws.update_cell, row_idx, COL_CONFIG, config_files)
    data_rows[row_idx - HEADER_ROW - 1][COL_CONFIG - 1] = config_files
