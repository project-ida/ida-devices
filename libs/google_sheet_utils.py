#!/usr/bin/env python3
"""
libs/sheet_utils.py

Google Sheets utilities for run monitoring using gspread.
This version does one initial fetch of the entire sheet into memory,
then services all read operations locally.  Only append/update calls
hit the API thereafter.

Requirements:
  - A service‐account JSON key at credentials.json
  - A config file at config/google_sheet_config.json defining:
      * spreadsheet_id, sheet_name, header_row,
      * columns.id_header, run_name_header, setup_header, end_header
"""

import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# ———————————————————————————————
# Load JSON config
# ———————————————————————————————
CONFIG_FILE = os.getenv('SHEET_CONFIG_PATH', 'config/google_sheet_config.json')
try:
    with open(CONFIG_FILE, 'r') as cf:
        cfg = json.load(cf)
        SPREADSHEET_ID  = cfg['spreadsheet_id']
        SHEET_NAME      = cfg['sheet_name']
        HEADER_ROW      = cfg.get('header_row', 1)
        cols            = cfg['columns']
        ID_HEADER       = cols['id_header']
        RUN_HEADER      = cols['google_drive_data_folders_header']
        SETUP_HEADER    = cols['setup_header']
        END_HEADER      = cols['end_header']
except Exception as e:
    raise RuntimeError(f"Failed to load sheet config from {CONFIG_FILE}: {e}")

# ———————————————————————————————
# Authenticate & fetch entire sheet
# ———————————————————————————————
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds  = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', SCOPES)
gc     = gspread.authorize(creds)

ws     = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
all_rows = ws.get_all_values()  # ONE HTTP call

# ———————————————————————————————
# Build header→col mapping and in-memory data_rows
# ———————————————————————————————
headers       = all_rows[HEADER_ROW-1]
header_to_col = {h: i+1 for i,h in enumerate(headers)}

# verify required headers exist
for h in (ID_HEADER, RUN_HEADER, SETUP_HEADER, END_HEADER):
    if h not in header_to_col:
        raise RuntimeError(f"Missing required header '{h}' in row {HEADER_ROW}")

COL_ID       = header_to_col[ID_HEADER]
COL_RUN_NAME = header_to_col[RUN_HEADER]
COL_SETUP    = header_to_col[SETUP_HEADER]
COL_END      = header_to_col[END_HEADER]

# rows *below* the header, indexed 0.. for in-memory
data_rows = all_rows[HEADER_ROW:]

# ———————————————————————————————
# In-memory lookups
# ———————————————————————————————

def load_run_names() -> list[str]:
    """Returns all run names in sheet order, skipping blanks."""
    names = []
    for row in data_rows:
        v = row[COL_RUN_NAME-1].strip()
        if v:
            names.append(v)
    return names

def find_run_row(run_name: str) -> int | None:
    """
    Return the 1-based sheet row index of the master row for run_name:
      * row[COL_RUN_NAME-1]==run_name and row[COL_ID-1] non-blank
    """
    for i, row in enumerate(data_rows, start=HEADER_ROW+1):
        if row[COL_RUN_NAME-1].strip() == run_name and row[COL_ID-1].strip():
            return i
    return None

def get_last_row() -> int:
    """
    Return the last sheet row number that has a non-blank ID.
    """
    last = HEADER_ROW
    for i, row in enumerate(data_rows, start=HEADER_ROW+1):
        if row[COL_ID-1].strip():
            last = i
    return last

# ———————————————————————————————
# Write operations (still hit the API)
# ———————————————————————————————

def append_run(run_name: str, setup_dt: datetime, end_dt: datetime | None = None):
    """
    Append a new row with auto-incremented ID, run_name, setup_dt, end_dt.
    Also update our in-memory data_rows so future reads see it.
    """
    # compute next ID from memory
    existing = [int(r[COL_ID-1]) for r in data_rows if r[COL_ID-1].isdigit()]
    next_id = max(existing)+1 if existing else 1

    # build new row up through COL_END
    new_row = [''] * len(headers)
    new_row[COL_ID-1]       = str(next_id)
    new_row[COL_RUN_NAME-1] = run_name
    new_row[COL_SETUP-1]    = setup_dt.strftime('%Y-%m-%d %H:%M:%S')
    if end_dt:
        new_row[COL_END-1]  = end_dt.strftime('%Y-%m-%d %H:%M:%S')

    ws.append_row(new_row, value_input_option='RAW')
    data_rows.append(new_row)  # keep memory in sync

def update_setup_time(run_name: str, setup_dt: datetime):
    """
    Overwrite the 'Setup' cell for the given run, both in sheet and in memory.
    """
    row_idx = find_run_row(run_name)
    if not row_idx:
        return
    col = COL_SETUP
    ws.update_cell(row_idx, col, setup_dt.strftime('%Y-%m-%d %H:%M:%S'))
    data_rows[row_idx-HEADER_ROW-1][col-1] = setup_dt.strftime('%Y-%m-%d %H:%M:%S')

def update_end_time(run_name: str, end_dt: datetime):
    """
    Overwrite the 'End' cell for the given run, both in sheet and in memory.
    """
    row_idx = find_run_row(run_name)
    if not row_idx:
        return
    col = COL_END
    ws.update_cell(row_idx, col, end_dt.strftime('%Y-%m-%d %H:%M:%S'))
    data_rows[row_idx-HEADER_ROW-1][col-1] = end_dt.strftime('%Y-%m-%d %H:%M:%S')
