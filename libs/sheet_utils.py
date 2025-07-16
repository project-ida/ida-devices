#!/usr/bin/env python3
"""
sheet_utils.py

Google Sheets integration utilities for run monitoring using gspread.
Handles loading existing run names, appending new runs,
updating start/end times, and maintaining row numbers.
Configuration (spreadsheet ID, sheet name) is loaded
from an external JSON config file (e.g., sheet_config.json),
while credentials.json remains a separate service account key file.
"""

import os
import sys
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from collections import Counter

# --- Fixed credentials file ---
SERVICE_ACCOUNT_FILE = 'credentials.json'  # Google service account key

# --- Load sheet config from external file ---
# Expected to contain JSON with keys: spreadsheet_id, sheet_name,
# optionally col_run_name, col_setup, col_end, header_row
CONFIG_FILE = os.getenv('SHEET_CONFIG_PATH', 'sheet_config.json')
try:
    with open(CONFIG_FILE, 'r') as cf:
        cfg = json.load(cf)
        SPREADSHEET_ID = cfg['spreadsheet_id']
        SHEET_NAME     = cfg['sheet_name']
        # Column indices (1-based) override defaults if provided
        COL_RUN_NAME = cfg.get('col_run_name', 7)   # G
        COL_SETUP    = cfg.get('col_setup', 12)     # L
        COL_END      = cfg.get('col_end', 17)       # Q
        HEADER_ROW   = cfg.get('header_row', 1)
except Exception as e:
    raise RuntimeError(f"Failed to load sheet config from {CONFIG_FILE}: {e}")

# Columns (1-indexed) are now set via config above
# These can be overridden in sheet_config.json under keys 'col_run_name', 'col_setup', 'col_end', 'header_row'
COL_RUN_NAME = cfg.get('col_run_name', 7)   # G
COL_SETUP    = cfg.get('col_setup', 12)     # L
COL_END      = cfg.get('col_end', 17)       # Q
HEADER_ROW   = cfg.get('header_row', 1)

# Authenticate with gspread using fixed credentials.json
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    SERVICE_ACCOUNT_FILE, SCOPES
)
gc = gspread.authorize(credentials)
sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)


def load_run_names() -> set:
    values = sheet.col_values(COL_RUN_NAME)
    values = [v.strip() for v in values if v.strip()]
    # Check for duplicates
    dups = [name for name, count in Counter(values).items() if count > 1]
    
    if dups:
        print(f"Warning: Duplicate run names found: {dups}. Fix spreadsheet and try again.")
        sys.exit(1)
 
    return (values)


def find_run_row(run_name: str) -> int | None:
    """
    Return the 1-based row index where run_name appears in column G,
    or None if not found.
    """
    values = sheet.col_values(COL_RUN_NAME)
    for idx, v in enumerate(values, start=1):
        if v.strip() == run_name:
            return idx
    return None


def get_last_row() -> int:
    """
    Return the last non-empty row number in column A.
    """
    values = sheet.col_values(1)
    return len(values)


def append_run(run_name: str, setup_dt: datetime, end_dt: datetime | None = None):
    """
    Append a new row at bottom with run_name, setup and optional end.
    Column A is set to max existing value in column A plus 1,
    regardless of its row position.
    """
    col_a_values = sheet.col_values(1)  # Column A values
    # Filter and convert to ints safely
    numbers = []
    for v in col_a_values:
        try:
            numbers.append(int(v))
        except (ValueError, TypeError):
            continue
    
    next_number = max(numbers) + 1 if numbers else 1
    
    # Build a row list for columns A through Q
    row = [''] * COL_END
    row[0] = str(next_number)  # Set column A
    row[COL_RUN_NAME - 1] = run_name
    row[COL_SETUP - 1] = setup_dt.strftime('%Y-%m-%d %H:%M:%S')
    if end_dt:
        row[COL_END - 1] = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    
    sheet.append_row(row, value_input_option='RAW')


def update_setup_time(run_name: str, setup_dt: datetime):
    """
    Find run row and set the setup time in column L.
    """
    row = find_run_row(run_name)
    if row:
        sheet.update_cell(row, COL_SETUP, setup_dt.strftime('%Y-%m-%d %H:%M:%S'))


def update_end_time(run_name: str, end_dt: datetime):
    """
    Find run row and set the end time in column Q.
    """
    row = find_run_row(run_name)
    if row:
        sheet.update_cell(row, COL_END, end_dt.strftime('%Y-%m-%d %H:%M:%S')) 
        
