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
from typing import List, Optional, Dict, Any
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials

class GoogleSheet:
    def __init__(self, config_file: str = None, creds_file: str = None):
        """
        Initialize the GoogleSheet object, loading config and authenticating.
        """
        config_file = config_file or os.getenv('SHEET_CONFIG_PATH', 'config/google_sheet_config.json')
        creds_file = creds_file or os.getenv('GOOGLE_CREDS', 'credentials.json')

        # Load config
        with open(config_file, 'r') as cf:
            cfg = json.load(cf)
            self.spreadsheet_id = cfg['spreadsheet_id']
            self.sheet_name = cfg['sheet_name']
            self.header_row = cfg.get('header_row', 1)
            cols = cfg['columns']
            self.id_header = cols['id_header']
            self.run_header = cols['google_drive_data_folders_header']
            self.setup_header = cols['setup_header']
            self.end_header = cols['end_header']
            self.daq_pc_header = cols['daq_laptop_name_header']
            self.digitizer_header = cols['digitizer_header']
            self.config_header = cols['compas_config_file_header']

        # Authenticate and fetch sheet
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scopes)
        gc = gspread.authorize(creds)
        self.ws = gc.open_by_key(self.spreadsheet_id).worksheet(self.sheet_name)
        all_rows = self.ws.get_all_values()

        # Build header map and in-memory rows
        self.headers = all_rows[self.header_row - 1]
        self.header_to_col = {name: idx + 1 for idx, name in enumerate(self.headers)}
        self.COL_ID = self.header_to_col[self.id_header]
        self.COL_RUN_NAME = self.header_to_col[self.run_header]
        self.COL_SETUP = self.header_to_col[self.setup_header]
        self.COL_END = self.header_to_col[self.end_header]
        self.COL_DAQ_PC = self.header_to_col[self.daq_pc_header]
        self.COL_DIGITIZER = self.header_to_col[self.digitizer_header]
        self.COL_CONFIG = self.header_to_col[self.config_header]
        self.data_rows = all_rows[self.header_row:]

    def _retry_api_call(self, fn, *args, retries: int = 3, delay: float = 1.0, **kwargs):
        last_exc = None
        for _ in range(retries):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last_exc = e
                time.sleep(delay)
        raise last_exc

    def find_run_row(self, run_name: str) -> Optional[int]:
        for sheet_row, row in enumerate(self.data_rows, start=self.header_row + 1):
            if (row[self.COL_RUN_NAME - 1].strip() == run_name
                    and row[self.COL_ID - 1].strip()):
                return sheet_row
        return None

    def append_run(self, run_name: str, setup_dt: datetime, end_dt: Optional[datetime] = None) -> None:
        existing_ids = [
            int(r[self.COL_ID - 1]) for r in self.data_rows
            if r[self.COL_ID - 1].isdigit()
        ]
        next_id = max(existing_ids) + 1 if existing_ids else 1
        new_row = [''] * len(self.headers)
        new_row[self.COL_ID - 1] = str(next_id)
        new_row[self.COL_RUN_NAME - 1] = run_name
        if setup_dt:
            new_row[self.COL_SETUP - 1] = setup_dt.strftime('%Y-%m-%d %H:%M:%S')
        if end_dt:
            new_row[self.COL_END - 1] = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        self._retry_api_call(self.ws.append_row, new_row, value_input_option='RAW')
        self.data_rows.append(new_row)


    def update_run_row(self, run_name: str, values: Dict[int, Any]) -> None:
        """
        Atomically update multiple fields for a run in the sheet.

        Parameters:
        run_name (str): The run name to update.
        values (Dict[int, Any]): Mapping of column indices to new values.
        """
        row_idx = self.find_run_row(run_name)
        if row_idx is None:
            return
        mem_idx = row_idx - self.header_row - 1
        row = self.data_rows[mem_idx]
        updated = False
        for col_idx, value in values.items():
            if value is None:
                continue
            if isinstance(value, datetime):
                value = value.strftime('%Y-%m-%d %H:%M:%S')
            elif not isinstance(value, str):
                value = str(value)
            if not row[col_idx - 1].strip():
                row[col_idx - 1] = value
                updated = True
        if updated:
            # Update the entire row in the sheet
            self._retry_api_call(
                self.ws.update,
                f"A{row_idx}:{chr(64+len(row))}{row_idx}",
                [row]
            )
