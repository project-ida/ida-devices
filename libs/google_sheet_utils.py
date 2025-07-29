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
import logging

import gspread
from oauth2client.service_account import ServiceAccountCredentials

class GoogleSheet:
    """
    Utility class for interacting with a Google Sheet for DAQ run monitoring.
    Handles authentication, reading, appending, and updating rows.

    Note:
        This class maintains an in-memory cache of sheet rows (`self.data_rows`)
        that is only updated by this process. If the sheet is edited externally
        (e.g., by another user or process), the cache may become stale and
        inconsistencies may occur. For best results, avoid concurrent edits.
    """
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
        """
        Retry a Google Sheets API call up to 'retries' times with exponential backoff.
        Logs every exception encountered during retries for better debugging.

        Parameters:
        fn: The function to call.
        *args: Positional arguments for the function.
        retries (int): Number of retry attempts.
        delay (float): Initial delay between retries in seconds.
        **kwargs: Keyword arguments for the function.

        Returns:
        The result of the function call if successful.

        Raises:
        Exception: The last exception encountered if all retries fail.
        """
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                logging.warning(
                    f"API call failed on attempt {attempt}/{retries}: {e}"
                )
                last_exc = e
                time.sleep(delay)
                delay *= 2  # Exponential backoff
        logging.error(f"API call failed after {retries} attempts.")
        raise last_exc

    def find_run_row(self, run_name: str) -> Optional[int]:
        """
        Find the row index for a given run name.

        Parameters:
        run_name (str): The run name to search for.

        Returns:
        Optional[int]: The row index if found, else None.
        """
        for sheet_row, row in enumerate(self.data_rows, start=self.header_row + 1):
            if (row[self.COL_RUN_NAME - 1].strip() == run_name
                    and row[self.COL_ID - 1].strip()):
                return sheet_row
        return None


    def append_run(self, run_name: str, setup_dt: datetime, end_dt: Optional[datetime] = None) -> None:
        """
        Append a new run to the sheet with the given setup and end times.

        Parameters:
        run_name (str): The run name.
        setup_dt (datetime): The setup/start time.
        end_dt (Optional[datetime]): The end time, if available.
        """
        next_id = self._get_next_id()
        new_row = self._build_new_row(run_name, setup_dt, end_dt, next_id)
        self._retry_api_call(self.ws.append_row, new_row, value_input_option='RAW')
        self.data_rows.append(new_row)

    def _get_next_id(self) -> int:
        """
        Get the next available integer ID for a new run.
        """
        existing_ids = [
            int(r[self.COL_ID - 1]) for r in self.data_rows
            if r[self.COL_ID - 1].isdigit()
        ]
        return max(existing_ids) + 1 if existing_ids else 1

    def _build_new_row(self, run_name: str, setup_dt: datetime, end_dt: Optional[datetime], next_id: int) -> List[str]:
        """
        Build a new row for appending to the sheet.
        """
        new_row = [''] * len(self.headers)
        new_row[self.COL_ID - 1] = str(next_id)
        new_row[self.COL_RUN_NAME - 1] = run_name
        if setup_dt:
            new_row[self.COL_SETUP - 1] = setup_dt.strftime('%Y-%m-%d %H:%M:%S')
        if end_dt:
            new_row[self.COL_END - 1] = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        return new_row


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
        updated = self._update_row_in_memory(row, values)
        if updated:
            self._push_row_update(row_idx, row)

    def _update_row_in_memory(self, row: List[str], values: Dict[int, Any]) -> bool:
        """
        Update the in-memory row with the provided values.

        Parameters:
        row (List[str]): The row to update.
        values (Dict[int, Any]): Mapping of column indices to new values.

        Returns:
        bool: True if any value was updated, False otherwise.
        """
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
        return updated

    def _push_row_update(self, row_idx: int, row: List[str]) -> None:
        """
        Push the updated row to the Google Sheet.

        Parameters:
        row_idx (int): The row index in the sheet.
        row (List[str]): The updated row data.
        """
        self._retry_api_call(
            self.ws.update,
            f"A{row_idx}:{chr(64+len(row))}{row_idx}",
            [row]
        )
