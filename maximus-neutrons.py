#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import re
import logging
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# --- Import local creds; add parent so "ida_db" or creds can be found ---
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# We will use psycopg2 directly so we can set per-row timestamps for History
try:
    import psycopg2
    import psycopg2.extras
except Exception:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    raise

# ------------------------------ DB HELPERS -------------------------------

class DB:
    """
    Minimal DB helper that supports:
      - insert_spectrum_now(table, counts_array)
      - insert_history_at(table, dt, value)
    Uses the timezone set on the server; we pass explicit timestamp for history.
    """

    def __init__(self):
        import psql_credentials as creds
        dsn = getattr(creds, "DSN", None)
        if not dsn:
            # Build DSN from individual fields
            host = getattr(creds, "host", None) or getattr(creds, "HOST", None)
            port = getattr(creds, "port", None) or getattr(creds, "PORT", None)
            dbname = getattr(creds, "dbname", None) or getattr(creds, "database", None) \
                     or getattr(creds, "DBNAME", None) or getattr(creds, "DATABASE", None)
            user = getattr(creds, "user", None) or getattr(creds, "USER", None)
            password = getattr(creds, "password", None) or getattr(creds, "PASSWORD", None)

            parts = []
            if host: parts.append(f"host={host}")
            if port: parts.append(f"port={port}")
            if dbname: parts.append(f"dbname={dbname}")
            if user: parts.append(f"user={user}")
            if password: parts.append(f"password={password}")
            dsn = " ".join(parts)

        self._dsn = dsn
        self._conn = None
        self.connect()

    def connect(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        self._conn = psycopg2.connect(self._dsn)
        self._conn.autocommit = True

    def reconnect(self):
        logging.warning("Reconnecting to PostgreSQL…")
        time.sleep(1.0)
        self.connect()

    def insert_spectrum_now(self, table: str, counts: np.ndarray) -> bool:
        """
        Insert one row into <table> with time=NOW() and the numeric array.
        Expects table schema: (time timestamp with time zone, channels double precision[])
        """
        sql = f"INSERT INTO {table} (time, channels) VALUES (NOW(), %s)"
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, (counts.tolist(),))
            return True
        except Exception as e:
            logging.error(f"Insert spectrum failed: {e}")
            return False

    def insert_history_at(self, table: str, dt: datetime, value: float) -> bool:
        """
        Insert one row into <table> at an explicit timestamp with a single-value array.
        """
        sql = f"INSERT INTO {table} (time, channels) VALUES (%s, %s)"
        arr = [float(value)]
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, (dt, arr))
            return True
        except Exception as e:
            logging.error(f"Insert history failed: {e}")
            return False


# ---------------------------- FILE UTILITIES -----------------------------

def wait_for_file_stable(path: str, polls: int = 5, interval: float = 0.2) -> bool:
    """
    Wait until file size stops changing across 'polls' checks.
    """
    try:
        last = -1
        stable = 0
        for _ in range(polls * 3):  # up to ~3*polls*interval seconds
            sz = os.path.getsize(path)
            if sz == last:
                stable += 1
                if stable >= polls:
                    return True
            else:
                stable = 0
                last = sz
            time.sleep(interval)
        return False
    except FileNotFoundError:
        return False


def parse_channel_from_filename(path: str, kind: str) -> Optional[int]:
    """
    kind: 'History' or 'Spectrum'
    Matches e.g. .../History0-1.csv -> 0, Spectrum3-xyz.csv -> 3
    """
    m = re.search(rf"{kind}(\d+)-", os.path.basename(path))
    return int(m.group(1)) if m else None


# --------------------------- PROCESSING LOGIC ----------------------------

def process_history_file(path: str, table_prefix: str, db: DB) -> None:
    """
    File format: one entry per line: "<second>, <value>"
    We take base = NOW() floored to minute (server-local notion),
    and timestamp each entry at base + second.
    """
    ch = parse_channel_from_filename(path, "History")
    if ch is None:
        logging.warning(f"Could not parse channel from filename: {path}")
        return
    table = f"{table_prefix}{ch}_history"

    # Read quickly; tolerate both "s,v" and "s, v"
    lines: List[str] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.strip() for ln in f if ln.strip()]

    # Determine base = now floored to minute (local time of this machine).
    # If you want DB-side time zone, you can also fetch server NOW(), but this is fine.
    now_local = datetime.now()
    base = now_local.replace(second=0, microsecond=0)

    ok_count, bad_count = 0, 0
    for ln in lines:
        try:
            # supports "12, 34" or "12,34"
            s_str, v_str = [x.strip() for x in ln.split(",", 1)]
            sec = int(s_str)
            val = float(v_str)
            # Clamp seconds into [0, 59] just in case
            if sec < 0:
                sec = 0
            elif sec > 59:
                sec = 59
            dt = base + timedelta(seconds=sec)
            if not db.insert_history_at(table, dt, val):
                # One retry after reconnect
                db.reconnect()
                if not db.insert_history_at(table, dt, val):
                    bad_count += 1
                else:
                    ok_count += 1
            else:
                ok_count += 1
        except Exception as e:
            logging.warning(f"Skipping malformed history line '{ln}': {e}")
            bad_count += 1

    logging.info(f"History→ {table}: inserted={ok_count}, skipped={bad_count}")


def process_spectrum_file(path: str, table_prefix: str, db: DB) -> None:
    """
    File format: one entry per line: "<channel>, <count>"
    We build a 1-D float array of counts and store with NOW() (DB-side).
    """
    ch = parse_channel_from_filename(path, "Spectrum")
    if ch is None:
        logging.warning(f"Could not parse channel from filename: {path}")
        return
    table = f"{table_prefix}{ch}_spectrum"

    counts: List[float] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                # supports "12, 34" or "12,34"
                ch_str, cnt_str = [x.strip() for x in ln.split(",", 1)]
                # int(ch_str)  # channel index not used here
                counts.append(float(cnt_str))
            except Exception:
                logging.warning(f"Skipping malformed spectrum line: {ln}")

    if not counts:
        logging.warning(f"No usable spectrum data in {path}")
        return

    arr = np.asarray(counts, dtype=float)

    if not db.insert_spectrum_now(table, arr):
        db.reconnect()
        if not db.insert_spectrum_now(table, arr):
            logging.error(f"Spectrum insert still failing for {path}")
            return

    logging.info(f"Spectrum→ {table}: bins={arr.size}")


# ------------------------------ WATCHER ---------------------------------

class Watcher:
    def __init__(self, folder: str, table_prefix: str):
        self.folder = folder
        self.table_prefix = table_prefix
        self.db = DB()
        self.last_filename = None  # simple de-dupe

    def on_created(self, event):
        if event.is_directory:
            return
        path = event.src_path
        # de-dup by exact name (helps with double notifications on some FS)
        fname = os.path.basename(path)
        if fname == self.last_filename:
            logging.info(f"Duplicate event skipped for {fname}")
            return

        # Wait for file to finish writing
        if not wait_for_file_stable(path):
            logging.warning(f"File never stabilized: {path}")
            return

        self.last_filename = fname
        try:
            if "History" in fname:
                process_history_file(path, self.table_prefix, self.db)
            elif "Spectrum" in fname:
                process_spectrum_file(path, self.table_prefix, self.db)
            else:
                logging.info(f"Ignoring non-matching file: {fname}")
        except Exception as e:
            logging.exception(f"Unhandled error processing {fname}: {e}")

    @staticmethod
    def on_modified(event):
        # Ignore to avoid double-processing while files are being written
        return

    def run(self):
        patterns = ["*.csv"]
        handler = PatternMatchingEventHandler(patterns=patterns, ignore_directories=True)
        handler.on_created = self.on_created
        handler.on_modified = self.on_modified

        observer = Observer()
        observer.schedule(handler, self.folder, recursive=False)
        observer.start()
        logging.info(f"Watching: {self.folder}")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Stopping observer…")
            observer.stop()
        observer.join()


# --------------------------------- MAIN ---------------------------------

def main():
    ap = argparse.ArgumentParser(description="Watch a folder and ingest History/Spectrum CSVs.")
    ap.add_argument("--folder", required=True, help="Folder to watch for .csv files")
    ap.add_argument("--table-prefix", required=True, help="Database table name prefix (e.g., 'neutrons_max')")
    ap.add_argument("--log-file", default="pulse_counter.log")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(args.log_file, encoding="utf-8")]
    )

    watcher = Watcher(args.folder, args.table_prefix)
    watcher.run()


if __name__ == "__main__":
    main()
