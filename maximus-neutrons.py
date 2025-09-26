import time
import os
import logging
import argparse
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# watchdog
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# Add the parent directory (../) to the Python path so ida_db & creds resolve as before
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# ---------------- DB init (unchanged behavior) ----------------

def init_db():
    """
    Initialize and return a PostgreSQL logger using your existing ida_db.pglogger.
    This is exactly the connection path that worked for you previously.
    """
    from ida_db import pglogger
    import psql_credentials as creds
    try:
        db_cloud = pglogger(creds)
        logging.info("Database connection initialized via ida_db.pglogger.")
        return db_cloud
    except Exception as e:
        logging.error(f"Failed to initialize database connection: {e}")
        return None

def reconnect_db():
    logging.warning("Attempting to reconnect to the database...")
    return init_db()

# -------- helper: get a psycopg cursor from the existing pglogger --------

def _get_psycopg_cursor_from_pglogger(db_cloud):
    """
    Try to reuse the already-working connection that ida_db.pglogger uses, so we can
    execute explicit-timestamp INSERTs for History without changing your setup.

    Returns (conn, cursor) or (None, None) if we cannot get one.
    """
    conn = None
    for attr in ("conn", "_conn", "connection"):
        if hasattr(db_cloud, attr):
            conn = getattr(db_cloud, attr)
            break
    if conn is None:
        return None, None
    try:
        cur = conn.cursor()
        return conn, cur
    except Exception:
        return None, None

def _open_psycopg_fallback():
    """
    If the pglogger object doesn't expose a connection, fall back to opening a new
    psycopg2 connection using the same credentials module you already have.
    """
    try:
        import psycopg2  # uses the installed driver on your system
        import psql_credentials as creds

        # Prefer DSN if present; otherwise build from fields commonly used in your env
        dsn = getattr(creds, "DSN", None)
        if not dsn:
            parts = []
            for k in ("host","HOST"): 
                if hasattr(creds, k): parts.append(f"host={getattr(creds,k)}"); break
            for k in ("port","PORT"): 
                if hasattr(creds, k): parts.append(f"port={getattr(creds,k)}"); break
            for k in ("dbname","database","DBNAME","DATABASE"):
                if hasattr(creds, k): parts.append(f"dbname={getattr(creds,k)}"); break
            for k in ("user","USER"):
                if hasattr(creds, k): parts.append(f"user={getattr(creds,k)}"); break
            for k in ("password","PASSWORD"):
                if hasattr(creds, k): parts.append(f"password={getattr(creds,k)}"); break
            if hasattr(creds, "sslmode"):
                parts.append(f"sslmode={getattr(creds,'sslmode')}")
            dsn = " ".join(parts)

        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        logging.info("Opened fallback psycopg2 connection using psql_credentials.")
        return conn, cur
    except Exception as e:
        logging.error(f"Could not open fallback psycopg2 connection: {e}")
        return None, None

# -------------------- file utilities --------------------

def _wait_for_file_stable(path, polls=5, interval=0.2):
    """Wait until file size stops changing to avoid reading half-written files."""
    try:
        last = -1
        stable = 0
        for _ in range(polls * 5):
            sz = os.path.getsize(path)
            if sz == last:
                stable += 1
                if stable >= polls:
                    return True
            else:
                stable = 0
                last = sz
            time.sleep(interval)
    except FileNotFoundError:
        return False
    return False

# -------------------- core processing --------------------

def process_file(event, table_prefix, db_cloud, state):
    if event.is_directory:
        return

    file_path = event.src_path
    if not _wait_for_file_stable(file_path):
        logging.warning(f"File never stabilized: {file_path}")
        return

    new_file = os.path.basename(file_path)
    # simple de-dup guard
    if new_file == state.get("last_filename"):
        logging.info("Duplicate file event. Skipping.")
        return
    state["last_filename"] = new_file

    logging.info(f"Processing: {file_path}")

    if "History" in new_file:
        process_history_file(file_path, table_prefix, db_cloud)
    elif "Spectrum" in new_file:
        process_spectrum_file(file_path, table_prefix, db_cloud)

def process_history_file(file_path, table_prefix, db_cloud):
    """
    HISTORY: lines are 'second, value' — we take base time = NOW() floored to the minute
    (on this machine), and insert one row per line at base + second.
    """
    channel_match = re.search(r"History(\d+)-", os.path.basename(file_path))
    channel = int(channel_match.group(1)) if channel_match else None
    table_name = f"{table_prefix}{channel}_history"

    # Read as pairs
    pairs = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for entry in f:
            entry = entry.strip()
            if not entry:
                continue
            try:
                s_str, v_str = [x.strip() for x in entry.split(",", 1)]
                sec = int(s_str)
                val = float(v_str)
                # Clamp seconds into 0..59 (change to rollover if you prefer)
                sec = 0 if sec < 0 else (59 if sec > 59 else sec)
                pairs.append((sec, val))
            except Exception:
                logging.warning(f"Skipping malformed history line: {entry}")

    if not pairs:
        logging.warning(f"No valid lines in history file: {file_path}")
        return

    # Base = now floored to minute (no timezone gymnastics needed)
    base = datetime.now().replace(second=0, microsecond=0)

    # Try to reuse the existing pglogger connection; else open a fallback
    conn, cur = _get_psycopg_cursor_from_pglogger(db_cloud)
    need_close = False
    if cur is None:
        conn, cur = _open_psycopg_fallback()
        need_close = bool(cur)

    if cur is None:
        logging.error("No working DB cursor; cannot insert history rows.")
        return

    # Insert one row per line with explicit timestamp
    sql = f"INSERT INTO {table_name} (time, channels) VALUES (%s, %s)"
    ok, bad = 0, 0
    for sec, val in pairs:
        dt = base + timedelta(seconds=sec)
        try:
            cur.execute(sql, (dt, [float(val)]))
            ok += 1
        except Exception as e:
            logging.warning(f"Failed to insert history row ({dt}, {val}): {e}")
            bad += 1

    if need_close:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

    logging.info(f"History→ {table_name}: inserted={ok}, skipped={bad}")

def process_spectrum_file(file_path, table_prefix, db_cloud):
    """
    SPECTRUM: lines are 'channel, count' — build a 1-D float array of counts
    and pass it to db_cloud.log() (which inserts NOW() on the DB side).
    """
    channel_match = re.search(r"Spectrum(\d+)-", os.path.basename(file_path))
    channel = int(channel_match.group(1)) if channel_match else None
    table_name = f"{table_prefix}{channel}_spectrum"

    counts = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                ch_str, cnt_str = [x.strip() for x in line.split(",", 1)]
                counts.append(float(cnt_str))
            except Exception:
                logging.warning(f"Skipping malformed spectrum line: {line}")

    if not counts:
        logging.warning(f"No usable spectrum data in: {file_path}")
        return

    arr = np.asarray(counts, dtype=float)   # *** key fix: pass numeric array ***
    success = db_cloud.log(table=table_name, channels=arr)
    if not success:
        logging.warning(f"Failed to log spectrum data from {file_path}; reconnecting and retrying.")
        db_cloud = reconnect_db()
        if db_cloud is None or not db_cloud.log(table=table_name, channels=arr):
            logging.error(f"Spectrum insert still failing for {file_path}")
            return

    logging.info(f"Spectrum→ {table_name}: bins={arr.size}")

# -------------------- watcher scaffolding (unchanged UX) --------------------

def on_created_factory(table_prefix, db_cloud, state):
    def _inner(event):
        process_file(event, table_prefix, db_cloud, state)
    return _inner

def on_modified(event):
    # Ignore to avoid re-processing partially written files
    pass

def start_observer(folder_path, table_prefix):
    db_cloud = init_db()
    if db_cloud is None:
        logging.error("Could not initialize database connection. Exiting.")
        sys.exit(1)

    logging.info(f"Started observer for path: {folder_path}")

    state = {"last_filename": None}
    observer = Observer()
    event_handler = PatternMatchingEventHandler(patterns=["*.csv"], ignore_directories=True)
    event_handler.on_created = on_created_factory(table_prefix, db_cloud, state)
    event_handler.on_modified = on_modified

    observer.schedule(event_handler, folder_path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping observer...")
        observer.stop()
    observer.join()

# -------------------- CLI --------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pulse Counter File Watcher")
    parser.add_argument('--table-prefix', required=True, help="Database table name prefix")
    parser.add_argument('--folder', required=True, help="Folder path to watch for .csv files")
    parser.add_argument("--log-file", default="pulse_counter.log")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(args.log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

    start_observer(args.folder, args.table_prefix)
