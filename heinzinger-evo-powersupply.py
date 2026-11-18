#!/usr/bin/env python3
"""Read-only Heinzinger EVO acquisition for Windows and Ubuntu (Python 3.10+).

Examples
--------
    python heinzinger-evo-powersupply.py --evo-host 192.168.0.100 --run-name scan-1
    python heinzinger-evo-powersupply.py --evo-host 192.168.0.100 --run-name scan-2 --gui
    python heinzinger-evo-powersupply.py --run-name gui-check --demo --gui

Optional arguments: --evo-port (6000), --evo-serial (464222713), --qps (250
TOTAL queries/s, alternating voltage/current), --root-file-mb (10 decimal MB).
--gui starts a localhost web server on a free port and opens the browser. Pause
freezes ONLY the viewer; Play returns to the latest 120-second window. Ctrl+C
finishes local saving. No supply settings or HV switching commands are sent.

Dependencies: numpy, uproot; real database uploads also require psycopg2.
These are already in requirements.txt. The optional existing Telegram notifier
uses requests. Keep evo_live.html next to this script; evo_demo.py is needed
only for --demo. On Windows, psycopg2-binary provides the psycopg2 import if
building psycopg2 locally is inconvenient. Device communication lives in
libs/evo_device.py; storage, averaging, database work and the viewer remain here.

For real runs, set COMPUTER_NAME and provide psql_credentials.py as in the other
device scripts. Optional telegram_credentials.py enables notifications. The
PostgreSQL tables must already exist: evo(time, channels) with channels ordered
[mean_voltage_V, mean_current_mA], and the existing root_files table. The evo
time column should be timestamp without time zone. All saved/displayed dates
and times use the acquisition computer's local clock, with no timezone suffix
or conversion on upload. This includes CSVs, database values, RAW data,
filenames, run metadata and terminal logging.
Missing PostgreSQL credentials or the psycopg2 driver prevent a real run from
starting. This local configuration check does not connect to the database:
network/database outages still allow acquisition and queue data for retry.
Database operations and alerts run in background threads, with bounded network
waits and rate-limited notifications. Telegram credentials remain optional.

Files and timings
-----------------
    ~/evo/DAQ/<run-name>/run.json, run.log
    ~/evo/DAQ/<run-name>/averages/YYYY-MM-DD.csv, YYYY-MM-DD.bookmark
    ~/evo/DAQ/<run-name>/RAW/<run-name>_000001.writing.root

Run names are exact, portable folder names. An existing run folder is an error;
there is no automatic suffix or resume. Each raw TTree (voltage or current)
has its own string `time` (local YYYY-MM-DD HH:MM:SS.ffffff) and float64 value
branch (`voltage` in V, `current` in signed mA). `time` is the PC request time,
not an instrument ADC timestamp. Each quantity retains its separate request
time, including microseconds. Only successful replies enter the trees.
Errors, service notifications and connection-time settings go into the log.
Raw values and one-second averages are rounded to four decimal places. CSV and
terminal values show exactly four places; ROOT/PostgreSQL numeric floats do not
retain trailing zeros. Daily CSV filenames follow the computer's local date.

Completed one-second intervals are averaged independently per quantity and printed.
Partial startup/shutdown seconds stay in the raw data. Missing quantities are
empty CSV fields / SQL NULL. Each average is appended and fsynced to its daily
CSV BEFORE it is eligible for upload. A bookmark is the number of data rows
(excluding the header) committed to PostgreSQL. It advances after commit via
temporary-file replacement; CSVs remain local backups. Backlog inserts use
batches; healthy operation sends each second as soon as possible.

Raw writing uses chunks (1,000 readings or five seconds), independent of the
averages. Actual file size is checked after each write; closing metadata and
chunks can slightly exceed the requested threshold. On rotation or Ctrl+C,
finish and close the file, then rename .writing.root to a timestamped .root.
Only AFTER root_files registration commits is it renamed .root2, making it
eligible for the separate cloud uploader. root_files paths are relative to
the computer's home folder, e.g. evo/DAQ/scan-1/RAW. Point the existing uploader
at ~/evo, using the same COMPUTER_NAME; it already excludes *.root files.

Only THIS run's pending CSV rows and closed ROOT files are retried. Earlier
runs are untouched and await a future, separate postprocessing script. An
interrupted .writing.root is not automatically recovered. There is no special
reconciliation of commit/rename interruptions or server-side deduplication;
an uncertain commit may be replayed. Demo runs are labelled synthetic, use
evo_demo.py, and NEVER write to PostgreSQL, send alerts or promote .root2 files.

CPU/RAM/free-disk thresholds never stop acquisition. Actual storage failures
are reported and retried; acquisition and the viewer continue. Memory queues
are bounded, so prolonged inability to save can lose readings, explicitly
counted/reported. Flush/fsync requests do not guarantee survival of every power
failure. In particular, the open ROOT file may be incomplete after a crash.
"""

from __future__ import annotations

import argparse
from collections import deque
import csv
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import math
import os
from pathlib import Path
import queue
import re
import signal
import socket
import sys
import threading
import time
import webbrowser

import numpy as np
import uproot

from libs.evo_device import (
    CHANNELS, DEFAULT_PORT, DEFAULT_SERIAL, EvoDevice, InstrumentLease, Reading,
)


# Acquisition/storage settings; device defaults are imported from libs/evo_device.
DATA_ROOT = Path.home() / "evo"
TABLE = DEVICE = "evo"
ROOT_CHUNK_READINGS = 1000
ROOT_FLUSH_SECONDS = 5.0
DB_RETRY_SECONDS = 5.0
DB_BATCH_ROWS = 1000
DB_SHUTDOWN_SECONDS = 5.0
HISTORY_SECONDS = 120.0
MAX_PENDING_READINGS = 30000
CSV_FIELDS = ("time", "voltage_V", "current_mA")


def local_now():
    return datetime.now().isoformat(sep=" ", timespec="microseconds")


def local_from_us(stamp):
    """Format an internal request-clock counter using the computer's local time."""
    seconds, micros = divmod(int(stamp), 1_000_000)
    return datetime.fromtimestamp(seconds).replace(microsecond=micros)


def atomic_text(path, text):
    """Close before replace on Windows; never overwrite a bookmark in place."""
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def validate_run_name(value):
    # Preserve the supplied name rather than silently sanitising it.
    reserved = {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"}
    reserved.update(f"{prefix}{n}" for prefix in ("COM", "LPT") for n in range(1, 10))
    if (not value or value in {".", ".."} or value[-1:] in {" ", "."}
            or re.search(r'[<>:"/\\|?*\x00-\x1f]', value)
            or value.split(".")[0].upper() in reserved):
        raise argparse.ArgumentTypeError("Use one Windows/Ubuntu-compatible folder name (no path separators).")
    return value


class RootRecorder:
    """Owned exclusively by the recording thread, never by the DB worker."""
    def __init__(self, session):
        self.session = session
        self.file = self.path = None
        self.part = 0
        self.first_us = self.last_us = None
        self.buffer = {name: [] for name in CHANNELS.values()}
        self.count = 0
        self.last_flush = time.monotonic()

    def add(self, reading: Reading):
        if self.count >= MAX_PENDING_READINGS:
            self.session.lost_readings(1, "ROOT recording buffer is full after a storage failure")
            return
        self.buffer[reading.channel].append((reading.time, round(reading.value, 4)))
        self.count += 1

    def flush(self, force=False):
        if not self.count or (not force and self.count < ROOT_CHUNK_READINGS
                              and time.monotonic() - self.last_flush < ROOT_FLUSH_SECONDS):
            return
        if self.file is None:
            self.part += 1
            self.path = self.session.folder / "RAW" / f"{self.session.args.run_name}_{self.part:06d}.writing.root"
            self.file = uproot.recreate(self.path, compression=uproot.ZLIB(1))
            for name in self.buffer:
                self.file.mktree(name, {"time": "string", name: "float64"})
            self.first_us = self.last_us = None
        stamps = []
        for name, readings in self.buffer.items():
            if readings:
                self.file[name].extend({"time": [local_from_us(r[0]).isoformat(sep=" ", timespec="microseconds")
                                               for r in readings],
                                        name: np.array([r[1] for r in readings], dtype=np.float64)})
                stamps.extend(r[0] for r in readings)
        self.file.file.sink.flush()
        # Windows' _commit/FlushFileBuffers requires a writable handle. A
        # read-only handle works on POSIX but raises EBADF on Windows.
        with self.path.open("r+b") as stream:
            os.fsync(stream.fileno())
        self.first_us = min(stamps) if self.first_us is None else min(self.first_us, min(stamps))
        self.last_us = max(stamps) if self.last_us is None else max(self.last_us, max(stamps))
        with self.session.lock:
            self.session.stats["records_written"] += self.count
            self.session.stats["records_synced"] = self.session.stats["records_written"]
            self.session.stats["current_file"] = self.path.name
            self.session.stats["last_sync_time"] = local_now()
        for readings in self.buffer.values():
            readings.clear()
        self.count = 0
        self.last_flush = time.monotonic()
        if self.path.stat().st_size >= self.session.args.root_file_mb * 1_000_000:
            self.close_part()

    def close_part(self):
        if self.file is None:
            return
        self.file.close()
        self.file = None
        if self.first_us is None:
            return
        # Include any closing metadata in the sync before publishing the file
        # as complete to the registration worker.
        with self.path.open("r+b") as stream:
            os.fsync(stream.fileno())
        first = local_from_us(self.first_us).strftime("%Y%m%d_%H%M%S")
        last = local_from_us(self.last_us).strftime("%Y%m%d_%H%M%S")
        closed = self.path.with_name(f"{self.session.args.run_name}_{first}-{last}_{self.part:06d}.root")
        self.path.rename(closed)
        with self.session.lock:
            self.session.pending_roots.append((closed, last))
            self.session.stats["current_file"] = closed.name
            self.session.stats["pending_root_files"] = len(self.session.pending_roots)
        self.session.log.info("Closed ROOT file: %s (%s bytes)", closed.name, closed.stat().st_size)
        self.session.db_wake.set()
        self.path = None

    def abandon_failed_file(self):
        # Preserve an uncertain file as .writing.root; retry the buffered chunk
        # in a NEW file rather than appending into a potentially partial write.
        if self.file is not None:
            try:
                self.file.close()
            except Exception:
                pass
        self.file = self.path = None
        self.first_us = self.last_us = None
        self.last_flush = time.monotonic()

    def close(self):
        self.flush(force=True)
        self.close_part()


class AveragesRecorder:
    def __init__(self, session):
        self.session = session
        self.file = self.day = None
        self.writer = None

    def append(self, second, values):
        stamp = datetime.fromtimestamp(second)
        day = stamp.strftime("%Y-%m-%d")
        if day != self.day:
            self.close()
            path = self.session.folder / "averages" / (day + ".csv")
            with self.session.lock:
                known_day = day in self.session.csv_days
            # This run owns its folder. A file not yet published in csv_days
            # can contain only a failed initial header, so retry its creation.
            # A previously published day is always appended, never overwritten.
            try:
                self.file = path.open("a" if known_day else "w", newline="", encoding="utf-8")
                self.writer = csv.writer(self.file, lineterminator="\n")
                if not known_day:
                    self.writer.writerow(CSV_FIELDS)
                    self.file.flush()
                    os.fsync(self.file.fileno())
                    atomic_text(path.with_suffix(".bookmark"), "0\n")
                    with self.session.lock:
                        self.session.csv_days[day] = {"path": path, "available": 0, "uploaded": 0}
                self.day = day
            except Exception:
                self.close()
                raise
        # A failed append is truncated back to the last synchronised position.
        # No row is exposed to the DB worker until flush + fsync both succeed.
        position = self.file.tell()
        try:
            self.writer.writerow([stamp.isoformat(sep=" ", timespec="seconds")] +
                                 [f"{value:.4f}" if value is not None else "" for value in values])
            self.file.flush()
            os.fsync(self.file.fileno())
        except Exception:
            try:
                self.file.seek(position)
                self.file.truncate()
                self.file.flush()
            except Exception:
                pass
            raise
        with self.session.lock:
            self.session.csv_days[day]["available"] += 1
            self.session.stats["averages_saved"] += 1
        self.session.db_wake.set()

    def close(self):
        if self.file is not None:
            self.file.close()
        self.file = self.writer = self.day = None


class OneSecondAverages:
    def __init__(self, start_us, emit):
        self.second = (start_us + 999999) // 1_000_000
        self.emit = emit
        self.values = {name: [] for name in CHANNELS.values()}

    def advance(self, watermark_us):
        # The reader sends ordered watermarks only when a query has completed
        # or while idle. A reply requested before a boundary is never prematurely
        # assigned to the following second, even when it arrives after it.
        while watermark_us // 1_000_000 > self.second:
            counts = [len(self.values[name]) for name in ("voltage", "current")]
            means = [round(math.fsum(self.values[name]) / len(self.values[name]), 4)
                     if self.values[name] else None for name in ("voltage", "current")]
            self.emit(self.second, means, counts)
            self.second += 1
            self.values = {name: [] for name in CHANNELS.values()}

    def add(self, reading: Reading):
        self.advance(reading.time)
        if reading.time // 1_000_000 == self.second:
            self.values[reading.channel].append(round(reading.value, 4))
        self.advance(reading.completed_us)


def load_database_config():
    """Validate local setup without requiring the database to be reachable."""
    try:
        import psql_credentials as creds
    except ImportError as error:
        raise ValueError("Cannot load psql_credentials.py. Copy the database credentials "
                         "file beside this script before starting a real run.") from error
    required = ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD")
    missing = [name for name in required if getattr(creds, name, None) is None]
    if missing:
        raise ValueError("psql_credentials.py is missing: " + ", ".join(missing))
    for name in ("PGHOST", "PGDATABASE", "PGUSER"):
        value = getattr(creds, name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} in psql_credentials.py must be a non-empty string")
    if not isinstance(creds.PGPASSWORD, str):
        raise ValueError("PGPASSWORD in psql_credentials.py must be a string")
    try:
        port = int(creds.PGPORT)
    except (TypeError, ValueError) as error:
        raise ValueError("PGPORT in psql_credentials.py must be an integer") from error
    if not 1 <= port <= 65535:
        raise ValueError("PGPORT in psql_credentials.py must be between 1 and 65535")
    try:
        import psycopg2
    except ImportError as error:
        raise ValueError("The psycopg2 database driver is missing. Install psycopg2 "
                         "(or psycopg2-binary on Windows) before starting a real run.") from error
    return dict(host=creds.PGHOST.strip(), port=port, dbname=creds.PGDATABASE,
                user=creds.PGUSER, password=creds.PGPASSWORD)


class DatabaseWorker:
    """Current-run retry only. CSV files and closed ROOT files are the backlog."""
    def __init__(self, session, connect_factory=None):
        self.session = session
        self.connect_factory = connect_factory or self.connect
        self.connection = None
        self.offsets = {}  # Byte offsets avoid rereading a growing CSV from row 1.
        self.shutdown_deadline = None

    @staticmethod
    def connect():
        config = load_database_config()
        import psycopg2
        conn = psycopg2.connect(**config, connect_timeout=3,
                                options="-c statement_timeout=5000",
                                keepalives=1, keepalives_idle=5,
                                keepalives_interval=2, keepalives_count=2)
        return conn

    def close_connection(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                pass
        self.connection = None

    def upload_averages(self):
        with self.session.lock:
            days = [(day, dict(info)) for day, info in self.session.csv_days.items()
                    if info["available"] > info["uploaded"]]
        if not days:
            return False
        day, info = min(days)
        path = info["path"]
        rows = []
        with path.open("rb") as stream:
            if day in self.offsets:
                stream.seek(self.offsets[day])
            else:
                header = next(csv.reader([stream.readline().decode("utf-8")]))
                if tuple(header) != CSV_FIELDS:
                    raise ValueError(f"Unexpected averages CSV header in {path}")
            for _ in range(min(DB_BATCH_ROWS, info["available"] - info["uploaded"])):
                line = stream.readline()
                if not line.endswith(b"\n"):
                    raise ValueError("A synchronised CSV row is missing or incomplete")
                record = next(csv.reader([line.decode("utf-8")]))
                if len(record) != 3:
                    raise ValueError("Invalid averages CSV row")
                # The CSV already contains the acquisition computer's local
                # wall-clock time. Send it unchanged, without timezone conversion.
                stamp = datetime.fromisoformat(record[0])
                channels = [float(value) if value else None for value in record[1:]]
                if any(value is not None and not math.isfinite(value) for value in channels):
                    raise ValueError("Non-finite CSV average")
                rows.append((stamp, channels))
            end_offset = stream.tell()
        with self.connection.cursor() as cursor:
            # A single INSERT also handles catch-up batches efficiently. Bind
            # every value; the table name is the fixed constant `evo`.
            placeholders = ",".join(["(%s, %s::double precision[])"] * len(rows))
            parameters = [value for row in rows for value in row]
            cursor.execute("INSERT INTO evo (time, channels) VALUES " + placeholders, parameters)
        self.connection.commit()
        uploaded = info["uploaded"] + len(rows)
        atomic_text(path.with_suffix(".bookmark"), f"{uploaded}\n")
        self.offsets[day] = end_offset
        with self.session.lock:
            self.session.csv_days[day]["uploaded"] = uploaded
            self.session.stats["averages_uploaded"] += len(rows)
        return True

    def register_root(self):
        with self.session.lock:
            pending = self.session.pending_roots[0] if self.session.pending_roots else None
        if pending is None:
            return False
        path, end_time = pending
        final = path.with_suffix(".root2")
        relative_dir = f"evo/DAQ/{self.session.args.run_name}/RAW"
        with self.connection.cursor() as cursor:
            cursor.execute("INSERT INTO root_files (time, computer, daq_folder, dir, file, device) "
                           "VALUES (%s, %s, %s, %s, %s, %s)",
                           (end_time, self.session.computer, self.session.args.run_name,
                            relative_dir, final.name, DEVICE))
        self.connection.commit()
        # Deliberately no commit/rename reconciliation in this first version.
        path.rename(final)
        with self.session.lock:
            self.session.pending_roots.popleft()
            self.session.stats["pending_root_files"] = len(self.session.pending_roots)
        self.session.log.info("Registered ROOT file and renamed to %s", final.name)
        return True

    def run(self):
        offline = False
        try:
            while True:
                if self.session.recording_done.is_set():
                    if self.shutdown_deadline is None:
                        self.shutdown_deadline = time.monotonic() + DB_SHUTDOWN_SECONDS
                    if not self.session.has_pending_database_work() or time.monotonic() >= self.shutdown_deadline:
                        break
                if not self.session.has_pending_database_work():
                    self.session.db_wake.wait(.2)
                    self.session.db_wake.clear()
                    continue
                try:
                    if self.connection is None:
                        self.connection = self.connect_factory()
                    self.upload_averages()
                    self.register_root()
                    if offline:
                        self.session.log.info("Database connection restored; processing pending work")
                        self.session.alert("database-recovered", "evo database connection restored")
                    offline = False
                    self.session.set_status(database_state="connected")
                except Exception as error:
                    self.close_connection()
                    self.session.set_status(database_state="offline")
                    if not offline:
                        self.session.log.warning("Database work pending: %s. Acquisition continues; retrying every %.0fs.",
                                                 error, DB_RETRY_SECONDS)
                    self.session.alert("database", f"evo database work is pending: {error}")
                    offline = True
                    # On shutdown leave pending files for explicit postprocessing.
                    if self.session.recording_done.wait(DB_RETRY_SECONDS):
                        break
        finally:
            self.close_connection()


def reduce_points(points, start, end, buckets=300):
    """Preserve endpoints, extrema and gaps; never average the raw plot."""
    bins, segment = {}, 0
    for index, (stamp, value) in enumerate(points):
        if value is None:
            segment += 1
            continue
        key = (segment, min(buckets - 1, max(0, int((stamp - start) / max(end - start, .001) * buckets))))
        point = (index, stamp, value, segment)
        if key not in bins:
            bins[key] = [point, point, point, point]
        else:
            group = bins[key]
            group[1] = point
            if value < group[2][2]:
                group[2] = point
            if value > group[3][2]:
                group[3] = point
    selected = sorted({point for group in bins.values() for point in group})
    xs, ys, previous_segment = [], [], None
    for _, stamp, value, segment in selected:
        if previous_segment is not None and segment != previous_segment:
            xs.append(stamp)
            ys.append(None)
        xs.append(stamp)
        ys.append(value)
        previous_segment = segment
    return {"time_s": xs, "values": ys}


class Session:
    def __init__(self, args, computer):
        self.args, self.computer = args, computer
        self.folder = DATA_ROOT / "DAQ" / args.run_name
        self.folder.parent.mkdir(parents=True, exist_ok=True)
        self.folder.mkdir(exist_ok=False)
        (self.folder / "RAW").mkdir()
        (self.folder / "averages").mkdir()
        self.log = logging.getLogger(f"evo.{id(self)}")
        self.log.setLevel(logging.INFO)
        self.log.propagate = False
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        formatter.converter = time.localtime
        for handler in (logging.FileHandler(self.folder / "run.log", encoding="utf-8"), logging.StreamHandler()):
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
        self.created_us = time.time_ns() // 1000
        self.start_ns = time.perf_counter_ns()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.reader_done = threading.Event()
        self.recording_done = threading.Event()
        self.db_wake = threading.Event()
        self.notification_stop = threading.Event()
        self.notifications = queue.Queue(maxsize=32)
        self.alert_times = {}
        self.messages = queue.Queue(maxsize=MAX_PENDING_READINGS)
        self.history = deque(maxlen=math.ceil(args.qps * HISTORY_SECONDS * 2) + 64)
        self.recent = deque(maxlen=2501)
        self.latest_ns = {}
        self.last_channel_send = {}
        self.csv_days = {}
        self.pending_roots = deque()
        self.stats = dict(state="connecting", synthetic=args.demo, error=None, stop_reason=None,
                          elapsed_s=0, target_qps=args.qps, achieved_qps=0, measurements_received=0,
                          records_written=0, records_synced=0, current_file=None, last_sync_time=None,
                          connection_id=0, reconnect_attempts=0, latest={"voltage": None, "current": None},
                          identity=None, settings_snapshot={}, settings_captured_time=None,
                          last_notification=None, sequence=0, output_dir=str(self.folder),
                          averages_saved=0, averages_uploaded=0, pending_root_files=0,
                          lost_readings=0, lost_averages=0,
                          database_state="disabled (demo)" if args.demo else "waiting")
        self.run_info = dict(schema_version=2, run_name=args.run_name, created_time=local_now(),
                             computer=computer, device=DEVICE, table=TABLE, synthetic=args.demo,
                             evo_host=args.evo_host, evo_port=args.evo_port, evo_serial=args.evo_serial,
                             qps=args.qps, root_file_mb=args.root_file_mb,
                             raw_time_format="YYYY-MM-DD HH:MM:SS.ffffff", timestamp_semantics="PC request time",
                             voltage_unit="V", current_unit="mA", average_interval_seconds=1,
                             value_decimal_places=4, time_zone="computer local time",
                             average_channels=["mean_voltage_V", "mean_current_mA"],
                             root_directory=f"evo/DAQ/{args.run_name}/RAW", state="recording")
        atomic_text(self.folder / "run.json", json.dumps(self.run_info, indent=2) + "\n")

    def set_status(self, **values):
        with self.lock:
            self.stats.update(values)

    def alert(self, key, message):
        if self.args.demo:
            return
        with self.lock:
            now = time.monotonic()
            if now - self.alert_times.get(key, -math.inf) < 300:
                return
            self.alert_times[key] = now
        try:
            self.notifications.put_nowait(f"{self.computer} / {self.args.run_name}: {message}")
        except queue.Full:
            pass

    def notify(self):
        # Import only when needed; a missing Telegram configuration is optional.
        while not self.notification_stop.is_set():
            try:
                message = self.notifications.get(timeout=.2)
            except queue.Empty:
                continue
            try:
                from libs.telegram_notifier import send_telegram_alert
                send_telegram_alert(message)
            except Exception as error:
                self.log.warning("Telegram alert could not be sent: %s", error)

    def lost_readings(self, count, reason):
        with self.lock:
            self.stats["lost_readings"] += count
            total = self.stats["lost_readings"]
            self.stats["error"] = reason
        if total == count or total % 1000 == 0:
            self.log.error("%s; %s raw readings lost so far", reason, total)
        self.alert("recording-loss", reason)

    def clock(self):
        try:
            self.messages.put_nowait(("clock", time.time_ns() // 1000))
        except queue.Full:
            pass

    def publish(self, reading: Reading):
        with self.lock:
            elapsed = (reading.send_ns - self.start_ns) / 1e9
            previous = self.last_channel_send.get(reading.channel)
            if previous is not None and reading.send_ns - previous > 3e9 / self.args.qps:
                self.history.append((elapsed, reading.channel, None))
            self.last_channel_send[reading.channel] = reading.send_ns
            self.history.append((elapsed, reading.channel, reading.value))
            self.stats["latest"][reading.channel] = dict(value=reading.value,
                                                        unit="V" if reading.channel == "voltage" else "mA")
            self.latest_ns[reading.channel] = reading.receive_ns
            self.recent.append(reading.receive_ns)
            self.stats["sequence"] += 1
            self.stats["measurements_received"] += 1
        try:
            self.messages.put_nowait(("reading", reading))
        except queue.Full:
            self.lost_readings(1, "Recording worker cannot keep up; its input queue is full")

    def has_pending_database_work(self):
        with self.lock:
            return bool(self.pending_roots) or any(info["available"] > info["uploaded"]
                                                   for info in self.csv_days.values())

    def snapshot(self):
        now = time.perf_counter_ns()
        with self.lock:
            result = dict(self.stats)
            result["latest"] = {name: dict(value, age_s=max(0, (now - self.latest_ns[name]) / 1e9))
                                if value is not None else None for name, value in self.stats["latest"].items()}
            while self.recent and now - self.recent[0] > 10_000_000_000:
                self.recent.popleft()
            elapsed = (now - self.start_ns) / 1e9
            result["elapsed_s"] = result.get("finished_elapsed_s", elapsed)
            result["achieved_qps"] = len(self.recent) / max(.001, min(10, elapsed))
            result["averages_pending"] = result["averages_saved"] - result["averages_uploaded"]
        return result

    def frame_snapshot(self):
        status = self.snapshot()
        end = status["elapsed_s"]
        start = max(0, end - HISTORY_SECONDS)
        with self.lock:
            while self.history and self.history[0][0] < start:
                self.history.popleft()
            history = list(self.history)
        frame = dict(status=status, elapsed_s=end, window_s=HISTORY_SECONDS, sequence=status["sequence"])
        for name in ("voltage", "current"):
            frame[name] = reduce_points([(stamp, value) for stamp, channel, value in history if channel == name], start, end)
        return frame

    def supply_notification(self, command, notification):
        with self.lock:
            changed = self.stats["last_notification"] != notification
        self.set_status(last_notification=notification)
        if changed:
            self.log.warning("Supply notification %s after %s", notification, command)
            self.alert("supply-notification", f"Supply notification: {notification}")

    def read(self):
        retries = 0
        try:
            device = EvoDevice(self.args.evo_host, port=self.args.evo_port,
                               serial=self.args.evo_serial, qps=self.args.qps,
                               stop=self.stop, on_idle=self.clock,
                               on_notification=self.supply_notification)
            while not self.stop.is_set():
                try:
                    self.set_status(state="connecting" if not retries else "retrying")
                    with device:
                        with self.lock:
                            self.stats["connection_id"] += 1
                            self.stats.update(identity=device.identity,
                                              settings_snapshot=device.settings_snapshot,
                                              settings_captured_time=device.settings_captured_time)
                        self.log.info("Connected: %s; connection-time settings: %s",
                                      device.identity, device.settings_snapshot)
                        while not self.stop.is_set():
                            for reading in device.measurements():
                                self.publish(reading)
                                if retries:
                                    self.log.info("EVO measurement replies resumed")
                                retries = 0
                                self.set_status(state="recording")
                except InterruptedError:
                    if self.stop.is_set():
                        break
                    raise
                except (OSError, ValueError) as error:
                    if self.stop.is_set():
                        break
                    self.log.warning("EVO connection/read failed: %s; reconnecting", error)
                    self.alert("device", f"EVO connection/read failed: {error}")
                    self.set_status(state="retrying")
                    with self.lock:
                        stamp = (time.perf_counter_ns() - self.start_ns) / 1e9
                        self.history.extend(((stamp, "voltage", None), (stamp, "current", None)))
                        self.stats["reconnect_attempts"] += 1
                    delay = (2, 5, 10, 30)[min(retries, 3)]
                    retries += 1
                    until = time.monotonic() + delay
                    while not self.stop.is_set() and time.monotonic() < until:
                        self.clock()
                        self.stop.wait(min(.1, max(0, until - time.monotonic())))
        except Exception as error:
            self.set_status(error=f"Reader failed: {error}")
            self.log.exception("Unexpected acquisition failure")
            self.stop.set()
        finally:
            self.clock()
            self.reader_done.set()

    def record(self):
        root = RootRecorder(self)
        csv_output = AveragesRecorder(self)
        pending_averages = deque()
        last_csv_retry = last_root_retry = 0.0

        def emit(second, means, counts):
            voltage = "N/A" if means[0] is None else f"{means[0]:.4f} V"
            current = "N/A" if means[1] is None else f"{means[1]:.4f} mA"
            with self.lock:
                pending = self.stats["averages_saved"] - self.stats["averages_uploaded"]
            self.log.info("%s | V=%s (n=%s) | I=%s (n=%s) | DB=%s, pending=%s",
                          datetime.fromtimestamp(second).isoformat(sep=" ", timespec="seconds"), voltage,
                          counts[0], current, counts[1], self.snapshot()["database_state"], pending)
            if len(pending_averages) < MAX_PENDING_READINGS:
                pending_averages.append((second, means))
            else:
                with self.lock:
                    self.stats["lost_averages"] += 1
                    self.stats["error"] = "Averages retry buffer is full; some averages were lost"
                self.log.error("Averages could not be saved: local retry buffer is full")
                self.alert("averages-loss", "Averages could not be saved: local retry buffer is full")

        averages = OneSecondAverages(self.created_us, emit)
        try:
            while not self.reader_done.is_set() or not self.messages.empty():
                try:
                    kind, value = self.messages.get(timeout=.1)
                    if kind == "reading":
                        root.add(value)
                        averages.add(value)
                    else:
                        averages.advance(value)
                except queue.Empty:
                    pass
                now = time.monotonic()
                if now >= last_csv_retry:
                    while pending_averages:
                        try:
                            second, means = pending_averages[0]
                            csv_output.append(second, means)
                            pending_averages.popleft()
                        except Exception as error:
                            self.storage_error("CSV", error)
                            last_csv_retry = now + 5
                            break
                if now >= last_root_retry:
                    try:
                        root.flush()
                    except Exception as error:
                        self.storage_error("ROOT", error)
                        root.abandon_failed_file()
                        last_root_retry = now + 5
        except Exception as error:
            self.storage_error("Recording worker", error)
            self.log.exception("Recording worker failed; device reader and viewer remain running")
            # Keep draining to avoid unlimited memory growth after a fatal bug.
            while not self.reader_done.is_set() or not self.messages.empty():
                try:
                    kind, _ = self.messages.get(timeout=.1)
                    if kind == "reading":
                        self.lost_readings(1, "Recording worker failed")
                except queue.Empty:
                    pass
        finally:
            try:
                while pending_averages:
                    second, means = pending_averages[0]
                    csv_output.append(second, means)
                    pending_averages.popleft()
                csv_output.close()
            except Exception as error:
                self.storage_error("Final CSV saving", error)
            try:
                root.close()
            except Exception as error:
                self.storage_error("Final ROOT saving", error)
                root.abandon_failed_file()
            self.recording_done.set()
            self.db_wake.set()

    def storage_error(self, label, error):
        activity = "Acquisition has stopped" if self.stop.is_set() else "Acquisition continues"
        message = f"{label} storage error: {error}. {activity}; saving may be incomplete."
        self.log.error(message, exc_info=True)
        self.set_status(error=message)
        self.alert("storage", message)


def start_viewer(session):
    html = Path(__file__).with_name("evo_live.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            if self.headers.get("Host") not in {f"127.0.0.1:{self.server.server_port}",
                                                f"localhost:{self.server.server_port}"}:
                self.send_error(403)
                return
            if self.path in {"/", "/index.html"}:
                body, content_type = html, "text/html; charset=utf-8"
            elif self.path in {"/api/status", "/api/frame"}:
                value = session.snapshot() if self.path == "/api/status" else session.frame_snapshot()
                body = json.dumps(value, allow_nan=False).encode("utf-8")
                content_type = "application/json"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="evo-viewer", daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}"
    session.log.info("Viewer: %s", url)
    threading.Thread(target=webbrowser.open, args=(url,), daemon=True).start()
    return server


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Read-only EVO acquisition: raw ROOT and one-second CSV/PostgreSQL averages.")
    parser.add_argument("--run-name", required=True, type=validate_run_name, help="Exact new folder name under ~/evo/DAQ")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--evo-host", help="EVO IP address or hostname")
    target.add_argument("--demo", action="store_true", help="Use evo_demo.py; disable PostgreSQL and Telegram")
    parser.add_argument("--evo-port", type=int, default=DEFAULT_PORT, help="EVO TCP port (default: 6000)")
    parser.add_argument("--evo-serial", default=DEFAULT_SERIAL, help="Expected EVO serial (default: 464222713)")
    parser.add_argument("--qps", type=float, default=250, help="Total queries/s, shared by V and I (default/max: 250)")
    parser.add_argument("--root-file-mb", type=float, default=10, help="Approximate ROOT rotation threshold in decimal MB (default: 10)")
    parser.add_argument("--gui", action="store_true", help="Start the local web viewer and open the browser")
    args = parser.parse_args(argv)
    if not math.isfinite(args.qps) or not 0 < args.qps <= 250:
        parser.error("--qps must be positive and no greater than 250 (4 ms minimum spacing)")
    if not math.isfinite(args.root_file_mb) or args.root_file_mb <= 0:
        parser.error("--root-file-mb must be positive and finite")
    if not 1 <= args.evo_port <= 65535 or not args.evo_serial.strip():
        parser.error("Provide a valid EVO port and non-empty serial")
    if args.evo_host is not None and not args.evo_host.strip():
        parser.error("--evo-host cannot be empty")
    if args.evo_host is not None:
        args.evo_host = args.evo_host.strip()
    if args.gui and not Path(__file__).with_name("evo_live.html").is_file():
        parser.error("evo_live.html must be next to this script when using --gui")
    args.evo_serial = args.evo_serial.strip()
    return args


def main(argv=None):
    args = parse_args(argv)
    computer = os.getenv("COMPUTER_NAME", "").strip()
    if not args.demo and not computer:
        raise SystemExit("Set COMPUTER_NAME before a real run, as for the existing ROOT scripts.")
    computer = computer or socket.gethostname()
    folder = DATA_ROOT / "DAQ" / args.run_name
    if folder.exists():
        raise SystemExit(f"Run {args.run_name!r} already exists at {folder}. Choose a different --run-name.")
    if not args.demo:
        try:
            load_database_config()
        except ValueError as error:
            raise SystemExit(f"Cannot start real EVO acquisition: {error}") from None
    simulator = lease = session = viewer = None
    threads = []
    previous_handlers = {}
    try:
        if args.demo:
            from evo_demo import start_simulator, DEMO_SERIAL
            simulator = start_simulator()
            args.evo_host, args.evo_port = simulator.server_address
            args.evo_serial = DEMO_SERIAL
        else:
            lease = InstrumentLease(args.evo_host, args.evo_port, args.evo_serial)
            lease.acquire()
        try:
            session = Session(args, computer)
        except FileExistsError:
            raise SystemExit(f"Run {args.run_name!r} already exists at {folder}. Choose a different --run-name.") from None

        def request_stop(_signal, _frame):
            # A repeated Ctrl+C must not interrupt or re-enter final file saving.
            if session.stop.is_set():
                return
            session.stop.set()
            session.log.info("Stopping: finishing local files; pending database work remains available for postprocessing.")
            session.set_status(state="stopping", stop_reason="terminal")

        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            number = getattr(signal, name, None)
            if number is not None:
                previous_handlers[number] = signal.signal(number, request_stop)
        session.log.info("%s | run=%s | files=%s", "SIMULATED DATA; PostgreSQL disabled" if args.demo else "Read-only evo acquisition", args.run_name, folder)
        session.log.info("Ctrl+C finishes saving. V/I queries alternate; no supply settings are changed.")
        if args.gui:
            viewer = start_viewer(session)
        threads = [threading.Thread(target=session.record, name="evo-recording"),
                   threading.Thread(target=session.read, name="evo-acquisition")]
        if not args.demo:
            threads.append(threading.Thread(target=DatabaseWorker(session).run, name="evo-database"))
            threading.Thread(target=session.notify, name="evo-notifications", daemon=True).start()
        for thread in threads:
            thread.start()
        while not session.stop.wait(.2):
            pass
        for thread in threads:
            while thread.is_alive():
                thread.join(.2)
        status = session.snapshot()
        session.set_status(state="error" if status["error"] else "saved",
                           finished_elapsed_s=(time.perf_counter_ns() - session.start_ns) / 1e9)
        session.run_info.update(state="error" if status["error"] else "stopped", finished_time=local_now(),
                                statistics=session.snapshot())
        atomic_text(folder / "run.json", json.dumps(session.run_info, indent=2, allow_nan=False) + "\n")
        status = session.snapshot()
        session.log.info("Local recording finished: %s raw readings saved; %s averages saved, %s pending; %s ROOT files pending registration.",
                         status["records_written"], status["averages_saved"], status["averages_pending"], status["pending_root_files"])
        return int(bool(status["error"]))
    finally:
        if session is not None:
            session.stop.set()
            if not any(thread.name == "evo-acquisition" and thread.ident is not None for thread in threads):
                session.reader_done.set()
            for thread in threads:
                if thread.ident is not None:
                    while thread.is_alive():
                        thread.join(.2)
            session.notification_stop.set()
        if viewer is not None:
            viewer.shutdown()
            viewer.server_close()
        if simulator is not None:
            simulator.shutdown()
            simulator.server_close()
        if lease is not None:
            lease.close()
        for number, handler in previous_handlers.items():
            signal.signal(number, handler)
        if session is not None:
            for handler in list(session.log.handlers):
                handler.close()
                session.log.removeHandler(handler)


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="backslashreplace")
    sys.exit(main())
