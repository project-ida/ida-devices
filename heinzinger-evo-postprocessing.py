#!/usr/bin/env python3
"""Finish pending database work from one Heinzinger EVO run (Windows/Ubuntu).

    python heinzinger-evo-postprocessing.py
    python heinzinger-evo-postprocessing.py --run-folder ~/evo/DAQ/scan-1
    python heinzinger-evo-postprocessing.py --run-folder ~/evo/DAQ/scan-1 --dry-run

With no arguments, ask only for the run folder. Supplying any arguments uses
normal command-line validation without prompts. --dry-run reports pending work
without loading credentials, connecting to PostgreSQL or changing any files.

Stop acquisition of the selected run first; acquisition in other run folders
can continue. A crash can leave run.json marked "recording", so that field is
not used to decide whether work can resume. This script processes only the
selected folder once, then exits; it does not watch for new runs or files.

The run must contain run.json, averages/ and RAW/ from the local-time EVO
acquisition script. CSV headers are time,voltage_V,current_mA. Each daily
.bookmark is the number of data rows already committed, excluding the header.
Upload subsequent rows to evo(time, channels), with channels [voltage_V,
current_mA], in batches of 1,000. Preserve saved local times and values exactly.
Advance the existing bookmark only after commit, using a temporary file and
os.replace as in acquisition. Missing/invalid bookmarks are errors; never
assume zero and replay data whose upload status is unknown. An unfinished
final CSV line is reported and left untouched.

Register completed .root files in root_files(time, computer, daq_folder, dir,
file, device), using their filename's end time and run.json's original computer,
run name and relative ROOT directory. Commit BEFORE renaming to .root2. The
existing uploader can then copy them. Skip .writing.root and .root2 files;
do not recover unfinished ROOT files or recalculate averages from RAW data.

Database connection failures retry every five seconds. Ctrl+C finishes the
current successful operation and its bookmark/rename, then stops. Rerunning
uses the same bookmarks and file extensions. As in acquisition, uncertain
commits and interruptions between commit and local progress updates have no
special reconciliation or server-side duplicate protection.

Only actual uploads require psycopg2 and psql_credentials.py beside this script
(PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD), as for acquisition. No device
connection or COMPUTER_NAME setting is needed: run.json identifies the original
acquisition computer, even when processing a copy elsewhere. Synthetic/demo
runs can be inspected with --dry-run but are never uploaded.
"""

import argparse
import csv
from datetime import datetime
from itertools import islice
import json
import logging
import math
import os
from pathlib import Path
import re
import signal
import sys
import threading


CSV_FIELDS = ("time", "voltage_V", "current_mA")
DB_BATCH_ROWS = 1000
DB_RETRY_SECONDS = 5.0
LOG = logging.getLogger("evo-postprocessing")


def atomic_text(path, text):
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def load_database_config():
    """Check local configuration without connecting to PostgreSQL."""
    try:
        import psql_credentials as creds
    except ImportError as error:
        raise ValueError("Cannot load psql_credentials.py beside this script.") from error
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
        raise ValueError("Install psycopg2 (or psycopg2-binary on Windows) before uploading.") from error
    return dict(host=creds.PGHOST.strip(), port=port, dbname=creds.PGDATABASE,
                user=creds.PGUSER, password=creds.PGPASSWORD)


def csv_rows(path, report_incomplete=False):
    """Stream complete CSV rows; keep memory bounded even for a long outage."""
    with path.open("rb") as stream:
        header = next(csv.reader([stream.readline().decode("utf-8")]), [])
        if tuple(header) != CSV_FIELDS:
            raise ValueError(f"Unexpected CSV header in {path}")
        for number, line in enumerate(stream, start=1):
            if not line.endswith(b"\n"):
                if report_incomplete:
                    LOG.warning("Leaving unfinished final CSV row untouched: %s (data row %s)", path.name, number)
                break
            try:
                record = next(csv.reader([line.decode("utf-8")], strict=True))
                if len(record) != 3:
                    raise ValueError("expected time, voltage and current")
                stamp = datetime.fromisoformat(record[0])
                if stamp.tzinfo is not None:
                    raise ValueError("expected a saved local timestamp without a timezone; no conversion is performed")
                channels = [float(value) if value else None for value in record[1:]]
                if any(value is not None and not math.isfinite(value) for value in channels):
                    raise ValueError("non-finite voltage/current")
            except (ValueError, csv.Error, UnicodeError) as error:
                raise ValueError(f"Invalid data row {number} in {path}: {error}") from error
            yield stamp, channels


def inspect_run(folder):
    with (folder / "run.json").open(encoding="utf-8") as stream:
        info = json.load(stream)
    if not isinstance(info, dict) or info.get("device") != "evo" or info.get("table") != "evo":
        raise ValueError("run.json must identify an EVO acquisition run and the evo table")
    for field in ("run_name", "computer", "root_directory"):
        if not isinstance(info.get(field), str) or not info[field].strip():
            raise ValueError(f"run.json is missing a valid {field}")
    if not isinstance(info.get("synthetic"), bool):
        raise ValueError("run.json is missing the synthetic/demo flag")
    if info.get("time_zone") != "computer local time":
        raise ValueError("This script expects an EVO run saved with local times. No time conversion is performed.")
    for name in ("averages", "RAW"):
        if not (folder / name).is_dir():
            raise ValueError(f"Missing {name} folder in {folder}")

    csv_jobs = []
    for path in sorted((folder / "averages").glob("*.csv")):
        total = sum(1 for _ in csv_rows(path, report_incomplete=True))
        bookmark = path.with_suffix(".bookmark")
        if not bookmark.is_file():
            raise ValueError(f"Missing {bookmark}; cannot determine which rows were already uploaded")
        try:
            uploaded = int(bookmark.read_text(encoding="utf-8").strip())
        except ValueError as error:
            raise ValueError(f"Invalid row count in {bookmark}") from error
        if not 0 <= uploaded <= total:
            raise ValueError(f"Bookmark {bookmark} says {uploaded} rows, but CSV has {total} complete rows")
        csv_jobs.append(dict(path=path, total=total, uploaded=uploaded))

    raw = folder / "RAW"
    writing = sorted(raw.glob("*.writing.root"))
    registered = sorted(raw.glob("*.root2"))
    pattern = re.compile(re.escape(info["run_name"]) + r"_(\d{8}_\d{6})-(\d{8}_\d{6})_\d{6,}\.root\Z")
    root_jobs = []
    for path in sorted(raw.glob("*.root")):
        if path.name.endswith(".writing.root"):
            continue
        match = pattern.fullmatch(path.name)
        if match is None:
            raise ValueError(f"Unrecognised completed EVO ROOT filename: {path.name}")
        for stamp in match.groups():
            datetime.strptime(stamp, "%Y%m%d_%H%M%S")
        if path.with_suffix(".root2").exists():
            raise ValueError(f"Both .root and .root2 exist for {path.stem}; leaving them untouched")
        root_jobs.append((path, match.group(2)))

    LOG.info("Run: %s | acquisition computer: %s | folder: %s", info["run_name"], info["computer"], folder)
    for job in csv_jobs:
        LOG.info("%s: %s rows, %s already uploaded, %s pending", job["path"].name,
                 job["total"], job["uploaded"], job["total"] - job["uploaded"])
    LOG.info("ROOT files: %s pending registration, %s already .root2, %s unfinished (left untouched)",
             len(root_jobs), len(registered), len(writing))
    for path in writing:
        LOG.info("Unfinished ROOT file left untouched: %s", path.name)
    return info, csv_jobs, root_jobs


class Database:
    def __init__(self, config, stop):
        import psycopg2
        self.driver = psycopg2
        self.config, self.stop = config, stop
        self.connection = None

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                pass
        self.connection = None

    def execute(self, query, parameters):
        """Retry the same batch on connection loss; progress changes happen after commit."""
        while not self.stop.is_set():
            try:
                if self.connection is None:
                    self.connection = self.driver.connect(
                        **self.config, connect_timeout=3, options="-c statement_timeout=5000",
                        keepalives=1, keepalives_idle=5, keepalives_interval=2, keepalives_count=2)
                    LOG.info("Database connected; processing pending work")
                with self.connection.cursor() as cursor:
                    cursor.execute(query, parameters)
                self.connection.commit()
                return True
            except (self.driver.OperationalError, self.driver.InterfaceError) as error:
                self.close()
                LOG.warning("Database work remains pending: %s. Retrying in %.0fs.", error, DB_RETRY_SECONDS)
                self.stop.wait(DB_RETRY_SECONDS)
        return False


def process_run(info, csv_jobs, root_jobs, database, stop):
    rows_uploaded = roots_registered = 0
    for job in csv_jobs:
        if job["uploaded"] == job["total"]:
            continue
        rows = csv_rows(job["path"])
        try:
            pending = islice(rows, job["uploaded"], job["total"])
            while not stop.is_set():
                batch = list(islice(pending, DB_BATCH_ROWS))
                if not batch:
                    break
                placeholders = ",".join(["(%s, %s::double precision[])"] * len(batch))
                parameters = [value for row in batch for value in row]
                if not database.execute("INSERT INTO evo (time, channels) VALUES " + placeholders, parameters):
                    break
                uploaded = job["uploaded"] + len(batch)
                atomic_text(job["path"].with_suffix(".bookmark"), f"{uploaded}\n")
                job["uploaded"] = uploaded
                rows_uploaded += len(batch)
                LOG.info("%s: uploaded %s rows; bookmark %s/%s", job["path"].name, len(batch), uploaded, job["total"])
        finally:
            rows.close()
        if stop.is_set():
            break

    for path, end_time in root_jobs:
        if stop.is_set():
            break
        final = path.with_suffix(".root2")
        parameters = (end_time, info["computer"], info["run_name"], info["root_directory"], final.name, "evo")
        query = ("INSERT INTO root_files (time, computer, daq_folder, dir, file, device) "
                 "VALUES (%s, %s, %s, %s, %s, %s)")
        if not database.execute(query, parameters):
            break
        # Preserve acquisition's ordering: commit first, then enable upload.
        path.rename(final)
        roots_registered += 1
        LOG.info("Registered ROOT file and renamed to %s", final.name)
    return rows_uploaded, roots_registered


def parse_args(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    parser = argparse.ArgumentParser(
        description="Finish pending CSV uploads and ROOT registrations from one EVO run.",
        epilog="Run without arguments to be prompted for the run folder. Stop acquisition of that run first.")
    parser.add_argument("--run-folder", required=True, help="Existing run folder containing run.json, averages and RAW")
    parser.add_argument("--dry-run", action="store_true", help="Show pending work without database access or file changes")
    if not argv:
        print("No command-line arguments supplied. Starting interactive setup.\n"
              "To see the available command-line options, run this script with --help.\n")
        while True:
            folder = input("Run folder (required): ").strip()
            if folder:
                break
            print("A run folder is required.")
        if len(folder) >= 2 and folder[0] == folder[-1] and folder[0] in {"'", '"'}:
            folder = folder[1:-1]
        argv = [f"--run-folder={folder}"]
    args = parser.parse_args(argv)
    if not args.run_folder.strip():
        parser.error("--run-folder cannot be empty")
    args.run_folder = Path(args.run_folder).expanduser().resolve()
    if not args.run_folder.is_dir():
        parser.error(f"Run folder does not exist: {args.run_folder}")
    return args


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    database = None
    previous_handlers = {}
    stop = threading.Event()
    try:
        args = parse_args(argv)
        info, csv_jobs, root_jobs = inspect_run(args.run_folder)
        pending_rows = sum(job["total"] - job["uploaded"] for job in csv_jobs)
        if args.dry_run:
            for path, end_time in root_jobs:
                LOG.info("Would register %s | time=%s | computer=%s | daq_folder=%s | dir=%s | device=evo",
                         path.with_suffix(".root2").name, end_time, info["computer"], info["run_name"], info["root_directory"])
            LOG.info("Dry run complete: %s average rows and %s ROOT registrations pending. No files changed.",
                     pending_rows, len(root_jobs))
            return 0
        if info["synthetic"]:
            raise ValueError("This is a synthetic/demo run. Use --dry-run to inspect it; demo data is never uploaded.")
        if not pending_rows and not root_jobs:
            LOG.info("No pending database work.")
            return 0
        config = load_database_config()

        def request_stop(_signal, _frame):
            if not stop.is_set():
                stop.set()
                LOG.info("Stopping after the current database operation and local progress update.")

        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            number = getattr(signal, name, None)
            if number is not None:
                previous_handlers[number] = signal.signal(number, request_stop)
        LOG.info("Processing selected run; acquisition of this run must be stopped. Ctrl+C stops postprocessing.")
        database = Database(config, stop)
        rows, roots = process_run(info, csv_jobs, root_jobs, database, stop)
        LOG.info("%s: uploaded %s average rows and registered %s ROOT files; %s rows and %s ROOT files remain pending.",
                 "Stopped" if stop.is_set() else "Finished", rows, roots, pending_rows - rows, len(root_jobs) - roots)
        return 130 if stop.is_set() else 0
    except EOFError:
        LOG.error("Interactive setup cancelled: no input available.")
        return 1
    except KeyboardInterrupt:
        LOG.info("Postprocessing cancelled.")
        return 130
    except (OSError, ValueError, csv.Error) as error:
        LOG.error("Postprocessing stopped: %s. Remaining work is left on disk.", error)
        return 1
    except Exception:
        LOG.exception("Postprocessing stopped; remaining work is left on disk")
        return 1
    finally:
        if database is not None:
            database.close()
        for number, handler in previous_handlers.items():
            signal.signal(number, handler)


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="backslashreplace")
    sys.exit(main())
