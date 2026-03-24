#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import asyncio
import logging
import socket
from datetime import datetime, timezone

import numpy as np
from asyncua import Client

HOST = "192.168.1.100"
PORT = 4840
URL = f"opc.tcp://{HOST}:{PORT}"
PRESSURE_NODE_ID = "ns=1;s=G3_740_Pressure"
TABLE_NAME = "hicube_pressure"

POLL_INTERVAL = 10.0
CONNECT_TIMEOUT = 30.0
WATCHDOG_INTERVAL = 60.0
RETRY_DELAY = 10.0

# Choose:
#   "local"  -> timestamp when Python receives the value
#   "source" -> OPC UA SourceTimestamp (falls back to ServerTimestamp/local)
TIMESTAMP_MODE = "local"

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler("hicube-neo-pressure.log"),
        logging.StreamHandler(),
    ],
)

logging.getLogger("asyncua.client.client").setLevel(logging.CRITICAL)
logging.getLogger("asyncua.client.ua_client").setLevel(logging.WARNING)


def init_db():
    try:
        from ida_db import pglogger
        import psql_credentials as creds_cloud

        db_cloud = pglogger(creds_cloud)
        logging.info(f"{format_timestamp(None)} database connected")
        return db_cloud
    except Exception as exc:
        logging.error(f"{format_timestamp(None)} database init failed: {exc!r}")
        return None


def reconnect_db():
    logging.warning(f"{format_timestamp(None)} reconnecting database")
    return init_db()


def tcp_probe(host, port, timeout=3.0):
    with socket.create_connection((host, port), timeout=timeout):
        return True


async def wait_for_port(host, port, attempts=5, timeout=3.0, delay=1.0):
    last_err = None
    for _ in range(attempts):
        try:
            await asyncio.to_thread(tcp_probe, host, port, timeout)
            return
        except Exception as exc:
            last_err = exc
            await asyncio.sleep(delay)
    raise last_err


def _to_local_aware(dt):
    if dt is None:
        return datetime.now().astimezone()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).astimezone()
    return dt.astimezone()


def format_timestamp(dt):
    return _to_local_aware(dt).isoformat()


def format_db_timestamp(dt):
    local_dt = _to_local_aware(dt).replace(tzinfo=None)
    return local_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def choose_timestamp(data_value):
    if TIMESTAMP_MODE == "local":
        return datetime.now().astimezone()
    if TIMESTAMP_MODE == "source":
        return data_value.SourceTimestamp or data_value.ServerTimestamp or datetime.now().astimezone()
    raise ValueError("TIMESTAMP_MODE must be 'local' or 'source'")


async def monitor_pressure(table_name):
    db_cloud = init_db()
    try:
        while True:
            client = None
            try:
                await wait_for_port(HOST, PORT, attempts=5, timeout=3.0, delay=1.0)

                client = Client(
                    url=URL,
                    timeout=CONNECT_TIMEOUT,
                    watchdog_intervall=WATCHDOG_INTERVAL,
                )
                client.session_timeout = 600_000

                await client.connect()
                node = client.get_node(PRESSURE_NODE_ID)
                logging.info(f"{format_timestamp(None)} connected")

                while True:
                    data_value = await node.read_data_value()
                    raw_pressure = float(data_value.Value.Value)
                    pressure = float(f"{raw_pressure:.3g}")
                    ts = choose_timestamp(data_value)

                    logging.info(f"{format_timestamp(ts)} pressure={pressure:.3g}")

                    if db_cloud is None:
                        db_cloud = reconnect_db()

                    if db_cloud is not None:
                        try:
                            success = db_cloud.log(
                                table=table_name,
                                channels=np.array([pressure], dtype=float),
                                time=format_db_timestamp(ts),
                            )
                            if not success:
                                logging.warning(
                                    f"{format_timestamp(None)} failed to log pressure data to table '{table_name}'"
                                )
                                db_cloud = reconnect_db()
                        except Exception as exc:
                            logging.error(f"{format_timestamp(None)} database logging failed: {exc!r}")
                            db_cloud = reconnect_db()

                    # No catch-up behavior: always wait after each successful read.
                    await asyncio.sleep(POLL_INTERVAL)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.warning(f"{format_timestamp(None)} reconnecting after error: {exc!r}")
                # No catch-up here either: reconnect and continue on the next loop.
                await asyncio.sleep(RETRY_DELAY)
            finally:
                if client is not None:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
    finally:
        if db_cloud is not None:
            try:
                db_cloud.close()
            except Exception:
                pass


def parse_args():
    parser = argparse.ArgumentParser(description="Pfeiffer HiCube Neo OPC UA pressure logger")
    parser.add_argument(
        "--table",
        default=TABLE_NAME,
        help=f"Database table name for pressure logs (default: {TABLE_NAME})",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if TIMESTAMP_MODE not in {"local", "source"}:
        raise ValueError("TIMESTAMP_MODE must be 'local' or 'source'")

    try:
        asyncio.run(monitor_pressure(args.table))
    except KeyboardInterrupt:
        logging.info(f"{format_timestamp(None)} keyboard interrupt received; exiting")


if __name__ == "__main__":
    main()
