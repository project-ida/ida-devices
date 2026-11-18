"""Read-only Heinzinger EVO Ethernet communication (standard library only).

EvoDevice owns the socket, SCPI allowlist, reply parsing, identity checks and
query pacing. It knows nothing about ROOT, CSV, PostgreSQL or the web viewer.
The acquisition script decides when to reconnect and how to process readings.

    with EvoDevice("192.168.0.100") as device:
        print(device.identity, device.settings_snapshot)
        for reading in device.measurements():  # One sequential V/I cycle.
            print(reading.channel, reading.time, reading.value)

Reading.time is the PC request time in Unix microseconds (UTC), not an ADC
timestamp. Voltage is in V; current is in signed mA. At most 250 commands/s
are sent in total. No settings, output-switching or reset commands are allowed.

Optional stop/on_idle/on_notification arguments let an acquisition loop stop
promptly, advance its clock while waiting, and report supply notifications.
They do not give this module access to application state. InstrumentLease
provides the real acquisition script's cross-process connection reservation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import math
from pathlib import Path
import re
import socket
import sys
import tempfile
import threading
import time


DEFAULT_PORT = 6000
DEFAULT_SERIAL = "464222713"
QUERY_TIMEOUT = 2.0
MEASUREMENTS = ("MEAS:VOLT?", "MEAS:CURR?")
CHANNELS = {"MEAS:VOLT?": "voltage", "MEAS:CURR?": "current"}
SNAPSHOT_COMMANDS = ("*OPT?", "SYST:SET?", "OUTP:STAT?", "OUTP:POL?", "VOLT?",
                     "CURR?", "CURR:LIM?", "CURR:PROT?", "CURR:PROT:MOD?", "STAT:OPER?")
READ_ONLY_COMMANDS = frozenset(("*IDN?",) + SNAPSHOT_COMMANDS + MEASUREMENTS)


def split_reply(raw):
    payload, separator, notification = raw.partition(";")
    notification = notification.strip()
    if separator and notification not in {"!SRQ!", "!RQS!"}:
        raise ValueError("Unrecognised response suffix")
    return payload.strip(), notification


def parse_reply(command, raw):
    """Adapted from the supplied reader: current readbacks are signed mA."""
    if command not in READ_ONLY_COMMANDS:
        raise ValueError("Command is outside the read-only allowlist")
    payload, notification = split_reply(raw)
    if not payload:
        raise ValueError("Empty reply")
    if command in {"*IDN?", "*OPT?", "SYST:SET?", "OUTP:POL?"}:
        if command == "SYST:SET?" and payload not in {"ETHTCP", "ETHHTTP", "UART", "LOC"}:
            raise ValueError("Invalid control-owner response")
        if command == "OUTP:POL?" and payload not in {"POS", "NEG"}:
            raise ValueError("Invalid polarity response")
        return None, notification
    unit = ("V" if command in {"VOLT?", "MEAS:VOLT?"} else
            "mA" if command in {"CURR?", "MEAS:CURR?", "CURR:LIM?", "CURR:PROT?"} else "")
    match = re.fullmatch(r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?)\s*([A-Za-z]*)", payload)
    if not match or (match[2] and match[2] != unit):
        raise ValueError(f"Malformed value or incorrect units: {raw!r}")
    value = float(match[1])
    if not math.isfinite(value):
        raise ValueError("Non-finite measurement")
    if command in {"OUTP:STAT?", "CURR:PROT:MOD?"} and value not in (0, 1):
        raise ValueError("Invalid binary status")
    if command == "STAT:OPER?" and (value != int(value) or not 0 <= value <= 65535):
        raise ValueError("Invalid operating status")
    return value, notification


@dataclass
class Reading:
    channel: str
    time: int
    send_ns: int
    receive_ns: int
    value: float
    completed_us: int


@dataclass
class Reply:
    command: str
    raw: str
    value: float | None
    time: int
    send_ns: int
    receive_ns: int
    completed_us: int
    notification: str

    def as_reading(self):
        if self.command not in CHANNELS or self.value is None:
            raise ValueError("Only measurement replies can be converted to readings")
        return Reading(CHANNELS[self.command], self.time, self.send_ns,
                       self.receive_ns, self.value, self.completed_us)


class InstrumentLease:
    """One real reader per serial/endpoint; OS locks disappear after a crash."""
    def __init__(self, host, port, serial):
        self.keys = (f"serial:{serial}", f"endpoint:{host.lower()}:{port}")
        self.handles = []

    def acquire(self):
        try:
            for key in self.keys:
                name = "HeinzingerEVO-" + hashlib.sha256(key.encode()).hexdigest()
                if sys.platform == "win32":
                    import ctypes
                    from ctypes import wintypes
                    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
                    kernel.CreateMutexW.argtypes = (ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR)
                    kernel.CreateMutexW.restype = wintypes.HANDLE
                    kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
                    kernel.CloseHandle.restype = wintypes.BOOL
                    ctypes.set_last_error(0)
                    handle = kernel.CreateMutexW(None, False, "Global\\" + name)
                    error = ctypes.get_last_error()
                    if not handle:
                        raise OSError("Cannot reserve the EVO connection")
                    if error == 183:
                        kernel.CloseHandle(handle)
                        raise RuntimeError("Another reader is already using this EVO serial or address")
                    self.handles.append((kernel, handle))
                else:
                    import fcntl
                    stream = (Path(tempfile.gettempdir()) / (name + ".lock")).open("a+b")
                    try:
                        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError:
                        stream.close()
                        raise RuntimeError("Another reader is already using this EVO serial or address") from None
                    self.handles.append((None, stream))
        except Exception:
            self.close()
            raise

    def close(self):
        for kernel, handle in reversed(self.handles):
            if kernel is None:
                handle.close()
            else:
                kernel.CloseHandle(handle)
        self.handles.clear()


class EvoDevice:
    """One EVO connection, reusable across reconnects; yields independent V/I readings."""
    def __init__(self, host, port=DEFAULT_PORT, serial=DEFAULT_SERIAL, qps=250,
                 timeout=QUERY_TIMEOUT, stop=None, on_idle=None, on_notification=None):
        if not host or not host.strip():
            raise ValueError("Provide an EVO hostname or IP address")
        if not math.isfinite(qps) or not 0 < qps <= 250:
            raise ValueError("EVO query rate must be positive and no greater than 250/s")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("EVO timeout must be positive and finite")
        if not 1 <= port <= 65535 or not serial.strip():
            raise ValueError("Provide a valid EVO port and non-empty serial")
        self.host, self.port, self.serial = host.strip(), port, serial.strip()
        self.qps, self.timeout = qps, timeout
        self.stop = stop if stop is not None else threading.Event()
        self.on_idle, self.on_notification = on_idle, on_notification
        self.sock = None
        self.last_send_ns = 0
        self.identity = None
        self.settings_snapshot = {}
        self.settings_captured_utc = None
        self._first_identity = None

    def connect(self):
        """Connect and validate identity/settings; close on an incomplete handshake."""
        self.close()
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.last_send_ns = 0
        try:
            identity = split_reply(self.query("*IDN?").raw)[0]
            fields = [field.strip() for field in identity.split(",")]
            if len(fields) != 4 or fields[0] != "Heinzinger" or fields[2] != self.serial:
                raise ValueError(f"Unexpected device identity: {identity!r}")
            if self._first_identity is not None and self._first_identity != identity:
                raise ValueError("Device identity changed after reconnection")
            self._first_identity = identity
            settings = {command: self.query(command).raw for command in SNAPSHOT_COMMANDS}
            self.identity = identity
            self.settings_snapshot = settings
            self.settings_captured_utc = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        except Exception:
            self.close()
            raise
        return self

    def close(self):
        sock, self.sock = self.sock, None
        if sock is not None:
            sock.close()

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_args):
        self.close()

    def query(self, command):
        """Return a timestamped reply; reject setters before anything is sent."""
        if command not in READ_ONLY_COMMANDS:
            raise ValueError("Refusing a non-read-only command")
        if self.sock is None:
            raise RuntimeError("Connect to the EVO before querying it")
        due = self.last_send_ns + int(1e9 / self.qps)
        while not self.stop.is_set():
            remaining = (due - time.perf_counter_ns()) / 1e9
            if remaining <= 0:
                break
            if remaining > .1:
                if self.on_idle is not None:
                    self.on_idle()
                self.stop.wait(min(.1, remaining))
            else:
                time.sleep(remaining)
        if self.stop.is_set():
            raise InterruptedError("Stopping")
        stamp = time.time_ns() // 1000
        send_ns = time.perf_counter_ns()
        self.last_send_ns = send_ns
        try:
            self.sock.settimeout(self.timeout)
            self.sock.sendall((command + "\n").encode("ascii"))
            deadline = time.monotonic() + self.timeout
            data = bytearray()
            while b"\n" not in data:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("EVO reply deadline expired")
                self.sock.settimeout(remaining)
                chunk = self.sock.recv(min(4096, 4097 - len(data)))
                if not chunk:
                    raise ConnectionError("EVO closed before completing its reply")
                data.extend(chunk)
                if len(data) > 4096:
                    raise ValueError("EVO reply exceeds 4096 bytes")
            receive_ns = time.perf_counter_ns()
            line, extra = bytes(data).split(b"\n", 1)
            if extra:
                raise ValueError("Extra reply bytes; refusing to associate them with another query")
            raw = line.rstrip(b"\r").decode("ascii")
            value, notification = parse_reply(command, raw)
        except (OSError, ValueError):
            # A partial/invalid response must never be reused for the next query.
            self.close()
            raise
        if notification and self.on_notification is not None:
            self.on_notification(command, notification)
        reply = Reply(command, raw, value, stamp, send_ns, receive_ns,
                      time.time_ns() // 1000, notification)
        if command not in CHANNELS and self.on_idle is not None:
            self.on_idle()
        return reply

    def measurements(self):
        """Yield voltage then current, each immediately after its own reply.

        The caller processes each reading before the next command is sent.
        This preserves the order of readings and any averaging watermarks.
        """
        for command in MEASUREMENTS:
            yield self.query(command).as_reading()
