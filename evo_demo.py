"""Local, read-only EVO simulator used by --demo in heinzinger-evo-powersupply.py.

All synthetic signal generation lives here. The acquisition script imports this
file only in demo mode, labels its output synthetic, and disables PostgreSQL,
ROOT-file registration and Telegram. No real instrument is contacted.
"""

import math
import socketserver
import threading
import time


DEMO_SERIAL = "SIMULATED-EVO"


class DemoServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class DemoHandler(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            line = self.rfile.readline(4097)
            if not line or len(line) > 4096:
                return
            try:
                command = line.decode("ascii").strip()
            except UnicodeDecodeError:
                return
            elapsed = time.monotonic() - self.server.started
            replies = {
                "*IDN?": f"Heinzinger,SIMULATED EVO,{DEMO_SERIAL},1.0",
                "*OPT?": "HMI,SWI,ARC", "SYST:SET?": "LOC", "OUTP:STAT?": "0",
                "OUTP:POL?": "NEG", "VOLT?": "-725.0", "CURR?": "-10.0",
                "CURR:LIM?": "-235.0", "CURR:PROT?": "-50.0",
                "CURR:PROT:MOD?": "0", "STAT:OPER?": "16",
                "MEAS:VOLT?": f"{-725 + 18 * math.sin(elapsed * 1.7):.4f} V",
                "MEAS:CURR?": f"{-10 + .8 * math.sin(elapsed * 2.3):.4f} mA",
            }
            if command not in replies:
                return
            time.sleep(.002)
            try:
                self.wfile.write((replies[command] + "\n").encode("ascii"))
                self.wfile.flush()
            except OSError:
                return


def start_simulator():
    server = DemoServer(("127.0.0.1", 0), DemoHandler)
    server.started = time.monotonic()
    threading.Thread(target=server.serve_forever, name="evo-simulator", daemon=True).start()
    return server
