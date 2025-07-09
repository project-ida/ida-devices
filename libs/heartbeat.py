import requests
import os
import sys
from datetime import datetime

# Get the directory of the calling script
caller_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

# Attempt to import https://healthchecks.io/ UUID from the caller's directory
try:
    # Temporarily add caller's directory to sys.path
    sys.path.append(caller_dir)
    from healthchecks_credentials import UUID
except ImportError as e:
    print(f"[{datetime.now()}] Failed to import from {caller_dir}/healthchecks_credentials: {e}")
    UUID = None
finally:
    # Clean up sys.path to avoid side effects
    if caller_dir in sys.path:
        sys.path.remove(caller_dir)

def send_heartbeat():
    """
    Pings healthchecks.io to signal that our data servers are still alive.

    Returns:
        bool: True if the ping was sent successfully, False otherwise.
    """
    try:
        response = requests.get(f"https://hc-ping.com/{UUID}", timeout=10)
        response.raise_for_status()  # Raise an exception for 4xx/5xx errors

        if response.text.strip() == "OK":
            return True
        else:
            print(f"[{datetime.now()}] Heartbeat send failed: Unexpected response: {repr(response.text)}")
            return False

    except requests.RequestException as e:
        print(f"[{datetime.now()}] Heartbeat send failed due to network error: {e}")
        return False

