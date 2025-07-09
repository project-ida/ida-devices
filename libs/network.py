import subprocess
import socket

def internet_available(timeout=2):
    """
    Checks for general internet connectivity and DNS resolution.
    
    Returns True if:
    - Can establish a TCP connection to a public IP (8.8.8.8:53)
    - Can resolve a hostname via DNS (e.g. 'example.com')
    """
    try:
        # 1. Check outbound TCP connectivity
        socket.create_connection(("8.8.8.8", 53), timeout=timeout)

        # 2. Check DNS resolution works
        socket.gethostbyname("example.com")  # or "google.com", etc.

        return True
    except socket.error:
        return False

def reset_wifi():
    """Resets Wi-Fi using NetworkManager's nmcli."""
    print("Resetting Wi-Fi...")
    try:
        subprocess.run(["nmcli", "radio", "wifi", "off"], check=True)
        time.sleep(2)
        subprocess.run(["nmcli", "radio", "wifi", "on"], check=True)
        time.sleep(10)  # Give time for reconnect
    except subprocess.CalledProcessError as e:
        print(f"Error resetting Wi-Fi: {e}")