import subprocess
import socket
import time
import platform

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
    """Reset Wi-Fi adapter in a cross-platform way."""
    print("Resetting Wi-Fi...")

    try:
        if platform.system() == "Linux":
            # Ubuntu / Debian with NetworkManager
            subprocess.run(["nmcli", "radio", "wifi", "off"], check=True)
            time.sleep(2)
            subprocess.run(["nmcli", "radio", "wifi", "on"], check=True)
        
        elif platform.system() == "Windows":
            # Disable & enable Wi-Fi adapter via netsh
            adapter = "Wi-Fi 2"  # Run "netsh interface show interface" to find the correct name
            subprocess.run(["netsh", "interface", "set", "interface", adapter, "admin=disable"], check=True)
            time.sleep(2)
            subprocess.run(["netsh", "interface", "set", "interface", adapter, "admin=enable"], check=True)
        
        else:
            print("reset_wifi not implemented for this OS")
        
        time.sleep(10)  # Wait for reconnection
    
    except subprocess.CalledProcessError as e:
        print(f"Error resetting Wi-Fi: {e}")
