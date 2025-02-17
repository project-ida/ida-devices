import serial
import time
import math
import re

class GPIBPowerSupply:
    def __init__(self, port="/dev/ttyUSB0", gpib_address=22, baudrate=115200):
        self.safe_delay = 0.2
        self.gpib_address = gpib_address

        # Initialize Serial Connection
        try:
            self.ser = serial.Serial(port, baudrate, timeout=0.5)
            self.sendCmd(f"++addr {gpib_address}")  # Set GPIB Address
            self.sendCmd("++auto 1")  # Enable auto-read mode
            self.turn_off()  # Ensure the device is off on startup
        except serial.SerialException as e:
            print(f"Error opening serial port: {e}")

    def sendCmd(self, cmd, read_response=True):
        """ Send a command via GPIB and optionally read the response. """
        print(f"Sending: {cmd}")
        self.ser.write(bytes(cmd + "\n", "utf-8"))
        time.sleep(self.safe_delay)

        if read_response:
            response = self.ser.read(256).decode(errors="ignore").strip()
            return response
        return None

    def turn_on(self):
        """ Turn on the power supply. """
        self.sendCmd("F1X", read_response=False)

    def turn_off(self):
        """ Turn off the power supply. """
        self.sendCmd("F0X", read_response=False)

    def untalk(self):
        """ Send UNT command. """
        self.sendCmd("UNT", read_response=False)

    def get_status(self):
        """ Get current and voltage from the power supply status. """
        self.untalk()
        time.sleep(self.safe_delay)
        response = self.sendCmd("G2X")

        if response:
            current, voltage = self.parse_status(response)
            print(f"Latest Current: {current} A, Latest Voltage: {voltage} V")
            return current, voltage

        return None, None

    def parse_status(self, response):
        """
        Extracts the latest current (after last 'ODCI') and voltage limit (after last 'V') from a status string.

        Args:
            response (str): The raw status string.

        Returns:
            tuple: (current in A, voltage in V) or (None, None) if parsing fails.
        """
        try:
            # Find the last occurrence of "ODCI" followed by a number
            current_matches = re.findall(r"ODCI[+-]?\d+\.\d+E[+-]?\d+", response)
            voltage_matches = re.findall(r"V[+-]?\d+\.\d+E[+-]?\d+", response)

            latest_current = float(current_matches[-1][4:]) if current_matches else None
            latest_voltage = float(voltage_matches[-1][1:]) if voltage_matches else None

            return latest_current, latest_voltage

        except Exception as e:
            print(f"Error parsing status: {e}")
            return None, None

    def set_current(self, current):  # Input in milliAmps
        """ Set the output current in mA. """
        if current > 100:
            current = 100  # Safety limit of 100 mA
        if current > -1:
            print(f"Setting current to {current} mA")
            current = float(current) / 1000  # Convert to Amps
            self.sendCmd(f"I{current}", read_response=False)
            time.sleep(self.safe_delay)
            self.sendCmd("D0X", read_response=False)

    def set_voltage_limit(self, voltage):
        """ Set the voltage limit. """
        self.sendCmd(f"V{voltage}", read_response=False)
        print(f"Set voltage limit to {voltage}V")
        time.sleep(self.safe_delay)
        self.sendCmd("D1X", read_response=False)
        time.sleep(self.safe_delay * 5)
        self.sendCmd("D0X", read_response=False)
