import prologix
import serial
import time
import math

class mysrs:

    def __init__(self, com_port, instrument_number):
        plx = prologix.prologix_USB(com_port)
        self.srs = plx.instrument(instrument_number)
        self.safe_delay = 0.2
        self.turned_off = 0
        self.turn_off()  # just in case
        time.sleep(self.safe_delay)

    def testing(self):
        self.srs.write('U0X')
        time.sleep(self.safe_delay)
        print(self.srs.read())
        self.srs.write('U1X')
        time.sleep(self.safe_delay)
        print(self.srs.read())
        self.srs.write('G1X')
        time.sleep(self.safe_delay)
        print(self.srs.read())
        time.sleep(self.safe_delay)

    def turn_on(self):
        self.srs.write("F1X")
        time.sleep(self.safe_delay)

    def turn_off(self):
        self.srs.write("F0X")
        time.sleep(self.safe_delay)

    def untalk(self):
        time.sleep(self.safe_delay * 2)
        self.srs.write('UNT')
        time.sleep(self.safe_delay * 2)

    def get_lim_voltage(self):
        self.srs.write('UNT')
        time.sleep(self.safe_delay * 2)
        self.srs.write('G4X')
        time.sleep(self.safe_delay * 2)
        self.srs.read()  # clearing buffer
        time.sleep(self.safe_delay * 2)
        self.srs.write('G4X')
        time.sleep(self.safe_delay)
        returnstring = self.srs.read()
        time.sleep(self.safe_delay)
        self.srs.write('UNT')
        time.sleep(self.safe_delay)
        try:
            thispart = returnstring.split("V")[1].split(",")[0][1:]
            print("get_lim_voltage:", thispart)
            return float(thispart)
        except:
            return -1.0

    def get_set_current(self):
        self.srs.write('UNT')
        time.sleep(self.safe_delay * 2)
        self.srs.write('G4X')
        time.sleep(self.safe_delay * 2)
        self.srs.read()  # clearing buffer
        time.sleep(self.safe_delay * 2)
        self.srs.write('G4X')
        time.sleep(self.safe_delay)
        returnstring = self.srs.read()
        time.sleep(self.safe_delay)
        self.srs.write('UNT')
        time.sleep(self.safe_delay)
        try:
            thispart = returnstring.split("I")[1].split(",")[0][1:]
            print("get_set_current:", thispart)
            return float(thispart)
        except:
            return -1.0

    def set_current(self, current):  # comes in microAmps
        if current > 100000:
            current = 100000
        if current >= 0:
            print("current is", current)
            current = float(current) / 1e6  # Convert to Amps
            self.srs.write(f"I{current}")
            print("HERE wrote current to", current)
            time.sleep(self.safe_delay)
            self.srs.write("D0X")
            time.sleep(self.safe_delay)

    def set_voltage_limit(self, voltage):
        self.srs.write(f"V{voltage}")
        print("wrote voltage limit to", voltage)
        time.sleep(self.safe_delay)
        self.srs.write("D1X")
        time.sleep(self.safe_delay * 5)
        self.srs.write("D0X")
        time.sleep(self.safe_delay)
