from util import Serial
from socket import socket, AF_INET, SOCK_STREAM, IPPROTO_TCP
from time import sleep

class PrologixBase:
    """
    Base class for Prologix controllers (Ethernet/USB)
    """
    def __init__(self):
        self._addr = self.addr
        self._auto = self.auto

    @property
    def addr(self):
        """Gets/Sets the GPIB address of the currently addressed instrument."""
        self._addr = int(self.ask("++addr"))
        return self._addr
    
    @addr.setter
    def addr(self, new_addr):
        self._addr = new_addr
        self.write(f"++addr {new_addr}")

    @property
    def auto(self):
        """Boolean. Read-after-write setting."""
        self._auto = bool(int(self.ask("++auto")))
        return self._auto
    
    @auto.setter
    def auto(self, val):
        self._auto = bool(val)
        self.write(f"++auto {int(self._auto)}")

    def version(self):
        """Check the Prologix firmware version."""
        return self.ask("++ver")

    @property
    def savecfg(self):
        """Boolean. Determines whether the controller should save settings in EEPROM."""
        resp = self.ask("++savecfg")
        if resp == 'Unrecognized command':
            raise Exception("Prologix controller does not support ++savecfg. Update firmware or risk EEPROM wear.")
        return bool(int(resp))
    
    @savecfg.setter
    def savecfg(self, val):
        self.write(f"++savecfg {int(bool(val))}")

    def instrument(self, addr, **kwargs):
        return Instrument(self, addr, **kwargs)

class PrologixEthernet(PrologixBase):
    """Interface to a Prologix GPIB-Ethernet controller."""
    def __init__(self, ip):
        self.bus = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)
        self.bus.settimeout(5)
        self.bus.connect((ip, 1234))
        self.bus.send(b'++mode 1\n')
        super().__init__()
    
    def write(self, command, lag=0.1):
        self.bus.send(f"{command}\n".encode())
        sleep(lag)

    def readall(self):
        return self.bus.recv(100).decode().strip()
    
    def ask(self, query, *args, **kwargs):
        self.write(query, *args, **kwargs)
        return self.readall()

class PrologixUSB(PrologixBase):
    """Interface to a Prologix GPIB-USB controller."""
    def __init__(self, port='/dev/ttyUSBgpib', log=False):
        self.bus = Serial(port, baudrate=115200, rtscts=True, log=log)
        self.bus.readall()
        self.savecfg = False
        super().__init__()
    
    def write(self, command, lag=0.1):
        self.bus.write(f"{command}\r".encode())
        sleep(lag)
    
    def readall(self):
        return self.bus.readall().decode().strip()
    
    def ask(self, query, *args, **kwargs):
        self.readall()
        self.write(query, *args, **kwargs)
        return self.readall()

controllers = {}

def prologix_ethernet(ip):
    if ip not in controllers:
        controllers[ip] = PrologixEthernet(ip)
    return controllers[ip]

def prologix_USB(port='/dev/ttyUSBgpib', log=False):
    if port not in controllers:
        controllers[port] = PrologixUSB(port)
    return controllers[port]

class Instrument:
    """Represents an instrument attached to a Prologix controller."""
    def __init__(self, controller, addr, delay=0.1, auto=True):
        self.addr = addr
        self.auto = auto
        self.delay = delay
        self.controller = controller

    def _get_priority(self):
        if self.auto != self.controller._auto:
            self.controller.auto = self.auto
        if self.addr != self.controller._addr:
            self.controller.addr = self.addr

    def ask(self, command):
        self.write(command)
        return self.read()
    
    def read(self):
        self._get_priority()
        if not self.auto:
            self.controller.write('++read eoi', lag=self.delay)
        return self.controller.readall()
    
    def write(self, command):
        self._get_priority()
        self.controller.write(command, lag=self.delay)
