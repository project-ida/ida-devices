import logging
from enum import Enum
from time import sleep
import serial

__all__ = ['KoradSerial', 'ChannelMode', 'OnOffState']

class ChannelMode(Enum):
    constant_current = 0
    constant_voltage = 1

class OnOffState(Enum):
    off = 0
    on = 1

class Status(object):
    def __init__(self, status):
        super(Status, self).__init__()
        self.raw = status
        self.channel1 = ChannelMode(status & 1)
        self.beep = OnOffState((status >> 4) & 1)
        self.lock = OnOffState((status >> 5) & 1)
        self.output = OnOffState((status >> 6) & 1)

    def __repr__(self):
        return "{0}".format(self.raw)

    def __str__(self):
        return f"Channel: {self.channel1.name}, Beep: {self.beep.name}, Lock: {self.lock.name}, Output: {self.output.name}"

def float_or_none(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

class KoradSerial(object):
    """ Wrapper for communicating with a programmable Korad KA3xxxP / KWR102 power supply. """

    class Serial(object):
        """ Serial communication handling. """
        def __init__(self, port, debug=False):
            super(KoradSerial.Serial, self).__init__()
            self.debug = debug
            self.port = serial.Serial(port, 9600, timeout=1)

        def read_character(self):
            return self.port.read(1).decode('ascii')

        def read_string(self, fixed_length=None):
            result = []
            c = self.read_character()
            while len(c) > 0 and ord(c) != 0:
                result.append(c)
                if fixed_length is not None and len(result) == fixed_length:
                    break
                c = self.read_character()
            return ''.join(result)

        def send(self, text):
            if self.debug:
                print("_send: ", text)
            sleep(0.1)
            self.port.write((text + "\r").encode('ascii'))

        def send_receive(self, text, fixed_length=None, retries=3, delay=0.2):
            for attempt in range(retries):
                try:
                    self.send(text)
                    sleep(delay)  

                    response = self.read_string().strip()
                    print(f"Raw Response from PSU: '{response}'")  

                    if not response:
                        raise ValueError("No response received from power supply.")

                    return response
                except Exception as e:
                    logging.warning(f"Retrying due to error: {e}")
                    sleep(delay)

            logging.error(f"Failed to get a valid response after {retries} attempts")
            return None

    def __init__(self, port, debug=False):
        super(KoradSerial, self).__init__()
        self.__serial = KoradSerial.Serial(port, debug)

        # Query device identity
        try:
            self._model = self.__serial.send_receive("*IDN?")
            sleep(0.2)
        except:
            self._model = None

        print(f"Detected model: {self._model}")

        self.is_single_channel = "KWR102" in self._model if self._model else False

        # If it's a single-channel PSU (KWR102), don't use `channels`
        if self.is_single_channel:
            self.voltage = self.SingleChannelVoltage(self.__serial)
            self.current = self.SingleChannelCurrent(self.__serial)
        else:
            self.channels = [KoradSerial.Channel(self.__serial, i) for i in range(1, 3)]

        self.output = KoradSerial.OnOffButton(self.__serial, "OUT1", "OUT0")
        self.output.on()
        sleep(0.2)

    class SingleChannelVoltage:
        def __init__(self, serial_):
            self.__serial = serial_

        @property
        def setpoint(self):
            return float_or_none(self.__serial.send_receive("VSET?", fixed_length=6))

        @setpoint.setter
        def setpoint(self, value):
            self.__serial.send(f"VSET:{value:05.2f}")

        @property
        def output(self):
            return float_or_none(self.__serial.send_receive("VOUT?", fixed_length=6))

    class SingleChannelCurrent:
        def __init__(self, serial_):
            self.__serial = serial_

        @property
        def setpoint(self):
            return float_or_none(self.__serial.send_receive("ISET?", fixed_length=6))

        @setpoint.setter
        def setpoint(self, value):
            self.__serial.send(f"ISET:{value:05.3f}")

        @property
        def output(self):
            return float_or_none(self.__serial.send_receive("IOUT?", fixed_length=6))

    class Channel:
        def __init__(self, serial_, channel_number):
            super(KoradSerial.Channel, self).__init__()
            self.__serial = serial_
            self.number = channel_number

        @property
        def current(self):
            return float_or_none(self.__serial.send_receive(f"ISET{self.number}?", fixed_length=6))

        @current.setter
        def current(self, value):
            self.__serial.send(f"ISET{self.number}:{value:05.3f}")

        @property
        def voltage(self):
            return float_or_none(self.__serial.send_receive(f"VSET{self.number}?", fixed_length=6))

        @voltage.setter
        def voltage(self, value):
            self.__serial.send(f"VSET{self.number}:{value:05.2f}")

        @property
        def output_voltage(self):
            return float_or_none(self.__serial.send_receive(f"VOUT{self.number}?", fixed_length=6))

        @property
        def output_current(self):
            return float_or_none(self.__serial.send_receive(f"IOUT{self.number}?", fixed_length=6))

    class OnOffButton:
        def __init__(self, serial_, on_command, off_command):
            super(KoradSerial.OnOffButton, self).__init__()
            self.__serial = serial_
            self._on = on_command
            self._off = off_command

        def on(self):
            self.__serial.send(self._on)

        def off(self):
            self.__serial.send(self._off)

    @property
    def model(self):
        return self._model

    def close(self):
        self.__serial.port.close()

    def open(self):
        self.__serial.port.open()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def status(self):
        status = self.__serial.send_receive("STATUS?")
        return Status(ord(status)) if status else None
