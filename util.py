from time import sleep, time, ctime
import numpy as np
import logging
import csv
from contextlib import contextmanager

class InstrumentError(Exception):
    """Raise this when talking to instruments fails."""
    pass

try:
    import serial
except ImportError:
    serial = None

def show_newlines(string):
    """Replace CR+LF with readable markers for debugging."""
    return string.replace('\r', '<CR>').replace('\n', '<LF>')

if serial:
    class Serial(serial.Serial):
        """Extended PySerial Serial class with logging and auto-appended termination characters."""
        def __init__(self, *args, log=False, term_chars='', **kwargs):
            self.logger = logging.getLogger('wanglib.util.Serial')
            self.logfile = log
            if self.logfile:
                self.start_logging(self.logfile)
            self.term_chars = term_chars
            super().__init__(*args, **kwargs)

        def start_logging(self, fname):
            lfh = logging.FileHandler(fname)
            self.logger.addHandler(lfh)
            self.logger.setLevel(logging.DEBUG)
            self.logger.debug('opened serial port')

        def write(self, data):
            data += self.term_chars
            super().write(data.encode())
            self.logger.debug('write: ' + show_newlines(data))

        def read(self, size=1):
            resp = super().read(size).decode()
            self.logger.debug(' read: ' + show_newlines(resp))
            return resp

        def readall(self, term_chars=None):
            resp = self.read(self.in_waiting)
            term_chars = term_chars or self.term_chars
            while term_chars and not resp.endswith(term_chars):
                resp += self.read(self.in_waiting)
            return resp

        def ask(self, query, lag=0.05):
            self.write(query)
            sleep(lag)
            return self.readall()

def num(string):
    """Convert string to integer or float."""
    return int(string) if '.' not in string else float(string)

def sciround(number, sigfigs=1):
    """Round a number to the desired number of significant figures."""
    exponent = np.floor(np.log10(number))
    return round(number, -int(exponent) + (sigfigs - 1))

@contextmanager
def notraceback():
    """Context manager to swallow keyboard interrupts."""
    try:
        yield
    except KeyboardInterrupt:
        pass

def save(fname, array):
    """Save a Numpy array to file, preventing overwrites."""
    fname = fname if fname.endswith('.npy') else fname + '.npy'
    try:
        open(fname, 'r')
    except IOError:
        np.save(fname, array)
    else:
        raise ValueError('File exists. Choose a different name.')

class Saver:
    """Sequential file saver."""
    def __init__(self, name, verbose=False):
        self.name = name
        self.n = 0
        self.verbose = verbose

    def save(self, array):
        fname = f"{self.name}{self.n:03d}.npy"
        save(fname, array)
        self.n += 1
        if self.verbose:
            print("saved as", fname)
