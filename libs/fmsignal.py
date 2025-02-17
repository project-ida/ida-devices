import numpy as np
from matplotlib import pyplot as plt

class fmsignal:
    def __init__(self):
        """Initialize an empty signal array."""
        self.signal = np.array([])

    def hold(self, value, duration=None, until=None):
        """Append a hold (constant value) to the signal.
        Can specify duration or until for absolute positioning.
        """
        if duration is not None:
            duration = int(duration)  
            constant_segment = np.full(duration, value)
        elif until is not None:
            if until <= len(self.signal):
                return  # If 'until' is less than current length, do nothing
            duration = until - len(self.signal)
            constant_segment = np.full(duration, value)
        else:
            raise ValueError("Either 'duration' or 'until' must be specified for hold.")
        self.signal = np.concatenate([self.signal, constant_segment])

    def ramp(self, startvalue, endvalue, duration):
        """Append a ramp (linear change) to the signal."""
        ramp_segment = np.linspace(startvalue, endvalue, num=duration)
        self.signal = np.concatenate([self.signal, ramp_segment])

    def plot(self):
        """Plot the signal."""
        plt.figure(figsize=(10, 4))
        plt.plot(self.signal, label='Profile')
        plt.xlabel('Time (steps)')
        plt.ylabel('Signal')
        plt.title('Generated Profile')
        plt.legend()
        plt.grid(True)
        plt.show()

    def get_array(self):
        """Get the numpy array of the signal."""
        return self.signal

    def square_ramp(self, startvalue, endvalue, duration):
        """Append a square ramp based on a square root function."""
        # Generate the square ramp using a square root function
        time_points = np.linspace(0, 1, duration)  # Normalized time points (0 to 1)
        square_segment = np.sqrt(time_points) * (endvalue - startvalue) + startvalue
        self.signal = np.concatenate([self.signal, square_segment])

    def convert_to_voltage(self):
        """Convert a linear ramp (assumed to be temperatures) into corresponding voltage values."""
        # Coefficients for the linear temperature-to-power model
        slope = 11.564
        intercept = 63.613

        # Assume resistance is 1 Ohm
        resistance = 1  # Ohms

        # Convert temperature to power
        power_signal = (self.signal - intercept) / slope

        # Convert power to voltage, ensuring no negative power
        voltage_signal = np.sqrt(np.maximum(power_signal * resistance, 0))

        return voltage_signal
