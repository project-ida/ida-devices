from PyQt5.QtCore import QTimer
import numpy as np
import socket
import argparse
from libs.fmsignal import fmsignal
import requests
import matplotlib
matplotlib.use('QtAgg')  # Requires PyQt5
import matplotlib.pyplot as plt
from matplotlib.widgets import Button, TextBox

def download_google_doc_as_text(url):
    """Convert Google Docs URL to text-download URL and download content as text."""
    base_url_part = url.split('/edit')[0]  # Split to remove '/edit' and any parameters after it.
    text_download_url = f"{base_url_part}/export?format=txt"
    response = requests.get(text_download_url)
    response.raise_for_status()  # Raises an HTTPError for bad responses
    return response.text

def execute_instructions(text, signal):
    text = text.lstrip('\ufeff')
    lines = text.strip().split('\n')
    for line in lines:
        exec(f'signal.{line}')

def generate_voltage_profile(signal):
    t = np.arange(len(signal.get_array()))
    voltage = signal.get_array()
    return t, voltage

running = False
text_box = None

def plot_profile(t, voltage, initial_t_value, doc_url):
    fig, ax = plt.subplots()
    plt.subplots_adjust(bottom=0.35)
    line, = ax.plot(t, voltage, label='Voltage Profile')
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Voltage (V)')
    plt.title('Preview Voltage Profile')
    
    start_index = np.abs(t - initial_t_value).argmin()
    pointer, = ax.plot([t[start_index]], [voltage[start_index]], 'ro', label='Current Position')
    time_text = ax.text(0.05, 0.95, 'Time: {:.2f}s'.format(t[start_index]), transform=ax.transAxes)
    
    axbox = plt.axes([0.25, 0.05, 0.15, 0.05])
    global text_box
    text_box = TextBox(axbox, 'Set Voltage:', initial=str(voltage[start_index]))

    # Display the document URL on the plot
    ax.text(0.5, 0.01, f'Document URL: {doc_url}', transform=ax.transAxes, fontsize=6, ha='center', color='gray')

    return fig, ax, line, pointer, time_text, start_index

def send_voltage(voltage, port):
    """ Send the new voltage value to the power supply control script. """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', port))
            s.sendall(str(voltage).encode())
            print(f"Voltage {voltage} sent to port {port}.")
    except Exception as e:
        print(f"Failed to send voltage due to: {str(e)}")

def update_profile(timer, t, voltage, pointer, time_text, port, fig):
    global running
    if not running or not plt.fignum_exists(fig.number):
        timer.stop()
        return

    try:
        # Move to the next point
        current_index = update_profile.current_index
        if current_index >= len(t):
            timer.stop()
            print("Profile execution completed.")
            return

        pointer.set_data([t[current_index]], [voltage[current_index]])
        time_text.set_text(f'Time: {t[current_index]:.2f}s')
        text_box.set_val(f"{voltage[current_index]:.2f}")
        send_voltage(voltage[current_index], port)
        fig.canvas.draw_idle()
        update_profile.current_index += 1
    except Exception as e:
        print(f"Error during profile update: {e}")
        timer.stop()

# Initialize the index as an attribute of the function
update_profile.current_index = 0

def on_start(event, timer):
    global running
    running = True
    timer.start(1000)  # Update every 1000 ms (1 second)

def on_pause(event):
    global running
    running = False

def on_send(event, port):
    new_voltage = float(text_box.text)
    print(f"Manually sending voltage: {new_voltage}")
    send_voltage(new_voltage, port)

def main():
    parser = argparse.ArgumentParser(description="Control and display voltage profile from a signal file.")
    parser.add_argument("--port", type=int, required=True, help="Port number to send voltage updates")
    parser.add_argument("--start", type=float, default=0, help="Start time for the profile")
    parser.add_argument("--signalfile", type=str, required=True, help="URL to the signal file containing the voltage profile")
    args = parser.parse_args()

    doc_text = download_google_doc_as_text(args.signalfile)
    signal = fmsignal()
    execute_instructions(doc_text, signal)

    t, voltage = generate_voltage_profile(signal)
    fig, ax, line, pointer, time_text, start_index = plot_profile(t, voltage, args.start, args.signalfile)
    update_profile.current_index = start_index

    start_ax = plt.axes([0.81, 0.05, 0.1, 0.075])
    pause_ax = plt.axes([0.70, 0.05, 0.1, 0.075])
    send_ax = plt.axes([0.45, 0.02, 0.2, 0.1])

    start_button = Button(start_ax, 'Start')
    pause_button = Button(pause_ax, 'Pause')
    send_button = Button(send_ax, 'Send Voltage')

    timer = QTimer()
    timer.timeout.connect(lambda: update_profile(timer, t, voltage, pointer, time_text, args.port, fig))

    start_button.on_clicked(lambda event: on_start(event, timer))
    pause_button.on_clicked(on_pause)
    send_button.on_clicked(lambda event: on_send(event, args.port))

    plt.show()

if __name__ == "__main__":
    main()
