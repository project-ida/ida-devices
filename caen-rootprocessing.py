import uproot
import numpy as np
import matplotlib.pyplot as plt
import os
import time
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import psycopg2
import re
import pandas as pd

# Add the parent directory (../) to the Python path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Monitor and process ROOT files in a specified directory.")
parser.add_argument("--source", required=True, help="Path to the directory containing ROOT files")
args = parser.parse_args()

# Set the data folder from the command-line argument
data_folder = args.source

# Set PSD thresholds for each channel
PSD_THRESHOLDS = {
    '0': 0.16,
    '1': 1.0,
    '2': 0.12,
    # Add more channels and their corresponding thresholds as needed
}

def fiducial_curve(x, *p):
    x = x.astype(float)

    return p[0] * np.exp(-x / p[1]) + p[2] * x + p[3]

# PostgreSQL connection details
from psql_credentials import PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

# Connect to PostgreSQL database
def connect_to_db():
    conn = psycopg2.connect(
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        host=PGHOST,
        port=PGPORT
    )
    return conn

# Function to insert CPS data into the database
def insert_cps_to_db(conn, table_name, time_value, cps_data):
    cps_data = [float(cps) for cps in cps_data]  # Convert NumPy values to Python floats
    with conn.cursor() as cur:
        query = f"""
            INSERT INTO {table_name} (time, channels)
            VALUES (%s, %s::double precision[]);
        """
        cur.execute(query, (time_value, cps_data))
    conn.commit()

# Function to insert energy spectrum into the database
def insert_spectrum_to_db(conn, table_name, time_value, energy_spectrum):
    energy_spectrum_list = energy_spectrum.tolist()
    energy_spectrum_str = ",".join(map(str, energy_spectrum_list))
    with conn.cursor() as cur:
        query = f"""
            INSERT INTO {table_name} (time, channels)
            VALUES (%s, %s::double precision[]);
        """
        cur.execute(query, (time_value, '{' + energy_spectrum_str + '}'))
    conn.commit()


# Function to get the last modified time of the earliest file in the folder
def get_earliest_file_last_modified_time(folder):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(('.root', '.root2'))]
    creation_times = [(file, os.path.getmtime(file)) for file in files]
    earliest_file, earliest_time = min(creation_times, key=lambda x: x[1])
    earliest_datetime = datetime.fromtimestamp(earliest_time)
    return earliest_file, earliest_datetime, earliest_time

# Function to get the time span covered by the ROOT file
def get_time_span_from_root(file_path):
    try:
        with uproot.open(file_path) as file:
            tree = file["Data_R"]
            timetag = tree["Timestamp"].array(library="np") * 1e-12
            start_time_relative = min(timetag)
            end_time_relative = max(timetag)
            return end_time_relative - start_time_relative
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")
        return None

# Function to estimate the acquisition start time
def estimate_acquisition_start(file_path):
    earliest_file, earliest_datetime, earliest_timestamp = get_earliest_file_last_modified_time(data_folder)
    print(f"Last modified time from earliest file: {earliest_datetime}")
    time_span_seconds = get_time_span_from_root(earliest_file)
    if time_span_seconds is None:
        return None
    acquisition_start_datetime = earliest_datetime - timedelta(seconds=time_span_seconds)
    acquisition_start_timestamp = earliest_timestamp - time_span_seconds
    return acquisition_start_datetime, acquisition_start_timestamp

# Function to check if the ROOT file is ready
def is_root_file_ready(file_path, tree_name='Data_R'):
    try:
        with uproot.open(file_path) as file:
            return tree_name in file
    except Exception as e:
        print(f"File {file_path} not ready: {e}")
        return False

# Function to get the table name for neutron and gamma data based on the file name
def get_table_name_from_filename(file_path, particle_type='neutron', data_type='history'):
    match = re.search(r'CH(\d)', file_path)
    if match:
        channel_number = match.group(1)
        return f"caen4ch_{particle_type}s_caen{channel_number}_{data_type}"
    else:
        raise ValueError(f"Could not extract channel number from file name: {file_path}")

# Function to get the channel number from the file name
def get_channel_number_from_filename(file_path):
    match = re.search(r'CH(\d)', file_path)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Could not extract channel number from file name: {file_path}")

# Function to process the ROOT file
def process_root_file(file_path):
    
    # Extract the channel number and get the corresponding PSD threshold
    channel_number = get_channel_number_from_filename(file_path)
    channel_number_int = int(channel_number) 
    psd_threshold = PSD_THRESHOLDS.get(channel_number, 0.15)  # Default to 0.15 if not specified
    
    if not is_root_file_ready(file_path):
        #print(f"File {file_path} does not have a valid ROOT structure. Skipping.")
        return False
    
    try:
        print("----------START-------------")
        
        acquisition_start_datetime, acquisition_start_timestamp = estimate_acquisition_start(file_path)
        print(f"--> acquisition_start_datetime: {acquisition_start_datetime}")
        if acquisition_start_timestamp is None:
            print(f"Skipping file {file_path} due to missing acquisition start information.")
            return False
            
        # Load CSV files
        fiducial_params_gammas_df = pd.read_csv("fiducial_params_gammas.csv")
        fiducial_params_neutrons_df = pd.read_csv("fiducial_params_neutrons.csv")     
        
        # Check if the channel exists in both files
        if channel_number_int in fiducial_params_gammas_df["ch"].values and channel_number_int in fiducial_params_neutrons_df["ch"].values:
            fiducial_params_gammas = fiducial_params_gammas_df[fiducial_params_gammas_df["ch"] == channel_number_int].iloc[:, 1:].values.flatten()
            fiducial_params_neutrons = fiducial_params_neutrons_df[fiducial_params_neutrons_df["ch"] == channel_number_int].iloc[:, 1:].values.flatten()
            use_fiducial_curves = True
        else:
            fiducial_params_gammas = None
            fiducial_params_neutrons = None
            use_fiducial_curves = False        
            
        with uproot.open(file_path) as file:
        
            tree = file["Data_R"]
            branches_to_import = ["Timestamp", "Energy","EnergyShort"]
            df = tree.arrays(branches_to_import,library="pd")
            df["PSP"] = (df['Energy']-df['EnergyShort'])/df['Energy']
            df["Timestamp"] = df["Timestamp"]/1e12
            
            dfn = df.copy()            
            dfg = df.copy()
            
            if use_fiducial_curves:
                print(f"Using fiducial parameters for channel {channel_number}:")
                print(f"  Fiducial Params (Gammas): {fiducial_params_gammas}")
                print(f"  Fiducial Params (Neutrons): {fiducial_params_neutrons}")

                # Selection of neutron events only
                gamma_up_values = fiducial_curve(df["Energy"], *fiducial_params_gammas)
                dfn = dfn[dfn["PSP"] > gamma_up_values]
                neutron_up_values = fiducial_curve(dfn["Energy"], *fiducial_params_neutrons)
                dfn = dfn[dfn["PSP"] < neutron_up_values]

                # Selection of gamma events only
                gamma_up_values = fiducial_curve(df["Energy"], *fiducial_params_gammas)
                dfg = dfg[dfg["PSP"] < gamma_up_values]

            else:
                print(f"Using default PSD threshold for channel {channel_number}: {psd_threshold}")
                
                dfn = dfn[dfn["PSP"] > psd_threshold] 
                dfg = dfg[dfg["PSP"] < psd_threshold]


            if len(df["Timestamp"]) == 0:
                print(f"No data found in {file_path}")
                return False

            table_name_neutron_history = get_table_name_from_filename(file_path, 'neutron', 'historynew')
            table_name_neutron_spectrum = get_table_name_from_filename(file_path, 'neutron', 'spectrumnew')
            table_name_gamma_history = get_table_name_from_filename(file_path, 'gamma', 'historynew')
            table_name_gamma_spectrum = get_table_name_from_filename(file_path, 'gamma', 'spectrumnew')
            
            gamma_abs_times = dfg["Timestamp"] + acquisition_start_timestamp
            neutron_abs_times = dfn["Timestamp"] + acquisition_start_timestamp
            
            min_timetag = min(df["Timestamp"])
            max_timetag = max(df["Timestamp"])
            print(f"current file min timetag: {min_timetag}")
            print(f"current file max timetag: {max_timetag}")            
            start_time = min(df["Timestamp"]) + acquisition_start_timestamp
            end_time = max(df["Timestamp"]) + acquisition_start_timestamp
            # Convert to human-readable datetime
            start_time_human = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
            end_time_human = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
            print(f"current file start time: {start_time_human}")
            print(f"current file end time: {end_time_human}")

            time_bins = np.arange(start_time, end_time + 1, 1)
            
            gamma_cps, _ = np.histogram(gamma_abs_times, bins=time_bins)
            neutron_cps, _ = np.histogram(neutron_abs_times, bins=time_bins)
            
            time_axis = [datetime.fromtimestamp(t) for t in (time_bins[:-1] + time_bins[1:]) / 2]
            
            conn = connect_to_db()
            
            for i, t in enumerate(time_axis[:-1]):  # Exclude the last time bin
                time_value = t.strftime('%Y-%m-%d %H:%M:%S')

                # Neutron CPS data
                neutron_cps_data = [neutron_cps[i]]
                insert_cps_to_db(conn, table_name_neutron_history, time_value, neutron_cps_data)

                # Gamma CPS data
                gamma_cps_data = [gamma_cps[i]]
                insert_cps_to_db(conn, table_name_gamma_history, time_value, gamma_cps_data)      
                
            gamma_energy_spectrum, _ = np.histogram(dfg['Energy'], bins=100) #, range=(0, max(df['Energy'])))
            gamma_spectrum_time_value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            insert_spectrum_to_db(conn, table_name_gamma_spectrum, gamma_spectrum_time_value, gamma_energy_spectrum)

            neutron_energy_spectrum, _ = np.histogram(dfn['Energy'], bins=100) #, range=(0, max(df['Energy'])))
            neutron_spectrum_time_value = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            insert_spectrum_to_db(conn, table_name_neutron_spectrum, neutron_spectrum_time_value, neutron_energy_spectrum)            

            conn.close()
            
            #print(f"File processed.")
            return True
            
    except OSError as e:
        print(f"Failed to process {file_path}: {e}")
        return False

import os

# Dictionary to track processed files
processed_files = {}

# Monitor folder for modified ROOT files
class ModifiedFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith(".root"):
            file_path = event.src_path  # File path for the current event

            # Check if the file has already been successfully processed
            if file_path in processed_files:
                print(f"Skipping {file_path} because it has already been processed.")
                return  # Skip processing this file

            # Debugging output
            #print(f"Processing file: {file_path}")
            #print(f"Current processed_files dictionary: {processed_files}")

            try:
                # Attempt to process the file
                
                file_processed = process_root_file(file_path)
                
                if file_processed:
                    # Mark the file as processed
                    processed_files[file_path] = True
                    
                    print(f"File processed successfully: {file_path}")

                    # Rename the file to end with .root2
                    new_file_path = file_path + "2"
                    os.rename(file_path, new_file_path)
                    print(f"File renamed to: {new_file_path}")
                    print("-----------END------------")
                #else:
                #    print(f"File was not processed successfully: {file_path}")
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")



if __name__ == "__main__":
    event_handler = ModifiedFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=data_folder, recursive=False)

    print(f"Monitoring directory: {data_folder}")
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

