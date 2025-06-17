# process_root_files.py
#
# Purpose:
#   Processes ROOT files from a nuclear physics experiment to extract neutron and gamma
#   event timestamps, storing them in a PostgreSQL database. Neutrons are filtered by
#   an energy threshold, and events are discriminated using PSD thresholds or fiducial
#   curves. Tracks processed files in a CSV to avoid reprocessing.
#
# Functionality:
#   - Reads ROOT files from a RAW subfolder, matching a user-specified channel pattern (e.g., _CH0@).
#   - Extracts timestamps, energy, and energy-short from ROOT trees, computing PSP for particle discrimination.
#   - Filters neutrons above an energy threshold (from psd-thresholds.csv or default 0).
#   - Uses PSD thresholds or fiducial curves (from fiducial_params_gammas/neutrons.csv) to separate neutrons/gammas.
#   - Inserts timestamps with picosecond precision into database tables (e.g., caen8ch_neutrons_caen0_timestamps).
#   - Tracks processed files in processed_files.csv and skips already processed files.
#
# Requirements:
#   - Folder structure: Parent folder with a Compass .txt file (containing "Start time = ..." on line 2) and a RAW subfolder with ROOT files.
#   - Files:
#     - psql_credentials.py: Defines PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD for PostgreSQL.
#     - psd-thresholds.csv: Columns ch (int), psd-threshold (float), energy-threshold (float) for channel-specific thresholds.
#     - fiducial_params_gammas.csv, fiducial_params_neutrons.csv (optional): Fiducial curve parameters for advanced filtering.
#
# Usage:
#   - Run: python process_root_files.py
#   - Prompts for folder path and channel number (default 0).
#   - Creates processed_files.csv to track progress.
#   - Outputs timestamps to PostgreSQL tables with prefix caen8ch.
#
# Notes:
#   - Ensure Google Drive folders are marked "Available Offline" if used.
#   - Database tables must exist with schema: time (timestamp), channels (double precision[]), ps (bigint).
#   - Energy threshold defaults to 0 if not specified, including all neutrons.

import argparse
import os
import pandas as pd
import uproot
import psycopg2
from psycopg2.extras import execute_values
import re
from datetime import datetime
import numpy as np
from pathlib import Path
import glob

# PostgreSQL connection details (replace with your credentials)
from psql_credentials import PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

# Load PSD thresholds from CSV file
thresholds_file = "psd-thresholds.csv"
if os.path.exists(thresholds_file):
    psd_thresholds_df = pd.read_csv(thresholds_file)
    psd_thresholds_df["ch"] = psd_thresholds_df["ch"].astype(int)
    psd_thresholds_df["psd-threshold"] = psd_thresholds_df["psd-threshold"].astype(float)
    psd_thresholds_df["energy-threshold"] = psd_thresholds_df["energy-threshold"].astype(float)
    PSD_THRESHOLDS = {
        row["ch"]: (row["psd-threshold"], row["energy-threshold"])
        for _, row in psd_thresholds_df.iterrows()
    }
    print("Loaded PSD and Energy Thresholds:")
    for channel, (psd_thresh, energy_thresh) in PSD_THRESHOLDS.items():
        print(f"Channel {int(channel)}: PSD Threshold = {psd_thresh}, Energy Threshold = {energy_thresh}")
else:
    print(f"Warning: File '{thresholds_file}' not found.")
    PSD_THRESHOLDS = {}  # Empty dictionary to avoid errors

# Fiducial curve function
def fiducial_curve(x, *p):
    x = x.astype(float)
    return p[0] * np.exp(-x / p[1]) + p[2] * x + p[3]

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

# Function to insert event timestamps with picosecond precision
def insert_timestamps_to_db(conn, table_name, time_value, ps_data, ps):
    with conn.cursor() as cur:
        query = f"""
            INSERT INTO {table_name} (time, channels, ps)
            VALUES (%s, %s::double precision[], %s)
        """
        cur.execute(query, (time_value, ps_data, ps))
    conn.commit()

# Batched insert many timestamp events with picosecond precision
def insert_many_timestamps_to_db(conn, table_name, rows, batch_size=1000):
    with conn.cursor() as cur:
        query = f"""
            INSERT INTO {table_name} (time, channels, ps)
            VALUES %s
        """
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            execute_values(cur, query, batch)
    conn.commit()

# Function to get the table name based on file name and particle type
def get_table_name_from_filename(file_path, table_prefix, particle_type='neutron', data_type='timestamps'):
    match = re.search(r'CH(\d)', file_path)
    if match:
        channel_number = match.group(1)
        return f"{table_prefix}_{particle_type}s_caen{channel_number}_{data_type}"
    else:
        raise ValueError(f"Could not extract channel number from file name: {file_path}")

# Function to get channel number from file name
def get_channel_number_from_filename(file_path):
    match = re.search(r'CH(\d)', file_path)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Could not extract channel number from file name: {file_path}")

# Function to extract the number before .root for sorting
def get_file_number(filename):
    match = re.search(r'_(\d+)\.root', filename)
    return int(match.group(1)) if match else 0

# Function to extract acquisition start time from .txt file
def get_acquisition_start_from_txt(folder_path):
    try:
        # Find the first .txt file in the folder
        txt_files = [f for f in os.listdir(folder_path) if f.endswith('.txt')]
        if not txt_files:
            raise FileNotFoundError(f"No .txt file found in {folder_path}")
        
        txt_file = os.path.join(folder_path, txt_files[0])
        
        # Read the second line
        with open(txt_file, 'r') as f:
            lines = f.readlines()
            if len(lines) < 2:
                raise ValueError(f"{txt_file} does not have enough lines")
            
            second_line = lines[1].strip()
            # Expect format: "Start time = Tue Feb 25 19:54:42 2025"
            if not second_line.startswith("Start time = "):
                raise ValueError(f"Second line in {txt_file} does not start with 'Start time = '")
            
            # Extract datetime string
            datetime_str = second_line.replace("Start time = ", "")
            # Parse datetime (format: Tue Feb 25 19:54:42 2025)
            dt = datetime.strptime(datetime_str, "%a %b %d %H:%M:%S %Y")
            # Convert to Unix timestamp
            return dt.timestamp()
    
    except Exception as e:
        print(f"Error reading acquisition start time from {txt_file}: {e}")
        raise

# Function to process a single ROOT file
def process_root_file(file_path, table_prefix, process_neutrons=True, process_gammas=True):
    try:
        # Get channel number
        channel_number = get_channel_number_from_filename(file_path)
        channel_number_int = int(channel_number)
        print(f"Extracted Channel Number: {channel_number_int}")

        # Get PSD and energy thresholds for the channel
        if channel_number_int in PSD_THRESHOLDS:
            psd_threshold, energy_threshold = PSD_THRESHOLDS[channel_number_int]
            print(f"Using loaded PSD Threshold for channel {channel_number_int}: {psd_threshold}, Energy Threshold: {energy_threshold}")
        else:
            psd_threshold, energy_threshold = (0.15, 0)
            print(f"Warning: Channel {channel_number_int} not found in PSD_THRESHOLDS. Using default PSD threshold: {psd_threshold} and Energy threshold: {energy_threshold}")

        # Get acquisition start time from .txt file in parent folder
        parent_folder = os.path.dirname(os.path.dirname(file_path))  # Parent of RAW
        acquisition_start_timestamp = get_acquisition_start_from_txt(parent_folder)

        # Load fiducial parameter CSVs
        fiducial_gammas_file = "fiducial_params_gammas.csv"
        fiducial_neutrons_file = "fiducial_params_neutrons.csv"
        use_fiducial_curves = False

        if os.path.exists(fiducial_gammas_file) and os.path.exists(fiducial_neutrons_file):
            fiducial_params_gammas_df = pd.read_csv(fiducial_gammas_file)
            fiducial_params_neutrons_df = pd.read_csv(fiducial_neutrons_file)
            use_fiducial_curves = True
            print("Fiducial parameter files loaded successfully.")
        else:
            print("Warning: Fiducial parameter files not found. Proceeding without fiducial curve filtering.")

        if use_fiducial_curves:
            if channel_number_int in fiducial_params_gammas_df["ch"].values and channel_number_int in fiducial_params_neutrons_df["ch"].values:
                fiducial_params_gammas = fiducial_params_gammas_df[fiducial_params_gammas_df["ch"] == channel_number_int].iloc[:, 1:].values.flatten()
                fiducial_params_neutrons = fiducial_params_neutrons_df[fiducial_params_neutrons_df["ch"] == channel_number_int].iloc[:, 1:].values.flatten()
                print(f"Using fiducial parameters for channel {channel_number_int}:")
                print(f"  Fiducial Params (Gammas): {fiducial_params_gammas}")
                print(f"  Fiducial Params (Neutrons): {fiducial_params_neutrons}")
            else:
                use_fiducial_curves = False
                print(f"Warning: Channel {channel_number_int} not found in fiducial parameter files. Using PSD threshold.")

        # Open ROOT file
        with uproot.open(file_path) as file:
            tree = file["Data_R"]
            timetag = tree["Timestamp"].array(library="np") * 1e-12
            if len(timetag) == 0:
                print(f"No data found in {file_path}")
                return False

            # Load data
            branches_to_import = ["Timestamp", "Energy", "EnergyShort"]
            df = tree.arrays(branches_to_import, library="pd")
            df["PSP"] = (df['Energy'] - df['EnergyShort']) / df['Energy']
            df["Timestamp"] = df["Timestamp"] / 1e12

            # Apply PSD or fiducial filtering
            dfn = df.copy() if process_neutrons else None
            dfg = df.copy() if process_gammas else None
            if use_fiducial_curves:
                if process_gammas:
                    gamma_up_values = fiducial_curve(df["Energy"], *fiducial_params_gammas)
                    dfg = dfg[dfg["PSP"] < gamma_up_values]
                if process_neutrons:
                    gamma_up_values = fiducial_curve(df["Energy"], *fiducial_params_gammas)
                    dfn = dfn[dfn["PSP"] > gamma_up_values]
                    neutron_up_values = fiducial_curve(dfn["Energy"], *fiducial_params_neutrons)
                    dfn = dfn[dfn["PSP"] < neutron_up_values]
            else:
                if process_neutrons:
                    dfn = dfn[dfn["PSP"] > psd_threshold]  # Neutrons
                if process_gammas:
                    dfg = dfg[dfg["PSP"] < psd_threshold]  # Gammas

            # Connect to database
            conn = connect_to_db()

            # Process neutrons if requested
            if process_neutrons and dfn is not None:
                neutron_abs_times = dfn["Timestamp"] + acquisition_start_timestamp
                above_threshold_mask = dfn["Energy"] >= energy_threshold
                neutron_abs_times_above = neutron_abs_times[above_threshold_mask]
                print(f"Filtered {len(neutron_abs_times_above)} neutron events above energy threshold ({energy_threshold}) out of {len(neutron_abs_times)} total neutron events")

                table_name_neutron_timestamps = get_table_name_from_filename(file_path, table_prefix, 'neutron', 'timestamps')
                neutron_rows = []
                for abs_time in neutron_abs_times_above:
                    time_floor = np.floor(abs_time)
                    time_value = datetime.fromtimestamp(time_floor).strftime('%Y-%m-%d %H:%M:%S')
                    subsecond_ps = int((abs_time - time_floor) * 1e12)
                    neutron_rows.append((time_value, [1.0], subsecond_ps))

                insert_many_timestamps_to_db(conn, table_name_neutron_timestamps, neutron_rows)
                print("Inserted neutron timestamps into database")

            # Process gammas if requested
            if process_gammas and dfg is not None:
                gamma_abs_times = dfg["Timestamp"] + acquisition_start_timestamp
                table_name_gamma_timestamps = get_table_name_from_filename(file_path, table_prefix, 'gamma', 'timestamps')
                gamma_rows = []
                for abs_time in gamma_abs_times:
                    time_floor = np.floor(abs_time)
                    time_value = datetime.fromtimestamp(time_floor).strftime('%Y-%m-%d %H:%M:%S')
                    subsecond_ps = int((abs_time - time_floor) * 1e12)
                    gamma_rows.append((time_value, [1.0], subsecond_ps))

                insert_many_timestamps_to_db(conn, table_name_gamma_timestamps, gamma_rows)
                print("Inserted gamma timestamps into database")

            conn.close()
            print(f"Done")
            return True

    except Exception as e:
        print(f"Failed to process {file_path}: {e}")
        return False

# Main function
def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Process ROOT files for neutron and/or gamma events.")
    parser.add_argument('-n', '--neutrons', action='store_true', help="Process only neutron events")
    parser.add_argument('-g', '--gammas', action='store_true', help="Process only gamma events")
    args = parser.parse_args()

    # Determine which particles to process
    process_neutrons = args.neutrons or (not args.neutrons and not args.gammas)
    process_gammas = args.gammas or (not args.neutrons and not args.gammas)
    if args.neutrons and args.gammas:
        process_neutrons = True
        process_gammas = True

    print(f"Processing: Neutrons={process_neutrons}, Gammas={process_gammas}")

    table_prefix = "caen8ch"  # Default table prefix
    csv_path = "processed_files.csv"
    default_channel = 0  # Default channel number

    # Check if CSV exists
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        total_files = len(df)
        unprocessed_files = len(df[~df['processed']])
        print(f"Found {total_files} files in {csv_path}, {unprocessed_files} remain to be processed.")
        print()
    else:
        # Prompt for folder and channel number
        folder_path = input("Enter the folder path containing the Compass .txt file (ROOT files in RAW subfolder): ")
        channel_input = input(f"Enter channel number (default '{default_channel}'): ") or default_channel
        file_pattern = f"_CH{channel_input}@"

        # Resolve the folder path to handle virtual file systems (e.g., Google Drive)
        try:
            folder_path = str(Path(folder_path).resolve())
            print(f"Resolved folder path: {folder_path}")
        except Exception as e:
            print(f"Error resolving path '{folder_path}': {e}")
            print("If using Google Drive, ensure the folder is marked 'Available Offline' or try using the path under ~/Library/CloudStorage/GoogleDrive-<your_email>/My Drive")
            return

        # Validate folder
        if not os.path.isdir(folder_path):
            print(f"Error: {folder_path} is not a valid directory")
            print("If using Google Drive, ensure the folder is marked 'Available Offline' or try using the path under ~/Library/CloudStorage/GoogleDrive-<your_email>/My Drive")
            return

        # Validate RAW subfolder
        raw_folder = os.path.join(folder_path, "RAW")
        if not os.path.isdir(raw_folder):
            print(f"Error: {raw_folder} subfolder does not exist")
            return
        
        # Build glob pattern to match files like *_CH0@*.root or .root2
        pattern = os.path.join(raw_folder, f"*{file_pattern}*.root*")

        # Get list of ROOT files matching pattern in RAW subfolder
        files = glob.glob(pattern)

        if not files:
            print(f"No files with channel number '{channel_input}' and containing '.root' found in {raw_folder}")
            return

        # Sort files by number before .root
        files.sort(key=get_file_number)

        # Create DataFrame with full paths
        df = pd.DataFrame({
            'filename': files,  # Use full paths directly from glob
            'processed': [False] * len(files)
        })
        df.to_csv(csv_path, index=False)
        total_files = len(df)
        print(f"Found {total_files} files to process. Created CSV: {csv_path}")
        print()

    # Process unprocessed files
    total_files = len(df)
    for index, row in df.iterrows():
        if not row['processed']:
            file_path = row['filename']
            current_file_number = index + 1
            if os.path.exists(file_path):
                print(f"Processing file {current_file_number} out of {total_files}: {os.path.basename(file_path)}")
                success = process_root_file(file_path, table_prefix, process_neutrons, process_gammas)
                if success:
                    df.at[index, 'processed'] = True
                    df.to_csv(csv_path, index=False)
                    print()
            else:
                print(f"File not found: {file_path}")
                df.at[index, 'processed'] = True
                df.to_csv(csv_path, index=False)

    print("Processing complete")

if __name__ == "__main__":
    main()