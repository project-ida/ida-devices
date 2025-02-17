import pandas as pd
import numpy as np
import sys
import math
import time
import threading
import datetime
import serial
import powersupply_current
from mitcf import pglogger
import psql_credentials as creds_cloud

# Initialize database logger
db_cloud = pglogger(creds_cloud)

# Initialize power supply
ps = powersupply_current.mysrs()
ps.testing()

this_current = 0
last_current = 0

try:
    while True:
        user_input = input("Enter new current value in microamps (or 'exit' to quit): ")
        if user_input.lower() == 'exit':
            break
        
        try:
            this_current = int(float(user_input) * 1e6)  # Convert to microamps for Keithley 220
            print(f"Setting current to {this_current} microamps")
        except ValueError:
            print("Invalid input. Please enter a numeric value.")
            continue
        
        # Always update the current setting
        ps.set_current(this_current)

        if this_current == 0:
            ps.turn_off()
        elif last_current == 0 and this_current > 0:
            ps.turn_on()
        
        last_current = this_current    
        
        try:
            outputvoltage = float(ps.get_output_voltage())
            outputcurrent = float(ps.get_output_current())
            outputpower = outputvoltage * outputcurrent
        except ValueError:
            outputvoltage, outputcurrent, outputpower = -1, -1, -1
            print("Error reading power supply output values.")
        
        if this_current >= 0:
            thisdata = pd.to_numeric([this_current, outputvoltage, outputcurrent, outputpower], errors='coerce')
            success_cloud = db_cloud.log(table="keithley_ps", channels=thisdata)
            if not success_cloud:
                print("Database logging failed!")
                
except KeyboardInterrupt:
    print("Script stopped by user.")
    ps.turn_off()
    ps.untalk()
    del ps
