#!/usr/bin/env python
#  -*- coding: utf-8 -*-
"""
    MCC 134 Functions Demonstrated:
        mcc134.t_in_read

    Purpose:
        Read a single data value for each channel in a loop.

    Description:
        This example demonstrates acquiring data using a software timed loop
        to read a single value from each selected channel on each iteration
        of the loop.
"""
from __future__ import print_function
from time import sleep
from sys import stdout
from daqhats import mcc134, HatIDs, HatError, TcTypes
from daqhats_utils import select_hat_device, tc_type_to_string

sys.path.append(os.path.expanduser("~/daqhats/examples/python/mcc134"))  # Adjust this path

# Constants
CURSOR_BACK_2 = '\x1b[2D'
ERASE_TO_END_OF_LINE = '\x1b[0K'


def main():
    """
    This function is executed automatically when the module is run directly.
    """
    #tc_type = TcTypes.TYPE_T   # change this to the desired thermocouple type
    delay_between_reads = 1  # Seconds
    channels = (0, 1, 2, 3)

    try:
        # Get an instance of the selected hat device object.
        address = select_hat_device(HatIDs.MCC_134)
        hat = mcc134(address)

        #for channel in channels:
        hat.tc_type_write(0, TcTypes.TYPE_K)
        hat.tc_type_write(1, TcTypes.TYPE_K)
        hat.tc_type_write(2, TcTypes.TYPE_T)
        hat.tc_type_write(3, TcTypes.TYPE_K)

        print('\nMCC 134 single data value read example')
        print('    Function demonstrated: mcc134.t_in_read')
        print('    Channels: ' + ', '.join(str(channel) for channel in channels))
        #print('    Thermocouple type: ' + tc_type_to_string(tc_type))
        try:
            input("\nPress 'Enter' to continue")
        except (NameError, SyntaxError):
            pass

        print('\nAcquiring data ... Press Ctrl-C to abort')

        # Display the header row for the data table.
        print('\n  Sample', end='')
        for channel in channels:
            print('     Channel', channel, end='')
        print('')

        try:
            samples_per_channel = 0


            while True:
                # Display the updated samples per channel count
                samples_per_channel += 1
                print('\r{:8d}'.format(samples_per_channel), end='')

                # Initialize an empty list to store the values from each channel
                values = []

                # Read a single value from each selected channel.
                for channel in channels:
                    value = hat.t_in_read(channel)
                    if value == mcc134.OPEN_TC_VALUE:
                        print('     Open     ', end='')
                    elif value == mcc134.OVERRANGE_TC_VALUE:
                        print('     OverRange', end='')
                    elif value == mcc134.COMMON_MODE_TC_VALUE:
                        print('   Common Mode', end='')
                    else:
                        print('{:12.2f} C'.format(value), end='')
                        # Append the value to the list instead of logging it directly
                        values.append(value)
                
                # After collecting all values, convert the list to a NumPy array
                import numpy as np
                values_array = np.array(values)
                
                # Log the array of values to the database
                from mitcf import pglogger
                import psql_credentials as creds_cloud
                db_cloud = pglogger(creds_cloud)
                success_cloud = db_cloud.log(table="thermocouple1", channels=values_array)
                if not success_cloud:
                    print('Failed to log')
                db_cloud.close()

                stdout.flush()

		

                # Wait the specified interval between reads.
                sleep(delay_between_reads)

        except KeyboardInterrupt:
            # Clear the '^C' from the display.
            print(CURSOR_BACK_2, ERASE_TO_END_OF_LINE, '\n')

    except (HatError, ValueError) as error:
        print('\n', error)


if __name__ == '__main__':
    # This will only be run when the module is called directly.
    main()
