__author__ = 'lab2'

from wanglib import prologix
import serial
import time
import math

class mysrs:

    def __init__(self):
        plx = prologix.prologix_USB('COM9')
        self.srs = plx.instrument(22)
        self.safe_delay = 0.2
        self.turned_off = 0
        self.turn_off() # just in case
        #self.set_voltage(7)
        time.sleep(self.safe_delay)
        pass

    def testing(self):
        self.srs.write('U0X')
        time.sleep(self.safe_delay)
        print self.srs.read()
        self.srs.write('U1X')
        time.sleep(self.safe_delay)
        print self.srs.read()
        self.srs.write('G1X')
        time.sleep(self.safe_delay)
        print self.srs.read()
        time.sleep(self.safe_delay)

    def turn_on(self):
        self.srs.write("F1X")
        time.sleep(self.safe_delay)

    def turn_off(self):
        self.srs.write("F0X")
        time.sleep(self.safe_delay)

    def untalk(self):
        time.sleep(self.safe_delay*2)
        self.srs.write('UNT')
        time.sleep(self.safe_delay*2)

    def get_lim_voltage(self):
        self.srs.write('UNT')
        time.sleep(self.safe_delay*2)
        self.srs.write('G4X')
        time.sleep(self.safe_delay*2)
        self.srs.read() #clearing buffer
        time.sleep(self.safe_delay*2)
        self.srs.write('G4X')
        time.sleep(self.safe_delay)
        returnstring = self.srs.read()
        time.sleep(self.safe_delay)
        self.srs.write('UNT')
        time.sleep(self.safe_delay)
        #print "FOUND: ",returnstring
        try:
            thispart = returnstring.split("V")[1]
            thispart = thispart.split(",")[0]
            finalstring = thispart[1:]

            float(finalstring)
            print "get_lim_voltage: ",finalstring
            return float(finalstring)
        except:
            return float(-1)

    def get_set_current(self):
        self.srs.write('UNT')
        time.sleep(self.safe_delay*2)
        self.srs.write('G4X')
        time.sleep(self.safe_delay*2)
        self.srs.read() #clearing buffer
        time.sleep(self.safe_delay*2)
        self.srs.write('G4X')
        time.sleep(self.safe_delay)
        returnstring = self.srs.read()
        time.sleep(self.safe_delay)
        self.srs.write('UNT')
        time.sleep(self.safe_delay)
        #print "FOUND: ",returnstring
        try:
            #thispart = returnstring.split(",")[0]
            thispart = returnstring.split("I")[1]
            thispart = thispart.split(",")[0]
            finalstring = thispart[1:]

            float(finalstring)
            print "get_set_current: ",finalstring
            return float(finalstring)
        except:
            return float(-1)

    def set_current(self,current): # comes in in microAmps
        if (current > 100000): # for safety
            current = 100000
        if current > -1:
            print "current is ",current
            #convert to amps
            current = float(current)/1000000;

            if current == 0:
                current = 0

            self.srs.write("I"+str(current)) #
            print "HERE wrote current to ",current
            time.sleep(self.safe_delay)
            self.srs.write("D0X")
            time.sleep(self.safe_delay)

    def set_voltage_limit(self,voltage):

        self.srs.write("V"+str(voltage))
        print "wrote voltage limit to ",voltage
        time.sleep(self.safe_delay)
        self.srs.write("D1X")
        time.sleep(self.safe_delay*5)
        self.srs.write("D0X")
        time.sleep(self.safe_delay)






    def get_u_from_p(self,target_p,current_u,current_i): # in mW and Ohm
        current_p = (current_u*current_i)*1000
        #print "BASED ON "+str(target_p)
        ampl_ratio = target_p/current_p
        #print "AND ON "+str(current_p)
        new_u = current_u*math.sqrt(ampl_ratio)
        return(new_u)
        #self.srs.write("VSET"+str(round(voltage,1)))

#    def set_power(self,target_power,current_r): # in mW and Ohm
#        if current_r != 0:
#            new_v = math.sqrt((target_power/1.)*(current_r/1.))
#            print new_v
#        #self.srs.write("VSET"+str(round(voltage,1)))

    def adjust_to_constant_power(set_power):
        v = self.get_output_voltage()
        i = self.get_output_current()
        output_p = i * v

        #print str(v)
        #print str(i)

        while (abs(output_p - set_power) > set_power * 0.1):  # if actual difference is more than 10 perc of set_power # ouput_power > set_power*1.1

            v = get_output_voltage()
            i = get_output_current()

            #print str(v)
            #print str(i)

            current_r = v / i # in kOhm

            #print str(current_r)
            #print str(set_power)

            new_v = math.sqrt(set_power/1000. * current_r*1000.)

            set_voltage(new_v)
            print str(new_v)

            v = get_output_voltage()
            i = get_output_current()
            output_p = i * v
            print '%s' % float('%.5g' % (output_p)) +"mW"

        # if (ouput_power < set_power*1.1):
        # break

