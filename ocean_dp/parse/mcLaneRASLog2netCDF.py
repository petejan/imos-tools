#!/usr/bin/python3

# raw2netCDF
# Copyright (C) 2019 Peter Jansen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import sys
import re
import os

from datetime import datetime, timedelta, UTC
from cftime import num2date, date2num
from glob2 import glob
from netCDF4 import Dataset
import numpy as np
from dateutil import parser

# source file must have 'timek' column for time
#  flag column is excluded
#
# parsers need to output
#  instrument
#  instrument_serial_number
#  time_coverage_start
#  time_coverage_end
# optional
#  date_created
#  history
#
# convert time to netCDF cf-timeformat (double days since 1950-01-01 00:00:00 UTC)

# search expressions within file

   # 1  03/31/19 15:00:02  32.7 Vbat  10.6 °C  PORT = 00
   #    Pre-sample acid flush         0 ml        0 sec  LB  0.0 V  . . .
   #    Flush port  = 49
   #    Intake flush                251 ml      201 sec  LB 32.0 V  Average I 74.0 mA Highest I 82.0 mA  Volume reached
   #    Flush port  = 00
   #    Sample                      501 ml      402 sec  LB 31.8 V  Average I 78.0 mA Highest I 87.0 mA  Volume reached
   #    Sample port = 01
   #    03/31/19 15:10:15  32.3 Vbat  14.3 °C  PORT = 01
   #    Post-sample acid flush       11 ml       10 sec  LB 31.8 V  Volume reached
   #    Flush port  = 49


#
# parse the file
#

event_ts = r'.*Event *(?P<event>\d{1,2}).*of.*(?P<time>\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2,4})'
event_data = r'\S*(?P<event>\d{1,2}) *(?P<time>\d{2}\/\d{2}\/\d{2,4} \d{2}:\d{2}:\d{2}) *(?P<vbat>[0-9\.]*).*Vbat.*PORT = (?P<port>\d*)'
sample_time_bv = r'.*Sample.*\d* ml\s*(?P<pump_time>\d+).*sec.*LB(?P<lowest_bat>.*).*V.*Volume reached'
sample_time_bv_current = r'.*Sample.*\d* ml\s*(?P<pump_time>\d+).*sec.*LB(?P<lowest_bat>.*).*V.*Average I (?P<current>.*) mA Highest I (?P<maxcurrent>.*) mA.*Volume reached'
pump_data = r'.*?(?P<event>[0-9.]+) +(?P<mlpmin>[0-9.]+) +(?P<ml>[0-9.]+) +(?P<vat>[0-9.]+) +(?P<cur>[0-9.]+) +(?P<maxcur>[0-9.]+)'
event_data_13 = r'.*?(?P<event>\d+)\|Sample.*?\|.*?(?P<time>\d{2}\/\d{2}\/\d{2,4} \d{2}:\d{2}:\d{2})\|.*?(?P<temp>[0-9.]+)\|.*?(?P<vbat>[0-9.]+)\|.*?(?P<vol>[0-9.]+)\|.*?(?P<duration>[0-9.]+)\| Volume reached'

def parse(files):

    output_names = []

    for filepath in files:

        instrument_model = 'RAS'
        instrument_serialnumber = 'Unknown'
        number_samples_read = 0

        print('file', filepath)

        with open(filepath, 'r', errors='ignore') as fp:
            line = fp.readline()

            while line:
                line = line.strip()
                #print("Line ", line)

                matchObj = re.match(event_ts, line)
                if matchObj:
                    print('match event_ts', matchObj, matchObj.groups())

                matchObj = re.match(event_data, line)
                if matchObj:
                    print('match event_data', matchObj, matchObj.groups())

                matchObj = re.match(event_data_13, line)
                if matchObj:
                    print('match event_data_13', matchObj, matchObj.groups())
                    event = int(matchObj.group('event'))
                    ts = datetime.strptime(matchObj.group('time'), '%m/%d/%Y %H:%M:%S')
                    vbat = float(matchObj.group('vbat'))
                    vol = float(matchObj.group('vol'))
                    duration = float(matchObj.group('duration'))
                    print('event', event, ts, vbat, vol, duration)

                matchObj = re.match(sample_time_bv_current, line)
                if matchObj:
                    print('match sample_time_bv_current', matchObj, matchObj.groups())

                matchObj = re.match(sample_time_bv, line)
                if matchObj:
                    print('match sample_time_bv', matchObj, matchObj.groups())

                matchObj = re.match(pump_data, line)
                if matchObj:
                    print('match pump_data', matchObj, matchObj.groups())

                line = fp.readline()

        #
        # build the netCDF file
        #

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        outputName = (os.path.basename(filepath) + ".nc")

        print("output file : %s" % outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

        ncOut.instrument = 'McLane ; ' + instrument_model
        ncOut.instrument_model = instrument_model
        ncOut.instrument_serial_number = instrument_serialnumber

        #     TIME:axis = "T";
        #     TIME:calendar = "gregorian";
        #     TIME:long_name = "time";
        #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

        tDim = ncOut.createDimension("TIME", number_samples_read)
        ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
        ncTimesOut.long_name = "time"
        ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
        ncTimesOut.calendar = "gregorian"
        ncTimesOut.axis = "T"

        # add timespan attributes
        ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

        # add creating and history entry
        ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
        ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

        ncOut.close()

        output_names.append(outputName)

    return output_names


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))
    parse(files)
