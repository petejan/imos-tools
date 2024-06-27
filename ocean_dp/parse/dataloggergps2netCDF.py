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
import os
import re
import sys

from datetime import datetime, timedelta, UTC

import pynmea2
from dateutil import parser

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

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

# lines to extract
# INFO : GPS Fix 6 Latitude -46.92788 Longitude -142.3721 sats 7 HDOP 1.28
# INFO: GPS String 'GPRMC' string GPRMC,001101.00,A,4655.67287,S,14222.32781,E,3.188,325.84,310819,,,A
# INFO: AT+CGSN = AT+CGSN\r\r\n300234063798830\r\n\r\nOK\r\n +CGSN index 3

gps_fix = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO : GPS Fix (\d+) Latitude (\S+) Longitude (\S+) sats (\S+) HDOP (\S+)')
gps_rmc = re.compile(r"(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: GPS String 'GPRMC' string (\S+)")
cgsn_expr = re.compile(r"(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: AT\+CGSN = AT\+CGSN\\r\\r\\n(\S+)\\r\\n\\r\\nOK\\r\\n \+CGSN index 3")

#
# parse the file
#

def nmea_parse(files):
    time = []
    xpos = []
    ypos = []

    sn = 'unknown'
    number_samples = 0
    for filepath in files:

        with open(filepath, 'rt', errors='ignore') as fp:
            line = fp.readline()
            while line:
                # check for gps fix
                matchobj = gps_fix.match(line)
                if matchobj:
                    data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                    print('gps time ', data_time)

                    time.append(data_time)

                    xpos.append(-float(matchobj.group(4)))
                    ypos.append(float(matchobj.group(3)))

                    number_samples += 1

                # check for gps rmc
                matchobj = gps_rmc.match(line)
                if matchobj:
                    #print("gps_rmc:matchObj.group(1) : ", matchobj.group(1))
                    #print("gps_rmc:matchObj.group(2) : ", matchobj.group(2))
                    data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")

                    msg = pynmea2.parse(matchobj.group(2))
                    #print (msg.fields)
                    if msg.datestamp:
                        dt = datetime.combine(msg.datestamp, msg.timestamp)
                        print('gps rmc ', data_time, 'gps time', dt)

                # check for gps rmc
                matchobj = cgsn_expr.match(line)
                if matchobj:
                    #print("cgsn_expr:matchObj.group(1) : ", matchobj.group(1))
                    #print("cgsn_expr:matchObj.group(2) : ", matchobj.group(2))
                    sn = matchobj.group(2)

                line = fp.readline()

    print("nSamples %d " % number_samples)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    ncOut.instrument = "NAL Research ; 9602-LP"
    ncOut.instrument_model = "9602-LP"
    ncOut.instrument_serial_number = sn

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME", number_samples)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = date2num(time, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

    ncVarOut = ncOut.createVariable("XPOS", "f8", ("TIME",), zlib=True)
    ncVarOut[:] = xpos

    ncVarOut = ncOut.createVariable("YPOS", "f8", ("TIME",), zlib=True)
    ncVarOut[:] = ypos

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

    ncOut.close()

    return outputName


if __name__ == "__main__":
    nmea_parse(sys.argv[1:])
