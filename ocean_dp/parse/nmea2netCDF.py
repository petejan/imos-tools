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

from datetime import datetime, timedelta
from dateutil import parser

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

import numpy as np

import pynmea2

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

# 103 pos NMEA $GPGGA,225830.80,4705.78892,S,14224.01553,E,1,12,0.74,9.7,M,-16.6,M,,*6C
# 845 pos NMEA $GPGGA,225831.00,4705.78888,S,14224.01545,E,1,12,0.84,9.7,M,-16.6,M,,*66
# 2291 pos NMEA $GPGGA,225831.40,4705.78880,S,14224.01531,E,1,12,0.87,9.8,M,-16.6,M,,*65
# 3077 pos NMEA $GPGGA,225831.60,4705.78876,S,14224.01521,E,1,12,0.87,9.8,M,-16.6,M,,*6F


#
# parse the file
#

def nmea_parse(files):
    time = []
    value = []

    filepath = files[1]
    number_samples = 0

    with open(filepath, 'rt', errors='ignore') as fp:
        line = fp.readline()
        while line:

            idx = line.find("$GPRMC")
            if idx > 0:

                msg = pynmea2.parse(line[idx:-1])
                dt = datetime.combine(msg.datestamp, msg.timestamp)
                print (msg.fields)

                time.append(dt)
                value.append((msg.latitude, msg.longitude, msg.spd_over_grnd))
                number_samples += 1

            line = fp.readline()

    print("nSamples %d " % number_samples)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    ncOut.instrument = "UBLOX ; LEA-6T"
    ncOut.instrument_model = "LEA-6T"
    ncOut.instrument_serial_number = "1"

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

    ncVarOut = ncOut.createVariable("XPOS", "f4", ("TIME",), zlib=True)
    ncVarOut[:] = [v[1] for v in value]

    ncVarOut = ncOut.createVariable("YPOS", "f4", ("TIME",), zlib=True)
    ncVarOut[:] = [v[0] for v in value]

    ncVarOut = ncOut.createVariable("SOG", "f4", ("TIME",), zlib=True)
    ncVarOut[:] = [v[2] for v in value]

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    nmea_parse(sys.argv)
