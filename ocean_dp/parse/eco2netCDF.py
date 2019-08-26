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
import re

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

# Count   Date    Time    Beta(700)       Betap(700)      bbp(700)        bb(700))        CHL(ug/l)       CDOM(ppb)
# 5       111518  002803  0.014909        0.000000        0.000000        0.052811        18.4190 373.1431
# 6       111518  002804  0.014909        0.000000        0.000000        0.052811        18.3625 373.1431
# 7       111518  002805  0.014909        0.000000        0.000000        0.052811        18.3851 373.1431
# 8       111518  002806  0.014909        0.000000        0.000000        0.052811        18.4303 373.1431
# 9       111518  002807  0.014909        0.000000        0.000000        0.052811        18.4303 373.1431
# 10      111518  004302  0.014909        0.000000        0.000000        0.052811        18.4755 373.1431
# 11      111518  004303  0.014909        0.000000        0.000000        0.052811        18.3851 373.1431

#
# parse the file
#

def eco_parse(files):
    time = []
    value = []

    filepath = files[1]
    number_samples = 0

    with open(filepath, 'r', errors='ignore') as fp:
        hdr_line = fp.readline()
        hdr_split = hdr_line.split("\t")
        hdr = [re.sub("\(.*\)\n?", "", x) for x in hdr_split[3:]]
        print (hdr)

        print(hdr_split)
        line = fp.readline()
        while line:

            line_split = line.split("\t")
            #print(line_split)

            t = line_split[1][0:4] + " 20" + line_split[1][4:] + " " + line_split[2]
            ts = datetime.strptime(t, "%m%d %Y %H%M%S")
            #print(ts)
            v = [float(x) for x in line_split[3:]]
            if len(v) == len(hdr):
                time.append(ts)
                value.append(v)
                number_samples += 1

            line = fp.readline()

    print("nSamples %d " % number_samples)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    ncOut.instrument = "WetLABs - BBFL2WB"
    ncOut.instrument_model = "BBFL2WB"
    ncOut.instrument_serial_number = "902"

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

    for i in range(0, len(hdr)):
        print(i, hdr[i], len(value[-1]))
        ncVarOut = ncOut.createVariable(hdr[i], "f4", ("TIME",), zlib=True)
        ncVarOut[:] = [v[i] for v in value]

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    eco_parse(sys.argv)
