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

# Date      Time      Vol         Size        Tau         Vol_sub               Size_sub              Depth
#
# 19800108 215756 0.000 0.000 0.000 0.000 0.000 646.060
# 20181119 020829 4179.526 296.146 0.000 4001.060 243.772 655.170
# 20181119 020832 4179.512 296.146 0.000 4001.047 243.772 655.150


#
# parse the file
#


def lisst_parse(files):
    time = []
    value = []

    hdr = ['VOL', 'SIZE', 'TAU', 'VOL_SUB', 'SIZE_SUB', 'PRES']

    filepath = files[1]
    number_samples = 0

    with open(filepath, 'r', errors='ignore') as fp:
        line = fp.readline()
        while line:

            line_split = line.split(" ")
            #print(line_split)

            try:
                t = line_split[0]+ " " + line_split[1]
                ts = datetime.strptime(t, "%Y%m%d %H%M%S")
                print(ts)
                if ts > datetime(2000,1,1):
                    v = [float(x) for x in line_split[2:]]
                    time.append(ts)
                    value.append(v)
                    number_samples += 1
            except ValueError:
                pass

            line = fp.readline()

    print("nSamples %d " % number_samples)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    ncOut.instrument = "Sequioa - LISST-25X"
    ncOut.instrument_model = "LISST-25X"
    ncOut.instrument_serial_number = "25x-157"

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
    lisst_parse(sys.argv)
