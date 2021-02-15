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

# 65114 records to read
# 07/02/19        05:38:02        695     59      700     160     544
# 07/02/19        05:38:03        695     59      700     161     544
# 07/02/19        05:38:04        695     61      700     156     544
# 07/02/19        05:38:05        695     61      700     151     544
# 07/02/19        05:38:06        695     57      700     154     544
# 07/02/19        05:53:23        695     62      700     203     555
# 07/02/19        05:53:51        695     71      700     202     552
# 07/02/19        05:53:52        695     69      700     203     552
# 07/02/19        05:53:53        695     67      700     205     552

#
# parse the file
#

line_re = "(\d\d\/\d\d\/\d\d\s+\d\d:\d\d:\d\d)\s*695\s*(\d+)\s*700\s*(\d+)\s*(\d+)$"

def eco_parse(files):
    time = []
    value = []

    filepath = files[0]
    number_samples = 0

    with open(filepath, 'r', errors='ignore') as fp:
        hdr_line = fp.readline().replace("\n", "")
        hdr_split = hdr_line.split("\t")
        hdr = [re.sub("\(.*\)\n?", "", x) for x in hdr_split[3:]]
        print (hdr)

        print(hdr_split)
        line = fp.readline()
        while line:
            matchObj = re.match(line_re, line)
            if matchObj:
                ts = datetime.strptime(matchObj.group(1), "%m/%d/%y\t%H:%M:%S")
                print(ts)
                time.append(ts)
                value.append([np.float(matchObj.group(2)), np.float(matchObj.group(3)), np.float(matchObj.group(4))])
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

    ncOut.instrument = "WetLABs ; FLNTUS"
    ncOut.instrument_model = "FLNTUS"
    ncOut.instrument_serial_number = "1215"

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

    ncVarOut = ncOut.createVariable('CHL_COUNT', "f4", ("TIME",), zlib=True)
    ncVarOut[:] = [v[0] for v in value]
    ncVarOut = ncOut.createVariable('NTU_COUNT', "f4", ("TIME",), zlib=True)
    ncVarOut[:] = [v[1] for v in value]
    ncVarOut = ncOut.createVariable('TEMP_COUNT', "f4", ("TIME",), zlib=True)
    ncVarOut[:] = [v[2] for v in value]

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    eco_parse(sys.argv[1:])
