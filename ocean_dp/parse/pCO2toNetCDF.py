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
import os
import re

from datetime import datetime

from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np

hdr_line_expr = r"Date Time \(UTC\)"


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

#
# parse the file
#

def parse(files):

    output_names = []
    column_names = []

    for filepath in files:

        print('file name', filepath)

        hdr = True
        cnt = 1
        note = ''
        times = []
        pressure = []
        xCO2_SW = []
        xCO2_Air = []
        with open(filepath, 'r', errors='ignore') as fp:
            line = fp.readline()
            while line:
                line = line.rstrip('\n')
                if hdr:
                    matchObj = re.match(hdr_line_expr, line)
                    if matchObj:
                        hdr = False
                        column_names = line.split(',')
                        print('hdr line', column_names)
                    else:
                        note += line + '\n'
                else:
                    row = line.split(',')
                    times.append(datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M:%S"))
                    pressure.append(np.float(row[1]))
                    xCO2_SW.append(np.float(row[2]))
                    xCO2_Air.append(np.float(row[3]))
                    cnt += 1

                line = fp.readline()

        #
        # build the netCDF file
        #

        print('samples ', cnt, len(times))
        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        outputName = filepath + ".nc"

        print("output file : %s" % outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4')

        ncOut.instrument = 'Battelle'
        ncOut.instrument_model = 'Seaology pCO2 monitor'
        ncOut.note = note

        ncOut.instrument_serial_number = 'unknown'

        #     TIME:axis = "T";
        #     TIME:calendar = "gregorian";
        #     TIME:long_name = "time";
        #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

        tDim = ncOut.createDimension("TIME", cnt-1)
        ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
        ncTimesOut.long_name = "time"
        ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
        ncTimesOut.calendar = "gregorian"
        ncTimesOut.axis = "T"
        ncTimesOut[:] = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

        # for each variable in the data file, create a netCDF variable
        ncVarOut = ncOut.createVariable("pressure", "d", ("TIME",), zlib=True)
        ncVarOut.long_name = "air pressure"
        ncVarOut.units = "kPA"
        ncVarOut[:] = pressure
        ncVarOut = ncOut.createVariable("xCO2_SW", "d", ("TIME",), zlib=True)
        ncVarOut.long_name = "xCO2 of Seawater"
        ncVarOut.units = "umol/mol"
        ncVarOut[:] = xCO2_SW
        ncVarOut = ncOut.createVariable("xCO2_AIR", "d", ("TIME",), zlib=True)
        ncVarOut.long_name = "xCO2 of Air"
        ncVarOut.units = "umol/mol"
        ncVarOut[:] = xCO2_Air

        # add timespan attributes
        ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

        # add creating and history entry
        ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
        ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

        ncOut.close()

        output_names.append(outputName)

    return output_names


if __name__ == "__main__":
    parse(sys.argv[1:])
