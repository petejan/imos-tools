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

from datetime import datetime
from netCDF4 import date2num, num2date
from netCDF4 import Dataset
import numpy as np

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
line_exp = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) INFO:.*MEASUREMENT\s*(\d*)\s*(\d*).*Temperature:\s*([0-9.-]*).*BPhase:\s*([0-9.-]*).*$"
done_line_expr = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) INFO:.*done time.*OBP=\s*([0-9.-]*).*OT=\s*([0-9.-]*).*$"
line_4831_expr = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) INFO: Optode Line : \s*(\d*)\s*(\d*) (.*)$"

# 2021-03-03 02:00:04 INFO: Optode Line : 4831 506 242.692 94.008 24.952 29.995 29.995 39.004 9.009 524.5 677.6 -21.1

#2019-12-03 05:00:07 INFO: Optode Line : MEASUREMENT   3830   1419 Oxygen:     330.55 Saturation:      94.27 Temperature:      10.19 DPhase:      34.15 BPhase:      34.15 RPhase:       0.00 BAmp:     2
#2019-12-03 06:00:07 INFO: Optode Line : MEASUREMENT   3830   1419 Oxygen:     330.59 Saturation:      94.19 Temperature:      10.15 DPhase:      34.17 BPhase:      34.17 RPhase:       0.00 BAmp:     2

def parse(file):

    number_samples_read = 0
    instrument_model = 'Optode 3830'
    instrument_serial_number = 'unknown'

    filepath = file[0]
    t = []
    bphase = []
    temp = []

    with open(filepath, 'r', errors='ignore') as fp:
        line = fp.readline()
        cnt = 1
        while line:
            matchObj = re.match(line_exp, line)
            if matchObj:
                try:
                    ts = datetime.strptime(matchObj.group(1), '%Y-%m-%d %H:%M:%S')
                    instrument_model = 'Optode ' + matchObj.group(2)
                    instrument_serial_number = matchObj.group(3)

                    bp = float(matchObj.group(5))
                    ot = float(matchObj.group(4))

                    t.append(ts)
                    bphase.append(bp)
                    temp.append(ot)

                    number_samples_read = number_samples_read + 1
                except ValueError as v:
                    print('Optode Value Error:', v)
                    print(line)
            matchObj = re.match(line_4831_expr, line)
            if matchObj:
                try:
                    ts = datetime.strptime(matchObj.group(1), '%Y-%m-%d %H:%M:%S')
                    instrument_model = 'Optode ' + matchObj.group(2)
                    instrument_serial_number = matchObj.group(3)

                    split = matchObj.group(4).split()
                    bp = float(split[4])
                    ot = float(split[2])

                    t.append(ts)
                    bphase.append(bp)
                    temp.append(ot)

                    number_samples_read = number_samples_read + 1
                except ValueError as v:
                    print('Optode Value Error:', v)
                    print(line)
            matchObj = re.match(done_line_expr, line)
            if matchObj:
                try:
                    ts = datetime.strptime(matchObj.group(1), '%Y-%m-%d %H:%M:%S')

                    bp = float(matchObj.group(2))
                    ot = float(matchObj.group(3))

                    t.append(ts)
                    bphase.append(bp)
                    temp.append(ot)

                    number_samples_read = number_samples_read + 1
                except ValueError as v:
                    print('Value Error:', v)
                    print(line)

            line = fp.readline()

    print("samplesRead %d" % (number_samples_read))

    if number_samples_read == 0:
        return

    print('instrument ', instrument_model, 'serial', instrument_serial_number)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    ncOut.instrument = 'Aanderra ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serial_number

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
    ncTimesOut[:] = date2num(t, units=ncTimesOut.units, calendar=ncTimesOut.calendar)

    ncVarOut = ncOut.createVariable('BPHASE', "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
    ncVarOut[:] = bphase
    ncVarOut.units = "1"
    ncVarOut.long_name = "optode bphase"

    ncVarOut = ncOut.createVariable('OTEMP', "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
    ncVarOut[:] = temp
    ncVarOut.units = "degrees_Celsius"
    ncVarOut.long_name = "optode temperature"

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])

