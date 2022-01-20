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

from datetime import datetime
from netCDF4 import date2num, num2date
from netCDF4 import Dataset
import numpy as np
from dateutil import parser
import os

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

# map RBR engineerng file name to netCDF variable name
nameMap = {}
nameMap["Temp"] = "TEMP"
nameMap["Pres"] = "PRES"
nameMap["Depth"] = "DEPTH"

# also map units .....

unitMap = {}
unitMap["C"] = "degrees_Celsius"

# RBR TDR-2050  6.51 014785 (Windows: 6.13 - Minimum required: 6.12)
# Host time     11/08/07 00:57:02
# Logger time   11/08/07 00:57:05
# Logging start 10/09/11 00:00:00
# Logging end   11/08/15 00:00:00
# Sample period          00:00:30
# Number of channels =  2, number of samples = 950515, mode: Logging Stopped by User
# E02%9.4f
# Calibration  1: 0.003459761001758
#                 -0.000251008321019
#                 0.000002480363738
#                 -0.000000074610405 Degrees_C
# Calibration  2: 13.776858223194999
#                 5925.153938237110200
#                 -148.264439112506010
#                 10.188319300301000 deciBars
# Atmospheric pressure 10.132500 dBar, water density 1.028100, (Simplified calculation)
# Latitude 0 degrees 0.000 minutes
# Memory type: 6 AT45DB642D_LP
#
#                          Temp      Pres     Depth
# 2010/09/11 00:00:00   10.0306    9.2912   -0.8344
# 2010/09/11 00:00:30   10.0360    9.2933   -0.8323


#
# parse the file
#

# search expressions within file

first_line_expr  = r"^RBR\s*(\S*)\s*\S*\s*(\S*)"
data_hdr_expr    = r"^.*Temp.*Pres.*$"
pres_hdr_expr    = r"^.*Pres.*$"
host_time_expr   = r"Host time\s*(.*)"


def parse(files):

    output_files = []
    for filepath in files:
        hdr = True
        dataLine = 0
        name = []
        number_samples_read = 0
        nVars = 0
        data = []
        ts = []
        logger_time = None
        file_created = None

        with open(filepath, 'r', errors='ignore') as fp:
            line = fp.readline()
            matchObj = re.match(first_line_expr, line)
            if not matchObj:
                print("Not a RBR .DAT file !")
                return None
            else:
                instrument_model = matchObj.group(1)
                instrument_serial_number = matchObj.group(2)
                print("instrument ", instrument_model, " : ", instrument_serial_number)

            cnt = 1
            while line:
                # print("Line n {}: data lines {} count {} : {}".format(cnt, dataLine, len(line),     line.strip()))
                if hdr:
                    matchObj = re.match(host_time_expr, line)
                    if matchObj:
                        #print("host_time_expr:matchObj.group() : ", matchObj.group())
                        print("host_time_expr:matchObj.group(1) : ", matchObj.group(1))
                        file_created = matchObj.group(1)

                    matchObj = re.match(data_hdr_expr, line)
                    if matchObj:
                        data_split = line.split()

                        print('data header', data_split)

                        # TODO: extract these from the data_split variable, and the channel expression

                        name.append({'var_name': 'TEMP', 'unit':'degrees_Celsius', 'col':2})
                        name.append({'var_name': 'PRES', 'unit':'dbar', 'col':3})

                        hdr = False
                    else:
                        matchObj = re.match(pres_hdr_expr, line)
                        if matchObj:
                            data_split = line.split()

                            print('data header', data_split)

                            # TODO: extract these from the data_split variable, and the channel expression

                            name.append({'var_name': 'PRES', 'unit': 'dbar', 'col': 2})

                            hdr = False
                else:

                    lineSplit = line.split()
                    if len(lineSplit) == 0:
                        break
                    #print(lineSplit)

                    d = np.zeros(len(name))
                    d.fill(np.nan)

                    t = parser.parse(lineSplit[0] + ' ' + lineSplit[1], yearfirst = True, dayfirst=False)
                    ts.append(t)
                    #print("timestamp %s" % t)

                    splitVarNo = 0
                    for v in name:
                        #print("{} : {}".format(splitVarNo, v))
                        d[splitVarNo] = float(lineSplit[v['col']])
                        splitVarNo = splitVarNo + 1
                    data.append(d)

                    #print(t, d)
                    number_samples_read = number_samples_read + 1

                    dataLine = dataLine + 1

                line = fp.readline()
                cnt += 1

        # trim data
        print("samplesRead %d data shape %s" % (number_samples_read, len(name)))

        #
        # build the netCDF file
        #

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        outputName = filepath + ".nc"

        print("output file : %s" % outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4')

        ncOut.instrument = 'RBR ; ' + instrument_model
        ncOut.instrument_model = instrument_model
        ncOut.instrument_serial_number = instrument_serial_number

        if logger_time:
            ncOut.logger_time = logger_time

        if file_created:
            ncOut.stop_time = file_created

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
        ncTimesOut[:] = date2num(ts, units=ncTimesOut.units, calendar=ncTimesOut.calendar)

        i = 0
        for v in name:
            print("Variable %s : unit %s" % (v['var_name'], v['unit']))
            varName = v['var_name']
            ncVarOut = ncOut.createVariable(varName, "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
            if v['unit']:
                ncVarOut.units = v['unit']
            x = np.array([d[i] for d in data])
            print ("var", x.shape, x)
            ncVarOut[:] = x

            i += 1

        ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
        ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath) + " source file created " + file_created)

        ncOut.close()

        output_files.append(outputName)

    return output_files


if __name__ == "__main__":
    parse(sys.argv[1:])

