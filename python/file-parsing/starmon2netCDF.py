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

# map sea bird name to netCDF variable name
nameMap = {}
nameMap["Temp"] = "TEMP"

# also map units .....

unitMap = {}
unitMap["C"] = "degrees_Celsius"

#
# parse the file
#

# search expressions within file

first_line_expr = r"\#B\tCreated:\s(.*)"

recorder_expr     = r"##\s*Recorder\s*(\d*)\s*([\S ]*)\s*(\S*)"
series_expr       = r"##\s*Series\s*(\d*)\s*(\S*)\((\S*)\)"
soft_version_expr = r"##\sVersion.(.*)"


def main(file):

    hdr = True
    dataLine = 0
    name = []
    number_samples_read = 0
    nVars = 0
    data = []
    ts = []

    filepath = file[1]

    with open(filepath, 'r', errors='ignore') as fp:
        line = fp.readline()
        matchObj = re.match(first_line_expr, line)
        if not matchObj:
            print("Not a Starmon-mini DAT file !")
            exit(-1)

        cnt = 1
        while line:
            #print("Line {}: {} : {}".format(cnt, dataLine, line.strip()))
            if hdr:
                if line[0] != '#':
                    hdr = False
                else:
                    matchObj = re.match(soft_version_expr, line)
                    if matchObj:
                        #print("soft_version_expr:matchObj.group() : ", matchObj.group())
                        #print("soft_version_expr:matchObj.group(1) : ", matchObj.group(1))
                        software = matchObj.group(1)

                    matchObj = re.match(first_line_expr, line)
                    if matchObj:
                        #print("first_line_expr:matchObj.group() : ", matchObj.group())
                        #print("first_line_expr:matchObj.group(1) : ", matchObj.group(1))
                        file_created = matchObj.group(1)

                    matchObj = re.match(recorder_expr, line)
                    if matchObj:
                        #print("recorder_expr:matchObj.group() : ", matchObj.group())
                        #print("recorder_expr:matchObj.group(1) : ", matchObj.group(1))
                        #print("recorder_expr:matchObj.group(2) : ", matchObj.group(2))
                        #print("recorder_expr:matchObj.group(3) : ", matchObj.group(3))
                        instrument_model = matchObj.group(2)
                        instrument_serial_number = matchObj.group(3)

                    matchObj = re.match(series_expr, line)
                    if matchObj:
                        #print("series_expr:matchObj.group() : ", matchObj.group())
                        #print("series_expr:matchObj.group(1) : ", matchObj.group(1))
                        #print("series_expr:matchObj.group(2) : ", matchObj.group(2))
                        #print("series_expr:matchObj.group(3) : ", matchObj.group(3))
                        nameN = matchObj.group(1)
                        ncVarName = matchObj.group(2)
                        if ncVarName in nameMap:
                            ncVarName = nameMap[ncVarName]
                        unit = matchObj.group(3)
                        if unit in unitMap:
                            unit = unitMap[unit]

                        name.insert(nVars, {'col': int(nameN), 'var_name': ncVarName, 'unit': unit})

            if not hdr:
                lineSplit = line.split('\t')
                #print(lineSplit)
                splitVarNo = 0
                d = np.zeros(len(name))
                d.fill(np.nan)
                t = datetime.strptime(lineSplit[1], '%d/%m/%Y %H:%M:%S')
                ts.append(t)
                #print("timestamp %s" % ts)
                for v in name:
                    #print("{} : {}".format(splitVarNo, v))
                    d[splitVarNo] = float(lineSplit[v['col']+2])
                    data.append(d)
                    splitVarNo = splitVarNo + 1
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

    ncOut.instrument = 'StarODDI - ' + instrument_model
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
    ncTimesOut[:] = date2num(ts, units=ncTimesOut.units, calendar=ncTimesOut.calendar)

    i = 0
    for v in name:
        print("Variable %s : unit %s" % (v['var_name'], v['unit']))
        varName = v['var_name']
        ncVarOut = ncOut.createVariable(varName, "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
        if v['unit']:
            ncVarOut.units = unit
        ncVarOut[:] = data

        i = i + 1

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath + " source file created " + file_created + " by software  " + software.replace("\t", " "))

    ncOut.close()

    return outputName


if __name__ == "__main__":
    main(sys.argv)

