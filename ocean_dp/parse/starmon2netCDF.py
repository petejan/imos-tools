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

from datetime import datetime, UTC

from glob2 import glob
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

# map starODDI name to netCDF variable name
nameMap = {}
nameMap["Temp"] = "TEMP"
nameMap["depth"] = "PRES"
nameMap["pitch"] = "PITCH"
nameMap["roll"] = "ROLL"

# also map units .....

unitMap = {}
unitMap["C"] = "degrees_Celsius"
unitMap["°C"] = "degrees_Celsius"

#
# parse the file
#

# search expressions within file

first_line_expr = r"\#B\tCreated:\s(.*)"
first_line_old_expr = r"\#0\tDate-time:\s(.*)"

recorder_expr     = r"##\s*Recorder\s*(\d*)\s*([\S ]*)\s*(\S*)"
series_expr       = r"##\s*Series\s*(\d*)\s*(\S*)\((\S*)\)"
soft_version_expr = r"##\sVersion.(.*)"
channel_expr      = r"#\d*\s*Channel\s*(\d*):\s*(\S*)\((\S*)\)"
axis_expr         = r"##\s*Axis\s*(\d*)\s*(\S*)\((\S*)\)"
recorder_old_expr = r"#1\s*Recorder:\s*.(.)(\d*)"


def parse(file):
    outputNames = []

    for filepath in file:
        hdr = True
        dataLine = 0
        name = []
        number_samples_read = 0
        nVars = 0
        data = []
        ts = []

        with open(filepath, 'r', errors='ignore') as fp:
            line = fp.readline()
            matchObj1 = re.match(first_line_old_expr, line)
            matchObj2 = re.match(first_line_expr, line)
            if not matchObj1 and not matchObj2:
                print("Not a Starmon-mini DAT file !")
                return None

            outputName = filepath + ".nc"

            print("output file : %s" % outputName)

            cnt = 1
            while line:
                #print("Line {}: {} : {}".format(cnt, dataLine, line.strip()))
                if hdr:
                    if line[0].isdigit():
                        hdr = False
                        print(name)
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
                        matchObj = re.match(first_line_old_expr, line)
                        if matchObj:
                            #print("first_line_old_expr:matchObj.group() : ", matchObj.group())
                            #print("first_line_old_expr:matchObj.group(1) : ", matchObj.group(1))
                            file_created = matchObj.group(1)

                        matchObj = re.match(recorder_expr, line)
                        if matchObj:
                            #print("recorder_expr:matchObj.group() : ", matchObj.group())
                            #print("recorder_expr:matchObj.group(1) : ", matchObj.group(1))
                            #print("recorder_expr:matchObj.group(2) : ", matchObj.group(2))
                            #print("recorder_expr:matchObj.group(3) : ", matchObj.group(3))
                            instrument_model = matchObj.group(2)
                            instrument_serial_number = matchObj.group(3)

                        matchObj = re.match(recorder_old_expr, line)
                        if matchObj:
                            print("recorder_old_expr:matchObj.group() : ", matchObj.group())
                            #print("recorder_old_expr:matchObj.group(1) : ", matchObj.group(1))
                            #print("recorder_old_expr:matchObj.group(2) : ", matchObj.group(2))
                            instrument_model = matchObj.group(1)
                            instrument_serial_number = matchObj.group(2)

                        matchObj = re.match(series_expr, line)
                        if matchObj:
                            print("series_expr:matchObj.group() : ", matchObj.group())
                            #print("series_expr:matchObj.group(1) : ", matchObj.group(1))
                            #print("series_expr:matchObj.group(2) : ", matchObj.group(2))
                            #print("series_expr:matchObj.group(3) : ", matchObj.group(3))
                            #nameN = matchObj.group(1)
                            nameN = int(matchObj.group(1)) + 1
                            ncVarName = matchObj.group(2)
                            if ncVarName in nameMap:
                                ncVarName = nameMap[ncVarName]
                            unit = matchObj.group(3)
                            if unit in unitMap:
                                unit = unitMap[unit]

                            name.insert(nVars, {'col': nameN, 'var_name': ncVarName, 'unit': unit})

                        matchObj = re.match(channel_expr, line)
                        if matchObj:
                            print("channel_expr:matchObj.group() : ", matchObj.group())
                            #print("channel_expr:matchObj.group(1) : ", matchObj.group(1))
                            #print("channel_expr:matchObj.group(2) : ", matchObj.group(2))
                            #print("channel_expr:matchObj.group(3) : ", matchObj.group(3))
                            nameN = matchObj.group(1)
                            ncVarName = matchObj.group(2)
                            if ncVarName in nameMap:
                                ncVarName = nameMap[ncVarName]
                            unit = matchObj.group(3)
                            if unit in unitMap:
                                unit = unitMap[unit]

                            name.insert(nVars, {'col': int(nameN), 'var_name': ncVarName, 'unit': unit})

                        matchObj = re.match(axis_expr, line)
                        if matchObj:
                            print("channel_expr:axis_expr.group() : ", matchObj.group())
                            #print("channel_expr:axis_expr.group(1) : ", matchObj.group(1))
                            #print("channel_expr:axis_expr.group(2) : ", matchObj.group(2))
                            #print("channel_expr:axis_expr.group(3) : ", matchObj.group(3))
                            nameN = int(matchObj.group(1)) + 1
                            print("axis_expr:nameN ", nameN)
                            ncVarName = matchObj.group(2)
                            if ncVarName in nameMap:
                                ncVarName = nameMap[ncVarName]
                            unit = matchObj.group(3)
                            if unit in unitMap:
                                unit = unitMap[unit]

                            #name.insert(nVars, {'col': nameN, 'var_name': ncVarName, 'unit': unit})

                if not hdr:
                    lineSplit = line.strip().split('\t')
                    #print(lineSplit)
                    splitVarNo = 0
                    d = np.zeros(len(name))
                    d.fill(np.nan)
                    t = None
                    try:
                        t = datetime.strptime(lineSplit[1], '%d/%m/%Y %H:%M:%S')
                    except ValueError:
                        pass
                    if t is None:
                        try:
                            t = datetime.strptime(lineSplit[1], '%d.%m.%y %H:%M:%S')
                        except ValueError:
                            pass
                    if t is None:
                        try:
                            t = datetime.strptime(lineSplit[1], '%d.%m.%Y %H:%M:%S')
                        except ValueError:
                            pass
                    if t is None:
                        try:
                            t = datetime.strptime(lineSplit[1], '%d-%m-%Y %H:%M:%S')
                        except ValueError:
                            pass
                    if t is None:
                        print("Could not parse time ", lineSplit[1])
                        return None

                    ts.append(t)
                    #print("timestamp %s" % ts)
                    for v in name:
                        #print("{} : {}".format(splitVarNo, v))
                        d[splitVarNo] = float(lineSplit[v['col']+1])
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

        ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

        ncOut.instrument = 'StarODDI ; ' + instrument_model
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
                ncVarOut.units = v['unit']
            ncVarOut[:] = np.array([d[i] for d in data])

            i += 1

        ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
        #ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + filepath + " source file created " + file_created + " by software  " + software.replace("\t", " "))
        ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + filepath + " source file created " + file_created)

        ncOut.close()

        outputNames.append(outputName)

    return outputNames


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))
    parse(files)

