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

# Model=TDR-2050
# Firmware=6.51
# Serial=014786
# HostVersion=(Ruskin version number - 1.13.13.201708090902)
#
# HostTime=02-Apr-2019 00:42:54.000
# LoggerTime=02-Apr-2019 00:42:55.000
# LoggingStartTime=16-Mar-2019 00:00:00.000
# LoggingEndTime=06-Apr-2019 00:00:00.000
# LoggingSamplingPeriod=00:00:02
# NumberOfChannels=2
# Channel[1].name=Temperature
# Channel[1].calibration=0.0035048094 -2.5129681E-4 2.5541863E-6 -6.1163275E-8
# Channel[1].units=°C (Degrees_C)
# Channel[2].name=Pressure
# Channel[2].calibration=11.082411 3863.0037 46.767945 17.122328
# Channel[2].units=dBar (deciBars)
#
# ResetStamp[1].time=02-Apr-2019 00:33:24.000
# ResetStamp[1].sample=735404
# ResetStamp[1].type=STOP STAMP
#
# NumberOfSamples=735403
#
#              Date & Time         Temp          Pres         Depth
# 16-Mar-2019 00:00:00.000    18.0493730    10.3806015     0.2460786
# 16-Mar-2019 00:00:02.000    18.0478890    10.3865881     0.2520164
# 16-Mar-2019 00:00:04.000    18.0462916    10.3941864     0.2595527
# 16-Mar-2019 00:00:06.000    18.0447509    10.3955679     0.2609230
# 16-Mar-2019 00:00:08.000    18.0433340    10.3918839     0.2572690


#
# parse the file
#

# search expressions within file

first_line_expr = r"^Model=(.*)"

serial_expr      = r"^Serial=(.*)"
created_expr     = r"^HostTime=(.*)"
logger_time_expr = r"^LoggerTime=(.*)"
channel_expr     = r"^Channel\[(\d*)\]\.(\S*)=(.*)$"
data_hdr_expr    = r"^.*Date & Time.*$"

soft_version_expr = r"^Firmware=(.*)"


def parse(file):

    hdr = True
    dataLine = 0
    name = []
    number_samples_read = 0
    nVars = 0
    data = []
    ts = []
    logger_time = None
    file_created = None

    filepath = file[0]

    with open(filepath, 'r', errors='ignore') as fp:
        line = fp.readline()
        matchObj = re.match(first_line_expr, line)
        if not matchObj:
            print("Not a RBR eng.txt file !")
            return None

        cnt = 1
        while line:
            #print("Line {}: {} : {}".format(cnt, dataLine, line.strip()))
            if hdr:
                matchObj = re.match(soft_version_expr, line)
                if matchObj:
                    #print("soft_version_expr:matchObj.group() : ", matchObj.group())
                    #print("soft_version_expr:matchObj.group(1) : ", matchObj.group(1))
                    software = matchObj.group(1)

                matchObj = re.match(first_line_expr, line)
                if matchObj:
                    #print("first_line_expr:matchObj.group() : ", matchObj.group())
                    #print("first_line_expr:matchObj.group(1) : ", matchObj.group(1))
                    instrument_model = matchObj.group(1)

                matchObj = re.match(serial_expr, line)
                if matchObj:
                    #print("serial_expr:matchObj.group() : ", matchObj.group())
                    #print("serial_expr:matchObj.group(1) : ", matchObj.group(1))
                    instrument_serial_number = matchObj.group(1)

                matchObj = re.match(logger_time_expr, line)
                if matchObj:
                    #print("logger_time_expr:matchObj.group() : ", matchObj.group())
                    #print("logger_time_expr:matchObj.group(1) : ", matchObj.group(1))
                    logger_time = matchObj.group(1)

                matchObj = re.match(created_expr, line)
                if matchObj:
                    #print("created_expr:matchObj.group() : ", matchObj.group())
                    #print("created_expr:matchObj.group(1) : ", matchObj.group(1))
                    file_created = matchObj.group(1)

                matchObj = re.match(channel_expr, line)
                if matchObj:
                    print("channel_expr:matchObj.group() : ", matchObj.group())
                    #print("channel_expr:matchObj.group(1) : ", matchObj.group(1))
                    #print("channel_expr:matchObj.group(1) : ", matchObj.group(2))
                    #print("channel_expr:matchObj.group(1) : ", matchObj.group(3))

                matchObj = re.match(data_hdr_expr, line)
                if matchObj:
                    data_split = line.split()

                    print('data header', data_split)

                    # TODO: extract these from the data_split variable, and the channel expression

                    data_col = 2
                    if 'Temp' in data_split:
                        name.append({'var_name': 'TEMP', 'unit':'degrees_Celsius', 'col':data_col})
                        data_col += 1
                    if 'Pres' in data_split:
                        name.append({'var_name': 'PRES', 'unit':'dbar', 'col':data_col})
                        data_col += 1

                    hdr = False
            else:

                lineSplit = line.split()
                if len(lineSplit) == 0:
                    break
                #print(lineSplit)
                splitVarNo = 0
                d = np.zeros(len(name))
                d.fill(np.nan)

                t = parser.parse(lineSplit[0] + ' ' + lineSplit[1], yearfirst = True, dayfirst=True)
                ts.append(t)
                #print("timestamp %s" % t)

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

    ncOut.instrument = 'RBR - ' + instrument_model
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
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath + " source file created " + file_created)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])

