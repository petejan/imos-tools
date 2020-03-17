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
from dateutil import parser

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

# * ID=Minilog-TD
# * Serial Number=6100
# * Study ID=pulse 6 60m
# * Start Time=2009,09,22,12,00,00
# * Finish Time=2010,03,21,00,24,29
# * Sample Period=00,10,00
# * Number of Deployments=3
# * Date(yyyy,mm,dd),Time(hh,mm,ss),Celsius (°C),Temp(AtoD),Meters (m),Depth(AtoD)
# 2009,09,22,12,00,00,12.1,88,0.0,11
# 2009,09,22,12,10,00,12.1,88,0.0,11
# 2009,09,22,12,20,00,12.0,89,0.0,11
# 2009,09,22,12,30,00,12.0,89,0.0,11
# 2009,09,22,12,40,00,12.0,89,0.0,11
# 2009,09,22,12,50,00,11.9,90,0.0,11


# Source File: C:\Users\jan079\Documents\Vemco\LoggerVUE\Minilog-II-T_354194_20131028_1.vld
# Source Device: Minilog-II-T-354194
# Study Description: PULSE-10 2013 (45m)
# Minilog Initialized: 2013-04-09 23:46:42 (UTC+0)
# Study Start Time: 2013-04-28 00:00:00
# Study Stop Time: 2013-10-28 00:30:00
# Sample Interval: 00:15:00
# Date(yyyy-mm-dd) Time(hh:mm:ss),Temperature (°C),ADC
# 2013-04-28 00:00:00,20.90,4491
# 2013-04-28 00:15:00,20.87,4498
# 2013-04-28 00:30:00,20.84,4504
# 2013-04-28 00:45:00,20.83,4505
# 2013-04-28 01:00:00,20.83,4505
# 2013-04-28 01:15:00,20.84,4503
# 2013-04-28 01:30:00,20.85,4501
# 2013-04-28 01:45:00,20.87,4498


# search expressions within file

id_expr = r"\* ID=(\S+)"
serial_expr = r"\* Serial Number=(\S+)"
source_expr_II = r"Source Device: ([^-]*-[^-]*-[^-]*)-(\d*)"
source_expr = r"Source Device: ([^-]*-[^-]*)-(\d*)$"

dataline_expr = r"^([\d.: \-,]+)\n"

#
# parse the file
#

def parse(files):

    filepath = files[0]

    dataLine = 0
    d = []

    nVariables = 1
    number_samples = 0
    data = []
    times = []
    instrument_model = "unknown"
    instrument_serialnumber = "unknown"
    type = 0

    with open(filepath, 'r', errors='ignore') as fp:
        cnt = 1
        line = fp.readline()

        while line:
            #print("Line {}: {} : {}".format(cnt, dataLine, line.strip()))

            matchObj = re.match(id_expr, line)
            if matchObj:
                print("id_expr:matchObj.group() : ", matchObj.group())
                print("id_expr:matchObj.group(1) : ", matchObj.group(1))
                instrument_model = matchObj.group(1)
            matchObj = re.match(serial_expr, line)
            if matchObj:
                print("serial_expr:matchObj.group() : ", matchObj.group())
                print("serial_expr:matchObj.group(1) : ", matchObj.group(1))
                instrument_serialnumber = matchObj.group(1)
            matchObj = re.match(source_expr, line)
            if matchObj:
                print("source_expr:matchObj.group() : ", matchObj.group())
                print("source_expr:matchObj.group(1) : ", matchObj.group(1))
                instrument_model = matchObj.group(1)
                instrument_serialnumber = matchObj.group(2)
            matchObj = re.match(source_expr_II, line)
            if matchObj:
                print("source_expr:matchObj.group() : ", matchObj.group())
                print("source_expr:matchObj.group(1) : ", matchObj.group(1))
                instrument_model = matchObj.group(1)
                instrument_serialnumber = matchObj.group(2)

            # print(line.strip())
            ls = line.strip()
            #print(ls)
            if ls == '* Date(yyyy,mm,dd),Time(hh,mm,ss),Celsius (C),Temp(AtoD),Meters (m),Depth(AtoD)':
                type = 1
                nVariables = 2
                print('type 1 (MiniLog-T, temp, depth)')
            elif ls == '* Date(yyyy,mm,dd),Time(hh,mm,ss),Celsius (°C),Temp(AtoD),Meters (m),Depth(AtoD)':
                    type = 1
                    nVariables = 2
                    print('type 1 (MiniLog-T, temp, depth)')
            elif ls == "* Date(yyyy,mm,dd),Time(hh,mm,ss),Celsius (C),Temp(AtoD)":
                type = 2
                nVariables = 1
                print('type 2 (MiniLog-T, temp)')
            elif ls == "* Date(yyyy,mm,dd),Time(hh,mm,ss),Celsius (°C),Temp(AtoD)":
                type = 2
                nVariables = 1
                print('type 2 (MiniLog-T, temp)')
            elif ls == "* Date(yyyy-mm-dd) Time(hh:mm:ss),Temperature (C),ADC":
                type = 3
                nVariables = 1
                print('type 3 (MiniLog-II, temp)')
            elif ls == "* Date(yyyy-mm-dd) Time(hh:mm:ss),Celsius (C),Temp(AtoD)":
                type = 4
                nVariables = 1
                print('type 4 (MiniLog-T, temp)')
            elif ls == "* Date(yyyy-mm-dd) Time(hh:mm:ss),Celsius (C),Temp(AtoD),Meters (m),Depth(AtoD)":
                type = 5
                nVariables = 2
                print('type 5 (MiniLog-T, temp, depth)')
            elif ls == "Date(yyyy-mm-dd),Time(hh:mm:ss),Temperature (C)":
                type = 6
                nVariables = 1
                print('type 6 (MiniLog-T, temp)')
            elif ls == "Date(yyyy-mm-dd),Time(hh:mm:ss),Temperature (C),Depth (m)":
                type = 7
                nVariables = 2
                print('type 7 (MiniLog-TD, temp, depth)')
            elif ls == "* Date(yyyy-mm-dd) Time(hh:mm:ss),Celsius (°C),Temp(AtoD)":
                type = 8
                nVariables = 1
                print('type 8 (MiniLog-T, temp)')
            elif ls == "* Date(yyyy-mm-dd) Time(hh:mm:ss),Celsius (°C),Temp(AtoD),Meters (m),Depth(AtoD)":
                type = 9
                nVariables = 2
                print('type 9 (MiniLog-TD, temp, depth)')
            elif ls == "Date(yyyy-mm-dd),Time(hh:mm:ss),Temperature (°C),Depth (m)":
                type = 10
                nVariables = 2
                print('type 10 (MiniLog-TD, temp, depth)')
            elif ls == "Date(yyyy-mm-dd),Time(hh:mm:ss),Temperature (°C)":
                type = 11
                nVariables = 1
                print('type 11 (MiniLog-T)')
            elif ls == "Date(yyyy-mm-dd) Time(hh:mm:ss),Temperature (°C),ADC" or ls == "Date(yyyy-mm-dd) Time(hh:mm:ss),Temperature (C),ADC":
                type = 12
                nVariables = 1
                print('type 12 (MiniLog-T)')

            matchObj = re.match(dataline_expr, line)
            if matchObj:
                #print("dataline_expr:matchObj.group() : ", matchObj.group())
                line_split = line.split(',')
                if type == 1:
                    ts = datetime(int(line_split[0]), int(line_split[1]), int(line_split[2]), int(line_split[3]), int(line_split[4]), int(line_split[5]))
                    d = [float(line_split[6]), float(line_split[8])]
                elif type == 2:
                    ts = datetime(int(line_split[0]), int(line_split[1]), int(line_split[2]), int(line_split[3]), int(line_split[4]), int(line_split[5]))
                    d = [float(line_split[6])]
                elif type == 3:
                    ts = datetime.strptime(line_split[0], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[1])]
                elif type == 4:
                    ts = datetime.strptime(line_split[0], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[1])]
                elif type == 5:
                    ts = datetime.strptime(line_split[0], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[1]), float(line_split[3])]
                elif type == 6:
                    ts = datetime.strptime(line_split[0] + ' ' + line_split[1], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[2])]
                elif type == 7:
                    ts = datetime.strptime(line_split[0] + ' ' + line_split[1], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[2]), float(line_split[3])]
                elif type == 8:
                    ts = datetime.strptime(line_split[0], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[1])]
                elif type == 9:
                    ts = datetime.strptime(line_split[0], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[1]), float(line_split[3])]
                elif type == 10:
                    ts = datetime.strptime(line_split[0] + ' ' + line_split[1], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[2]), float(line_split[3])]
                elif type == 11:
                    ts = datetime.strptime(line_split[0] + ' ' + line_split[1], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[2])]
                elif type == 12:
                    ts = datetime.strptime(line_split[0], '%Y-%m-%d %H:%M:%S')
                    d = [float(line_split[1])]

                #print(ts, d)
                times.append(ts)
                data.append(d)

                number_samples += 1

            line = fp.readline()
            cnt += 1

    if type == 0:
        print("Unknown type")
        exit(-1)

    if nVariables < 1:
        print('No Variables, exiting')
        exit(-1)

    # trim data to what was read
    print("nSamples %d nVariables %d" % (number_samples, nVariables))

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    ncOut.instrument = 'Vemco - ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

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
    ncTimesOut[:] = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

    # for each variable in the data file, create a netCDF variable
    ncVarOut = ncOut.createVariable("TEMP", "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
    ncVarOut.units = "degrees_Celsius"
    ncVarOut[:] = np.array([d[0] for d in data])

    if nVariables == 2:
        ncVarOut = ncOut.createVariable("PRES", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
        ncVarOut.units = "dbar"
        ncVarOut[:] = np.array([d[1] for d in data])

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])
