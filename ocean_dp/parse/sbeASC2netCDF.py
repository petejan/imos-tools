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

from datetime import datetime, timedelta
from dateutil import parser

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

import os

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
nameMap["temperature"] = "TEMP"
nameMap["conductivity"] = "CNDC"
nameMap["pressure"] = "PRES"

# example:
# * Sea-Bird SBE37 Data File:
# * FileName = I:\science\SOTS\SAZ-20_Data\SBE37-1777-1000m\SBE37-1777-1000m.asc
# * Software Version 1.59
# * Temperature SN = 1777
# * Conductivity SN = 1777
# * System UpLoad Time = Mar 24 2019 23:34:24
# ** SAZ20-2018
# ** 1000m
# **
# * ds
# * SBE37-SM V 2.6b  SERIAL NO. 1777    24 Mar 2019  23:34:51
# * not logging: received stop command
# * sample interval = 600 seconds
# * samplenumber = 55726, free = 243867
# * do not transmit real-time data
# * do not output salinity with each sample
# * do not output sound velocity with each sample
# * do not store time with each sample
# * number of samples to average = 4
# * serial sync mode disabled
# * wait time after serial sync sampling = 30 seconds
# * internal pump not installed
# * temperature = 11.72 deg C^M
#
# * S>
# * SBE37-SM V 2.6b  1777
# * temperature:  21-mar-15
# *     TA0 = 4.038028e-05
# *     TA1 = 2.689302e-04
# *     TA2 = -2.059206e-06
# *     TA3 = 1.484023e-07
# * conductivity:  21-mar-15
# *     G = -9.625585e-01
# *     H = 1.291223e-01
# *     I = -7.715407e-05
# *     J = 2.536878e-05
# *     CPCOR = -9.570000e-08
# *     CTCOR = 3.250000e-06
# *     WBOTC = 9.645460e-06
# * pressure S/N 2432215, range = 5076 psia:  12-mar-15
# *     PA0 = -1.980161e+00
# *     PA1 = 2.435868e-01
# *     PA2 = -1.592412e-07
# *     PTCA0 = 7.238512e+01
# *     PTCA1 = -2.274251e-01
# *     PTCA2 = -5.191362e-03
# *     PTCSB0 = 2.481788e+01
# *     PTCSB1 = -1.625000e-03
# *     PTCSB2 = 0.000000e+00
# *     POFFSET = 0.000000e+00
# * rtc:  21-mar-15
# *     RTCA0 = 9.999703e-01
# *     RTCA1 = 1.803684e-06
# *     RTCA2 = -3.297514e-08^M
#
# * S>
# *END*
# start time =  03 Mar 2018  00:00:01
# sample interval = 600 seconds
# start sample number = 1
#  18.7545, 0.00001,    0.296


# search expressions within file

first_line_expr   = r"\* Sea-Bird (.*) Data File:"

end_expr          = r"\*END\*"
model_serial_expr = r"\* (SBE.*) [Vv] ?(\S*).*SERIAL NO. (\S*)"
sample_interval   = r"\* sample interval = (\d*) (\S*)"

start_time_expr   = r"start time =\s*(.*)"
sample_int2_expr  = r"sample interval =\s*(\d*)\s*(.*)"
first_sample_expr = r"start sample number =\s*(\d*)"
n_samples_expr    = r"\* samplenumber = (\d*)"

cal_expr          = r"\* ((\S*).*):\s*(.*)"
cal_val_expr      = r"\*\s*(\S*) = ([0-9e+-\.]*)"

navr_expr        = r"\* number of samples to average = (\s*)"

nortd_expr        = r"\* do not transmit real-time data"
nosal_expr        = r"\* do not output salinity with each sample"
novel_expr        = r"\* do not output sound velocity with each sample"
notime_expr       = r"\* do not store time with each sample"
hastime_expr       = r"\* store time with each sample"

#
# parse the file
#

def sbe_asc_parse(files):

    outputNames = []
    for filepath in files:

        hdr = True
        sensor = False
        dataLine = 0
        name = []
        text = []
        number_samples_read = 0
        nVars = 0

        cal_param = None
        cal_sensor = None
        cal_tags = []
        data = None
        no_time = False
        number_samples = 1
        times = None
        nVariables = 0

        instrument_model = 'unknown'
        instrument_serialnumber = 'unknown'
        sample_interval = None

        with open(filepath, 'r', errors='ignore') as fp:
            line = fp.readline()
            matchObj = re.match(first_line_expr, line)
            if not matchObj:
                print("Not a Sea Bird ASC file !")
                continue

            cnt = 1
            while line:
                #print("Line {}: {} : {}".format(cnt, dataLine, line.strip()))

                if hdr:
                    if sensor:
                        matchObj = re.match(cal_val_expr, line)
                        if matchObj:
                            #print("cal_val_expr:matchObj.group() : ", matchObj.group())
                            #print("cal_val_expr:matchObj.group(1) : ", matchObj.group(1))
                            cal_param = matchObj.group(1)
                            cal_value = matchObj.group(2)
                            cal_tags.append((cal_sensor, cal_param, cal_value))
                            print("calibration type %s param %s value %s" % (cal_sensor, cal_param, cal_value))

                        else:
                            sensor = False

                    matchObj = re.match(cal_expr, line)
                    if matchObj:
                        print("cal_expr:matchObj.group() : ", matchObj.group())
                        #print("cal_expr:matchObj.group(1) : ", matchObj.group(1))
                        sensor = True
                        cal_param = None
                        cal_sensor = matchObj.group(2)
                        cal_tags.append((cal_sensor, "comment", matchObj.group(1) + " " + matchObj.group(3)))

                    matchObj = re.match(notime_expr, line)
                    if matchObj:
                        no_time = True
                    matchObj = re.match(hastime_expr, line)
                    if matchObj:
                        no_time = False

                    matchObj = re.match(model_serial_expr, line)
                    if matchObj:
                        print("model_serial_expr:matchObj.group() : ", matchObj.group())
                        print("model_serial_expr:matchObj.group(1) : ", matchObj.group(1))
                        print("model_serial_expr:matchObj.group(1) : ", matchObj.group(2))
                        print("model_serial_expr:matchObj.group(1) : ", matchObj.group(3))
                        instrument_model = matchObj.group(1)
                        instrument_serialnumber = matchObj.group(3)

                    matchObj = re.match(n_samples_expr, line)
                    if matchObj:
                        #print("n_samples_expr:matchObj.group() : ", matchObj.group())
                        number_samples = int(matchObj.group(1))

                    matchObj = re.match(end_expr, line)
                    if matchObj:
                        #print("end_expr:matchObj.group() : ", matchObj.group())
                        hdr = False

                else:

                    dataL = True
                    matchObj = re.match(start_time_expr, line)
                    if matchObj:
                        #print("start_time_expr:matchObj.group() : ", matchObj.group())
                        start_time = parser.parse(matchObj.group(1))
                        #print("start time ", start_time)
                        dataL = False

                    matchObj = re.match(sample_int2_expr, line)
                    if matchObj:
                        #print("sample_int2_expr:matchObj.group() : ", matchObj.group())
                        sample_interval = int(matchObj.group(1))
                        dataL = False

                    matchObj = re.match(first_sample_expr, line)
                    if matchObj:
                        #print("first_sample_expr:matchObj.group() : ", matchObj.group())
                        dataL = False

                    if dataL and (line.count(",") > 0):
                        lineSplit = line.strip().split(",")
                        if data is None:
                            print("First data line : ", line.strip())
                            # does the file have time record for each sample, if so it uses 2 values on the line
                            if no_time:
                                nVariables = len(lineSplit)
                            else:
                                nVariables = len(lineSplit) - 2  # 2 for the date and time
                                times = []

                            print("number variables ", nVariables)
                            print("data split number ", len(lineSplit), instrument_model)
                            data = np.zeros((number_samples, nVariables))
                            data.fill(np.nan)
                            name.insert(0, {'col': 0, 'var_name': "TEMP", 'comment': None, 'unit': "degrees_Celsius"})
                            try:
                                if instrument_model.index("37") >= 0:
                                    if nVariables >= 2:
                                        name.insert(1, {'col': 1, 'var_name': "CNDC", 'comment': None, 'unit': "S/m"})
                                    if nVariables >= 3:
                                        name.insert(2, {'col': 2, 'var_name': "PRES", 'comment': None, 'unit': "dbar"})
                                    if nVariables >= 4:
                                        name.insert(3, {'col': 3, 'var_name': "PSAL", 'comment': None, 'unit': "1"})
                            except ValueError:
                                if nVariables >= 2:
                                    name.insert(1, {'col': 1, 'var_name': "PRES", 'comment': None, 'unit': "dbar"})

                            print("variables:")
                            for v in name:
                                print(" ", v)

                        if not no_time:
                            times.append(parser.parse(lineSplit[-2] + " " + lineSplit[-1]))

                        #print(line.strip())
                        splitVarNo = 0
                        try:
                            for v in lineSplit:
                                if splitVarNo < len(name):
                                    data[number_samples_read][splitVarNo] = float(lineSplit[splitVarNo])
                                splitVarNo = splitVarNo + 1
                            number_samples_read = number_samples_read + 1
                        except ValueError:
                            print("bad line ", lineSplit)
                            pass

                        dataLine = dataLine + 1

                line = fp.readline()
                cnt += 1

        if nVariables < 1:
            print('No Variables, exiting')
            exit(-1)

        # if there is no times, calculate from sample interval and start time
        if times is None:
            times = [start_time + timedelta(seconds=sample_interval*x) for x in range(0, number_samples_read)]

        # trim data to what was read
        odata = data[:number_samples_read]
        print("nSamples %d samplesRead %d nVariables %d data shape %s read %s" % (number_samples, number_samples_read, nVariables, data.shape, odata.shape))

        #
        # build the netCDF file
        #

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        outputName = filepath + ".nc"
        print("output file : %s" % outputName)
        outputNames.append(outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4')

        ncOut.instrument = 'Sea-Bird Electronics ; ' + instrument_model
        ncOut.instrument_model = instrument_model
        ncOut.instrument_serial_number = instrument_serialnumber
        if sample_interval:
            ncOut.instrument_sample_interval = np.float(sample_interval)
        #ncOut.instrument_model = instrument_model

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
        ncTimesOut[:] = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

        # add any RTC calibration constants to the TIME variable
        for c in cal_tags:
            if c[0] == 'rtc':
                try:
                    ncTimesOut.setncattr('calibration_' + c[1], np.float(c[2]))
                except ValueError:
                    ncTimesOut.setncattr('calibration_' + c[1], c[2])

        # for each variable in the data file, create a netCDF variable
        i = 0
        for v in name:
            print("Variable %s : unit %s" % (v['var_name'], v['unit']))
            varName = v['var_name']
            ncVarOut = ncOut.createVariable(varName, "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max

            #print("Create Variable %s : %s" % (ncVarOut, data[v[0]]))
            ncVarOut[:] = odata[:, v['col']]
            ncVarOut.units = v['unit']

            # add any calibration values for this variable
            for c in cal_tags:
                add = False
                cal_name = nameMap.get(c[0])
                if cal_name == varName:
                    try:
                        ncVarOut.setncattr('calibration_' + c[1], np.float32(c[2]))
                    except ValueError:
                        ncVarOut.setncattr('calibration_' + c[1], c[2])


            i = i + 1

        # add timespan attributes
        ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

        # add creating and history entry
        ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
        ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

        ncOut.close()

    return outputNames


if __name__ == "__main__":
    sbe_asc_parse(sys.argv[1:])
