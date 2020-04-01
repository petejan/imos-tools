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

# fixed format reading of output from Sea-Bird electronics instrument dd command output (format = 3, engineering units)

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

# ds
# SBE37SM-RS232 3.0h  SERIAL NO. 6962  23 Mar 2010 06:07:37
# vMain =  7.02, vLith =  3.26
# samplenumber = 262098, free = 297142
# not logging, stop command
# sample interval = 60 seconds
# data format = converted engineering
# transmit real-time = no
# sync mode = no
# pump installed = no
# <Executed/>
# dc
# SBE37SM-RS232 V 3.0h  6962
# temperature:  15-May-09
#     TA0 = 2.243980e-05
#     TA1 = 2.780597e-04
#     TA2 = -2.782710e-06
#     TA3 = 1.589985e-07
# conductivity:  15-May-09
#     G = -1.006941e+00
#     H = 1.477867e-01
#     I = -2.227526e-04
#     J = 3.824996e-05
#     CPCOR = -9.570000e-08
#     CTCOR = 3.250000e-06
#     WBOTC = 2.551112e-07
# pressure S/N 2863059, range = 508 psia  04-May-09
#     PA0 = -9.528925e-03
#     PA1 = 1.676997e-03
#     PA2 = 1.202383e-11
#     PTCA0 = 5.232644e+05
#     PTCA1 = 1.922083e+00
#     PTCA2 = -1.141774e-01
#     PTCB0 = 2.507875e+01
#     PTCB1 = -8.500000e-04
#     PTCB2 = 0.000000e+00
#     PTEMPA0 = -6.912989e+01
#     PTEMPA1 = 5.083005e-02
#     PTEMPA2 = -5.488727e-07
#     POFFSET = 0.000000e+00
# <Executed/>
# dd
# start time = 21 May 2009 16:37:26
# start sample number = 1915
#
#  13.4082, 0.00004,   -0.119, 22 Sep 2009, 00:00:01
#  13.4073, 0.00003,   -0.117, 22 Sep 2009, 00:01:01
#  13.4061, 0.00003,   -0.118, 22 Sep 2009, 00:02:01
#  13.4053, 0.00003,   -0.117, 22 Sep 2009, 00:03:01
#  13.4041, 0.00003,   -0.118, 22 Sep 2009, 00:04:01
#  13.4029, 0.00003,   -0.118, 22 Sep 2009, 00:05:01
#  13.4018, 0.00003,   -0.120, 22 Sep 2009, 00:06:01

var_temp = {'name': 'TEMP', 'attributes': {'units' : 'degrees_Celsius', 'instrument_uncertainty' : np.float32(0.005)}}
var_cndc = {'name': 'CNDC', 'attributes': {'units' : 'S/m', 'instrument_uncertainty' : np.float32(0.0005)}}
var_pres = {'name': 'PRES', 'attributes': {'units' : 'dbar', 'instrument_uncertainty' : np.float32(0.1/100 * 2000)}}
var_dox  = {'name': 'DOX', 'attributes': {'units' : 'ml/l', 'instrument_uncertainty' : np.float32(0.07)}}
var_psal = {'name': 'PSAL', 'attributes': {'units' : '1'}}

var_names3 = [var_temp, var_cndc, var_pres]
var_names5 = [var_temp, var_cndc, var_pres, var_dox, var_psal]

model_serial_expr = r".*(SBE\S*) (\S*).*SERIAL NO. (\S*).*$"

temp_cal_expr     = r"^temperature: \s*(\S*)"
cond_cal_expr     = r"^conductivity: \s*(\S*)"
pres_cal_expr     = r"^pressure S/N \s*(\S*), range = (\S*) psia\s*(\S*)"

cal_val_expr      = r"^\s*(\S*) = ([0-9e+-\.]*)"

#
# parse the file
#

def parse(files):

    filepath = files[0]

    dataLine = 0
    d = []

    temp_cal = False
    cond_cal = False
    pres_cal = False
    data_lines = False

    nVariables = 0
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
            #print('line dataline ', data_lines, " : ", line)
            if ~data_lines:
                # match calibrations
                # temp
                if temp_cal:
                    matchObj = re.match(cal_val_expr, line)
                    if matchObj:
                        #print("cal_val_expr:matchObj.group() : ", matchObj.group())
                        var_temp['attributes']['calibration_'+matchObj.group(1)] = float(matchObj.group(2))
                    else:
                        temp_cal = False
                matchObj = re.match(temp_cal_expr, line)
                if matchObj:
                    temp_cal = True
                    var_temp['attributes']['calibration_date'] =  matchObj.group(1)

                # conductivity
                if cond_cal:
                    matchObj = re.match(cal_val_expr, line)
                    if matchObj:
                        #print("cal_val_expr:matchObj.group() : ", matchObj.group())
                        var_cndc['attributes']['calibration_'+matchObj.group(1)] = float(matchObj.group(2))
                    else:
                        cond_cal = False
                matchObj = re.match(cond_cal_expr, line)
                if matchObj:
                    cond_cal = True
                    var_cndc['attributes']['calibration_date'] =  matchObj.group(1)

                # pressure
                if pres_cal:
                    matchObj = re.match(cal_val_expr, line)
                    if matchObj:
                        #print("cal_val_expr:matchObj.group() : ", matchObj.group())
                        var_pres['attributes']['calibration_'+matchObj.group(1)] = float(matchObj.group(2))
                    else:
                        pres_cal = False
                matchObj = re.match(pres_cal_expr, line)
                if matchObj:
                    pres_cal = True
                    var_pres['attributes']['calibration_date'] =  matchObj.group(3)
                    var_pres['attributes']['calibration_range_psia'] =  matchObj.group(2)
                    var_pres['attributes']['calibration_SN'] =  matchObj.group(1)

                # match the serial number expression
                #print("Line {}: {} : {}".format(cnt, dataLine, line.strip()))
                matchObj = re.match(model_serial_expr, line)
                # print("match ", matchObj)
                if matchObj:
                    #print("model_serial_expr:matchObj.group() : ", matchObj.group())
                    print("model_serial_expr:matchObj.group(1) : ", matchObj.group(1))
                    #print("model_serial_expr:matchObj.group(1) : ", matchObj.group(2))
                    print("model_serial_expr:matchObj.group(3) : ", matchObj.group(3))
                    instrument_model = matchObj.group(1)
                    instrument_serialnumber = matchObj.group(3)

            line_split = line.split(',')

            #print("splits ", len(line_split))

            if len(line_split) >= 5 and line[0] == ' ':
                ts = datetime.strptime(line_split[-2].strip() + ' ' + line_split[-1].strip(), "%d %b %Y %H:%M:%S")
                d = [float(v) for v in line_split[0:-2]]
                nVariables = len(d)

                #print(ts, d)

                times.append(ts)
                data.append(d)

                data_lines = True # we're now in the data section, stop looking for headers

                number_samples += 1

            line = fp.readline()
            cnt += 1

    # trim data to what was read
    print("nSamples %d nVariables %d" % (number_samples, nVariables))

    print('instrument ', instrument_model, ':', instrument_serialnumber)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    ncOut.instrument = 'Sea-Bird Electronics ; ' + instrument_model
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

    var_names = var_names3
    if nVariables == 5:
        var_names = var_names5

    # for each variable in the data file, create a netCDF variable
    for v in range(0, nVariables):
        ncVarOut = ncOut.createVariable(var_names[v]["name"], "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
        for a in var_names[v]["attributes"]:
            ncVarOut.setncattr(a, var_names[v]["attributes"][a])
        ncVarOut[:] = np.array([d[v] for d in data])

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
