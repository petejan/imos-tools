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

sn_expr = r"SBE\s+\S+\s+V\s+\S+\s+SERIAL NO. (\S*)"
hw_expr = r"<HardwareData DeviceType = '(\S*)' SerialNumber = '(\S*)'>"

#   8.7883,  3.65110,   31.803, 2.4296, 2.9464, 0.1280, 0.0789, 4.7138, 1.5351, 101297400,  8.629, 30 Sep 2009 00:00:43

var_temp = {'name': 'TEMP', 'attributes': {'units' : 'degrees_Celsius', 'instrument_uncertainty' : np.float32(0.005)}}
var_cndc = {'name': 'CNDC', 'attributes': {'units' : 'S/m', 'instrument_uncertainty' : np.float32(0.0005)}}
var_pres = {'name': 'PRES', 'attributes': {'units' : 'dbar', 'instrument_uncertainty' : np.float32(0.1/100 * 2000)}}
var_volt1 = {'name': 'V0', 'attributes': {'units' : 'V'}}
var_volt2 = {'name': 'V1', 'attributes': {'units' : 'V'}}
var_volt3 = {'name': 'V2', 'attributes': {'units' : 'V'}}
var_volt4 = {'name': 'V3', 'attributes': {'units' : 'V'}}
var_volt5 = {'name': 'V4', 'attributes': {'units' : 'V'}}
var_volt6 = {'name': 'V5', 'attributes': {'units' : 'V'}}
var_tgp = {'name': 'TOTAL_GAS_PRESSURE', 'attributes': {'units' : 'millibars'}}
var_tgtd = {'name': 'GTD_TEMP', 'attributes': {'units' : 'degrees_Celsius'}}
var_psal = {'name': 'PSAL', 'attributes': {'units' : '1'}}

var_names11 = [var_temp, var_cndc, var_pres, var_volt1, var_volt2, var_volt3, var_volt4, var_volt5, var_volt6, var_tgp, var_tgtd]
var_names12 = [var_temp, var_cndc, var_pres, var_volt1, var_volt2, var_volt3, var_volt4, var_volt5, var_volt6, var_tgp, var_tgtd, var_psal]


# parse the file
# we don't get the serial number from the download files, so get it from the input arguments
def parse(filepath, sn='unknown'):

    dataLine = 0
    d = []

    nVariables = 0
    number_samples = 0
    data = []
    times = []
    instrument_model = "SBE16plusV2"
    if sn:
        instrument_serialnumber = sn
    else:
        instrument_serialnumber = 'unknown'

    type = 0

    with open(filepath, 'r', errors='ignore') as fp:
        cnt = 1
        line = fp.readline()

        while line:
            #print("Line {}: {} : {}".format(cnt, dataLine, line.strip()))
            matchObj = re.match(sn_expr, line)
            if matchObj:
                # print("sn_expr:matchObj.group() : ", matchObj.group())
                print("sn_expr:matchObj.group(1) : ", matchObj.group(1))
                instrument_serialnumber = "0160" + matchObj.group(1)
            matchObj = re.match(hw_expr, line)
            if matchObj:
                # print("sn_expr:matchObj.group() : ", matchObj.group())
                print("sn_expr:matchObj.group(1) : ", matchObj.group(1))
                print("sn_expr:matchObj.group(2) : ", matchObj.group(2))
                instrument_model = matchObj.group(1)
                instrument_serialnumber = matchObj.group(2)

            line_split = line.split(',')

            #print("splits ", len(line_split))

            if len(line_split) > 10:
                ts = datetime.strptime(line_split[-1].strip(), "%d %b %Y %H:%M:%S")
                d = [float(v) for v in line_split[0:-1]]
                nVariables = len(d)

                #print(ts, d)

                times.append(ts)
                data.append(d)

                number_samples += 1

            line = fp.readline()
            cnt += 1

    # trim data to what was read
    print("nSamples %d nVariables %d" % (number_samples, nVariables))

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

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

    var_names = var_names11
    if nVariables == 12:
        var_names = var_names12

    # for each variable in the data file, create a netCDF variable
    for v in range(0, nVariables):
        print("variable : ", var_names[v]["name"])
        ncVarOut = ncOut.createVariable(var_names[v]["name"], "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
        for a in var_names[v]["attributes"]:
            ncVarOut.setncattr(a, var_names[v]["attributes"][a])
        ncVarOut[:] = np.array([d[v] for d in data])

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    files = []
    sn = None
    sn_file_next = False
    for f in sys.argv[1:]:
        print('arg', f)
        if f == '--sn':
            sn_file_next = True
        elif sn_file_next:
            sn = f
            sn_file_next = False
        else:
            files.extend(glob(f))

    if sn:
        print('using serial number :', sn)

    parse(files[0], sn)
