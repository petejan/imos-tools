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
import csv

from datetime import datetime, timedelta, UTC

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

import numpy as np
import re

# attempt at general csv parser
#
# example from spotter
# name,time,batteryPower,batteryVoltage,latitude,longitude,meanDirection,meanDirectionalSpread,meanPeriod,peakDirection,peakDirectionalSpread,peakPeriod,significantWaveHeight,solarVoltage,spotter_id,spotter_name
# spotter,2023-05-18T05:54:55Z,,,-46.8669,141.79413,271.956,62.711,9.6,271.373,60.157,10.24,4.53,,SPOT-1011,
# spotter,2023-05-18T06:24:55Z,,,-46.8715,141.77798,263.749,68.836,9.1,236.275,60.149,11.36,3.83,,SPOT-1011,
#
# parse the file
#

nameMap = {}
nameMap["latitude"] = {'name': "YPOS", 'units': "degrees_north"}
nameMap["longitude"] = {'name': "XPOS", 'units': 'degrees_east'}
nameMap["significantWaveHeight"] = {'name': "Hs", 'units': 'm'}
nameMap["meanDirection"] = {'name': "WAVE_DIR_MEAN", 'units': 'degrees'}
nameMap["meanDirectionalSpread"] = {'name': "WAVE_DIR_MEAN_SPREAD", 'units': 'degrees'}
nameMap["meanPeriod"] = {'name': "Tav", 'units': 's'}
nameMap["peakDirection"] = {'name': "WAVE_DIR_PEAK", 'units': 'degrees'}
nameMap["peakDirectionalSpread"] = {'name': "WAVE_DIR_PEAK_SPREAD", 'units': 'degrees'}
nameMap["peakPeriod"] = {'name': "Tp", 'units': 's'}


def csv_parse(f, model=None, serial=None):
    time = []
    value = []
    hdr = None
    units = None

    filepath = f[0]
    number_samples = 0

    with open(filepath, 'r', errors='ignore') as fp:

        reader = csv.DictReader(fp)
        hdr = []
        units = []
        first = True
        for line in reader:

            #print(line)
            v = []
            for k in line.keys():
                #print(line[k])
                if k == 'time':
                    ts = datetime.strptime(line[k], '%Y-%m-%dT%H:%M:%SZ')
                elif k in nameMap:
                    if first:
                        hdr.append(nameMap[k]['name'])
                        units.append(nameMap[k]['units'])
                    v.append(float(line[k]))

            time.append(ts)
            value.append(v)
            number_samples += 1
            first = False
            #print(ts, hdr, v)

            # t = line_split[1][0:4] + " 20" + line_split[1][4:] + " " + line_split[2]
            # #print(t)
            # try:
            #     ts = datetime.strptime(t, "%m%d %Y %H%M%S")
            #     #print(ts)
            #     v = [float(x) for x in line_split[3:]]
            #     if len(v) == len(hdr):
            #         time.append(ts)
            #         value.append(v)
            #         number_samples += 1
            # except ValueError:
            #     pass
            
            line = fp.readline()

    print("nSamples %d " % number_samples)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    if model:
        ncOut.instrument = "SOFAR: " + model
        ncOut.instrument_model = model
    if serial:
        ncOut.instrument_serial_number = serial

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
    ncTimesOut[:] = date2num(time, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

    for i in range(0, len(hdr)):
        print(i, hdr[i], len(value[-1]))
        ncVarOut = ncOut.createVariable(hdr[i], "f4", ("TIME",), zlib=True)
        ncVarOut.units = units[i]
        ncVarOut[:] = [v[i] for v in value]

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
    s = None
    m = None
    for arg in sys.argv[1:]:
        if arg.startswith('-model='):
            m = arg.replace('-model=', '')
        elif arg.startswith('-serial='):
            s = arg.replace('-serial=', '')
        else:
            files.append(arg)

    print('model ', m)
    csv_parse(files, serial=s, model=m)
