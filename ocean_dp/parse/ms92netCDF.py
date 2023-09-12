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

first_line_expr = r"In-situ Marine Optics"
data_start_expr = r"-------------------------------"
serial_expr     = r"MS9.* .SN:(.*)."
setup_expr      = r"(.*).=.(.*)$"
in_air_expr     = r"IN-AIR$"
in_water_expr   = r"IN-WATER$"
wavelengths_expr   = r"WAVELENGTHS.=.\[(.*)\]$"

def parse(file):

    hdr = True
    dataLine = 0
    name = []
    number_samples_read = 0
    nVars = 0
    data = []
    ts = []
    setup = []
    timezone = 0

    for filepath in file[1:]:

        with open(filepath, 'r', errors='ignore') as fp:
            line = fp.readline()

            matchObj = re.match(first_line_expr, line)
            if not matchObj:
                print("Not a MS9 log file !")
                return None

            cnt = 1
            while line:
                #print(line)

                if hdr:
                    matchObj = re.match(data_start_expr, line)
                    if matchObj:
                        #print("data_start_expr:matchObj.group() : ", matchObj.group())
                        #print("data_start_expr:matchObj.group(1) : ", matchObj.group(1))
                        hdr = False

                    matchObj = re.match(serial_expr, line)
                    if matchObj:
                        print("serial_expr:matchObj.group() : ", matchObj.group())
                        print("serial_expr:matchObj.group(1) : ", matchObj.group(1))
                        instrument_serial_number = matchObj.group(1)

                    matchObj = re.match(setup_expr, line)
                    if matchObj:
                        #print("setup_expr:matchObj.group() : ", matchObj.group())
                        #print("setup_expr:matchObj.group(1) : ", matchObj.group(1))
                        #print("setup_expr:matchObj.group(2) : ", matchObj.group(2))
                        setup.append((matchObj.group(1).replace(" ", "_"), matchObj.group(2)))
                        if matchObj.group(1) == 'TIMEZONE':
                            timezone = float(matchObj.group(2))
                        if matchObj.group(1) == 'DETECTOR UNITS':
                            units = matchObj.group(2)

                    matchObj = re.match(in_air_expr, line)
                    if matchObj:
                        setup.append(("IN_AIR", "1"))

                    matchObj = re.match(in_water_expr, line)
                    if matchObj:
                        setup.append(("IN_WATER", "1"))

                    matchObj = re.match(wavelengths_expr, line)
                    if matchObj:
                        #print("wavelengths_expr:matchObj.group() : ", matchObj.group())
                        #print("wavelengths_expr:matchObj.group(1) : ", matchObj.group(1))

                        wavelengths = matchObj.group(1)
                        wlens = [float(x) for x in wavelengths.split(",")]
                        #print (wlens)

                else:
                    lineSplit = line.split(',')
                    if (lineSplit[0].startswith('MS9')):
                        #print(lineSplit)
                        t = parser.parse(lineSplit[2] + " " + lineSplit[3], dayfirst=True)
                        ts.append(t-timedelta(hours=timezone))
                        #print("timestamp %s" % (t-timedelta(hours=timezone)))
                        dat = [float(d) for d in lineSplit[-9:]]
                        data.append(dat)
                        #print(t-timedelta(hours=timezone), dat)

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
    print("file timezone", timezone)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    ncOut.instrument = 'In-situ Marine Optics ; ' + 'MS9'
    ncOut.instrument_model = 'MS9'
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

    t_diff = np.diff(ncTimesOut[:])
    t_diff_min = min(t_diff)
    print('minimum time diff', t_diff_min)
    if t_diff_min <= 0:
        print('**** WARNING time not-monotonic ** minimum time difference', t_diff_min)
        print('indexes', np.where(t_diff <= 0))
    if (ncTimesOut[:] > date2num(datetime.now(), calendar=ncTimesOut.calendar, units=ncTimesOut.units)).any():
        print('**** WARNING time in future')
        idx = np.where(ncTimesOut[:] > date2num(datetime.now(), calendar=ncTimesOut.calendar, units=ncTimesOut.units))
        print('indexes', idx)
        print(num2date(ncTimesOut[idx], units=ncTimesOut.units, calendar=ncTimesOut.calendar, only_use_cftime_datetimes=False, only_use_python_datetimes=True))
    if (ncTimesOut[:] < date2num(datetime(1980, 1, 1), calendar=ncTimesOut.calendar, units=ncTimesOut.units)).any():
        print('**** WARNING time before 1980')
        idx = np.where(ncTimesOut[:] < date2num(datetime(1980, 1, 1), calendar=ncTimesOut.calendar, units=ncTimesOut.units))
        print('indexes', idx)
        print(num2date(ncTimesOut[idx], units=ncTimesOut.units, calendar=ncTimesOut.calendar, only_use_cftime_datetimes=False, only_use_python_datetimes=True))

    for s in setup:
        print(s)
        ncOut.setncattr("comment_setup_" + s[0].lower(), s[1])

    ncOut.createDimension("WAVELENGTH", len(wlens))
    ncVarOut = ncOut.createVariable('WAVELENGTH', "f4", ("WAVELENGTH"), fill_value=None)
    ncVarOut.units = "nm"
    ncVarOut[:] = wlens

    ncVarOut = ncOut.createVariable('RAD', "f4", ("WAVELENGTH", "TIME"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut.units = units
    data_array = np.array([d for d in data])
    print(data_array.shape)
    ncVarOut[:] = data_array.transpose()

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv)

