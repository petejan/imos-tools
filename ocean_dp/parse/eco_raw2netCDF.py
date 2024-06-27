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

from datetime import datetime, timedelta, UTC
from dateutil import parser
from glob2 import glob

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

import numpy as np
import re

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

# 65114 records to read
# 07/02/19        05:38:02        695     59      700     160     544
# 07/02/19        05:38:03        695     59      700     161     544
# 07/02/19        05:38:04        695     61      700     156     544
# 07/02/19        05:38:05        695     61      700     151     544
# 07/02/19        05:38:06        695     57      700     154     544
# 07/02/19        05:53:23        695     62      700     203     555
# 07/02/19        05:53:51        695     71      700     202     552
# 07/02/19        05:53:52        695     69      700     203     552
# 07/02/19        05:53:53        695     67      700     205     552

nameMap = {}
nameMap["Chl"] = "CPHL"
nameMap["NTU"] = "TURB"

#
# parse the file
#

line_re = r'(\d{2}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2})\s([0-9\t\- ]*)$'

line_re_test = r'(\d{2}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2})(?:\t\-?([0-9]+))+$'

dev_sn_expr = r'ECO\s*(?P<inst>\S*)-(?P<serial>\S*)'
dev_col_expr = r'COLUMNS=(?P<cols>\d*)'
dev_created_expr = r'Created on:\s*(.*)$'
dev_col_cal_expr = r'(\S*)=(\d)(.*)$'

def dev_file_parse(dev_file):

    dev_cal_cols = []
    with open(dev_file, 'r', errors='ignore') as fp:
        line = fp.readline()
        while line:
            line_s = line.strip()
            #print(line_s)
            matchObj = re.match(dev_sn_expr, line_s)
            if matchObj:
                dev_type = matchObj.group(1)
                dev_sn = matchObj.group(2)

            matchObj = re.match(dev_created_expr, line_s)
            if matchObj:
                dev_cal_date = matchObj.group(1)

            matchObj = re.match(dev_col_cal_expr, line_s)
            if matchObj:
                dev_cal = matchObj.group(1)
                dev_caln = matchObj.group(2)
                dev_cal_values = matchObj.group(3)
                if dev_cal != 'N/U' and dev_cal != 'COLUMNS' and dev_cal != 'DATE' and dev_cal != 'TIME':

                    if dev_cal == 'PAR':
                        line_im = fp.readline()
                        line_a1 = fp.readline()
                        line_a0 = fp.readline()
                        v = [float(line_im.split("=")[1].strip()), float(line_a1.split("=")[1].strip()), float(line_a0.split("=")[1].strip())]
                    else:
                        v = dev_cal_values.split("\t")

                    dev_cals = (dev_cal, dev_caln, v)
                    dev_cal_cols.append(dev_cals)

            line = fp.readline()

    ret = dev_type, dev_sn, dev_cal_date, dev_cal_cols
    print('device calibration', ret)

    return ret


def eco_parse(files, dev_file):
    time = []
    value = []
    first_line_values = 0
    last_time = datetime(2000, 1, 1)

    filepath = files[0]
    number_samples = 0

    dev_file_info = None
    if dev_file:
        dev_file_info = dev_file_parse(dev_file)

    with open(filepath, 'r', errors='ignore') as fp:
        line = fp.readline()
        while line:
            line_s = line.strip()
            #print(line_s)
            matchObj = re.match(line_re, line_s)
            if matchObj:
                try:
                    ts = datetime.strptime(matchObj.group(1), "%m/%d/%y\t%H:%M:%S")
                    values_split = matchObj.group(2).split('\t')
                    #print(len(values_split), values_split)
                    # assume the fist line has the correct number of values
                    if first_line_values == 0:
                        first_line_values = len(values_split)
                    # does this line have the same number of values as the first
                    if len(values_split) == first_line_values and ts > last_time:

                            values = [float(x) if len(x) <= 5 else np.nan for x in values_split]
                            print(ts, values)

                            #if values[0] == 700 and values[2] == 695 and values[4] == 460 and values[6] > 500:
                            #if values[0] == 695 and values[2] == 700:
                            if ts == last_time:
                                ts = ts + timedelta(seconds=0.1)
                            last_time = ts
                            time.append(ts)
                            value.append(values)

                            number_samples += 1
                except ValueError:
                    pass

            line = fp.readline()

    print("nSamples %d times %d" % (number_samples, len(time)))

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    if dev_file_info:
        ncOut.instrument = "WetLABs ; " + dev_file_info[0]
        ncOut.instrument_model = dev_file_info[0]
        ncOut.instrument_serial_number = dev_file_info[1]
        ncOut.instrument_calibration_date = dev_file_info[2]

    else:
        ncOut.instrument = "WetLABs ; unknown"
        ncOut.instrument_model = "unknown"
        ncOut.instrument_serial_number = "unknown"

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

    print('number values', len(value[0]))

    if dev_file_info:
        #print(dev_file_info[3])
        for i in range(0, len(dev_file_info[3])):
            info = dev_file_info[3][i]
            #print(info)
            idx = int(info[1])
            ncVarName = info[0]
            if ncVarName in nameMap:
                # print('name map ', nameMap[varName])
                ncVarName = nameMap[info[0]]

            ncVarOut = ncOut.createVariable(ncVarName, "f4", ("TIME",), zlib=True)
            if info[0] == 'PAR':
                ncVarOut[:] = [info[2][0] * 10 ** ((v[idx-3] - info[2][1])/info[2][2]) for v in value]
                ncVarOut.setncattr('calibration_Im', info[2][0])
                ncVarOut.setncattr('calibration_a1', info[2][1])
                ncVarOut.setncattr('calibration_a0', info[2][2])
            else:
                scale = float(info[2][1])
                dark = float(info[2][2])
                print(ncVarName, 'scale, dark, index', scale, dark, idx-3, value[0][idx - 3], len(value[0]) - idx + 3)
                ncVarOut[:] = [((v[idx - 3] - dark) * scale) if (len(v) - idx + 3) >= 1 else np.nan for v in value]
                ncVarOut.setncattr('calibration_scale', scale)
                ncVarOut.setncattr('calibration_dark', dark)

            ncVarOut = ncOut.createVariable('ECO_' + dev_file_info[0] + '_' + ncVarName, "f4", ("TIME",), zlib=True)
            ncVarOut.units = '1'
            if ncVarName == 'CPHL':
                ncVarOut.CH_DIGITAL_DARK_COUNT = float(info[2][2])
                ncVarOut.CH_DIGITAL_SCALE_FACTOR = float(info[2][1])

            if ncVarName == 'TURB':
                ncVarOut.TURB_DIGITAL_DARK_COUNT = float(info[2][2])
                ncVarOut.TURB_DIGITAL_SCALE_FACTOR = float(info[2][1])

            ncVarOut[:] = [v[idx - 3] if (len(v) - idx + 3) >= 1 else np.nan for v in value]

    else:
        for i in range(0, len(value[0])):
            ncVarOut = ncOut.createVariable('V_'+str(i), "f4", ("TIME",), zlib=True)
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
    dev_file = None
    dev_file_next = False
    for f in sys.argv[1:]:
        print('arg', f)
        if f == '--dev':
            dev_file_next = True
        elif dev_file_next:
            dev_file = f
            dev_file_next = False
        else:
            files.extend(glob(f))

    if dev_file:
        print('using device file', dev_file)

    eco_parse(files, dev_file)
