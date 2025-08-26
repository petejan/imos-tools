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
import os

from datetime import datetime, timedelta, UTC
from dateutil import parser

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


# [Fri May 13 05:00:59.414 2011]
# [Fri May 13 05:00:59.414 2011] mvs 1
# [Fri May 13 05:01:00.394 2011] 07/01/01 05:03:54        5255
# [Fri May 13 05:01:01.164 2011] mvs 0
# [Fri May 13 05:01:01.174 2011]
# [Fri May 13 05:01:01.174 2011] Ser PAR-135
# [Fri May 13 05:01:01.174 2011] Ver PARS 4.02
# [Fri May 13 05:01:01.184 2011] Ave 310
# [Fri May 13 05:01:01.194 2011] Pkt 1
# [Fri May 13 05:01:01.194 2011] Set 1
# [Fri May 13 05:01:01.204 2011] Rec 1
# [Fri May 13 05:01:01.204 2011] Asv 4
# [Fri May 13 05:01:01.204 2011] Int 00:00:05
# [Fri May 13 05:01:01.214 2011] Dat 07/01/01
# [Fri May 13 05:01:01.224 2011] Clk 05:03:55
# [Fri May 13 05:01:01.234 2011] Mem 171
# 5672 records to read
# 10/21/00        00:44:36        5349
# 10/21/00        00:44:42        5374
# 10/21/00        00:45:22        4427
# 10/21/00        00:47:52        5247
# 10/21/00        00:59:42        8151
# 10/21/00        01:01:02        8010
# 10/21/00        01:01:53        7995
# 10/21/00        01:02:16        7992
# 10/21/00        01:04:17        13420
# 10/21/00        01:12:13        7479
# 10/21/00        01:12:29        7477

ser_exp = '\[(.*)\] Ser.(.*)$'
ver_exp = '\[(.*)\] Ver.(.*)$'
ave_exp = '\[(.*)\] Ave.(.*)$'
dat_exp = '\[(.*)\] Dat.(.*)$'
clk_exp = '\[(.*)\] Clk.(.*)$'

line_re = r'.*(\d{2}/\d{2}/\d{2}.*\d{2}:\d{2}:\d{2})\s([0-9\t\- ]*)$'

#
# parse the file
#

def eco_parse(files):
    time = []
    value = []

    filepath = files[0]
    number_samples = 0

    instrument_serial_number = 'unknown'
    instrument_fw_version = None
    instrument_ave_setting = None
    instrument_date = None
    instrument_time = None
    last_time = datetime(1900,1,1)
    first_line_values = 0

    with open(filepath, 'r', errors='ignore') as fp:
        line = fp.readline()
        while line:
            line = line.replace("\n", "")
            if len(line) > 0:
                #print(line)
                matchObj = re.match(ser_exp, line)
                if matchObj:
                    instrument_serial_number = matchObj.group(2)
                    print("instrument_serial_number", instrument_serial_number)
                matchObj = re.match(ver_exp, line)
                if matchObj:
                    instrument_fw_version = matchObj.group(2)
                matchObj = re.match(ave_exp, line)
                if matchObj:
                    instrument_ave_setting = matchObj.group(2)
                matchObj = re.match(dat_exp, line)
                if matchObj:
                    instrument_date = [matchObj.group(1), matchObj.group(2)]
                matchObj = re.match(clk_exp, line)
                if matchObj:
                    instrument_time = [matchObj.group(1), matchObj.group(2)]

                line_s = line.strip()
                matchObj = re.match(line_re, line_s)
                if matchObj:
                    #print('match line', matchObj.group(1))
                    try:
                        ts = datetime.strptime(matchObj.group(1), "%m/%d/%y\t%H:%M:%S")
                        #print('time stamp', ts)
                        values_split = matchObj.group(2).split('\t')
                        #print('values', values_split)
                        # print(len(values_split), values_split)
                        # assume the fist line has the correct number of values
                        if first_line_values == 0:
                            first_line_values = len(values_split)
                        # does this line have the same number of values as the first
                        if len(values_split) == first_line_values and ts > last_time:

                            values = [float(x) if len(x) <= 5 else np.nan for x in values_split]
                            print(ts, values)

                            # if values[0] == 700 and values[2] == 695 and values[4] == 460 and values[6] > 500:
                            # if values[0] == 695 and values[2] == 700:
                            if ts == last_time:
                                ts = ts + timedelta(seconds=0.1)
                            last_time = ts
                            time.append(ts)
                            value.append(values)
                            print(ts, values)

                            number_samples += 1
                    except ValueError:
                        pass

            line = fp.readline()

    print("nSamples %d " % number_samples)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = os.path.basename(filepath) + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    ncOut.instrument = "WetLABs ; ECO-PARS"
    ncOut.instrument_model = "ECO-PARS"
    ncOut.instrument_serial_number = instrument_serial_number
    if instrument_fw_version:
        ncOut.instrument_fw_version = instrument_fw_version
    if instrument_ave_setting:
        ncOut.instrument_ave_version = instrument_ave_setting

    time_diff = timedelta(0)
    if instrument_time and instrument_date:
        print('instrument time', instrument_time, 'instrument date', instrument_date)

        date_download = datetime.strptime(instrument_time[0], '%Y-%m-%d %H:%M:%S.%f')
        inst_time = datetime.strptime(instrument_date[1] + " " + instrument_time[1], '%m/%d/%y %H:%M:%S')

        time_diff = date_download - inst_time
        print('date_download', date_download, 'inst_time', instrument_time, 'diff', time_diff)

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
    time = [t+time_diff for t in time]
    ncTimesOut[:] = date2num(time, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

    ncVarOut = ncOut.createVariable('PAR_COUNT', "f4", ("TIME",), zlib=True)
    ncVarOut.sensor_SeaVoX_L22_code = 'SDN:L22::TOOL0676'
    ncVarOut.units = '1'
    ncVarOut.long_name = 'par raw counts'
    ncVarOut.coordinates = 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH'
    ncVarOut[:] = value

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    eco_parse(sys.argv[1:])
