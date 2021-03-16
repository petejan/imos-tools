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

import numpy
import pytz
from netCDF4 import date2num, num2date
from netCDF4 import Dataset
import numpy as np
from dateutil import parser

import sqlite3
from datetime import datetime, timezone

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

nameMap["temp14"] = "TEMP"
nameMap["cond10"] = "CNDC"
nameMap["pres24"] = "PRES"
nameMap["fluo10"] = "CPHL"
nameMap["par_00"] = "PAR"
nameMap["turb00"] = "NTU"

# also map units .....

unitMap = {}
unitMap["C"] = "degrees_Celsius"


def parse(file):

    filepath = file[0]

    outputName = filepath + ".nc"

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    conn = sqlite3.connect(filepath)

    cur = conn.cursor()

    cur.execute('SELECT * FROM instruments')

    row = cur.fetchone()

    instrument_model = row[2]
    instrument_serial_number = str(row[1])
    ncOut.instrument = 'RBR ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serial_number

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME")

    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"

    # fetch the channel info
    cur = conn.cursor()
    cur.execute('SELECT * FROM channels')

    row = cur.fetchone()
    print(cur.description)

    channel_list = {}
    while (row):
        print('channels', row[1])
        channel_list[row[0]] = {
                                          'shortName': row[1],
                                          'longName': row[2],
                                          'units': row[3],
                                          'longNamePlainText': row[4],
                                          'unitsPlainText': row[5]
                                          }
        row = cur.fetchone()

    #print(channel_list)

    cur = conn.cursor()
    cur.execute('SELECT * FROM epochs')
    row = cur.fetchone()
    print('epochs count (deploymentID, start_time, end_time)', row)
    start_time = datetime.fromtimestamp(row[1]/1000, tz=pytz.UTC)
    end_time = datetime.fromtimestamp(row[2]/1000, tz=pytz.UTC)
    print('start_time =', start_time, 'end_time =', end_time)

    # fetch the data
    cur = conn.cursor()
    cur.execute('SELECT * FROM data') # downsample100 has smaller data table

    # build the variables needed
    print(cur.description)
    data_channels = []
    i = 0
    for channel_desc in cur.description:
        print('data header', channel_desc)
        matchObj = re.match("channel(\d*)", channel_desc[0])
        if matchObj:
            id = matchObj.group(1)
            ch = channel_list[int(id)]
            print('channel id', id, ch)
            var_name = ch['shortName']
            if var_name in nameMap:
                var_name = nameMap[var_name]

            nc_var_out = ncOut.createVariable(var_name, "f4", ("TIME",), zlib=True)
            nc_var_out.long_name = ch['longNamePlainText']
            nc_var_out.units = ch['unitsPlainText']

            data_channels.append([i, ch, nc_var_out])
        i = i + 1

    # read data table into netCDF variables
    cur.arraysize = 1024*1024
    records = cur.fetchmany()
    print('array size', cur.arraysize, len(records))
    data = numpy.array(records, dtype=float)
    print(data[:, 0])

    t0 = date2num(datetime(1950, 1, 1), units=ncTimesOut.units, calendar=ncTimesOut.calendar) - date2num(datetime(1970, 1, 1), units=ncTimesOut.units, calendar=ncTimesOut.calendar)

    sample = 0
    while len(records) > 0:
        #timestamp = datetime.fromtimestamp(data[:, 0] / 1000, tz=timezone.utc)
        #print(timestamp, records)
        #print(data[:, 0].shape)
        #ncTimesOut[sample:sample+len(records)] = data[:, 0]/1000/24/3600 - t0 # date2num(timestamp, units=ncTimesOut.units, calendar=ncTimesOut.calendar)
        
        ncTimesOut[sample:sample + len(records)] = data[:, 0]/1000/24/3600-t0

        for channel_desc in data_channels:
            channel_desc[2][sample:sample+len(records)] = data[:, channel_desc[0]]

        sample = sample + len(records)

        records = cur.fetchmany()
        print('fetch records=', len(records), 'sample number=', sample)
        data = numpy.array(records, dtype=float)

        # some feedback
        #sample = sample + len(records)
        #if (sample % 1000) == 0:
        #    print(sample, timestamp)
        #if sample > 1024*1024:
        #    break

    conn.close()

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])

