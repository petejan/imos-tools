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

import datetime
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct

gps_dict = ['type', 'date', 'time', 'latdm', 'latNS', 'lonDM', 'lonEW', 'lock', 'error']
ctd_dict = ['type', 'date', 'time', 'srate', 'sam', 'crc']
imm_dict = ['type', 'date', 'time', 'depth', 'dir', 'np', 'pre', 'ul', 'll', 'ip', 'err_to', 'trip', 'err_c', 'vacume', 'slow_d', 'water', 'storm', 'mode']
aadi_dict = ['type', 'date', 'time', 'srate', 'sam', 'crc']


def parse(filepath):

    nv = -1
    f = open(filepath)
    line = f.readline()
    while line:
        #print(line)

        if len(line) > 1:
            line_split = line.split()

            if nv == -1:
                samples_read = 0
                line_type = line_split[0]
                if line_type == 'GPS':
                    values = dict(zip(gps_dict, line_split))
                    nv = -1
                elif line_type == 'IMM':
                    values = dict(zip(imm_dict, line_split))
                    nv = -1
                elif line_type == 'CTD':
                    values = dict(zip(ctd_dict, line_split))
                    nv = 3
                elif line_type == 'AADI':
                    values = dict(zip(aadi_dict, line_split))
                    nv = 2

                dt = datetime.datetime.strptime(values['date'] + " " + values['time'], '%m/%d/%Y %H:%M:%S')
                print(dt)
            else:
                samples_read += len(line_split)
                samples_to_read = int(values['sam'])
                print(values['type'], 'samples to read ', samples_to_read, values['sam'], samples_read/nv)
                if samples_to_read <= samples_read/nv:
                    nv = -1
                    print('finished reading')

            print(values)

        line = f.readline()
    f.close()

    # # create the netCDF file
    # outputName = filepath + ".nc"
    #
    # print("output file : %s" % outputName)
    #
    # ncOut = Dataset(outputName, 'w', format='NETCDF4')
    #
    # # add time variable
    #
    # #     TIME:axis = "T";
    # #     TIME:calendar = "gregorian";
    # #     TIME:long_name = "time";
    # #     TIME:units = "days since 1950-01-01 00:00:00 UTC";
    #
    # tDim = ncOut.createDimension("TIME")
    # ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    # ncTimesOut.long_name = "time"
    # ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    # ncTimesOut.calendar = "gregorian"
    # ncTimesOut.axis = "T"
    #
    # ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
    #
    # ncOut.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    # ncOut.setncattr("time_coverage_end", ts.strftime(ncTimeFormat))
    #
    # # add creating and history entry
    # ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    # ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)
    #
    # return outputName


if __name__ == "__main__":
    for s in sys.argv[1:]:
        parse(s)
