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

from datetime import datetime, timedelta
from dateutil import parser

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

import numpy as np
import csv

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

# 2018-08-21 22:11:15.010, COM6, Rx, 6.92
# 2018-08-21 22:11:15.010, COM6, Rx, -6.92
# 2018-08-21 22:11:15.030, COM6, Rx, -6.91


#
# parse the file
#

def sbe_asc_parse(file):

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = file + ".nc"

    print("output file : %s" % outputName)

    dataset = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    dataset.instrument = "Lowell - MAT-1"
    dataset.instrument_model = "MAT-1"
    dataset.instrument_serial_number = "1805306"

    time = dataset.createDimension('TIME', None)
    times = dataset.createVariable('TIME', np.float64, ('TIME',))

    times.units = 'days since 1950-01-01 00:00:00'
    times.calendar = 'gregorian'

    yaw = dataset.createVariable('head', np.float32, ('TIME', ))
    pitch = dataset.createVariable('pitch', np.float32, ('TIME', ))
    roll = dataset.createVariable('roll', np.float32, ('TIME', ))

    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                #ts = datetime.strptime(row[0] + ' ' + row[1], '%Y-%m-%d %H:%M:%S.%f')
                try:
                    ts = datetime.strptime(row[0], '%Y-%m-%dT%H:%M:%S.%f')
                    times[line_count-1] = date2num(ts, units=times.units, calendar=times.calendar)

                    #acc[line_count-1, :] = row[2:5]
                    #mag[line_count-1, :] = row[5:8]
                    yaw[line_count-1] = row[1]
                    pitch[line_count-1] = row[2]
                    roll[line_count-1] = row[3]

                    line_count += 1
                    if (line_count % 1000) == 0:
                        print (ts)
                except ValueError:
                    print('value error ', row)

        print(f'Processed {line_count} lines.')

        print(f'wrote {len(times)} lines.')

    dataset.close()


    return outputName


if __name__ == "__main__":
    sbe_asc_parse(sys.argv[1])
