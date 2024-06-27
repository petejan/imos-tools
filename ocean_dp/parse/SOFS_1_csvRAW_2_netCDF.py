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
import os.path
import sys
import re

from datetime import datetime, timedelta, UTC

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


# parse the file
# we don't get the serial number from the download files, so get it from the input arguments
def parse(filepath, sn='unknown'):

    number_samples = 0
    optode_bphase = []
    optode_temp = []
    times = []

    instrument_model = 'Optode 3830'
    if sn:
        instrument_serialnumber = sn
    else:
        instrument_serialnumber = 'unknown'

    with open(filepath, 'r', errors='ignore') as fp:
        cnt = 1
        line = fp.readline()
        hdr = line.split(',')
        print('header', hdr)

        line = fp.readline()

        while line:
            line_split = line.split(',')

            #print(line_split)

            if len(line_split) >= 18:
                ts = datetime.strptime(line_split[1].strip(), "%Y-%m-%d %H:%M:%S")
                #print(ts)

                if (len(line_split[6]) > 0) & (len(line_split[7]) > 0):
                    times.append(ts)

                    optode_temp.append(float(line_split[6]))
                    optode_bphase.append(float(line_split[7]))

                    number_samples += 1

                if (len(line_split[8]) > 0) & (len(line_split[9]) > 0):
                    times.append(ts+timedelta(minutes=30))

                    optode_temp.append(float(line_split[8]))
                    optode_bphase.append(float(line_split[9]))

                    number_samples += 1

            line = fp.readline()
            cnt += 1

    # trim data to what was read
    print("nSamples", (number_samples))

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    ncOut.instrument = 'Aanderaa ; ' + instrument_model
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

    ncVarOut = ncOut.createVariable("BPHASE", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = optode_bphase
    ncVarOut.units = "1"

    ncVarOut = ncOut.createVariable("OTEMP", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = optode_temp
    ncVarOut.units = 'degrees_Celsius'

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

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
