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
import os

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

# 2011-08-03 12:05:30 ,len=292 ,site=20 ,datatype=MRU ,retryCount=0 ,CSQ=5 ,filesToTx=1 ,numModems=1 ,filePointer=49 ,min=-2.72 ,scale=140.13 ,BatteryVolts=13.42 ,zAccelAverage=-9.83 ,zAccelStd=2.15 ,waveheight=14.39 ,log[0]=-2.69 ,log[1]=-2.12 ,log[2]=-2.19 ,log[3]=-2.05 ,log[4]=-2.12 ,log[5]=-2.42 ,log[6]=-2.20 ,log[7]=-1.59 ,log[8]=-1.52 ,log[9]=-2.01 ,log[10]=-1.82 ,log[11]=-1.65

		# double df = 2.5/256;
		# add("df", "#0.###E0", df);
		# add("log[0]", (b[0] / scale) + offset);
		# for (int j = 1; j < 256; j++)
		# {
		# 	d = (b[j] / scale) + offset;
		# 	double f = j * 2.5 / 256.0;
		# 	double wds = Math.pow(10, d) / Math.pow(2 * Math.PI * f , 4) / df;
		# 	add("wds["+j+"]", "#0.###E0", wds);
		# }


#
# parse the file
#


def datalogger(file):

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = file + ".nc"

    print("output file : %s" % outputName)

    dataset = Dataset(outputName, 'w', format='NETCDF4')

    dataset.instrument = "LORD Sensing Microstrain ; 3DM-GX1"
    dataset.instrument_model = "3DM-GX1"
    dataset.instrument_serial_number = "unknown"  # Pulse-8, 10 9475, Pulse-9 5925

    time = dataset.createDimension('TIME', None)
    v = dataset.createDimension('FREQ', 256)
    times = dataset.createVariable('TIME', np.float64, ('TIME',))

    times.units = 'days since 1950-01-01 00:00:00'
    times.calendar = 'gregorian'

    freq_var = dataset.createVariable('FREQ', np.float32, ('FREQ', ), fill_value=np.nan)

    swh_var = dataset.createVariable('WAVE_HEIGHT_SIG', np.float32, ('TIME', ), fill_value=np.nan)
    lat_var = dataset.createVariable('YPOS', np.float32, ('TIME', ), fill_value=np.nan)
    lon_var = dataset.createVariable('XPOS', np.float32, ('TIME', ), fill_value=np.nan)

    nds_var = dataset.createVariable('NON_DIR_SPEC', np.float32, ('TIME', 'FREQ', ), fill_value=np.nan)

    lat = np.nan
    lon = np.nan
    df = 2.5/256
    freq = np.arange(start=0, stop=2.5, step=df)
    #print("freq", freq)
    freq_var[:] = freq

    nds = np.zeros(256)
    ts_start = None
    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            #print('row ', row)

            # parse the data line
            ts = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S ')
            row_dict = {}
            # split the name=value parse into a dict
            for r in row:
                if r.find('=') > 0:
                    split = r.split('=')
                    #print("split", split)
                    row_dict[split[0]] = split[1].strip()

            # save the GPS lat/lon also
            if row_dict['datatype'] == 'GPS':
                lat = float(row_dict['lat'])
                lon = float(row_dict['lon'])

            # MRU data, contains wave height and spectra
            if row_dict['datatype'] == 'MRU':
                if not ts_start:
                    ts_start = ts  # save the first timestamp

                times[line_count] = date2num(ts, units=times.units, calendar=times.calendar)
                lat_var[line_count] = lat
                lon_var[line_count] = lon
                swh_var[line_count] = float(row_dict['waveheight'])
                # read the wave spectra (acceleration)
                for i in range(0, 256):
                    nds[i] = float(row_dict['log['+str(i)+']'])

                # convert to wave displacemet spectra
                wds = 10**nds / (2*freq*np.pi)**4 / df
                wds[0] = np.nan # first displancement is nan (DC offset)
                #print("wds", wds)
                # write the spectra to the netCDF file
                nds_var[line_count, :] = wds

                ts_end = ts # save the timestamp, for the last timestamp

                line_count += 1
                if (line_count % 1000) == 0:
                    print (ts)

        print(f'Processed {line_count} lines.')

        print(f'wrote {len(times)} times.')

    dataset.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    dataset.setncattr("time_coverage_end", ts_end.strftime(ncTimeFormat))

    # add creating and history entry
    dataset.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    dataset.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(file))

    dataset.close()

    return outputName


if __name__ == "__main__":
    datalogger(sys.argv[1])
