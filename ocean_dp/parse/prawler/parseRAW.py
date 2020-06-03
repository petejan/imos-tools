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
from datetime import timedelta, datetime

import glob

times = []
data = []
locations = []

def dm(x):
    degrees = int(x) // 100
    minutes = x - 100*degrees

    return degrees, minutes


def decimal_degrees(degrees, minutes):

    return degrees + minutes/60


def parse(files):

    fn = []
    for f in files:
        fn.extend(glob.glob(f))

    times_out = []
    depth_out = []
    temp_out = []
    cndc_out = []
    dox2_out = []
    dox2_temp_out = []
    flu_out = []
    bs_out = []

    profile_n_out = []
    profile_sample_out = []

    n_profile = 0
    line_number = 0
    sample_n = 0
    loc_sample = {}

    last_gps = None
    for filepath in fn:
        nv = -1
        print('file name', filepath)

        f = open(filepath)
        line = f.readline()
        while line:
            line = line.strip()
            # print(line_number, line)
            if len(line) > 1:
                if line.startswith("%%"):
                    line_number = 0

                if line.startswith('%%PRAWC'):
                    type = 'PRAWC'
                    sample_n = 0
                    n_profile += 1
                elif line.startswith('%%GPS'):
                    type = 'GPS'
                elif line.startswith('%%PRAWE'):
                    type = 'PRAWE'

                if line_number == 1:
                    hdr = line.split(',')
                    print("Header ", hdr)
                elif line_number > 1:
                    line_split = line.split(',')
                    if len(line_split) > 1:
                        # print("line split ", line_split)
                        dictionary = dict(zip(hdr, line_split))
                        #print(dictionary)
                        if type == 'PRAWC':
                            sec = int(dictionary['EP'], 16)
                            times_out.append(timedelta(seconds=sec) + datetime(1970,1,1))
                            profile_sample_out.append(sample_n)
                            depth_out.append(float(dictionary['CD']))
                            temp_out.append(float(dictionary['CT']))
                            cndc_out.append(float(dictionary['CC']))
                            dox2_out.append(float(dictionary['O2']))
                            dox2_temp_out.append(float(dictionary['OT']))
                            flu_out.append(float(dictionary['CH']))
                            bs_out.append(float(dictionary['TB']))

                            profile_n_out.append(n_profile)

                            sample_n += 1
                        if type == 'GPS':
                            loc_sample['ts'] = dictionary['DT']
                            loc_sample['datetime'] = datetime.strptime(dictionary['DT'], "%Y-%m-%dT%H:%M:%SZ")
                            loc_sample['latitude'] = float(dictionary['LAT'])/100
                            loc_sample['longitude'] = float(dictionary['LON'])/100

                            locations.append(loc_sample)


                line_number += 1

            line = f.readline()

    if n_profile == 0:
        print('no profiles found, returning')
        return None

    # create the netCDF file
    outputName = 'prawler' + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    ncOut.instrument = 'NOAA - Prawler'
    ncOut.instrument_model = 'Prawler'
    ncOut.instrument_serial_number = '4'
    ncOut.number_of_profiles = np.int32(n_profile)

    if last_gps:
        ncOut.latitude = last_gps['latitude']
        ncOut.longitude = last_gps['longitude']

    # add time variable

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    #print(locations)
    ncOut.createDimension("POS", len(locations))
    nc_var_out = ncOut.createVariable("XPOS", "f4", ("POS"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = [l['longitude'] for l in locations]
    nc_var_out.units = 'degrees_East'
    nc_var_out = ncOut.createVariable("YPOS", "f4", ("POS"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = [l['latitude'] for l in locations]
    nc_var_out.units = 'degrees_North'
    nc_var_out = ncOut.createVariable("TPOS", "d", ("POS"), fill_value=np.nan, zlib=True)
    nc_var_out.long_name = "position time"
    nc_var_out.units = "days since 1950-01-01 00:00:00 UTC"
    nc_var_out.calendar = "gregorian"
    nc_var_out[:] = [date2num(datetime.strptime(loc['ts'], "%Y-%m-%dT%H:%M:%SZ"), calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC") for loc in locations]


    tDim = ncOut.createDimension("TIME")
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = date2num(times_out , calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    nc_var_out = ncOut.createVariable("PRES", "f4", ("TIME"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = depth_out
    nc_var_out.units = 'dbar'
    nc_var_out = ncOut.createVariable("TEMP", "f4", ("TIME"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = temp_out
    nc_var_out.units = 'degrees_Celsius'
    nc_var_out = ncOut.createVariable("CNDC", "f4", ("TIME"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = cndc_out
    nc_var_out.units = 'S/m'
    nc_var_out = ncOut.createVariable("DOX2_RAW", "f4", ("TIME"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = dox2_out
    #nc_var_out.units = 'umol/kg'
    nc_var_out.comment = 'RAW DOX2, referenced to PSAL=0'
    nc_var_out = ncOut.createVariable("DOX2_TEMP", "f4", ("TIME"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = dox2_temp_out
    nc_var_out.units = 'degrees_Celsius'
    nc_var_out = ncOut.createVariable("FLU", "f4", ("TIME"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = flu_out
    nc_var_out.units = 'counts'
    nc_var_out = ncOut.createVariable("BS", "f4", ("TIME"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = bs_out
    nc_var_out.units = 'counts'

    nc_var_out = ncOut.createVariable("PROFILE", "i4", ("TIME"), fill_value=-1, zlib=True)
    nc_var_out[:] = profile_n_out
    nc_var_out.units = 'count'
    nc_var_out = ncOut.createVariable("PROFILE_SAMPLE", "i4", ("TIME"), fill_value=-1, zlib=True)
    nc_var_out[:] = profile_sample_out
    nc_var_out.units = 'count'

    ncOut.setncattr("time_coverage_start", times_out[0].strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", times_out[-1].strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])
