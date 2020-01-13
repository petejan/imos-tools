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
from datetime import timedelta

import glob

gps_dict = ['type', 'date', 'time', 'latDM', 'latNS', 'lonDM', 'lonEW', 'lock', 'error']
ctd_dict = ['type', 'date', 'time', 'srate', 'sam', 'crc']
imm_dict = ['type', 'date', 'time', 'depth', 'dir', 'np', 'pre', 'ul', 'll', 'ip', 'err_to', 'trip', 'err_c', 'vacume', 'slow_d', 'water', 'storm', 'mode']
aadi_dict = ['type', 'date', 'time', 'srate', 'sam', 'crc']

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

    last_gps = None
    for filepath in fn:
        nv = -1
        print('file name', filepath)

        f = open(filepath)
        line = f.readline()
        while line:
            #print(line)

            if len(line) > 1:
                line_split = line.split()
                if len(line_split) > 1:
                    line_type = line_split[0]
                    if line_type in ['GPS', 'IMM', 'CTD', 'AADI']:
                        samples_read = 0
                        if line_type == 'GPS':
                            values = dict(zip(gps_dict, line_split))
                            latitude = decimal_degrees(*dm(float(values['latDM'])))
                            if values['latNS'] == 'S':
                                latitude = -latitude
                            print('latitude ', latitude)
                            longitude = decimal_degrees(*dm(float(values['lonDM'])))
                            if values['lonEW'] == 'W':
                                longitude = -longitude
                            print('longitude ', longitude)
                            values['latitude'] = latitude
                            values['longitude'] = longitude
                            locations.append(values)

                            last_gps = values
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

                        #print(values)

                        dt = datetime.datetime.strptime(values['date'] + " " + values['time'], '%m/%d/%Y %H:%M:%S')
                        #print(dt)
                        if dt not in times:
                            times.append(dt)
                            data.append([])
                        data_block = {'dt': dt, 'hdr': values, line_type: []}
                        idx = times.index(dt)
                        #print ('times idx', idx, times[idx])
                    else:
                        samples_read += len(line_split)
                        data_block[values['type']].extend(line_split)
                        samples_to_read = int(values['sam'])
                        # print(values['type'], 'samples to read ', samples_to_read, values['sam'], samples_read/nv)
                        if samples_to_read <= samples_read/nv:
                            nv = -1
                            # print('finished reading')
                            data[idx].append(data_block)
                            #print(data)

            line = f.readline()
        f.close()

        print('finished reading, processing')

        times_out = []
        depth_out = []
        temp_out = []
        cndc_out = []
        dox2_out = []
        dox2_temp_out = []
        profile_n_out = []
        profile_sample_out = []

        print('data length', len(data))
        n_profile = 0
        for d in data:
            #print('data loop', d)
            if len(d) > 0:
                x = d[0]
                #print(x['hdr'])
                hdr = x['hdr']
                dt = int(hdr['srate'])
                samples = int(hdr['sam'])
                depth = np.zeros(samples)
                depth.fill(np.nan)
                temp = np.zeros(samples)
                temp.fill(np.nan)
                cndc = np.zeros(samples)
                cndc.fill(np.nan)

                dox_temp = np.zeros(samples)
                dox_temp.fill(np.nan)
                dox2 = np.zeros(samples)
                dox2.fill(np.nan)

                profile_n = np.ones(samples, dtype=int) * n_profile
                profile_sample = range(0, samples)

                for n in range(0, samples):
                    times_out.append(x['dt'] + timedelta(seconds = (dt * n)))

                for x in d:
                    print (x['dt'], x['hdr'])
                    if 'CTD' in x:
                        n = 0
                        raw_data = x['CTD']
                        for vals in range(0, len(raw_data), 3):
                            depth_val = float(raw_data[vals])/100
                            temp_val = float(raw_data[vals+1])/1000
                            cndc_val = float(raw_data[vals+2])/10000

                            #print(n, depth_val, temp_val, cndc_val)

                            depth[n] = depth_val
                            temp[n] = temp_val
                            cndc[n] = cndc_val

                            n += 1

                        depth_out.extend(depth)
                        temp_out.extend(temp)
                        cndc_out.extend(cndc)

                    if 'AADI' in x:
                        n = 0
                        raw_data = x['AADI']
                        for vals in range(0, len(raw_data), 2):
                            dox_temp_val = float(raw_data[vals])/1000
                            dox2_val = float(raw_data[vals+1])/100

                            #print(n, dox_temp_val, dox2_val)

                            dox_temp[n] = dox_temp_val
                            dox2[n] = dox2_val

                            n += 1

                        dox2_out.extend(dox2)
                        dox2_temp_out.extend(dox_temp)

                profile_n_out.extend(profile_n)
                profile_sample_out.extend(profile_sample)

                n_profile += 1

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
    nc_var_out[:] = [l['latitude'] for l in locations]
    nc_var_out.units = 'days since 1950-01-01 00:00:00 UTC'
    nc_var_out.long_name = "position time"
    nc_var_out.units = "days since 1950-01-01 00:00:00 UTC"
    nc_var_out.calendar = "gregorian"
    nc_var_out[:] = [date2num(datetime.datetime.strptime(loc['date'] + " " + loc['time'], '%m/%d/%Y %H:%M:%S'), calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC") for loc in locations]


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
    nc_var_out = ncOut.createVariable("PROFILE", "i4", ("TIME"), fill_value=-1, zlib=True)
    nc_var_out[:] = profile_n_out
    nc_var_out.units = 'count'
    nc_var_out = ncOut.createVariable("PROFILE_SAMPLE", "i4", ("TIME"), fill_value=-1, zlib=True)
    nc_var_out[:] = profile_sample_out
    nc_var_out.units = 'count'

    ncOut.setncattr("time_coverage_start", times_out[0].strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", times_out[-1].strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])
