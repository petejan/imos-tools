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

from glob2 import glob
from netCDF4 import date2num
from netCDF4 import Dataset

import numpy as np
import os
import re

import struct
from collections import namedtuple

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
from setuptools.glob import glob2

nameMap = {}
nameMap["BV"] = "battery"
nameMap["OBP"] = "OPTODE_BPHASE"
nameMap["OT"] = "OPTODE_TEMP"
nameMap["CHL"] = "ECO_FLNTUS_CPHL"
nameMap["NTU"] = "ECO_FLNTUS_TURB"
nameMap["PAR"] = "PAR_VOLT"
nameMap["meanLoad"] = "mean_load"

def gps_dm_degree (lat_dm, lat_ns):
    lat_d = int(lat_dm / 100)
    lat = lat_d + (lat_dm - (lat_d * 100)) / 60
    if lat_ns == 'S' or lat_ns == 'W':
        lat = -lat

    return lat


def round_time(dt=None, roundTo=3600):
    """Round a datetime object to any time lapse in seconds
    dt : datetime.datetime object, default now.
    roundTo : Closest number of seconds to round to, default 1 hour.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
    """
    if dt is None:
        dt = datetime.now()

    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds+roundTo/2) // roundTo * roundTo

    return dt + timedelta(0, rounding-seconds, -dt.microsecond)


def create_nc_var(d_array, imu_times, name_name, imu_array, nc_var, done_ind, imu_ind):

    d_array.fill(np.nan)
    print('len imu', len(imu_ind), len(done_ind), d_array.shape)
    for i in range(len(imu_ind)):
        print(name_name, i, imu_times[i], imu_ind[i], done_ind[i])
        if len(d_array.shape) == 3:
            d_array[:, :, done_ind[i]] = imu_array[imu_ind[i]][name_name]
        else:
            d_array[:, done_ind[i]] = imu_array[imu_ind[i]][name_name]

    nc_var[:] = d_array


def datalogger(outputName, files):

    decode_dict = {'StabQ0': 0, 'StabQ1': 1, 'StabQ2': 2, 'StabQ3': 3,
                   'MagFieldX': 4, 'MagFieldY': 5, 'MagFieldZ': 6,
                   'AccelX': 7, 'AccelY': 8, 'AccelZ': 9,
                   'CompAngleRateX': 10, 'CompAngleRateY': 11, 'CompAngleRateZ': 12,
                   'Timer': 13,
                   'CheckSum': 14,
                   'Load': 15,
                   }

    scale = [1/ 8192.0, 1/ 8192.0, 1/ 8192.0, 1/ 8192.0,
             1/ (32768000.0 / 2000), 1/ (32768000.0 / 2000), 1/ (32768000.0 / 2000),
             9.81/ (32768000.0 / 8500), 9.81/ (32768000.0 / 8500), 9.81/ (32768000.0 / 8500),
             1 / (32768000.0 / 10000), 1 / (32768000.0 / 10000), 1 / (32768000.0 / 10000),
             0.2, 1, 1]

    start_data = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) \*\*\*\*\*\* START RAW MRU DATA \*\*\*\*\*\*')
    end_data = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) \*\*\*\*\*\* END DATA \*\*\*\*\*\*')
    done_str = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: \d done time \d+ ,(.*)$')

    gps_fix = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO : GPS Fix (\d+) Latitude (\S+) Longitude (\S+) sats (\S+) HDOP (\S+)')
    gps_fix2 = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO : GPS RMC Fix (\d+) Latitude (\S+) Longitude (\S+)')

    gps_rmc = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: GPS String \'GPRMC\' string (\S+)')

    sn = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: Station Name (\S+)')
    sn_imu = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: MRU SerialNumber (\d+)')

    # TODO: process the output of this parser into frequency spectra, wave height

    # TODO: process all historical data, Pulse, SOFS-1 .... to SOFS-10

    instrument_serial_number = 'unknown'
    instrument_imu_serial_number = 'unknown'

    ts_start = None
    ts_end = None

    sample = 0

    done_array = []
    imu_array = []
    gps_array = []
    rmc_array = []

    pos = 0
    samples_read = 0

    xs = None

    for file in files:

        with open(file, "rb") as f:
            byte = f.read(1)
            while byte != b"":
                if byte[0] == 0x0c: # start of IMU stabQ packet
                    pos = f.tell()

                    packet = f.read(30+4)
                    if len(packet) == 34:
                        # check the checksum
                        decode_s = struct.unpack('>15H', packet[0:30])
                        cksum = 12
                        for i in range(0, 14):
                            cksum = cksum + decode_s[i]
                        cksum = cksum & 0xffff
                        if cksum == decode_s[14]:
                            # check sum ok, decode the packet
                            samples_read += 1

                            decode = struct.unpack('>4h3h3h3hHHf', packet)

                            # scale each value in the packet
                            decode_scale = []
                            for x in range(0,len(decode)):
                                d = decode[x] * scale[x]
                                decode_scale.append(d)

                            # print decoded data as dict
                            #decode_d = dict(zip(decode_dict, decode))
                            #print('decode timer', decode_d['Timer'], 'sample', sample)

                            # save data to arrays
                            if sample == 0:
                                accel_samples = np.full([3072, 3], np.nan)
                                mag_samples = np.full([3072, 3], np.nan)
                                gyro_samples = np.full([3072, 3], np.nan)
                                quat_samples = np.full([3072, 4], np.nan)

                                load_samples = np.full([3072], np.nan)

                                samples_read = 0

                            if sample < 3072:
                                accel_samples[sample, 0] = decode_scale[decode_dict['AccelX']]
                                accel_samples[sample, 1] = decode_scale[decode_dict['AccelY']]
                                accel_samples[sample, 2] = decode_scale[decode_dict['AccelZ']]

                                quat_samples[sample, 0] = decode_scale[decode_dict['StabQ0']]
                                quat_samples[sample, 1] = decode_scale[decode_dict['StabQ1']]
                                quat_samples[sample, 2] = decode_scale[decode_dict['StabQ2']]
                                quat_samples[sample, 3] = decode_scale[decode_dict['StabQ3']]

                                mag_samples[sample, 0] = decode_scale[decode_dict['MagFieldX']]
                                mag_samples[sample, 1] = decode_scale[decode_dict['MagFieldY']]
                                mag_samples[sample, 2] = decode_scale[decode_dict['MagFieldZ']]

                                gyro_samples[sample, 0] = decode_scale[decode_dict['CompAngleRateX']]
                                gyro_samples[sample, 1] = decode_scale[decode_dict['CompAngleRateY']]
                                gyro_samples[sample, 2] = decode_scale[decode_dict['CompAngleRateZ']]

                                load_samples[sample] = decode_scale[decode_dict['Load']]

                            # find the sample index from the IMU timer, need to detect missed samples in the record
                            if sample == 0:
                                t0 = int(decode_scale[decode_dict['Timer']] * 5)/5

                            sample_t = int((decode_scale[decode_dict['Timer']] - t0) * 5 + 0.5)
                            if (sample_t - sample) != 0:
                                print('re-sync time sample', sample, sample_t, t0, decode_scale[decode_dict['Timer']], (decode_scale[decode_dict["Timer"]] - t0) * 5)

                            sample = sample_t

                            sample += 1
                        else:
                            print(file, 'bad checksum', f.tell())
                            f.seek(pos+1)
                    else:
                        print(file, 'short packet', sample)

                elif (byte[0] >= 0x20) and (byte[0] < 128):  # start of string
                    if xs is None:
                        xs = bytearray()
                        print('start of string', f.tell())
                    xs.append(byte[0])

                elif (xs is not None) and (byte[0] == 13):

                        s = xs.decode('ascii')
                        xs = None

                        print('string',len(s),' :', s)

                        data_time = None

                        # check for start of data
                        matchobj = start_data.match(s)
                        if matchobj:
                            if sample > 0:  # save the sample data if we had some before the START_DATA
                                print('** START, saving IMU sample data', start_time, sample)
                                imu_array.append({'time': start_time, 'accel': accel_samples, 'q': quat_samples, 'mag': mag_samples, 'gyro': gyro_samples, 'load': load_samples})
                                sample = 0

                            data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                            print('start_data time ', data_time)
                            start_time = data_time

                        # check for end data
                        matchobj = end_data.match(s)
                        if matchobj:
                            print('end data ', sample, 'samples read', samples_read)
                            if sample > 0:  # save the sample data
                                print('end of IMU sample data', start_time, sample)
                                imu_array.append({'time': start_time, 'accel': accel_samples, 'q': quat_samples, 'mag': mag_samples, 'gyro': gyro_samples, 'load': load_samples})
                                sample = 0


                        # check for done
                        matchobj = done_str.match(s)
                        if matchobj:
                            data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                            print('done time ', data_time)
                            start_time = data_time

                            # split the done string, extract name=value paris, for names in nameMap add to done_array
                            split = re.split(" ,| ", matchobj.group(2))
                            print(split)
                            done_dict = {'time': data_time}
                            for i in split:
                                nv = i.split('=')
                                name = nv[0]
                                if name in nameMap:
                                    try:
                                        value = np.float(nv[1])
                                    except (ValueError, IndexError):
                                        value = np.nan
                                    done_dict.update({nameMap[name]: value})

                            done_array.append(done_dict)

                        # check for gps fix
                        gps = None
                        matchobj = gps_fix.match(s)
                        if matchobj:
                            data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                            #print('gps fix time ', data_time)
                            gps_array.append({'time': data_time, 'lon': np.float(matchobj.group(4))*-1, 'lat': np.float(matchobj.group(3))})

                        matchobj = gps_fix2.match(s)
                        if matchobj:
                            data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                            #print('gps fix time ', data_time)
                            gps_array.append({'time': data_time, 'lon': np.float(matchobj.group(4))*-1, 'lat': np.float(matchobj.group(3))})

                        matchobj = gps_rmc.match(s)
                        if matchobj:
                            data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")

                            rmc_split = matchobj.group(2).split(',')
                            #print('rmc split', rmc_split)
                            if rmc_split[2] == 'A':
                                try:
                                    lat_dm = float(rmc_split[3])
                                    lat_ns = rmc_split[4]
                                    lon_dm = float(rmc_split[5])
                                    lon_ew = rmc_split[6]

                                    lat = gps_dm_degree(lat_dm, lat_ns)
                                    lon = gps_dm_degree(lon_dm, lon_ew)

                                    data_time_rmc = datetime.strptime(rmc_split[9] + ' ' + rmc_split[1] + '000', "%d%m%y %H%M%S.%f")
                                    #print('rmc time ', data_time, data_time_rmc)

                                    rmc_array.append({'time': data_time, 'gps_time': data_time_rmc, 'lat': lat, 'lon': lon})
                                except ValueError:
                                    pass

                        # check for serial number
                        matchobj = sn.match(s)
                        if matchobj:
                            instrument_serial_number = matchobj.group(2)

                        matchobj = sn_imu.match(s)
                        if matchobj:
                            instrument_imu_serial_number = matchobj.group(2)
                else:
                    print(file, 'junk ', byte[0], 'at', f.tell())

                    pass

                byte = f.read(1)

    # add the last record if we did not get a START_DATA
    if sample > 0:
        print('saveing last IMU sample data', start_time, sample)
        imu_array.append({'time': start_time, 'accel': accel_samples, 'q': quat_samples, 'mag': mag_samples, 'gyro': gyro_samples, 'load': load_samples})

    # print serial number information
    print('instrument serial number', instrument_serial_number)
    print('instrument_imu serial number', instrument_imu_serial_number)

    # done data times
    done_array.sort(key=lambda x: x.get('time'))
    done_times = []
    for d in done_array:
        #print(d['time'])
        done_times.append(round_time(d['time']))

    n_times = len(done_times)
    print('final times', n_times, done_times[0], done_times[-1])

    ts_start = min(done_times)
    ts_end = max(done_times)

    # imu data times
    imu_times = []
    imu_array.sort(key=lambda x: x.get('time'))
    for d in imu_array:
        #print('imu ts', d['time'])
        imu_times.append(round_time(d['time']))

    print('imu times index', np.where(np.in1d(done_times, imu_times)))

    # gps data times
    gps_times = []
    gps_array.sort(key=lambda x: x.get('time'))
    for d in gps_array:
        #print('gps ts', d['time'])
        gps_times.append(round_time(d['time']))

    #
    # build the netCDF file
    #

    # TODO: sort time

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    print("output file : %s" % outputName)

    dataset = Dataset(outputName, 'w', format='NETCDF4')

    dataset.instrument = "Campbell Scientific; CR1000"
    dataset.instrument_model = "CR1000"
    dataset.instrument_serial_number = instrument_serial_number

    dataset.instrument_imu = "LORD Sensing Microstrain ; 3DM-GX1"
    dataset.instrument_imu_model = "3DM-GX1"
    dataset.instrument_imu_serial_number = instrument_imu_serial_number

    time_dim = dataset.createDimension('TIME', len(done_times))
    # create the time array
    times = dataset.createVariable('TIME', np.float64, ('TIME',))

    times.units = 'days since 1950-01-01 00:00:00 UTC'
    times.calendar = 'gregorian'
    times.comment = 'start of 10 minute sampling window'

    if len(imu_array) > 0:
        sample_dim = dataset.createDimension('sample_time', 3072)
        v = dataset.createDimension('vector', 3)
        q = dataset.createDimension('quaternion', 4)

        sample_t_var = dataset.createVariable('sample_time', np.float32, ('sample_time', ))  # create the cooridnate variable
        sample_t_var[:] = np.arange(0, 3072) * 0.2
        sample_t_var.units = 'seconds'
        sample_t_var.long_name = 'time_of_sample within window'

        imu_sample_var = dataset.createVariable('TIME_SAMPLE_START', np.float, ('TIME',), fill_value=np.nan)
        imu_sample_var.units = 'seconds'
        imu_sample_var.comment = 'time offset to start of samples'

        accel_var = dataset.createVariable('acceleration', np.float32, ('sample_time', 'vector', 'TIME'), fill_value=np.nan, zlib=True)
        mag_var = dataset.createVariable('magnetic', np.float32, ('sample_time', 'vector', 'TIME'), fill_value=np.nan, zlib=True)
        gyro_var = dataset.createVariable('rotational_velocity', np.float32, ('sample_time', 'vector', 'TIME'), fill_value=np.nan, zlib=True)

        quat_var = dataset.createVariable('orientation', np.float32, ('sample_time', 'quaternion', 'TIME'), fill_value=np.nan, zlib=True)
        quat_var.comment = 'quaternion order is W, X, Y, Z'

        load_var = dataset.createVariable('load', np.float32, ('sample_time', 'TIME'), fill_value=np.nan, zlib=True)

    if len(gps_array) > 0:
        xpos_var = dataset.createVariable('XPOS', np.float, ('TIME',), fill_value=np.nan)
        ypos_var = dataset.createVariable('YPOS', np.float, ('TIME',), fill_value=np.nan)
        gps_sample_var = dataset.createVariable('TIME_GPS_FIX', np.float, ('TIME',), fill_value=np.nan)
        gps_sample_var.units = 'seconds'
        gps_sample_var.comment = 'time offset to gps fix'

    if len(rmc_array) > 0:
        rmc_diff_var = dataset.createVariable('TIME_DIFF', np.float, ('TIME',), fill_value=np.nan)
        rmc_diff_var.units = 'seconds'
        rmc_diff_var.comment = 'difference between TIME and utc time from gps'

    print("write time data samples", len(done_array))
    times[:] = date2num(done_times, units=times.units, calendar=times.calendar)

    # for each value in done array, add a netCDF variable for that name
    for n in done_array[0]:
        print('done string variable', n)
        if n != 'time':
            done_var = dataset.createVariable(n, np.float32, ('TIME',), fill_value=np.nan)

            # done_var[:] = [v[n] for v in done_array]
            for i in range(len(done_array)):
                try:
                    done_var[i] = done_array[i][n]
                except KeyError:
                    pass

    if len(imu_array) > 0:
        print("write imu, samples", len(imu_array))

        xy, done_ind, imu_ind = np.intersect1d(done_times, imu_times, return_indices=True)
        print('done times idx', len(done_ind), done_ind)
        print('imu times idx', len(imu_ind), imu_ind)
        print('xy times idx', len(xy), xy)

        d_array = np.full([len(done_times)], np.nan)
        for i in range(len(xy)):
            done_idx = done_ind[i]
            imu_idx = imu_ind[i]

            print('sample_t', i, imu_array[imu_idx]['time'])

            d_array[done_idx] = (imu_array[imu_idx]['time'] - done_times[done_idx]).total_seconds()

        imu_sample_var[:] = d_array

        d_array = np.empty([3072, 3, len(done_times)])
        create_nc_var(d_array, imu_times, 'accel', imu_array, accel_var, done_ind, imu_ind)
        create_nc_var(d_array, imu_times, 'mag', imu_array, mag_var, done_ind, imu_ind)
        create_nc_var(d_array, imu_times, 'gyro', imu_array, gyro_var, done_ind, imu_ind)

        d_array = np.empty([3072, 4, len(done_times)])
        create_nc_var(d_array, imu_times, 'q', imu_array, quat_var, done_ind, imu_ind)

        d_array = np.empty([3072, len(done_times)])
        create_nc_var(d_array, imu_times, 'load', imu_array, load_var, done_ind, imu_ind)

    if len(gps_array) > 0:
        print("write gps data samples", len(gps_array))

        # indices = np.in1d(gps_times, done_times)
        # print('gps indicies', indices)
        # xy, done_ind, gps_ind = np.intersect1d(done_times, gps_times, return_indices=True)
        # print('done times idx', len(done_ind), done_ind)
        # print('imu times idx', len(gps_ind), gps_ind)
        # print('xy times idx', len(xy), xy)

        d_array = np.full([len(done_times)], np.nan)
        y_array = np.full([len(done_times)], np.nan)
        x_array = np.full([len(done_times)], np.nan)

        # for i in range(len(gps_times)):
        #     done_idx = np.where(np.asarray(done_times) == gps_times[i])
        #
        #     if len(done_idx) > 0:
        #         if len(done_idx[0]) > 0:
        #             done_i = done_idx[0][0]
        #             print('gps time', gps_array[i]['time'], done_i)
        #             x_array[done_i] = gps_array[i]['lon']
        #             y_array[done_i] = gps_array[i]['lat']
        #             d_array[done_i] = (gps_array[i]['time'] - done_times[done_i]).total_seconds()

        j = 0
        for i in range(len(done_times)):
            while j < len(gps_times):
                if gps_times[j] >= done_times[i]:
                    break
                j += 1
            while j < len(gps_times):
                if gps_times[j] == done_times[i]:
                    #print('gps time', gps_times[j], i)
                    x_array[i] = gps_array[j]['lon']
                    y_array[i] = gps_array[j]['lat']
                    d_array[i] = (gps_array[j]['time'] - done_times[i]).total_seconds()
                elif gps_times[j] > done_times[i]:
                    break
                j += 1

        gps_sample_var[:] = d_array
        xpos_var[:] = x_array
        ypos_var[:] = y_array

    if len(rmc_array) > 0:
        print("write rmc time diff samples", len(rmc_array))

        rmc_times = []
        rmc_array.sort(key=lambda x: x.get('time'))
        for d in rmc_array:
            #print('rmc ts', d['time'])
            rmc_times.append(round_time(d['time']))

        d_array = np.full([len(done_times)], np.nan)

        # for i in range(len(rmc_array)):
        #     done_idx = np.where(np.asarray(done_times) == rmc_times[i])
        #
        #     if len(done_idx) > 0:
        #         if len(done_idx[0]) > 0:
        #             done_i = done_idx[0][0]
        #             #print('rmc time', rmc_array[i]['time'])
        #             d_array[done_i] = (rmc_array[i]['time'] - rmc_array[i]['gps_time']).total_seconds()

        j = 0
        for i in range(len(done_times)):
            while j < len(rmc_times):
                if rmc_times[j] >= done_times[i]:
                    break
                j += 1
            while j < len(rmc_times):
                if rmc_times[j] == done_times[i]:
                    #print('gps time', gps_times[j], i)
                    d_array[i] = (rmc_array[j]['time'] - rmc_array[j]['gps_time']).total_seconds()
                elif rmc_times[j] > done_times[i]:
                    break
                j += 1

        rmc_diff_var[:] = d_array

    # create the time coverage attributes
    dataset.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    dataset.setncattr("time_coverage_end", ts_end.strftime(ncTimeFormat))

    # add creating and history entry
    dataset.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    dataset.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(files[0]) + "...")

    dataset.close()

    return outputName


if __name__ == "__main__":

    outfile = sys.argv[1]
    outfile = 'MRU.nc'

    files = []
    for f in sys.argv[2:]:
        files.extend(glob(f))

    files.sort()
    for f in files:
        print(f)

    datalogger(outfile, files)
