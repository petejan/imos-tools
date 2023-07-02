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
import glob
import sys

from datetime import datetime, timedelta

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

import numpy as np
import csv
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


def get_dict_ts(ts, type):
    done_dict = {'data_time': ts, 'type': type}
    data_time_hour = round_time(ts)
    done_dict['data_time_hours'] = data_time_hour
    done_dict['data_nc_time_hour'] = date2num(data_time_hour, units='days since 1950-01-01 00:00:00', calendar='gregorian')

    return done_dict

def datalogger(files):

    files_to_process = []
    for f in files:
        if "*" in f:
            files_to_process.extend(glob.glob(f))
        else:
            files_to_process.append(f)

    #print(files_to_process)

    decoder = namedtuple('decode', 'StabQ0 StabQ1 StabQ2 StabQ3 '
                                   'MagFieldX MagFieldY MagFieldZ AccelX AccelY AccelZ '
                                   'CompAngleRateX CompAngleRateY CompAngleRateZ '
                                   'Timer CheckSum Load')

    scale = [1/ 8192.0, 1/ 8192.0, 1/ 8192.0, 1/ 8192.0,
             1/ (32768000.0 / 2000), 1/ (32768000.0 / 2000), 1/ (32768000.0 / 2000),
             9.81/ (32768000.0 / 8500), 9.81/ (32768000.0 / 8500), 9.81/ (32768000.0 / 8500),
             1 / (32768000.0 / 10000), 1 / (32768000.0 / 10000), 1 / (32768000.0 / 10000),
             0.2, 1, 1]

    start_data = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) \*\*\*\*\*\* START RAW MRU DATA \*\*\*\*\*\*')
    end_data = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) \*\*\*\*\*\* END DATA \*\*\*\*\*\*')
    done_str = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: \d done time \d+ ,(.*)')

    gps_fix = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO : GPS Fix (\d+) Latitude (\S+) Longitude (\S+) sats (\S+) HDOP (\S+)')

    # TODO: also check RMC strings for date time offset

    sn = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: MRU SerialNumber (\d+)')

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = files_to_process[0] + ".nc"

    print("output file : %s" % outputName)
    dataset = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    dataset.instrument = "Campbell Scientific; CR1000"
    dataset.instrument_model = "CR1000"
    dataset.instrument_serial_number = "unknown"

    dataset.instrument_imu = "LORD Sensing Microstrain ; 3DM-GX1"
    dataset.instrument_imu_model = "3DM-GX1"
    dataset.instrument_imu_serial_number = "unknown"

    dataset.instrument_load = "Sensing Systems ; 10826-3*"
    dataset.instrument_load_model = "10826-3*"
    dataset.instrument_load_serial_number = "unknown"

    time = dataset.createDimension('TIME', None)
    sample_d = dataset.createDimension('SAMPLE', 3072)

    f = dataset.createDimension('FREQ', 256)
    v = dataset.createDimension('VECTOR', 3)
    q = dataset.createDimension('QUAT', 4)

    times = dataset.createVariable('TIME', np.float64, ('TIME',))

    times.units = 'days since 1950-01-01 00:00:00'
    times.calendar = 'gregorian'

    # TODO: process the output of this parser into frequency spectra, wave height

    # TODO: process all historical data, Pulse, SOFS-1 .... to SOFS-10

    ts_start = None
    ts_end = None
    t_idx = None
    t0 = 0
    t_last = 0
    samples_red = 0
    sample = 0
    n_times = -1

    done_list = []
    gps_list = []
    imu_list = []
    done_dict = None
    imu_dict = {}
    gps_dict = None

    last_imu = False

    for file in files_to_process:

        print('processing ', file)

        with open(file, "rb") as f:
            byte = f.read(1)
            while byte != b"":
                if byte[0] == 0x0c: # start of IMU stabQ packet
                    packet = f.read(30+4)
                    if len(packet) == 34:
                        last_imu = False
                        # check the checksum
                        decode_s = struct.unpack('>15H', packet[0:30])
                        cksum = 12
                        for i in range(0, 14):
                            cksum = cksum + decode_s[i]
                        cksum = cksum & 0xffff
                        if cksum == decode_s[14]:
                            # check sum ok, decode the packet
                            decode = struct.unpack('>4h3h3h3hHHf', packet)

                            # scale each value in the packet
                            decode_scale = []
                            for x in range(0, len(decode)):
                                d = decode[x] * scale[x]
                                decode_scale.append(d)
                            # create a dict from the decoded data (could just use indexes, but this is clearer
                            decode = decoder._asdict(decoder._make(decode_scale))
                            #print('decode ', decode)

                            if sample == 0:
                                for x in decode.keys():
                                    print(x)
                                    imu_dict[x] = np.full(3072, np.nan)
                                imu_dict['array'] = 3072
                                t0 = decode['Timer']

                            sample = int((decode['Timer'] - t0) * 5)
                            # print('time sample ', sample)
                            try:
                                for x in decode.keys():
                                    imu_dict[x][sample] = decode[x]
                            except KeyError:
                                print('key error', x)
                                pass
                            t_last = decode['Timer']
                            sample += 1 # kick along a bit so that next time its not zero
                            samples_red += 1
                        else:
                            print('bad checksum ', f.tell())
                    else:
                        print('short packet', samples_red, sample)

                elif byte[0] > 0x20: # start of string
                    # convert bytes to a string
                    xs = bytearray()
                    while byte[0] != 13:
                        if byte[0] < 128:
                            xs.append(byte[0])
                        byte = f.read(1)
                        if not byte:
                            break
                    s = xs.decode('ascii')
                    #print('string : ', s)

                    data_time = None
                    # check for start of data
                    matchobj = start_data.match(s)
                    if matchobj:
                        imu_dict = get_dict_ts(datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S"), 'imu')
                        last_imu = False

                    # check for end data
                    matchobj = end_data.match(s)
                    if matchobj:
                        print('end data ', sample, samples_red)
                        last_imu = True

                    # check for done
                    matchobj = done_str.match(s)
                    if matchobj:
                        done_dict = get_dict_ts(datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S"), 'done')

                        nvp = re.split(",|\ ", matchobj.group(2))
                        for nv in nvp:
                            if len(nv):
                                n_v = nv.split("=")
                                done_dict[n_v[0]] = np.float(n_v[1])

                    # check for gps fix
                    matchobj = gps_fix.match(s)
                    if matchobj:
                        gps_dict = get_dict_ts(datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S"), 'gps')

                        # the actual data
                        gps_dict['lon'] = -np.float(matchobj.group(4))
                        gps_dict['lat'] = np.float(matchobj.group(3))
                        gps_dict['fix'] = np.float(matchobj.group(2))

                    # check for serial number
                    matchobj = sn.match(s)
                    if matchobj:
                        dataset.instrument_imu_serial_number = matchobj.group(2)

                else:
                    #print('junk ', byte[0])
                    pass

                byte = f.read(1)
                if gps_dict:
                    #print('gps dict', gps_dict)
                    gps_list.append(gps_dict)
                    gps_dict = None
                if done_dict:
                    print('done dict', done_dict)
                    done_list.append(done_dict)
                    done_dict = None

                if (len(byte) == 0) & (imu_dict is not None):
                    last_imu = True
                if last_imu and imu_dict:
                    print('imu_dict dict', imu_dict)
                    imu_list.append(imu_dict)
                    imu_dict = {}
                    sample = 0
                    samples_red = 0

    # done data list
    time_list = [data['data_nc_time_hour'] for data in done_list]
    time_list.extend([data['data_nc_time_hour'] for data in gps_list])
    time_list.extend([data['data_nc_time_hour'] for data in imu_list])

    tn = np.array(np.unique(time_list))
    print('times ', tn)

    times[:] = tn
    ts_start = num2date(np.min(tn), units='days since 1950-01-01 00:00:00', calendar='gregorian')
    ts_end = num2date(np.max(tn), units='days since 1950-01-01 00:00:00', calendar='gregorian')

    # find index IMU data timestamp in timestamps
    idx = np.zeros(len(done_list), dtype=int)
    x = 0
    for i in done_list:
        idx[x] = np.where(tn == i['data_nc_time_hour'])[0]
        x += 1

    print('done idx :', idx)

    if len(done_list) > 0:
        for k in done_list[0].keys():
            print('key: ', k)

        # TODO: adapt to keys found
        bat_var = dataset.createVariable('vbat', np.float32, ('TIME',), fill_value=np.nan)
        if 'OBP' in done_list:
            obp_var = dataset.createVariable('optode_bphase', np.float32, ('TIME',), fill_value=np.nan)
            otemp_var = dataset.createVariable('optode_temp', np.float32, ('TIME',), fill_value=np.nan)
        if 'CHL' in done_list:
            chl_var = dataset.createVariable('CHL', np.float32, ('TIME',), fill_value=np.nan)
            ntu_var = dataset.createVariable('NTU', np.float32, ('TIME',), fill_value=np.nan)
        if 'PAR' in done_list:
            par_var = dataset.createVariable('PAR', np.float32, ('TIME',), fill_value=np.nan)
        mean_load_var = dataset.createVariable('mean_load', np.float32, ('TIME',), fill_value=np.nan)

        bat_var[idx] = np.array([data['BV'] for data in done_list])
        if 'OBP' in done_list:
            obp_var[idx] = np.array([data['OBP'] for data in done_list])
            otemp_var[idx] = np.array([data['OT'] for data in done_list])
        if 'CHL' in done_list:
            chl_var[idx] = np.array([data['CHL'] for data in done_list])
            ntu_var[idx] = np.array([data['NTU'] for data in done_list])
        if 'PAR' in done_list:
            par_var[idx] = np.array([data['PAR'] for data in done_list])

        mean_load_var[idx] = np.array([data['meanLoad'] for data in done_list])

    # find index IMU data timestamp in timestamps
    idx = np.zeros(len(gps_list), dtype=int)
    x = 0
    for i in gps_list:
        idx[x] = np.where(tn == i['data_nc_time_hour'])[0]
        x += 1

    print('gps idx :', idx)

    if len(gps_list) > 0:
        for k in gps_list[0].keys():
            print('key: ', k)

        xpos_var = dataset.createVariable('XPOS', np.float32, ('TIME',), fill_value=np.nan)
        ypos_var = dataset.createVariable('YPOS', np.float32, ('TIME',), fill_value=np.nan)

        xpos_var[idx] = np.array([data['lon'] for data in gps_list])
        ypos_var[idx] = np.array([data['lat'] for data in gps_list])

    # IMU data

    if len(imu_list) > 0:
        accel_var = dataset.createVariable('accel', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
        mag_var = dataset.createVariable('mag_field', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
        gyro_var = dataset.createVariable('gyro_rate', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
        quat_var = dataset.createVariable('quaternion', np.float32, ('TIME', 'SAMPLE', 'QUAT'), fill_value=np.nan)

        load_var = dataset.createVariable('load', np.float32, ('TIME', 'SAMPLE'), fill_value=np.nan)

        # find index IMU data timestamp in timestamps
        idx = np.zeros(len(imu_list), dtype=int)
        x = 0
        for i in imu_list:
            idx[x] = np.where(tn == i['data_nc_time_hour'])[0]
            x += 1

        print('imu idx :', idx)

        for k in imu_list[0].keys():
            print('key: ', k)

        # TODO: adapt to keys found
        accel_var[idx, :, 0] = np.array([data['AccelY'] for data in imu_list])
        accel_var[idx, :, 1] = np.array([data['AccelY'] for data in imu_list])
        accel_var[idx, :, 2] = np.array([data['AccelZ'] for data in imu_list])

        quat_var[idx, :, 0] = np.array([data['StabQ0'] for data in imu_list])
        quat_var[idx, :, 1] = np.array([data['StabQ1'] for data in imu_list])
        quat_var[idx, :, 2] = np.array([data['StabQ2'] for data in imu_list])
        quat_var[idx, :, 3] = np.array([data['StabQ3'] for data in imu_list])

        mag_var[idx, :, 0] = np.array([data['MagFieldX'] for data in imu_list])
        mag_var[idx, :, 1] = np.array([data['MagFieldY'] for data in imu_list])
        mag_var[idx, :, 2] = np.array([data['MagFieldZ'] for data in imu_list])

        gyro_var[idx, :, 0] = np.array([data['CompAngleRateX'] for data in imu_list])
        gyro_var[idx, :, 1] = np.array([data['CompAngleRateY'] for data in imu_list])
        gyro_var[idx, :, 2] = np.array([data['CompAngleRateZ'] for data in imu_list])

        load_var[idx, :] = np.array([data['Load'] for data in imu_list])

    dataset.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    dataset.setncattr("time_coverage_end", ts_end.strftime(ncTimeFormat))

    # add creating and history entry
    dataset.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    dataset.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(files[0]))

    dataset.close()

    return outputName


if __name__ == "__main__":
    datalogger(sys.argv[1:])
