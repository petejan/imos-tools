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


def datalogger(files):

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

    outputName = files[0] + ".nc"

    print("output file : %s" % outputName)
    dataset = Dataset(outputName, 'w', format='NETCDF4')

    dataset.instrument = "Campbell Scientific; CR1000"
    dataset.instrument_model = "CR1000"
    dataset.instrument_serial_number = "unknown"

    dataset.instrument_imu = "LORD Sensing Microstrain ; 3DM-GX1"
    dataset.instrument_imu_model = "3DM-GX1"
    dataset.instrument_imu_serial_number = "unknown"

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
    data_dict = {}
    data_dict['data_time'] = {}
    data_dict['time_days'] = []

    for file in files:

        with open(file, "rb") as f:
            byte = f.read(1)
            while byte != b"":
                if byte[0] == 0x0c: # start of IMU stabQ packet
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
                            decode = struct.unpack('>4h3h3h3hHHf', packet)

                            # scale each value in the packet
                            decode_scale = []
                            for x in range(0,len(decode)):
                                d = decode[x] * scale[x]
                                decode_scale.append(d)
                            # create a dict from the decoded data (could just use indexes, but this is clearer
                            decode = decoder._asdict(decoder._make(decode_scale))
                            #print('decode ', decode)

                            # TODO: check timer if we have missed a sample
                            if sample == 0:
                                for x in decode.keys():
                                    data_dict[x] = np.zeros(3072)
                                data_dict['array'] = 3072
                                t0 = decode['Timer']

                            sample = int((decode['Timer'] - t0) * 5)
                            # print('time sample ', sample)
                            for x in decode.keys():
                                data_dict[x][sample] = decode[x]
                            t_last = decode['Timer']
                            sample += 1 # kick along a bit so that next time its not zero
                            samples_red += 1
                        else:
                            print('bad checksum ', f.tell())
                    else:
                        print('short packet', samples_red, sample)

                elif byte[0] > 0x20: # start of string
                    data_dict = {}

                    xs = bytearray()
                    while byte[0] != 13:
                        xs.append(byte[0])
                        byte = f.read(1)
                    s = xs.decode('ascii')
                    #print('string : ', s)

                    data_time = None
                    # check for start of data
                    matchobj = start_data.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        data_dict['data_time'] = data_time
                        data_dict['type'] = 'imu'

                        print('data time ', data_time)

                        sample = 0
                        samples_red = 0

                    # check for end data
                    matchobj = end_data.match(s)
                    if matchobj:
                        print('end data ', sample, samples_red)

                    # check for done
                    matchobj = done_str.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        data_dict['data_time'] = data_time
                        data_dict['type'] = 'done'

                        print('done time ', data_time)
                        done = matchobj
                        nvp = re.split(",|\ ", matchobj.group(2))
                        print(nvp)
                        for nv in nvp:
                            if len(nv):
                                n_v = nv.split("=")
                                data_dict[n_v[0]] = np.float(n_v[1])

                    # check for gps fix
                    matchobj = gps_fix.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        data_dict['data_time'] = data_time
                        data_dict['type'] = 'gps'

                        data_dict['lon'] = -np.float(matchobj.group(4))
                        data_dict['lat'] = np.float(matchobj.group(3))

                        print('gps time ', data_time)

                        gps = matchobj
                        print('gps', s)

                    # update time bounds if we got a time
                    if data_time:
                        data_time_hour = round_time(data_time)
                        data_dict['data_time_hours'] = data_time_hour
                        data_dict['data_nc_time'] = date2num(data_time, units=times.units, calendar=times.calendar)

                        # keep time stats, start (first) and end (last), maybe should use min/max
                        ts_end = data_time_hour
                        if ts_start is None:
                            ts_start = data_time_hour

                    # check for serial number
                    matchobj = sn.match(s)
                    if matchobj:
                        instrument_imu_serial_number = matchobj.group(2)

                    print('data', s, data_dict)

                else:
                    #print('junk ', byte[0])
                    pass

                byte = f.read(1)

    print(data_dict)
    times[:] = data_dict["data_nc_time"]

    xpos_var = dataset.createVariable('XPOS', np.float32, ('TIME',), fill_value=np.nan)
    ypos_var = dataset.createVariable('YPOS', np.float32, ('TIME',), fill_value=np.nan)

    bat_var = dataset.createVariable('vbat', np.float32, ('TIME',), fill_value=np.nan)
    obp_var = dataset.createVariable('optode_bphase', np.float32, ('TIME',), fill_value=np.nan)
    otemp_var = dataset.createVariable('optode_temp', np.float32, ('TIME',), fill_value=np.nan)
    chl_var = dataset.createVariable('CHL', np.float32, ('TIME',), fill_value=np.nan)
    ntu_var = dataset.createVariable('NTU', np.float32, ('TIME',), fill_value=np.nan)
    par_var = dataset.createVariable('PAR', np.float32, ('TIME',), fill_value=np.nan)
    mean_load_var = dataset.createVariable('mean_load', np.float32, ('TIME',), fill_value=np.nan)

    accel_var = dataset.createVariable('accel', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
    mag_var = dataset.createVariable('mag_field', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
    gyro_var = dataset.createVariable('gyro_rate', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
    quat_var = dataset.createVariable('quaternion', np.float32, ('TIME', 'SAMPLE', 'QUAT'), fill_value=np.nan)

    load_var = dataset.createVariable('load', np.float32, ('TIME', 'SAMPLE'), fill_value=np.nan)

    # save data to netCDF
    # accel_var[t_idx, sample, 0] = decode['AccelX']
    # accel_var[t_idx, sample, 1] = decode['AccelY']
    # accel_var[t_idx, sample, 2] = decode['AccelZ']
    #
    # quat_var[t_idx, sample, 0] = decode['StabQ0']
    # quat_var[t_idx, sample, 1] = decode['StabQ1']
    # quat_var[t_idx, sample, 2] = decode['StabQ2']
    # quat_var[t_idx, sample, 3] = decode['StabQ3']
    #
    # mag_var[t_idx, sample, 0] = decode['MagFieldX']
    # mag_var[t_idx, sample, 1] = decode['MagFieldY']
    # mag_var[t_idx, sample, 2] = decode['MagFieldZ']
    #
    # gyro_var[t_idx, sample, 0] = decode['CompAngleRateX']
    # gyro_var[t_idx, sample, 1] = decode['CompAngleRateY']
    # gyro_var[t_idx, sample, 2] = decode['CompAngleRateZ']
    #
    # load_var[t_idx, sample] = decode['Load']

    dataset.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    dataset.setncattr("time_coverage_end", ts_end.strftime(ncTimeFormat))

    # add creating and history entry
    dataset.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    dataset.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(files[0]))

    dataset.close()

    return outputName


if __name__ == "__main__":
    datalogger(sys.argv[1:])
