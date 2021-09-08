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
    done_str = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: \d done time \d+ ,BV=(\S+) ,PT=(\S+) ,OBP=(\S+) ,OT=(\S+) ,CHL=(\S+) ,NTU=(\S+) PAR=(\S+) ,meanAccel=(\S+) ,meanLoad=(\S+)')
    done_pulse_str = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: \d done time \d+ ,BV=(\S+) ,PT=(\S+) ,meanAccel=(\S+) ,meanLoad=(\S+)')

    gps_fix = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO : GPS Fix (\d+) Latitude (\S+) Longitude (\S+) sats (\S+) HDOP (\S+)')

    # TODO: also check RMC strings for date time offset

    sn = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: Station Name (\S+)')
    sn_imu = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: MRU SerialNumber (\d+)')

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

    xpos_var = dataset.createVariable('XPOS', np.float32, ('TIME',), fill_value=np.nan)
    ypos_var = dataset.createVariable('YPOS', np.float32, ('TIME',), fill_value=np.nan)

    bat_var = dataset.createVariable('vbat', np.float32, ('TIME',), fill_value=np.nan)

    mean_load_var = dataset.createVariable('mean_load', np.float32, ('TIME',), fill_value=np.nan)

    accel_var = dataset.createVariable('accel', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
    mag_var = dataset.createVariable('mag_field', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
    gyro_var = dataset.createVariable('gyro_rate', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
    quat_var = dataset.createVariable('quaternion', np.float32, ('TIME', 'SAMPLE', 'QUAT'), fill_value=np.nan)

    obp_var = None

    load_var = dataset.createVariable('load', np.float32, ('TIME', 'SAMPLE'), fill_value=np.nan)

    # TODO: process the output of this parser into frequency spectra, wave height

    # TODO: process all historical data, Pulse, SOFS-1 .... to SOFS-10

    ts_start = None
    ts_end = None
    t_idx = None
    t0 = 0
    t_last = 0
    samples_red = 0
    sample = 0
    last_sample = -1
    times_dict = {}
    n_times = -1

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
                            # print('decode ', decode)

                            # save data to netCDF
                            accel_var[t_idx, sample, 0] = decode['AccelX']
                            accel_var[t_idx, sample, 1] = decode['AccelY']
                            accel_var[t_idx, sample, 2] = decode['AccelZ']

                            quat_var[t_idx, sample, 0] = decode['StabQ0']
                            quat_var[t_idx, sample, 1] = decode['StabQ1']
                            quat_var[t_idx, sample, 2] = decode['StabQ2']
                            quat_var[t_idx, sample, 3] = decode['StabQ3']

                            mag_var[t_idx, sample, 0] = decode['MagFieldX']
                            mag_var[t_idx, sample, 1] = decode['MagFieldY']
                            mag_var[t_idx, sample, 2] = decode['MagFieldZ']

                            gyro_var[t_idx, sample, 0] = decode['CompAngleRateX']
                            gyro_var[t_idx, sample, 1] = decode['CompAngleRateY']
                            gyro_var[t_idx, sample, 2] = decode['CompAngleRateZ']

                            load_var[t_idx, sample] = decode['Load']

                            # find the sample index from the IMU timer, need to detech missed samples in the record
                            if sample == 0:
                                t0 = int(decode['Timer'] * 5)/5

                            sample_t = int((decode['Timer'] - t0) * 5 + 0.5)
                            if (sample_t - sample) != 0:
                                print('time sample ', sample, sample_t, t0, decode['Timer'], (decode["Timer"] - t0) * 5)

                            sample = sample_t
                            if sample == last_sample:
                                sample += 1
                            last_sample = sample

                            samples_red += 1
                        else:
                            print('bad checksum ', f.tell())
                    else:
                        print('short packet', samples_red, sample)

                elif byte[0] > 0x20: # start of string
                    xs = bytearray()
                    while byte[0] != 13:
                        if byte[0] < 128:
                            xs.append(byte[0])
                        byte = f.read(1)
                    s = xs.decode('ascii')
                    #print('string : ', s)

                    data_time = None
                    # check for start of data
                    matchobj = start_data.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        print('data time ', data_time)

                        sample = 0
                        samples_red = 0

                    # check for end data
                    matchobj = end_data.match(s)
                    if matchobj:
                        print('end data ', sample, samples_red)

                    # check for done
                    done = None
                    matchobj = done_str.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        print('done time ', data_time)
                        done = matchobj
                        print('done', s)

                    # check for done_pulse
                    done_pulse = None
                    matchobj = done_pulse_str.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        print('done time ', data_time)
                        done_pulse = matchobj
                        print('done_pulse', s)

                    # check for gps fix
                    gps = None
                    matchobj = gps_fix.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        print('gps time ', data_time)

                        gps = matchobj
                        print('gps', s)

                    # update time bounds if we got a time
                    if data_time:
                        data_time = round_time(data_time)

                        # check is this time is in the existing times
                        if data_time in times_dict:
                            t_idx = times_dict.get(data_time)
                        else:
                            n_times += 1
                            t_idx = n_times
                            times_dict[data_time] = t_idx

                        # print('got time', data_time, t_idx, n_times)
                        times[t_idx] = date2num(data_time, units=times.units, calendar=times.calendar)
                        if gps:
                            xpos_var[t_idx] = -np.float(gps.group(4))
                            ypos_var[t_idx] = np.float(gps.group(3))
                        if done:
                            bat_var[t_idx] = np.float(done.group(2))
                            mean_load_var[t_idx] = np.float(done.group(10))
                            if not obp_var:
                                obp_var = dataset.createVariable('optode_bphase', np.float32, ('TIME',), fill_value=np.nan)
                                otemp_var = dataset.createVariable('optode_temp', np.float32, ('TIME',), fill_value=np.nan)
                                chl_var = dataset.createVariable('CHL', np.float32, ('TIME',), fill_value=np.nan)
                                ntu_var = dataset.createVariable('NTU', np.float32, ('TIME',), fill_value=np.nan)
                                par_var = dataset.createVariable('PAR', np.float32, ('TIME',), fill_value=np.nan)

                            obp_var[t_idx] = np.float(done.group(4))
                            otemp_var[t_idx] = np.float(done.group(5))
                            chl_var[t_idx] = np.float(done.group(6))
                            ntu_var[t_idx] = np.float(done.group(7))
                            par_var[t_idx] = np.float(done.group(8))
                        if done_pulse:
                            bat_var[t_idx] = np.float(done_pulse.group(2))
                            mean_load_var[t_idx] = np.float(done_pulse.group(5))

                        # keep time stats, start (first) and end (last), maybe should use min/max
                        ts_end = data_time
                        if ts_start is None:
                            ts_start = data_time

                    # check for serial number
                    matchobj = sn.match(s)
                    if matchobj:
                        dataset.instrument_serial_number = matchobj.group(2)

                    matchobj = sn_imu.match(s)
                    if matchobj:
                        dataset.instrument_imu_serial_number = matchobj.group(2)
                else:
                    #print('junk ', byte[0])
                    pass

                byte = f.read(1)

    dataset.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    dataset.setncattr("time_coverage_end", ts_end.strftime(ncTimeFormat))

    # add creating and history entry
    dataset.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    dataset.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(files[0]))

    dataset.close()

    return outputName


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    datalogger(files)
