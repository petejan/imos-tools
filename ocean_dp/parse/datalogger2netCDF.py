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


def datalogger(files):

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
    done_str = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: \d done time \d+ ,BV=(\S+) ,PT=(\S+) ,OBP=(\S+) ,OT=(\S+) ,CHL=(\S+) ,NTU=(\S+) PAR=(\S+) ,meanAccel=(\S+) ,meanLoad=(\S+)')
    done_pulse_str = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: \d done time \d+ ,BV=(\S+) ,PT=(\S+) ,meanAccel=(\S+) ,meanLoad=(\S+)')

    gps_fix = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO : GPS Fix (\d+) Latitude (\S+) Longitude (\S+) sats (\S+) HDOP (\S+)')
    gps_fix2 = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO : GPS RMC Fix (\d+) Latitude (\S+) Longitude (\S+)')

    gps_rmc = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: GPS String \'GPRMC\' string (\S+)')

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

    time_dim = dataset.createDimension('TIME', None)
    sample_dim = dataset.createDimension('SAMPLE', 3072)

    f = dataset.createDimension('FREQ', 256)
    v = dataset.createDimension('VECTOR', 3)
    q = dataset.createDimension('QUAT', 4)

    times = dataset.createVariable('TIME', np.float64, ('TIME',))

    times.units = 'days since 1950-01-01 00:00:00'
    times.calendar = 'gregorian'

    xpos_var = dataset.createVariable('XPOS', np.float, ('TIME',), fill_value=np.nan)
    ypos_var = dataset.createVariable('YPOS', np.float, ('TIME',), fill_value=np.nan)

    bat_var = dataset.createVariable('vbat', np.float32, ('TIME',), fill_value=np.nan)

    mean_load_var = dataset.createVariable('mean_load', np.float32, ('TIME',), fill_value=np.nan)

    accel_var = dataset.createVariable('accel', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
    mag_var = dataset.createVariable('mag_field', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
    gyro_var = dataset.createVariable('gyro_rate', np.float32, ('TIME', 'SAMPLE', 'VECTOR'), fill_value=np.nan)
    quat_var = dataset.createVariable('quaternion', np.float32, ('TIME', 'SAMPLE', 'QUAT'), fill_value=np.nan)
    quat_var.comment = 'quaternion order is W, X, Y, Z'

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
    start_time = None
    end_time = None

    for file in files:
        lat = np.nan
        lon = np.nan

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
                            #decode = decoder._asdict(decoder._make(decode_scale))
                            # print('decode ', decode)

                            # save data to netCDF
                            if sample == 0:
                                accel_samples = np.zeros([3072, 3])
                                mag_samples = np.zeros([3072, 3])
                                gyro_samples = np.zeros([3072, 3])
                                quat_samples = np.zeros([3072, 4])

                                load_samples = np.zeros([3072])

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

                            # accel_var[t_idx, sample, 0] = decode_scale[decode_dict['AccelX']]
                            # accel_var[t_idx, sample, 1] = decode_scale[decode_dict['AccelY']]
                            # accel_var[t_idx, sample, 2] = decode_scale[decode_dict['AccelZ']]
                            #
                            # quat_var[t_idx, sample, 0] = decode_scale[decode_dict['StabQ0']]
                            # quat_var[t_idx, sample, 1] = decode_scale[decode_dict['StabQ1']]
                            # quat_var[t_idx, sample, 2] = decode_scale[decode_dict['StabQ2']]
                            # quat_var[t_idx, sample, 3] = decode_scale[decode_dict['StabQ3']]
                            #
                            # mag_var[t_idx, sample, 0] = decode_scale[decode_dict['MagFieldX']]
                            # mag_var[t_idx, sample, 1] = decode_scale[decode_dict['MagFieldY']]
                            # mag_var[t_idx, sample, 2] = decode_scale[decode_dict['MagFieldZ']]
                            #
                            # gyro_var[t_idx, sample, 0] = decode_scale[decode_dict['CompAngleRateX']]
                            # gyro_var[t_idx, sample, 1] = decode_scale[decode_dict['CompAngleRateY']]
                            # gyro_var[t_idx, sample, 2] = decode_scale[decode_dict['CompAngleRateZ']]
                            #
                            # load_var[t_idx, sample] = decode_scale[decode_dict['Load']]

                            # find the sample index from the IMU timer, need to detect missed samples in the record
                            if sample == 0:
                                t0 = int(decode_scale[decode_dict['Timer']] * 5)/5

                            sample_t = int((decode_scale[decode_dict['Timer']] - t0) * 5 + 0.5)
                            if (sample_t - sample) != 0:
                                print('time sample ', sample, sample_t, t0, decode_scale[decode_dict['Timer']], (decode_scale[decode_dict["Timer"]] - t0) * 5)

                            sample = sample_t

                            sample += 1
                            last_sample = sample

                            samples_red += 1
                        else:
                            print('bad checksum ', f.tell())
                    else:
                        print('short packet', samples_red, sample)

                elif byte[0] > 0x20: # start of string
                    xs = bytearray()
                    while byte and (byte[0] != 13):
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
                        print('start_data time ', data_time)
                        start_time = data_time

                        sample = 0
                        samples_red = 0

                    # check for end data
                    matchobj = end_data.match(s)
                    if matchobj:
                        print('end data ', start_time, sample, samples_red)
                        end_time = start_time
                        data_time = end_time

                    done = None
                    # check for done
                    matchobj = done_str.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        print('done time ', data_time)
                        done = matchobj
                        start_time = None
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
                        #print('time ', data_time)

                        gps = matchobj
                        print('gps', s)

                    matchobj = gps_fix2.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        #print('time ', data_time)

                        gps = matchobj
                        print('gps', s)

                    matchobj = gps_rmc.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        rmc_split = matchobj.group(2).split(',')
                        if rmc_split[2] == 'A':
                            lat_dm = float(rmc_split[3])
                            lat_ns = rmc_split[4]
                            lon_dm = float(rmc_split[5])
                            lon_ew = rmc_split[6]

                            lat = gps_dm_degree(lat_dm, lat_ns)
                            lon = gps_dm_degree(lon_dm, lon_ew)

                            data_time_rmc = datetime.strptime(rmc_split[9] + ' ' + rmc_split[1] + '000', "%d%m%y %H%M%S.%f")

                        #print('time ', data_time, data_time_rmc, lat, lon, rmc_split)

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

                        #print('got time', data_time, t_idx, n_times)
                        times[t_idx] = date2num(data_time, units=times.units, calendar=times.calendar)
                        if gps:
                            # xpos_var[t_idx] = -np.float(gps.group(4))
                            # ypos_var[t_idx] = np.float(gps.group(3))
                            xpos_var[t_idx] = lon
                            ypos_var[t_idx] = lat

                            lat = np.nan
                            lon = np.nan

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

                        if end_time:
                            print('done write MRU samples', sample)

                            accel_var[t_idx] = accel_samples
                            gyro_var[t_idx] = gyro_samples
                            mag_var[t_idx] = mag_samples
                            quat_var[t_idx] = quat_samples
                            load_var[t_idx] = load_samples

                            end_time = None

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

    print('final times', n_times, times[0], times[-1])

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
