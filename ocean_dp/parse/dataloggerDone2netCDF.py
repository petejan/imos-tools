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

# parse the datalogger done records, and create a time vector from start:end with 1 hr step
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


def datalogger(outputName, files):

    done_str = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: \d done time \d+ ,BV=(\S+) ,PT=(\S+) ,OBP=(\S+) ,OT=(\S+) ,CHL=(\S+) ,NTU=(\S+) PAR=(\S+) ,meanAccel=(\S+) ,meanLoad=(\S+)')
    done_pulse_str = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: \d done time \d+ ,BV=(\S+) ,PT=(\S+) ,meanAccel=(\S+) ,meanLoad=(\S+)')

    sn = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: Station Name (\S+)')
    sn_imu = re.compile(r'(\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}) INFO: MRU SerialNumber (\d+)')

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

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

    times = dataset.createVariable('TIME', np.float64, ('TIME',))

    times.units = 'days since 1950-01-01 00:00:00'
    times.calendar = 'gregorian'

    # TODO: process all historical data, Pulse, SOFS-1 .... to SOFS-10

    time_list = []

    sample = 0

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
                            print("IMU packet", sample)
                        else:
                            print('bad checksum ', f.tell())
                    else:
                        print('short packet', sample)

                elif byte[0] > 0x20: # start of string

                    # read until CR
                    xs = bytearray()
                    while byte and (byte[0] != 13):
                        if byte[0] < 128:
                            xs.append(byte[0])
                        byte = f.read(1)
                    if len(xs) == 0:
                        break

                    s = xs.decode('ascii')
                    #print('string : ', len(xs), s)

                    data_time = None

                    # check for done
                    matchobj = done_str.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        print('done time ', data_time)

                    # check for done_pulse
                    done_pulse = None
                    matchobj = done_pulse_str.match(s)
                    if matchobj:
                        data_time = datetime.strptime(matchobj.group(1), "%Y-%m-%d %H:%M:%S")
                        print('done time ', data_time)

                    # update time bounds if we got a time
                    if data_time:
                        data_time = round_time(data_time)

                        time_list.append(data_time)

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

    print('final times', min(time_list), max(time_list))

    ts_start = round_time(min(time_list))
    ts_end = round_time(max(time_list))

    dataset.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    dataset.setncattr("time_coverage_end", ts_end.strftime(ncTimeFormat))

    # add creating and history entry
    dataset.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    dataset.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(files[0]))

    dataset.close()

    return outputName


if __name__ == "__main__":

    outfn = sys.argv[1]

    files = []
    for f in sys.argv[2:]:
        files.extend(glob(f))

    datalogger(outfn, files)
