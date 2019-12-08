#!/usr/bin/python3

# gps_imu_2018
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


from netCDF4 import Dataset, num2date, chartostring, date2num
from dateutil import parser
from datetime import datetime
from datetime import timedelta

import numpy as np
import matplotlib.pyplot as plt

import sys
import struct


def gps_imu_2018(netCDFfiles):

    ncOut = Dataset("gps.nc", 'w', format='NETCDF4')

    ncOut.instrument = "CSIRO GPS/IMU Logger"
    ncOut.instrument_model = "SOFS-7.5"
    ncOut.instrument_serial_number = "2018"

    compress = False # uncompressed day 608MB, 667MB compressed, RAW data is 178 MB, uncompressed is smaller than compressed

    # add time
    tDim = ncOut.createDimension("TIME")
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=compress)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"

    ncOut.createDimension("VECTOR", 3)

    # add variables

    accel_out_var = ncOut.createVariable("ACCEL", "f4", ("TIME", "VECTOR"), fill_value=np.nan, zlib=compress)
    gyro_out_var = ncOut.createVariable("GYRO", "f4", ("TIME", "VECTOR"), fill_value=np.nan, zlib=compress)
    compass_out_var = ncOut.createVariable("COMPASS", "f4", ("TIME", "VECTOR"), fill_value=np.nan, zlib=compress)

    typeCount = [0, 0, 0, 0, 0, 0, 0, 0, 0]
    imu_timedelta = timedelta(seconds=1/40.033)
    print(imu_timedelta)
    imu_ts = None
    ts_start = None

    print("file to process", len(netCDFfiles[1:]))
    imu_n = 0
    imu_total = 0
    last_ts = None

    for fn in netCDFfiles[1:]:
        print(fn)
        with open(fn, "rb") as f:
            errorCount = 0
            while True:
                byte = f.read(3)
                if not byte:
                    break
                (type, N) = struct.unpack("<BH", byte)
                #print ('type %d N %d' %(type, N))
                pos = f.tell()
                if (type < 9) & (N < 400):
                    pkt = f.read(N)
                    check = f.read(1)
                    if not check:
                        break
                    checkB = ord(check)
                    s = 0
                    for b in pkt:
                        s = s + b
                    s = s & 0xff
                    if (checkB == s):
                        typeCount[type] = typeCount[type] + 1
                        try:
                            if type == 4:
                                # print("pkt " , ord(pkt[0]), ord(pkt[1]), ord(pkt[2]), ord(pkt[3]), ord(pkt[4]), ord(pkt[5]), ord(pkt[6]), ord(pkt[7]))
                                time = struct.unpack("<BBBBBBH", pkt)
                                ts = datetime(time[6], time[5], time[4], time[2], time[1], time[0])
                                if last_ts:
                                    time_d = ts - last_ts
                                    print("imu sample/sec", imu_n / time_d.total_seconds())

                                last_ts = ts
                                print ("time %4d-%02d-%02d %02d:%02d:%02d : %s imu samples %d" % (time[6], time[5], time[4], time[2], time[1], time[0], ts, imu_n))
                                imu_ts = ts
                                if not ts_start:
                                    ts_start = ts
                                imu_n = 0
                            if type == 0:
                                nmea = pkt.decode('utf-8')
                                #print("%d pos NMEA %s" % ( pos, nmea[:-2] ))
                            if type == 1:
                                ubx = struct.unpack("<BBBBH", pkt[0:6])
                                #print ("ubx " , ubx)
                            if type == 7:
                                text = pkt.decode('utf-8')
                                text = text[:-1]
                                print ("text : %s" % text)
                            if type == 5:
                                # print("pkt " , ord(pkt[0]), ord(pkt[1]), ord(pkt[2]), ord(pkt[3]))
                                adc = struct.unpack("<i", pkt)
                                #print "adc ", adc[0]
                            if type == 6:
                                # print("pkt " , ord(pkt[0]), ord(pkt[1]), ord(pkt[2]), ord(pkt[3]))
                                count = struct.unpack("<HHHHHHHHH", pkt)
                                print("count ", count)
                                print("found ", typeCount)
                            if type == 3:
                                # there are 2440 of these in 1 min, or 40.67 / sec
                                imu = struct.unpack("<hhhhhhhhhhlllll", pkt)
                                #print ("imu  " , imu)
                                #print ("imu %d : compass %f %f %f, gyro %f %f %f, accel %f %f %f" % (imu_n, imu[0]*4915.0/32768, imu[1]*4915.0/32768, imu[2]*4915.0/32768, imu[3]*2000.0/32768, imu[4]*2000.0/32768, imu[5]*2000.0/32768, imu[6]*4.0/32768, imu[7]*4.0/32768, imu[8]*4.0/32768))
                                imu_n += 1
                                if imu_ts:
                                    #print("imu ts", imu_ts)
                                    ncTimesOut[imu_total] = date2num(imu_ts, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

                                    accel_out_var[imu_total] = [imu[6]*4.0/32768, imu[7]*4.0/32768, imu[8]*4.0/32768]
                                    gyro_out_var[imu_total] = [imu[3]*2000.0/32768, imu[4]*2000.0/32768, imu[5]*2000.0/32768]
                                    compass_out_var[imu_total] = [imu[0]*4915.0/32768, imu[1]*4915.0/32768, imu[2]*4915.0/32768]

                                    imu_ts += imu_timedelta
                                    imu_total += 1

                            if type == 2:
                                imu = struct.unpack("<hhhhhhhhhhlllll", pkt)
                                print("imu", imu_n, imu)
                                imu_n += 1
                                #print imu[0], imu[1], imu[2], imu[3], imu[4], imu[5], imu[6], imu[7], imu[8]
                        except (UnicodeDecodeError, struct.error):
                            print (pos, " Error : ", pkt)
                            errorCount = errorCount + 1
                    else:
                        print(pos, " sum error ", checkB, s)
                        f.seek(pos)

    print("error count", errorCount)

    # add some summary metadata
    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", imu_ts.strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + netCDFfiles[1])

    ncOut.close()


if __name__ == "__main__":
    gps_imu_2018(sys.argv)
