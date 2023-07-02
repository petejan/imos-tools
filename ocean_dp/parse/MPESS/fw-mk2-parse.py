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
import pytz

import datetime
from datetime import timedelta
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct

import zlib


def mpess(file):
    print('decode file : ', file)
    ensemble_header_len = 512
    sample_length = 4+16+128+4+4
    timed = timedelta(seconds=0.1)

    # create the netCDF file
    outputName = file + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    # create the netCDF variables

    # add time
    tDim = ncOut.createDimension("TIME")
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    #ncTimesOut[:] = date2num(np.array([ data['ts'] for data in data_array]) , calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

    ncOut.createDimension("VECTOR", 3)
    ncOut.createDimension("MATRIX", 9)

    # add variables

    var_burst = ncOut.createVariable("BURST", "u2", ("TIME",), zlib=True)

    var_accel = ncOut.createVariable("ACCEL", "f4", ("TIME", "VECTOR"), fill_value=np.nan, zlib=True)

    var_gyrp = ncOut.createVariable("GYRO", "f4", ("TIME", "VECTOR"), fill_value=np.nan, zlib=True)

    var_mag = ncOut.createVariable("MAG", "f4", ("TIME", "VECTOR"), fill_value=np.nan, zlib=True)

    var_orient = ncOut.createVariable("ORIENT", "f4", ("TIME", "MATRIX"), fill_value=np.nan, zlib=True)

    var_pres = ncOut.createVariable("PRES", "f4", ("TIME",), fill_value=np.nan, zlib=True)
    var_pres.units = 'dbarA'  # info_dict["PT_CalUnits"].decode("utf-8").strip()

    var_tension = ncOut.createVariable("TENSION", "f4", ("TIME",), fill_value=np.nan, zlib=True)

    sample = 0

    with open(file, "rb") as binary_file:

        hdr = binary_file.read(32)

        if len(hdr) == 32:
            (id,) = struct.unpack("32s", hdr)
            print ('id= ', id)
            if (id == b'MPESS data file\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'):
                print('id ok')

                hdr_main = binary_file.read(4096-32)
                s = ""
                for i in range(0, len(hdr_main)):
                    if hdr_main[i] != 0:
                        s += chr(hdr_main[i])
                    else:
                        if len(s) > 0:
                            print(s)
                            if s.startswith("sizeof ensemble_header = "):
                                ensemble_header_len = int(s[s.index("=")+1:])
                                print("ensemble length", ensemble_header_len)
                        if s.startswith("sizeof sample_buffer = "):
                            sample_length = int(s[s.index("=") + 1:])
                            print("sizeof sample_buffer = ", sample_length)
                        s = ""

                ens_hdr = binary_file.read(ensemble_header_len)

                sample_n = 0
                while len(ens_hdr) == 512:

                    (time, n_samples, burst_cnt, vbat) = struct.unpack(">IIIf", ens_hdr[0:16])
                    ens_time = datetime.datetime.utcfromtimestamp(time)

                    print("ensemble hdr ", time, ens_time.strftime('%Y-%m-%d %H:%M:%S'), n_samples, burst_cnt, vbat)

                    for i in range(0, n_samples):
                        sample_bin = binary_file.read(sample_length)
                        (cycle, vpres, imu, tension, used, unused) = struct.unpack(">II128s16sB3s", sample_bin)
                        #print(sample_n, 'sample', cycle, vpres, used)

                        #print('tension', tension[0])
                        if tension[0] == 0x55 and tension[1] == 0xAA:
                            (h, t) = struct.unpack("<2sf", tension[0:6])
                            #print('tension', t)
                        else:
                            print('bad tension', tension[0])
                            t = np.nan

                        if imu[0] == 0xCC:
                            #print(imu)
                            imu_dec = struct.unpack(">s18fIH", imu[0:79])
                            #print('imu accel', imu_dec[1:4], imu_dec[19])
                        else:
                            print('bad imu', imu[0])
                            imu_dec = np.zeros(20)
                            imu_dec[1:4] = np.nan

                        if (imu[0] != 204) or not ((tension[0] == 85) or (tension[0] == 83)):
                            print()
                            print('bad sample', binary_file.tell(), sample_bin)
                            print()

                        print(ens_time.strftime('%Y-%m-%d %H:%M:%S'), sample_n, vpres, t, imu_dec[1:4], imu_dec[19])

                        ncTimesOut[sample] = date2num(ens_time, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")
                        var_tension[sample] = t
                        var_accel[sample] = imu_dec[1:4]

                        ens_time += timed
                        sample_n += 1
                        sample += 1

                    ens_footer = binary_file.read(512)
                    print('ensemble footer', ens_footer)
                    (lat, over) = struct.unpack(">II", ens_footer[0:8])
                    print('latency', lat, 'over', over)
                    sample_n = 0

                    ens_hdr = binary_file.read(512)

    # add some summary metadata
    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + file)

    ncOut.close()


if __name__ == "__main__":
    mpess(sys.argv[1])

