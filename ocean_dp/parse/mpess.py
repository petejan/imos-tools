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
import re
import pytz

import datetime
from datetime import timedelta
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct

from si_prefix import si_format

import ctypes


def mpess(filepath):

    checksum_errors = 0
    no_sync = 0
    total_samples = 0
    time_array = []

    with open(filepath, "rb") as binary_file:
        data = binary_file.read(4)

        while len(data) == 4:
            (sample_type,) = struct.unpack('>L', data)
            #print("0x%08x" % sample_type, type(sample_type))

            if sample_type == 0x5a5a5a5a:
                print("info sample")
                packet = binary_file.read(100)
                info = struct.unpack(">LLBBH6LlLbBB3BHLLL3f8s3fL", packet)
                keys = ['Sys_OppMode', 'Sys_SerialNumber', 'Sys_Platform', 'Sys_UnitNumber', 'Dep_Number', 'Dep_Int_NrOfSamples', 'Dep_Norm_SampleInterval',
                        'Dep_Norm_NrOfSamples', 'Dep_StartTime', 'Dep_StopTime', 'RTC_LastTimeSync', 'RTC_DriftSec', 'RTC_DriftInterval', 'RTC_Calibration',
                        'Sns_SensorsPresent', 'Sns_IMU_Model', 'Unused1', 'Unused2', 'Unused3', 'Sns_LC_Serial', 'Sns_IMU_Serial', 'Batt_ReplaceTime', 'Batt_TotalSampleCnt',
                        'Batt_VoltCal1', 'Batt_VoltCal2', 'Batt_VoltCal3',
                        'PT_CalUnits', 'PT_Cal1', 'PT_Cal2', 'PT_Cal3', 'Checksum']
                info_dict = dict(zip(keys, info))
                print("seiral number 0x%08x" % info_dict["Sys_SerialNumber"])
                #print(info_dict)
                ds = datetime.datetime.fromtimestamp(info_dict["Dep_StartTime"], tz=pytz.UTC).replace(tzinfo=None)
                print("start time", ds)
            elif (sample_type & 0xffff0000) == 0x55AA0000:
                sensors = sample_type & 0xffff
                print("intensive sample", sensors)
                packet = binary_file.read(8)
                (ts, bat) = struct.unpack('>LL', packet)
                ds  = datetime.datetime.fromtimestamp(ts, tz=pytz.UTC).replace(tzinfo=None)
                print(ds)
                samples = 1
                while samples > 0:
                    packet1 = binary_file.read(22*4)
                    d = struct.unpack(">LfL3f3f3f9f1f", packet1)
                    samples = d[0]
                    #print("samples to follow", samples)
                    time_array.append(ds)
                    ds = ds + timedelta(milliseconds=10)
                    total_samples += 1
                #print(samples)
                packet = binary_file.read(8)
                (ts, bat) = struct.unpack('>LL', packet)
            elif (sample_type & 0xffff0000)  == 0xAA550000:
                sensors = sample_type & 0xffff
                print("normal sample", sensors)
                packet = binary_file.read(4)
                (ts, ) = struct.unpack('>L', packet)
                ds  = datetime.datetime.fromtimestamp(ts, tz=pytz.UTC).replace(tzinfo=None)
                print(ds)
                packet = binary_file.read(8)
                (bat, pres) = struct.unpack('>ff', packet)
                print("bat", bat, "press", pres)
                samples = info_dict["Dep_Norm_NrOfSamples"]
                while samples > 0:
                    packet1 = binary_file.read(20*4)
                    d = struct.unpack(">fL3f3f3f9f", packet1)
                    samples -= 1
                    time_array.append(ds)
                    ds = ds + timedelta(milliseconds=100)
                    total_samples += 1
                    #print("samples to follow", samples)
                #print(samples)
                packet = binary_file.read(12)
                (ts, bat, checksum) = struct.unpack('>LLL', packet)
            else:
                print("unknown data 0x%08x" % sample_type)

            data = binary_file.read(4)

    instrument_model = "MPESS"
    instrument_serialnumber = "%02d" % info_dict["Sys_UnitNumber"]
    number_samples_read = total_samples

    # create the netCDF file
    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    ncOut.instrument = 'CSIRO - ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

    ncOut.deployment_number = np.int32(info_dict["Dep_Number"])

    print(len(time_array))

    tDim = ncOut.createDimension("TIME", number_samples_read)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = date2num(time_array, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    #ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    #ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    mpess(sys.argv[1])

