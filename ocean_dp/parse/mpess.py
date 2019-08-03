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
    data_array = []
    burst_number = 0

    with open(filepath, "rb") as binary_file:
        data = binary_file.read(4)

        while len(data) == 4:
            (sample_type,) = struct.unpack('>L', data)
            #print("0x%08x" % sample_type, type(sample_type))

            if sample_type == 0x5a5a5a5a:

                print("info block")
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
                info_dict.update({'start_time': ds})

            elif (sample_type & 0xffff0000) == 0x55AA0000:

                sensors = sample_type & 0xffff
                print("intensive sample, sensors", sensors)

                packet = binary_file.read(8)
                (ts, bat) = struct.unpack('>Lf', packet)
                ds  = datetime.datetime.fromtimestamp(ts, tz=pytz.UTC).replace(tzinfo=None)
                print('intensive sample timestamp', ds, 'bat', bat)

                samples = 1
                intensive_keys = ['samples', 'line_force', 'imu_ts', 'accelX', 'accelY', 'accelZ', 'gyroX', 'gyroY', 'gyroZ', 'magX', 'magY', 'magZ',
                                  'orient-M11', 'orient-M12', 'orient-M13', 'orient-M21', 'orient-M22', 'orient-M23', 'orient-M31', 'orient-M32', 'orient-M33', 'pres']

                # read all remaining samples
                while samples > 0:
                    packet1 = binary_file.read(22*4)
                    d = struct.unpack(">LfL3f3f3f9f1f", packet1)
                    samples = d[0]
                    ds = ds + timedelta(milliseconds=100)
                    #print("samples to follow", samples)

                    intensive_dict = dict(zip(intensive_keys, d))
                    intensive_dict.update({'ts': ds})
                    intensive_dict.update({'burst': burst_number})

                    data_array.append(intensive_dict)
                    #print("intensive sample", intensive_dict)
                    total_samples += 1
                #print(samples)

                #packet = binary_file.read(12)
                #(ts, bat, checksum) = struct.unpack('>LLL', packet)

                packet = binary_file.read(8)
                (ts, bat) = struct.unpack('>LL', packet)

                burst_number += 1

            elif (sample_type & 0xffff0000)  == 0xAA550000:

                sensors = sample_type & 0xffff
                print("normal sample, sensors", sensors)

                packet = binary_file.read(12)
                (ts, bat, pres) = struct.unpack('>Lff', packet)
                ds  = datetime.datetime.fromtimestamp(ts, tz=pytz.UTC).replace(tzinfo=None)
                print('normal sample timestamp', ds, 'bat', bat, 'pres', pres)

                normal_keys = ['line_force', 'imu_ts', 'accelX', 'accelY', 'accelZ', 'gyroX', 'gyroY', 'gyroZ', 'magX', 'magY', 'magZ',
                               'orient-M11', 'orient-M12', 'orient-M13', 'orient-M21', 'orient-M22', 'orient-M23', 'orient-M31', 'orient-M32', 'orient-M33']

                samples = info_dict["Dep_Norm_NrOfSamples"]

                # read all samples
                while samples > 0:
                    packet1 = binary_file.read(20*4)
                    d = struct.unpack(">fL3f3f3f9f", packet1)
                    samples -= 1
                    ds = ds + timedelta(milliseconds=100)

                    normal_dict = dict(zip(normal_keys, d))
                    normal_dict.update({'ts': ds})
                    normal_dict.update({'pres': pres})
                    normal_dict.update({'burst': burst_number})

                    data_array.append(normal_dict)

                    #print("normal sample ", normal_dict)
                    total_samples += 1
                    #print("samples to follow", samples)
                #print(samples)

                packet = binary_file.read(12)
                (ts, bat, checksum) = struct.unpack('>LLL', packet)
                burst_number += 1

            else:
                print("unknown data 0x%08x" % sample_type)
                no_sync += 1
                if no_sync >= 10:
                    print("sync not found")

                    return None

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
    ncOut.intensive_samples = np.int32(info_dict["Dep_Int_NrOfSamples"])
    ncOut.normal_samples_per_burst = np.int32(info_dict["Dep_Norm_NrOfSamples"])


    print(len(data_array))

    ncOut.createDimension("VECTOR", 3)
    ncOut.createDimension("MATRIX", 9)

    # add time
    tDim = ncOut.createDimension("TIME", number_samples_read)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = date2num(np.array([ data['ts'] for data in data_array]) , calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

    # add variables

    nc_var_out = ncOut.createVariable("BURST", "u2", ("TIME",), zlib=True)
    nc_var_out[:] = np.array([ data['burst'] for data in data_array])

    nc_var_out = ncOut.createVariable("ACCEL", "f4", ("TIME", "VECTOR"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = np.array([ [data['accelX'], data['accelY'], data['accelZ']] for data in data_array])

    nc_var_out = ncOut.createVariable("GYRO", "f4", ("TIME", "VECTOR"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = np.array([ [data['gyroX'], data['gyroY'], data['gyroZ']] for data in data_array])

    nc_var_out = ncOut.createVariable("MAG", "f4", ("TIME", "VECTOR"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = np.array([ [data['magX'], data['magY'], data['magZ']] for data in data_array])

    nc_var_out = ncOut.createVariable("ORIENT", "f4", ("TIME", "MATRIX"), fill_value=np.nan, zlib=True)
    nc_var_out[:] = np.array([ [data['orient-M11'], data['orient-M12'], data['orient-M13'],
                                data['orient-M21'], data['orient-M22'], data['orient-M23'],
                                data['orient-M31'], data['orient-M32'], data['orient-M33']] for data in data_array])

    nc_var_out = ncOut.createVariable("PRES", "f4", ("TIME",), fill_value=np.nan, zlib=True)
    nc_var_out[:] = np.array([ data['pres'] for data in data_array])
    nc_var_out.units = 'dbarA'  # info_dict["PT_CalUnits"].decode("utf-8").strip()

    nc_var_out = ncOut.createVariable("LOAD", "f4", ("TIME",), fill_value=np.nan, zlib=True)
    nc_var_out[:] = np.array([ data['line_force'] for data in data_array])

    # add some summary metadata
    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    mpess(sys.argv[1])

