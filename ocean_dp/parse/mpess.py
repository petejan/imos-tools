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

sensor_tension_msk = 0b010
sensor_imu_msk = 0b100
sensor_pres_msk = 0b001

imu_keys = ['imu_ts', 'accelX', 'accelY', 'accelZ', 'gyroX', 'gyroY', 'gyroZ',
                      'magX', 'magY', 'magZ',
                      'orient-M11', 'orient-M12', 'orient-M13', 'orient-M21', 'orient-M22', 'orient-M23', 'orient-M31', 'orient-M32', 'orient-M33']

info_keys = ['Sys_OppMode', 'Sys_SerialNumber', 'Sys_Platform', 'Sys_UnitNumber',
             'Dep_Number', 'Dep_Int_NrOfSamples', 'Dep_Norm_SampleInterval', 'Dep_Norm_NrOfSamples', 'Dep_StartTime', 'Dep_StopTime',
             'RTC_LastTimeSync', 'RTC_DriftSec', 'RTC_DriftInterval', 'RTC_Calibration',
             'Sns_SensorsPresent', 'Sns_IMU_Model', 'Unused1', 'Unused2', 'Unused3', 'Sns_LC_Serial', 'Sns_IMU_Serial',
             'Batt_ReplaceTime', 'Batt_TotalSampleCnt', 'Batt_VoltCal1', 'Batt_VoltCal2', 'Batt_VoltCal3',
             'PT_CalUnits', 'PT_Cal1', 'PT_Cal2', 'PT_Cal3', 'crc']

info_keys_21 = ['Sys_OppMode', 'Sys_SerialNumber', 'Sys_UnitNumber',
             'Dep_Number', 'Dep_Int_NrOfSamples', 'Dep_Norm_SampleInterval', 'Dep_Norm_NrOfSamples', 'Dep_StartTime', 'Dep_StopTime',
             'RTC_LastTimeSync', 'RTC_DriftSec', 'RTC_DriftInterval', 'RTC_Calibration',
             'Sns_SensorsPresent', 'Sns_IMU_Model', 'Unused',
             'Batt_ReplaceTime', 'Batt_TotalSampleCnt',
             'Batt_VoltCal1', 'Batt_VoltCal2', 'Batt_VoltCal3',
             'PTi_CalUnits', 'PTi_Cal1', 'PTi_Cal2', 'PTi_Cal3', 'PTx_CalUnits', 'PTx_Cal1', 'PTx_Cal2', 'crc']

# create a default info dict incase the one in the file is corrupt
info_dictx = {'Sys_OppMode': 1, 'Sys_SerialNumber': 0, 'Sys_Platform': 0, 'Sys_UnitNumber': 1,
             'Dep_Number': 0, 'Dep_Int_NrOfSamples': 72000, 'Dep_Norm_SampleInterval': 3600, 'Dep_Norm_NrOfSamples': 6000, 'Dep_StartTime': 0, 'Dep_StopTime': 0,
             'RTC_LastTimeSync': 0, 'RTC_DriftSec': 0, 'RTC_DriftInterval': 1, 'RTC_Calibration': 0,
             'Sns_SensorsPresent': 7, 'Sns_IMU_Model': 3,
             'Unused1': 0, 'Unused2': 0, 'Unused3': 0,
             'Sns_LC_Serial': 0, 'Sns_IMU_Serial': 0, 'Batt_ReplaceTime': 0,
             'Batt_TotalSampleCnt': 0, 'Batt_VoltCal1': 0, 'Batt_VoltCal2': 0, 'Batt_VoltCal3': 0,
             'PT_CalUnits': b'dbarA', 'PT_Cal1': 0, 'PT_Cal2': 0, 'PT_Cal3': 0}

info_dict = {'Sys_OppMode': 1, 'Sys_SerialNumber': 0, 'Sys_UnitNumber': 1,
             'Dep_Number': 0, 'Dep_Int_NrOfSamples': 72000, 'Dep_Norm_SampleInterval': 3600, 'Dep_Norm_NrOfSamples': 6000, 'Dep_StartTime': 0, 'Dep_StopTime': 0,
             'RTC_LastTimeSync': 0, 'RTC_DriftSec': 0, 'RTC_DriftInterval': 1, 'RTC_Calibration': 0,
             'Sns_SensorsPresent': 7, 'Sns_IMU_Model': 3,
             'Unused': 0,
             'Batt_ReplaceTime': 0, 'Batt_TotalSampleCnt': 0, 'Batt_VoltCal': 0,
             'PTi_CalUnits': b'dbarA', 'PTi_Cal': 0, 'PTx_CalUnits': b'kg', 'PTx_Cal': 0}


def mpess(filepath):

    crc_errors = 0
    no_sync = 0
    total_samples = 0
    data_array = []
    burst_number = 0

    with open(filepath, "rb") as binary_file:

        data = binary_file.read(4)

        while len(data) == 4:
            pos = binary_file.tell()  # save the position incase we get a CRC or sync error
            crc_error = False
            sync_error = False

            (sample_type,) = struct.unpack('>L', data)
            #print("0x%08x" % sample_type, type(sample_type))

            if sample_type == 0x5a5a5a5a:  # info sample

                print("info block")

                packet = binary_file.read(100)
                info = struct.unpack(">LLBBH6LlLbBB3BHLLL3f8s3fL", packet)

                #packet = binary_file.read(104)
                #info = struct.unpack(">LLHH6LlLbBBBLL3f8s3f4s2fL", packet)

                info_dict = dict(zip(info_keys, info))
                #info_dict = dict(zip(info_keys_21, info))

                print("serial number 0x%08x" % info_dict["Sys_SerialNumber"])
                print(info_dict)

                ds = datetime.datetime.fromtimestamp(info_dict["Dep_StartTime"], tz=pytz.UTC).replace(tzinfo=None)
                print("start time", ds)
                info_dict.update({'start_time': ds})

                crc = zlib.crc32(packet[0:-4])
                #print("crc", crc, info_dict['crc'])

                # check the CRC
                if crc != info_dict['crc']:
                    crc_errors += 1

            elif (sample_type & 0xffff0000) == 0x55AA0000:

                sensors = sample_type & 0xffff
                print("intensive sample, sensors", sensors)
                crc = zlib.crc32(data)

                # create the sample decoders
                intensive_keys = ['samples']
                unpack = ">L"
                read_len = 4
                if sensors & sensor_tension_msk != 0:
                    intensive_keys.append('line_tension')
                    unpack += "f"
                    read_len += 4
                if sensors & sensor_imu_msk != 0:
                    intensive_keys.extend(imu_keys)
                    unpack += "L3f3f3f9f"
                    read_len += 19*4
                if sensors & sensor_pres_msk != 0:
                    intensive_keys.append('pres')
                    unpack += "f"
                    read_len += 4

                packet = binary_file.read(8)
                crc = zlib.crc32(packet, crc)
                (ts, bat) = struct.unpack('>Lf', packet)
                ds = datetime.datetime.fromtimestamp(ts, tz=pytz.UTC).replace(tzinfo=None)
                print('intensive sample timestamp', ds, 'bat', bat)

                #print('intensive samples keys', intensive_keys)

                samples = info_dict["Dep_Int_NrOfSamples"]
                # read all remaining samples
                while samples > 0:
                    packet1 = binary_file.read(read_len)
                    crc = zlib.crc32(packet1, crc)

                    # unpack the binary data based on the sensor packing
                    d = struct.unpack(unpack, packet1)
                    samples = d[0]
                    ds = ds + timedelta(milliseconds=100)  # re-create the sample timestamp
                    #print("samples to follow", samples)

                    # add data to dictionary
                    intensive_dict = {'ts': ds}
                    intensive_dict.update({'burst': burst_number})
                    intensive_dict.update(dict(zip(intensive_keys, d)))
                    #print("intensive sample", intensive_dict)

                    # add data dictionary to the data array
                    data_array.append(intensive_dict)

                    total_samples += 1

                #print(samples)

                packet_end = binary_file.read(8)
                if len(packet_end) != 8:
                    break

                crc = zlib.crc32(packet_end[0:4], crc)
                (bat, crc_packet) = struct.unpack('>LL', packet_end)
                #print("bat, crc", bat, crc, crc_packet)

                # check the CRC
                if crc != crc_packet:
                    crc_errors += 1
                    crc_error = True
                    print('bad crc')
                    break
                    # probably should remove the last sample also

                burst_number += 1

            elif (sample_type & 0xffff0000) == 0xAA550000:

                sensors = sample_type & 0xffff
                print("normal sample, sensors", sensors)
                crc = zlib.crc32(data)

                # create the sample decoders
                single_sample_keys = ['ts', 'bat']
                single_sample_unpack = ">Lf"
                single_sample_len = 8
                end_single_sample_keys = ['bat']
                end_single_sample_unpack = ">f"
                end_single_sample_len = 4

                normal_keys = []
                unpack = ">"
                read_len = 0
                if sensors & sensor_tension_msk != 0:
                    normal_keys.append('line_tension')
                    unpack += "f"
                    read_len += 4
                if sensors & sensor_imu_msk != 0:
                    normal_keys.extend(imu_keys)
                    unpack += "L3f3f3f9f"
                    read_len += 19*4
                if sensors & sensor_pres_msk != 0:
                    single_sample_keys.append('pres')
                    single_sample_unpack += "f"
                    single_sample_len += 4
                    end_single_sample_keys.append('pres')
                    end_single_sample_unpack += 'f'
                    end_single_sample_len += 4

                end_single_sample_keys.append('crc')
                end_single_sample_unpack += 'L'
                end_single_sample_len += 4

                # read the data from the file
                packet = binary_file.read(single_sample_len)
                crc = zlib.crc32(packet, crc)

                # unpack the data, and create a sample dictionary
                single_sample = struct.unpack(single_sample_unpack, packet)
                single_sample_dict = dict(zip(single_sample_keys, single_sample))
                ds = datetime.datetime.fromtimestamp(single_sample_dict['ts'], tz=pytz.UTC).replace(tzinfo=None)
                print('normal sample timestamp', ds, single_sample_dict)

                #print("normal keys", normal_keys, "unpck", unpack)
                samples = info_dict["Dep_Norm_NrOfSamples"]

                if sensors & sensor_pres_msk != 0:
                    pres = single_sample_dict['pres']

                # read all samples
                while samples > 0:
                    packet1 = binary_file.read(read_len)
                    crc = zlib.crc32(packet1, crc)

                    # unpack the binary data based on the sensor packing
                    d = struct.unpack(unpack, packet1)
                    samples -= 1
                    ds = ds + timedelta(milliseconds=100) # re-create the sample timestamp

                    # add all the data to a dictionary
                    normal_dict = {'ts': ds}
                    normal_dict.update({'burst': burst_number})
                    if sensors & sensor_pres_msk != 0:
                        normal_dict.update({'pres': pres})
                    normal_dict.update(dict(zip(normal_keys, d)))

                    #print("normal sample ", normal_dict)

                    # add all the data to a data array
                    data_array.append(normal_dict)

                    pres = np.nan  # clear it for writing to next sample

                    total_samples += 1
                    #print("samples to follow", samples)
                #print(samples)

                packet_end = binary_file.read(end_single_sample_len)
                crc = zlib.crc32(packet_end[0:-4], crc)

                end_single_sample = struct.unpack(end_single_sample_unpack, packet_end)
                end_single_sample_dict = dict(zip(end_single_sample_keys, end_single_sample))
                print(end_single_sample_dict)

                # check the CRC
                #print("crc", crc, end_single_sample_dict['crc'])
                if crc != end_single_sample_dict['crc']:
                    crc_errors += 1
                    crc_error = True
                    # probably should remove the last sample also

                # update the last pressure entry
                if sensors & sensor_pres_msk != 0:
                    pres = end_single_sample_dict['pres']
                    normal_dict.update({'pres': pres})
                data_array[len(data_array)-1] = normal_dict

                burst_number += 1

            else:
                print("unknown data 0x%08x pos %d" % (sample_type, binary_file.tell()))
                no_sync += 1
                sync_error = True
                if no_sync >= 10:
                    print("sync not found")
                    return None

            if crc_error or sync_error:
                binary_file.seek(pos - 3)  # move to the next byte after sync read, as the packet had a CRC error

            data = binary_file.read(4)

    instrument_model = "MPESS"
    instrument_serialnumber = "%02d" % info_dict["Sys_UnitNumber"]
    number_samples_read = total_samples

    # create the netCDF file
    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    ncOut.instrument = 'CSIRO ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

    ncOut.deployment_number = np.int32(info_dict["Dep_Number"])
    ncOut.intensive_samples = np.int32(info_dict["Dep_Int_NrOfSamples"])
    ncOut.normal_samples_per_burst = np.int32(info_dict["Dep_Norm_NrOfSamples"])

    # create a sensor string to document the sensors attached
    sensor_str = ""
    if sensors & sensor_imu_msk != 0:
        sensor_str += "IMU ({Sns_IMU_Model})".format_map(info_dict)
        if 'Sns_IMU_Serial' in info_dict:
            sensor_str += "-{Sns_IMU_Serial}".format_map(info_dict)
    if sensors & sensor_tension_msk != 0:
        if len(sensor_str) > 0:
            sensor_str += "; "
        sensor_str += "TENSION"
        if 'Sns_LC_Serial' in info_dict:
            sensor_str += "(PTv%x)" % (info_dict["Sns_LC_Serial"])
    if sensors & sensor_pres_msk != 0:
        if len(sensor_str) > 0:
            sensor_str += "; "
        sensor_str += "PRES"

    ncOut.sensors = sensor_str

    print("total data records ", len(data_array))

    # create the netCDF variables

    # add time
    tDim = ncOut.createDimension("TIME", number_samples_read)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = date2num(np.array([ data['ts'] for data in data_array]) , calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

    if sensors & sensor_imu_msk != 0:
        ncOut.createDimension("VECTOR", 3)
        ncOut.createDimension("MATRIX", 9)

    # add variables

    nc_var_out = ncOut.createVariable("BURST", "u2", ("TIME",), zlib=True)
    nc_var_out[:] = np.array([ data['burst'] for data in data_array])

    if sensors & sensor_imu_msk != 0:
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

    if sensors & sensor_pres_msk != 0:
        nc_var_out = ncOut.createVariable("PRES", "f4", ("TIME",), fill_value=np.nan, zlib=True)
        nc_var_out[:] = np.array([ data['pres'] for data in data_array])
        nc_var_out.units = 'dbarA'  # info_dict["PT_CalUnits"].decode("utf-8").strip()

    if sensors & sensor_tension_msk != 0:
        nc_var_out = ncOut.createVariable("TENSION", "f4", ("TIME",), fill_value=np.nan, zlib=True)
        nc_var_out[:] = np.array([ data['line_tension'] for data in data_array])

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

