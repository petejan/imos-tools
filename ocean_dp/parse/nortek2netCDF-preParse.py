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
import os.path
import sys
import re

import datetime
import traceback
from datetime import timedelta
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct

from si_prefix import si_format

import ctypes
from array import array

# nortek data codes (these are in hex) from 'system integrator manual october 2017'
#  0 User Configuration

#  1 Aquadopp Velocity Data
#  2 Vectrino distance data

#  6 Head Configuration
#  7 Hardware Configuration

# 10 Aquadopp Diagnostics Data Header

# 11 Vector and Vectrino Probe Check data
# 12 Vector Velocity Data
# 20 Vector System Data
# 21 Vector Velocity Data Header

# 24 AWAC Velocity Profile Data

# 21 Aquadopp Profiler Velocity Data

# 24 Continental Data

# 2a High Resolution Aquadopp Profiler Data

# 30 AWAC Wave Data
# 31 AWAC Wave Data Header
# 36 AWAC Wave Data SUV
# 42 AWAC Stage Data

# 50 Vectrino velocity data header
# 51 Vectrino velocity data

# 60 Wave parameter estimates
# 61 Wave band estimates
# 62 Wave energy spectrum
# 63 Wave Fourier coefficient spectrum Cleaned up AST time series

# 65 Awac Processed Velocity Profile Data

# 71 Vector with IMU

# 80 Aquadopp Diagnostics Data

packet_id = {}
packet_id['User Configuration'] = 0
packet_id['Head Configuration'] = 4
packet_id['Hardware Configuration'] = 5
packet_id['Aquadopp Velocity Data'] = 1
packet_id['Aquadopp Diagnostics Data'] = 128
packet_id['Aquadopp Diagnostics Data Header'] = 6
packet_id['Vector and Vectrino Probe Check Data'] = 7
packet_id['Vector Velocity Data Header'] = 18
packet_id['Vector System Data'] = 17
packet_id['Vector Velocity Data'] = 16
packet_id['Vector With IMU'] = 113
packet_id['Aquadopp Profiler Velocity Data'] = 33
packet_id['HR Aquadopp Profiler Velocity Data'] = 42
packet_id['AWAC Wave Data'] = 48
packet_id['AWAC wave Data Header'] = 49

packet_decoder = {}

packet_decoder[0] = {'name': 'User Configuration', 'keys': ['T1', 'T2', 'T3', 'T4', 'T5', 'NPing', 'AvgInt', 'NBeam', 'TimCtrlReg', 'PwrCtrlReg',
                            'A1', 'B0', 'B1', 'CompassUpdRate', 'CoordSys', 'NBins', 'BinLen', 'MeasInterval',
                            'DeplyName', 'WrapMode', 'clockDeploy', 'DiagInterval', 'Mode', 'AdjSoundSpeed',
                            'NSampDiag', 'NBeamsCellDiag', 'NPingsDiag', 'ModeTest', 'AnaInAddr', 'SWVersion',
                            'salinity', 'VelAdjTable', 'comments', 'spare1', 'Proc', 'spare2', 'Mode', 'DynPercPos',
                            'wT1', 'wT2', 'wT3', 'NSamp', 'wA1', 'wB0', 'wB1', 'spare3', 'AnaOutScale', 'CorrThresh',
                            'spare3', 'TiLag2', 'spare4', 'QualConst', 'checksum'], 'unpack': "<18H6sH6sI9H180s80s48sH50s14H22s24sH"}

packet_decoder[4] = {'name': 'Head Configuration', 'keys': ['head_config', 'head_frequency', 'head_type', 'head_serial', 'system', 'spare', 'NBeam', 'checksum'], 'unpack': "<HHH12s176s22sHH"}

packet_decoder[5] = {'name': 'Hardware Configuration', 'keys': ['serial', 'config', 'frequency', 'PICversion', 'HWrevision', 'RecSize', 'status', 'spare', 'FWversion', 'checksum'], 'unpack': "<14s6H12s4sH"}

packet_decoder[1] = {'name': 'Aquadopp Velocity Data', 'keys': ['time_bcd', 'error', 'AnaIn1', 'battery', 'soundSpd_Anain2', 'head', 'pitch', 'roll',
                            'presMSB', 'status', 'presLSW', 'temp', 'vel_b1', 'vel_b2', 'vel_b3', 'amp1', 'amp2', 'amp3', 'fill', 'checksum'], 'unpack': "<6s7hBBH4h4BH"}

packet_decoder[128] = {'name': 'Aquadopp Diagnostics Data', 'keys': ['time_bcd', 'error', 'AnaIn1', 'battery', 'soundSpd_Anain2', 'head', 'pitch', 'roll',
                            'presMSB', 'status', 'presLSW', 'temp', 'vel_b1', 'vel_b2', 'vel_b3', 'amp1', 'amp2', 'amp3', 'fill', 'checksum'], 'unpack': "<6s7hBBH4h4BH"}

packet_decoder[6] = {'name': 'Aquadopp Diagnostics Data Header', 'keys': ['records', 'cell', 'noise1', 'noise2', 'noise3', 'noise4', 'proc1', 'proc2',
                            'proc3', 'proc4', 'dis1', 'dis2', 'dist3', 'dist4', 'spare', 'checksum'], 'unpack': "<2H4B8H6sH"}

packet_decoder[7] = {'name': 'Vector and Vectrino Probe Check Data', 'keys': ['samples', 'firstsample', 'AmpB1...', 'AmpB2...', 'AmpB3...', 'checksum'], 'unpack': "<HH{0}B{0}B{0}BH"}

packet_decoder[18] = {'name': 'Vector Velocity Data Header', 'keys': ['time_bcd', 'NRecords', 'noise1', 'noise2', 'noise3', 'spare', 'corr1', 'corr2', 'corr3', 'spare1', 'spare3', 'checksum'], 'unpack': "<6sH3BB3B1B20BH"}
packet_decoder[17] = {'name': 'Vector System Data', 'keys': ['time_bcd', 'battery', 'soundSpd', 'head', 'pitch', 'roll', 'temp', 'error', 'status', 'anain', 'checksum'], 'unpack': "<6s6HBBHH"}
packet_decoder[16] = {'name': 'Vector Velocity Data', 'keys': ['AnaIn2LSB', 'count', 'presMSB', 'AnaIn2MSB', 'presLSW', 'AnaIn1', 'vel1', 'vel2', 'vel3', 'amp1', 'amp2', 'amp3', 'corr1', 'corr2', 'corr3', 'checksum'], 'unpack': "<BBBB5H3B3BH"}
# AHRSid IMU data packet 0xcc (204)
packet_decoder[113] = {'name': 'Vector With IMU', 'keys': ['EnsCnt', 'AHRSid', 'accelX', 'accelY', 'accelZ', 'angRateX', 'angRateY', 'angRateZ', 'MagX', 'MagY', 'MagZ', 'M11', 'M12', 'M13', 'M21', 'M22', 'M23', 'M31', 'M32', 'M33', 'timer', 'IMUchSum', 'checksum'], 'unpack': "<BB18fIHH"}

packet_decoder[33] = {'name': 'Aquadopp Profiler Velocity Data', 'keys': ['time_bcd', 'error', 'AnaIn1', 'battery', 'soundSpd_Anain2', 'head', 'pitch', 'roll',
                              'presMSB', 'status', 'presLSW', 'temp', 'vel_b1...', 'vel_b2...', 'vel_b3...', 'amp1...', 'amp2...', 'amp3...', 'checksum' ], 'unpack': '<6s7hBBHH{0}h{0}BH'}

packet_decoder[42] = {'name': 'HR Aquadopp Profiler Velocity Data', 'keys': ['time_bcd', 'ms', 'error', 'battery', 'soundSpd', 'head', 'pitch', 'roll',
                                                                             'presMSB', 'status', 'presLSW', 'temp', 'AnaIn1', 'AnaIn2', 'beams', 'cells',
                                                                             'VelLag2_b1', 'VelLag2_b2', 'VelLag2_b3',
                                                                             'AmpLag2_b1', 'AmpLag2_b3', 'AmpLag2_b3',
                                                                             'CorrLag2_b1', 'CorrLag2_b2', 'CorrLag2_b3',
                                                                             'spare1', 'spare2', 'spare3',
                                                                             'vel...', 'amp...', 'corr...', 'checksum'], 'unpack': '<6s7hBBHHHHBB3H3B3B3H{0}h{0}B{0}BH'}

packet_decoder[48] = {'name': 'AWAC Wave Data', 'keys': ['pressure', 'distance1', 'anaIn', 'vel1', 'vel2', 'vel3', 'dist1_vel4', 'amp1', 'amp2', 'amp3', 'amp4', 'checksum' ], 'unpack': '<7h4BH'}

packet_decoder[49] = {'name': 'AWAC wave Data Header', 'keys': ['time_bcd', 'NRecords', 'blanking', 'battery', 'sound_speed', 'heading', 'pitch', 'roll', 'minPres', 'maxPres',
                                                                'temperature', 'cell_size', 'noise1', 'noise2', 'noise3', 'noise4', 'progmagn1', 'progmagn2', 'progmagn3', 'progmagn4', 'spare', 'checksum'], 'unpack': "<6s11H4B4H14sH"}


# TODO: how to map the above into netCDF attributes....

packet_decode2netCDF = {}
packet_decode2netCDF[0] = {'decode': 'head_frequency', 'attrib': 'head_frequency_kHz'}
packet_decode2netCDF[1] = {'decode': 'T1', 'attrib': 'tx_pulse_length'}
packet_decode2netCDF[2] = {'decode': 'T2', 'attrib': 'blank_distance'}
packet_decode2netCDF[3] = {'decode': 'T3', 'attrib': 'receive_length'}
packet_decode2netCDF[4] = {'decode': 'T4', 'attrib': 'time_between_pings'}
packet_decode2netCDF[5] = {'decode': 'T5', 'attrib': 'time_between_bursts'}
packet_decode2netCDF[6] = {'decode': 'NBeam', 'attrib': 'number_beams'}
packet_decode2netCDF[7] = {'decode': 'MeasInterval', 'attrib': 'mesurement_interval'}
packet_decode2netCDF[8] = {'decode': 'AvgInt', 'attrib': 'averaging_interval'}
packet_decode2netCDF[9] = {'decode': 'FWversion', 'attrib': 'firmware_version'}

coord_system = None

coord_systems = ['ENU', 'XYZ', 'BEAM']


def create_netCDF_var(ncfile, name, type, comment, units, dims):

    if type.startswith('f'):
        fill = np.NaN
    elif type.startswith('i'):
        fill = -1

    ncVarOut = ncfile.createVariable(name, type, dims, fill_value=fill, zlib=True)  # fill_value defaults to max
    ncVarOut.comment = comment
    ncVarOut.units = units

    return ncVarOut


def fill_var_keys(number_samples, keys):
    # fill in keys with variable number of values (keys ending in ...) in packet_decoder
    # TODO: move this code so its not done every time
    # print(type(keys))
    keys_out = []
    for k in keys:
        if k.endswith("..."):
            kn = k.replace("...", "")
            for i in range(0, number_samples):
                keys_out.append(kn + "[" + str(i) + "]")
        else:
            keys_out.append(k)
    # print(keys_out)

    return keys_out


def clean_serial(sn):
    snx = bytearray(sn)
    for x in range(len(snx)):
        # print("byte ", x , sn[x])
        if snx[x] in [0xc0, 0x07, 0x83, 0x06, 0x11, 0x02]:
            snx[x] = 32
        if not ((0x2e <= snx[x] <= 0x39) or (0x40 < snx[x] <= 0x7a)):
            snx[x] = 32

    sn_string = snx.decode("utf-8", errors='ignore').strip()

    return sn_string


def bcd_time_to_datetime(bcd):
    y = []
    for x in bcd:
        y.append(int((((x & 0xf0) / 16) * 10) + (x & 0xf)))

    dt = datetime.datetime(y[4] + 2000, y[5], y[2], y[3], y[0], y[1])

    return dt


def build_vector_system_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack):
    time_start = datetime.datetime.now()
    time_id = d['time_bcd']

    number_sys_samples = len(pkt_pos_list)

    tDim = ncOut.createDimension("SYS_TIME", number_sys_samples)
    ncSysTimesOut = ncOut.createVariable("SYS_TIME", "d", ("SYS_TIME",), zlib=True)
    ncSysTimesOut.long_name = "system data time"
    ncSysTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncSysTimesOut.calendar = "gregorian"
    times_array = np.zeros([number_sys_samples])

    # TODO: check head_config for magnetometer, tilt and pressure sensor
    head = create_netCDF_var(ncOut, "HEADING_MAG", "f4", "heading magnetic", "degrees", ("SYS_TIME",))
    pitch = create_netCDF_var(ncOut, "PITCH", "f4", "pitch", "degrees", ("SYS_TIME",))
    roll = create_netCDF_var(ncOut, "ROLL", "f4", "roll", "degrees", ("SYS_TIME",))
    bat = create_netCDF_var(ncOut, "BATT", "f4", "battery voltage", "V", ("SYS_TIME",))
    itemp = create_netCDF_var(ncOut, "ITEMP", "f4", "instrument temperature", "degrees_Celsius", ("SYS_TIME",))
    sspeed = create_netCDF_var(ncOut, "SSPEED", "f4", "sound speed", "m/s", ("SYS_TIME",))
    error = create_netCDF_var(ncOut, "ERROR", "i1", "error code", "1", ("SYS_TIME",))
    status = create_netCDF_var(ncOut, "STATUS", "i1", "status", "1", ("SYS_TIME",))

    var_list = []
    var_list.append((head, d['head'], 10, np.empty([number_sys_samples], dtype=np.float32)))
    var_list.append((pitch, d['pitch'], 10, np.empty([number_sys_samples], dtype=np.float32)))
    var_list.append((roll, d['roll'], 10, np.empty([number_sys_samples], dtype=np.float32)))
    var_list.append((bat, d['battery'], 10, np.empty([number_sys_samples], dtype=np.float32)))
    var_list.append((itemp, d['temp'], 100, np.empty([number_sys_samples], dtype=np.float32)))
    var_list.append((sspeed, d['soundSpd'], 10, np.empty([number_sys_samples], dtype=np.float32)))

    var_list.append((error, d['error'], 10, np.empty([number_sys_samples], dtype=np.int8)))
    var_list.append((status, d['status'], 10, np.empty([number_sys_samples], dtype=np.int8)))

    # NaN fill any float variables
    for v in var_list:
        if v[3].dtype == 'float32':
            v[3][:] = np.NaN

    sample = 0
    while sample < number_sys_samples:
        binary_file.seek(pkt_pos_list[sample])
        packet_data = binary_file.read(pkt_len[pkt_id] * 2 - 4)
        packetDecode = struct.unpack(unpack, packet_data)

        dt = bcd_time_to_datetime(packetDecode[time_id])
        times_array[sample] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

        for v in var_list:
            v[3][sample] = packetDecode[v[1]] / v[2]

        sample += 1

    print('read-data took', datetime.datetime.now() - time_start)

    ncSysTimesOut[:] = times_array
    for v in var_list:
        v[0][:] = v[3]

    return


def build_aquaprohr_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack, beams, cells):
    time_start = datetime.datetime.now()

    number_samples = len(pkt_pos_list)

    tDim = ncOut.createDimension("TIME", number_samples)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    times_array = np.zeros([number_samples])

    pres_array = np.empty([number_samples])
    pres_array[:] = np.NaN

    print('HR pro, number beams, cells, cooridnate', beams, cells, coord_system)
    ncOut.createDimension("CELL", cells)

    # TODO: check head_config for magnetometer, tilt and pressure sensor
    head = create_netCDF_var(ncOut, "HEADING_MAG", "f4", "heading magnetic", "degrees", ("TIME",))
    pitch = create_netCDF_var(ncOut, "PITCH", "f4", "pitch", "degrees", ("TIME",))
    roll = create_netCDF_var(ncOut, "ROLL", "f4", "roll", "degrees", ("TIME",))
    pres = create_netCDF_var(ncOut, "PRES", "f4", "pres", "dbar", ("TIME",))
    bat = create_netCDF_var(ncOut, "BATT", "f4", "battery voltage", "V", ("TIME",))
    itemp = create_netCDF_var(ncOut, "ITEMP", "f4", "instrument temperature", "degrees_Celsius", ("TIME",))
    sspeed = create_netCDF_var(ncOut, "SSPEED", "f4", "sound speed", "m/s", ("TIME",))
    analog1 = create_netCDF_var(ncOut, "ANALOG1", "f4", "analog input 1", "counts", ("TIME",))
    analog2 = create_netCDF_var(ncOut, "ANALOG2", "f4", "analog input 2", "counts", ("TIME",))

    # is a AquaPro HR always in BEAM coordinates?
    if coord_system == 2:
        vel1 = create_netCDF_var(ncOut, "VEL_B1", "f4", "velocity beam 1", "m/s", ("TIME", "CELL"))
        vel2 = create_netCDF_var(ncOut, "VEL_B2", "f4", "velocity beam 2", "m/s", ("TIME", "CELL"))
        vel3 = create_netCDF_var(ncOut, "VEL_B3", "f4", "velocity beam 3", "m/s", ("TIME", "CELL"))
    elif coord_system == 1:
        vel1 = create_netCDF_var(ncOut, "VEL_X", "f4", "velocity X", "m/s", ("TIME", "CELL"))
        vel2 = create_netCDF_var(ncOut, "VEL_Y", "f4", "velocity Y", "m/s", ("TIME", "CELL"))
        vel3 = create_netCDF_var(ncOut, "VEL_Z", "f4", "velocity Z", "m/s", ("TIME", "CELL"))
    else:
        vel1 = create_netCDF_var(ncOut, "UCUR_MAG", "f4", "current east", "m/s", ("TIME", "CELL"))
        vel2 = create_netCDF_var(ncOut, "VCUR_MAG", "f4", "current north", "m/s", ("TIME", "CELL"))
        vel3 = create_netCDF_var(ncOut, "WCUR", "f4", "current up", "m/s", ("TIME", "CELL"))

    absci1 = create_netCDF_var(ncOut, "ABSIC1", "i2", "amplitude beam 1", "counts", ("TIME", "CELL"))
    absci2 = create_netCDF_var(ncOut, "ABSIC2", "i2", "amplitude beam 2", "counts", ("TIME", "CELL"))
    absci3 = create_netCDF_var(ncOut, "ABSIC3", "i2", "amplitude beam 3", "counts", ("TIME", "CELL"))

    sample = 0
    time_id = d['time_bcd']
    pred_msb_id = d['presMSB']
    pred_lsb_id = d['presLSW']

    imu_vec_list = []
    imu_vec_list.append((vel1, d['vel[0]'], 1000, np.empty([number_samples, cells], dtype=np.float32)))
    imu_vec_list.append((vel2, d['vel[' + str(cells) + ']'], 1000, np.empty([number_samples, cells], dtype=np.float32)))
    imu_vec_list.append(
        (vel3, d['vel[' + str(cells * 2) + ']'], 1000, np.empty([number_samples, cells], dtype=np.float32)))

    var_list = []
    var_list.append((head, d['head'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((pitch, d['pitch'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((roll, d['roll'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((bat, d['battery'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((itemp, d['temp'], 100, np.empty([number_samples], dtype=np.float32)))
    var_list.append((sspeed, d['soundSpd'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((analog1, d['AnaIn1'], 1, np.empty([number_samples], dtype=np.float32)))
    var_list.append((analog2, d['AnaIn2'], 1, np.empty([number_samples], dtype=np.float32)))

    imu_vec_list.append((absci1, d['amp[0]'], 1, np.zeros([number_samples, cells], dtype=int)))
    imu_vec_list.append((absci2, d['amp[' + str(cells) + ']'], 1, np.zeros([number_samples, cells], dtype=int)))
    imu_vec_list.append((absci3, d['amp[' + str(cells * 2) + ']'], 1, np.zeros([number_samples, cells], dtype=int)))

    # NaN fill any float variables
    for v in var_list:
        if v[3].dtype == 'float32':
            v[3][:] = np.NaN
    for v in imu_vec_list:
        if v[3].dtype == 'float32':
            v[3][:] = np.NaN

    while sample < len(pkt_pos_list):
        binary_file.seek(pkt_pos_list[sample])
        packet_data = binary_file.read(pkt_len[pkt_id] * 2 - 4)
        packetDecode = struct.unpack(unpack, packet_data)

        dt = bcd_time_to_datetime(packetDecode[time_id])
        times_array[sample] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

        for v in var_list:
            v[3][sample] = packetDecode[v[1]] / v[2]
        for v in imu_vec_list:
            for i in range(cells):
                v[3][sample, i] = packetDecode[v[1] + i] / v[2]

        pres_array[sample] = ((packetDecode[pred_msb_id] * 65536) + packetDecode[pred_lsb_id]) * 0.001

        sample += 1

    print('read-data took', datetime.datetime.now() - time_start)

    ncTimesOut[:] = times_array
    for v in var_list:
        v[0][:] = v[3]
    for v in imu_vec_list:
        print('saving', v[0].name, 'shape', v[3].shape)
        v[0][:, :] = v[3]
    pres[:] = pres_array

    return ncTimesOut


def build_aquadopp_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack):
    time_start = datetime.datetime.now()

    number_samples = len(pkt_pos_list)

    tDim = ncOut.createDimension("TIME", number_samples)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    times_array = np.zeros([number_samples])

    pres_array = np.empty([number_samples], dtype=np.float32)
    pres_array[:] = np.NaN

    if coord_system == 2:
        vel1 = create_netCDF_var(ncOut, "VEL_B1", "f4", "velocity beam 1", "m/s", ("TIME",))
        vel2 = create_netCDF_var(ncOut, "VEL_B2", "f4", "velocity beam 2", "m/s", ("TIME",))
        vel3 = create_netCDF_var(ncOut, "VEL_B3", "f4", "velocity beam 3", "m/s", ("TIME",))
    elif coord_system == 1:
        vel1 = create_netCDF_var(ncOut, "VEL_X", "f4", "velocity X", "m/s", ("TIME",))
        vel2 = create_netCDF_var(ncOut, "VEL_Y", "f4", "velocity Y", "m/s", ("TIME",))
        vel3 = create_netCDF_var(ncOut, "VEL_Z", "f4", "velocity Z", "m/s", ("TIME",))
    else:
        vel1 = create_netCDF_var(ncOut, "UCUR_MAG", "f4", "current east", "m/s", ("TIME",))
        vel2 = create_netCDF_var(ncOut, "VCUR_MAG", "f4", "current north", "m/s", ("TIME",))
        vel3 = create_netCDF_var(ncOut, "WCUR", "f4", "current up", "m/s", ("TIME",))

    head = create_netCDF_var(ncOut, "HEADING_MAG", "f4", "heading magnetic", "degrees", ("TIME",))
    pitch = create_netCDF_var(ncOut, "PITCH", "f4", "pitch", "degrees", ("TIME",))
    roll = create_netCDF_var(ncOut, "ROLL", "f4", "roll", "degrees", ("TIME",))
    pres = create_netCDF_var(ncOut, "PRES", "f4", "pres", "dbar", ("TIME",))
    bat = create_netCDF_var(ncOut, "BATT", "f4", "battery voltage", "V", ("TIME",))
    itemp = create_netCDF_var(ncOut, "ITEMP", "f4", "instrument temperature", "degrees_Celsius", ("TIME",))
    sspeed = create_netCDF_var(ncOut, "SSPEED", "f4", "sound speed", "m/s", ("TIME",))

    absci1 = create_netCDF_var(ncOut, "ABSIC1", "i2", "amplitude beam 1", "counts", ("TIME",))
    absci2 = create_netCDF_var(ncOut, "ABSIC2", "i2", "amplitude beam 2", "counts", ("TIME",))
    absci3 = create_netCDF_var(ncOut, "ABSIC3", "i2", "amplitude beam 3", "counts", ("TIME",))

    error = create_netCDF_var(ncOut, "ERROR", "i2", "error code", "counts", ("TIME",))
    status = create_netCDF_var(ncOut, "STATUS", "i2", "status code", "counts", ("TIME",))

    error.comment = '0: compass, 1:measurement data, 2: sensor data, 3: tag bit, 4: flash, 6: serial CT sensor error'
    status.comment = '0: orientation (0 = up, 1 = down), 1: scaling (0=mm/s, 1=0.1 m/s), 2: pitch (0=ok, 1=out of range), 3: roll, 4,5: wake state, 6,7: power'

    sample = 0
    time_id = d['time_bcd']
    pred_msb_id = d['presMSB']
    pred_lsb_id = d['presLSW']

    var_list = []
    var_list.append((vel1, d['vel_b1'], 1000, np.empty([number_samples], dtype=np.float32)))
    var_list.append((vel2, d['vel_b2'], 1000, np.empty([number_samples], dtype=np.float32)))
    var_list.append((vel3, d['vel_b3'], 1000, np.empty([number_samples], dtype=np.float32)))
    var_list.append((head, d['head'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((pitch, d['pitch'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((roll, d['roll'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((bat, d['battery'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((itemp, d['temp'], 100, np.empty([number_samples], dtype=np.float32)))
    var_list.append((sspeed, d['soundSpd_Anain2'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((absci1, d['amp1'], 1, np.zeros([number_samples], dtype=int)))
    var_list.append((absci2, d['amp2'], 1, np.zeros([number_samples], dtype=int)))
    var_list.append((absci3, d['amp3'], 1, np.zeros([number_samples], dtype=int)))

    var_list.append((error, d['error'], 1, np.zeros([number_samples], dtype=int)))
    var_list.append((status, d['status'], 1, np.zeros([number_samples], dtype=int)))

    # NaN fill any float variables
    for v in var_list:
        if v[3].dtype == 'float32':
            v[3][:] = np.NaN

    while sample < len(pkt_pos_list):
        binary_file.seek(pkt_pos_list[sample])
        packet_data = binary_file.read(pkt_len[pkt_id] * 2 - 4)
        packetDecode = struct.unpack(unpack, packet_data)

        dt = bcd_time_to_datetime(packetDecode[time_id])
        times_array[sample] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")
        for v in var_list:
            v[3][sample] = packetDecode[v[1]] / v[2]
        pres_array[sample] = ((packetDecode[pred_msb_id] * 65536) + packetDecode[pred_lsb_id]) * 0.001

        sample += 1

    print('read-data took', datetime.datetime.now() - time_start)

    ncTimesOut[:] = times_array
    for v in var_list:
        v[0][:] = v[3]
    pres[:] = pres_array

    return ncTimesOut


def build_vector_velocity_data(ncOut, binary_file, pkt_pos, pkt_pos_list, pkt_len, pkt_id, d, unpack, number_data_samples):
    time_start = datetime.datetime.now()

    number_samples = len(pkt_pos_list)
    print('Vector number of samples', number_data_samples)

    tDim = ncOut.createDimension("TIME", number_samples)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"

    times_array = np.zeros([number_samples])

    time_id = d['time_bcd']

    noise1 = create_netCDF_var(ncOut, "NOISE1", "i1", "noise amplitude beam 1", "counts", ("TIME",))
    noise2 = create_netCDF_var(ncOut, "NOISE2", "i1", "noise amplitude beam 2", "counts", ("TIME",))
    noise3 = create_netCDF_var(ncOut, "NOISE3", "i1", "noise amplitude beam 3", "counts", ("TIME",))

    corr1 = create_netCDF_var(ncOut, "CORR1", "i1", "noise correlation beam 1", "counts", ("TIME",))
    corr2 = create_netCDF_var(ncOut, "CORR2", "i1", "noise correlation beam 1", "counts", ("TIME",))
    corr3 = create_netCDF_var(ncOut, "CORR3", "i1", "noise correlation beam 1", "counts", ("TIME",))

    var_list = []
    var_list.append((noise1, d['noise1'], 1, np.zeros([number_samples], dtype=np.int8)))
    var_list.append((noise2, d['noise2'], 1, np.zeros([number_samples], dtype=np.int8)))
    var_list.append((noise3, d['noise3'], 1, np.zeros([number_samples], dtype=np.int8)))

    var_list.append((corr1, d['corr1'], 1, np.zeros([number_samples], dtype=np.int8)))
    var_list.append((corr2, d['corr2'], 1, np.zeros([number_samples], dtype=np.int8)))
    var_list.append((corr3, d['corr3'], 1, np.zeros([number_samples], dtype=np.int8)))

    ncOut.createDimension("BURST", number_data_samples)

    vel1 = create_netCDF_var(ncOut, "VEL_B1", "f4", "velocity beam 1", "m/s", ("TIME", "BURST"))
    vel2 = create_netCDF_var(ncOut, "VEL_B2", "f4", "velocity beam 2", "m/s", ("TIME", "BURST"))
    vel3 = create_netCDF_var(ncOut, "VEL_B3", "f4", "velocity beam 3", "m/s", ("TIME", "BURST"))

    amp1 = create_netCDF_var(ncOut, "AMP_B1", "f4", "amplitude beam 1", "1", ("TIME", "BURST"))
    amp2 = create_netCDF_var(ncOut, "AMP_B2", "f4", "amplitude beam 2", "1", ("TIME", "BURST"))
    amp3 = create_netCDF_var(ncOut, "AMP_B3", "f4", "amplitude beam 3", "1", ("TIME", "BURST"))

    corr1 = create_netCDF_var(ncOut, "CORR_B1", "f4", "correlation beam 1", "1", ("TIME", "BURST"))
    corr2 = create_netCDF_var(ncOut, "CORR_B2", "f4", "correlation beam 2", "1", ("TIME", "BURST"))
    corr3 = create_netCDF_var(ncOut, "CORR_B3", "f4", "correlation beam 3", "1", ("TIME", "BURST"))

    pres = create_netCDF_var(ncOut, "PRES", "f4", "pres", "dbar", ("TIME", "BURST"))
    ana1 = create_netCDF_var(ncOut, "ANALOG1", "f4", "pres", "dbar", ("TIME", "BURST"))
    ana2 = create_netCDF_var(ncOut, "ANALOG2", "f4", "pres", "dbar", ("TIME", "BURST"))

    cache_samples = 1000
    cache_sample = 0

    # deal with IMU packets
    has_IMU = False
    if packet_id['Vector With IMU'] in pkt_pos:
        has_IMU = True

        ncOut.createDimension("vector", 3)
        ncOut.createDimension("matrix", 9)

        accel = create_netCDF_var(ncOut, "ACCEL", "f4", "acceleration", "m/s^2", ("TIME", "BURST", "vector"))
        ang_rate = create_netCDF_var(ncOut, "ANG_RATE", "f4", "angular rate", "radians/s", ("TIME", "BURST", "vector"))
        mag = create_netCDF_var(ncOut, "MAG", "f4", "magnetic", "gauss", ("TIME", "BURST", "vector"))

        vid_unpack = packet_decoder[packet_id['Vector With IMU']]['unpack']
        vid_len = pkt_len[packet_id['Vector With IMU']] * 2 - 4

        vid_keys = packet_decoder[packet_id['Vector With IMU']]['keys']

        binary_file.seek(pkt_pos[packet_id['Vector With IMU']][0])
        vid_packet_data = binary_file.read(2)
        vid_packetDecode = struct.unpack("<BB", vid_packet_data)
        AHRS_id = vid_packetDecode[1]
        print('AHRS_id', hex(AHRS_id))

        has_orient = False
        if AHRS_id == 0xCC:
            has_orient = True
            orient = create_netCDF_var(ncOut, "ORIENTATION", "f4", "orientation matrix", "1", ("TIME", "BURST", "matrix"))

        if AHRS_id == 0xD2:
            vid_keys = ['EnsCnt', 'AHRSid', 'accelX', 'accelY', 'accelZ', 'angRateX', 'angRateY', 'angRateZ', 'MagX', 'MagY', 'MagZ', 'timer', 'IMUchSum', 'checksum']
            vid_unpack = "<BB9fIHH"

        vid_d = dict(zip(vid_keys, range(len(vid_keys))))

    pres_array = np.empty([number_samples, number_data_samples], dtype=np.float32)
    ana1_array = np.empty([number_samples, number_data_samples], dtype=np.float32)
    ana2_array = np.empty([number_samples, number_data_samples], dtype=np.float32)

    pres_array[:] = np.NaN
    ana1_array[:] = np.NaN
    ana2_array[:] = np.NaN

    time_id = d['time_bcd']
    vvd_keys = packet_decoder[packet_id['Vector Velocity Data']]['keys']
    vvd_d = dict(zip(vvd_keys, range(len(vvd_keys))))

    pres_msb_id = vvd_d['presMSB']
    pres_lsb_id = vvd_d['presLSW']
    ana1_id = vvd_d['AnaIn1']
    ana2_msb_id = vvd_d['AnaIn2MSB']
    ana2_lsb_id = vvd_d['AnaIn2LSB']

    vec_list = []
    vec_list.append({'netCDF': vel1, 'ids': [vvd_d['vel1']], 'scale': 0.001, 'array': np.empty([cache_samples, number_data_samples], dtype=np.float32)})
    vec_list.append({'netCDF': vel2, 'ids': [vvd_d['vel2']], 'scale': 0.001, 'array': np.empty([cache_samples, number_data_samples], dtype=np.float32)})
    vec_list.append({'netCDF': vel3, 'ids': [vvd_d['vel3']], 'scale': 0.001, 'array': np.empty([cache_samples, number_data_samples], dtype=np.float32)})

    vec_list.append({'netCDF': amp1, 'ids': [vvd_d['amp1']], 'scale': 1, 'array': np.empty([cache_samples, number_data_samples], dtype=np.int8)})
    vec_list.append({'netCDF': amp2, 'ids': [vvd_d['amp2']], 'scale': 1, 'array': np.empty([cache_samples, number_data_samples], dtype=np.int8)})
    vec_list.append({'netCDF': amp3, 'ids': [vvd_d['amp3']], 'scale': 1, 'array': np.empty([cache_samples, number_data_samples], dtype=np.int8)})

    vec_list.append({'netCDF': corr1, 'ids': [vvd_d['corr1']], 'scale': 1, 'array': np.empty([cache_samples, number_data_samples], dtype=np.int8)})
    vec_list.append({'netCDF': corr2, 'ids': [vvd_d['corr2']], 'scale': 1, 'array': np.empty([cache_samples, number_data_samples], dtype=np.int8)})
    vec_list.append({'netCDF': corr3, 'ids': [vvd_d['corr3']], 'scale': 1, 'array': np.empty([cache_samples, number_data_samples], dtype=np.int8)})

    for v in vec_list:
        if v['array'].dtype == 'float32':
            v['array'][:] = np.NaN

    if has_IMU:
        imu_vec_list = []
        imu_vec_list.append({'netCDF': accel, 'ids': [vid_d['accelX'], vid_d['accelY'], vid_d['accelZ']], 'scale': 9.81, 'array': np.empty([cache_samples, number_data_samples, 3], dtype=np.float32)})
        imu_vec_list.append(
            {'netCDF': ang_rate, 'ids': [vid_d['angRateX'], vid_d['angRateY'], vid_d['angRateZ']], 'scale': 1.0,
             'array': np.empty([cache_samples, number_data_samples, 3], dtype=np.float32)})

        imu_vec_list.append(
            {'netCDF': mag, 'ids': [vid_d['angRateX'], vid_d['MagX'], vid_d['MagX']], 'scale': 1.0,
             'array': np.empty([cache_samples, number_data_samples, 3], dtype=np.float32)})

        if has_orient:
            imu_vec_list.append(
                {'netCDF': orient,
                 'ids': [vid_d['M11'], vid_d['M12'], vid_d['M13'], vid_d['M21'], vid_d['M22'], vid_d['M23'], vid_d['M31'], vid_d['M32'], vid_d['M33']],
                 'scale': 1.0,
                 'array': np.empty([cache_samples, number_data_samples, 9], dtype=np.float32)})

        # fill all imu vector data with NaNs
        for v in imu_vec_list:
            if v['array'].dtype == 'float32':
                v['array'][:] = np.NaN
    sample = 0
    cache_sample_start = 0

    vvd_pkt_n = 0
    vid_pkt_n = 0

    while sample < len(pkt_pos_list):
        binary_file.seek(pkt_pos_list[sample])
        packet_data = binary_file.read(pkt_len[pkt_id] * 2 - 4)
        packetDecode = struct.unpack(unpack, packet_data)

        dt = bcd_time_to_datetime(packetDecode[time_id])

        times_array[sample] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

        for v in var_list:
            v[3][sample] = packetDecode[v[1]] / v[2]

        # loop through 'Vector Velocity Data' and 'Vector with IMU' data for this system data
        vvd_pos = pkt_pos[packet_id['Vector Velocity Data']]
        if (sample + 1) < len(pkt_pos_list):
            last_pos = pkt_pos_list[sample + 1]
        else:
            last_pos = vvd_pos[-1]

        vvd_unpack = packet_decoder[packet_id['Vector Velocity Data']]['unpack']
        vvd_len = pkt_len[packet_id['Vector Velocity Data']] * 2 - 4
        vvd_sample = 0
        # skip to the next position after this 'Vector Velocity Data Header'
        while vvd_pos[vvd_pkt_n] < pkt_pos_list[sample]:
            print('skipping packet', vvd_pkt_n)
            vvd_pkt_n += 1

        while vvd_sample < number_data_samples and vvd_pkt_n < len(vvd_pos) and vvd_pos[vvd_pkt_n] < last_pos:
            binary_file.seek(vvd_pos[vvd_pkt_n])
            packet_data = binary_file.read(vvd_len)
            packetDecode = struct.unpack(vvd_unpack, packet_data)

            # print('vector velocity sample', sample, vvd_sample, packetDecode[1], vvd_sample)

            pres_array[sample, vvd_sample] = ((packetDecode[pres_msb_id] * 65536) + packetDecode[pres_lsb_id]) * 0.001
            ana1_array[sample, vvd_sample] = packetDecode[ana1_id]
            ana2_array[sample, vvd_sample] = (packetDecode[ana2_msb_id] * 256) + packetDecode[ana2_lsb_id]

            for v in vec_list:
                v['array'][cache_sample, vvd_sample] = packetDecode[v['ids'][0]] * v['scale']

            vvd_sample += 1
            vvd_pkt_n += 1

        if has_IMU:
            vid_pos = pkt_pos[packet_id['Vector With IMU']]
            if (sample + 1) < len(pkt_pos_list):
                last_pos = pkt_pos_list[sample + 1]
            else:
                last_pos = vvd_pos[-1]

            vid_len = pkt_len[packet_id['Vector With IMU']] * 2 - 4
            vid_sample = 0
            # skip to the next position after this 'Vector Velocity Data Header'
            while vid_pos[vid_pkt_n] < pkt_pos_list[sample]:
                print('skipping packet', vid_pkt_n)
                vid_pkt_n += 1

            while vid_sample < number_data_samples and vid_pkt_n < len(vid_pos) and vid_pos[vid_pkt_n] < last_pos:
                binary_file.seek(vid_pos[vid_pkt_n])
                packet_data = binary_file.read(vid_len)
                packetDecode = struct.unpack(vid_unpack, packet_data)

                # print('vector IMU sample', sample, vid_sample, packetDecode[1], vid_sample)

                for v in imu_vec_list:
                    for i in range(len(v['ids'])):
                        v['array'][cache_sample, vid_sample, i] = packetDecode[v['ids'][i]] * v['scale']

                vid_sample += 1
                vid_pkt_n += 1

        cache_sample += 1
        if cache_sample >= cache_samples:
            print('write cache samples', sample, cache_sample_start, cache_sample, 'pres', pres_array[cache_sample_start, 0])
            for v in vec_list:
                v['netCDF'][cache_sample_start:cache_sample_start + cache_sample] = v['array']
                if v['array'].dtype == 'float32':
                    v['array'][:] = np.NaN

            for v in imu_vec_list:
                v['netCDF'][cache_sample_start:cache_sample_start + cache_sample] = v['array']
                if v['array'].dtype == 'float32':
                    v['array'][:] = np.NaN

            cache_sample = 0
            cache_sample_start = sample

        sample += 1

    print('read-data took', datetime.datetime.now() - time_start)

    # save data to netCDF file variable
    ncTimesOut[:] = times_array
    for v in var_list:
        v[0][:] = v[3]

    pres[:] = pres_array
    ana1[:] = ana1_array
    ana2[:] = ana2_array

    print('post write cache samples', cache_sample)

    for v in vec_list:
        v['netCDF'][cache_sample_start:cache_sample_start + cache_sample, :] = v['array'][0:cache_sample, :]

    if has_IMU:
        for v in imu_vec_list:
            v['netCDF'][cache_sample_start:cache_sample_start + cache_sample, :, :] = v['array'][0:cache_sample, :, :]

    return ncTimesOut


def build_aquapro_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack, number_bins):
    time_start = datetime.datetime.now()

    number_samples = len(pkt_pos_list)
    tDim = ncOut.createDimension("TIME", number_samples)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    times_array = np.zeros([number_samples])

    pres_array = np.empty([number_samples], dtype=np.float32)
    pres_array[:] = np.NaN

    ncOut.createDimension("CELL", number_bins)

    # TODO: check head_config for magnetometer, tilt and pressure sensor
    head = create_netCDF_var(ncOut, "HEADING_MAG", "f4", "heading magnetic", "degrees", ("TIME",))
    pitch = create_netCDF_var(ncOut, "PITCH", "f4", "pitch", "degrees", ("TIME",))
    roll = create_netCDF_var(ncOut, "ROLL", "f4", "roll", "degrees", ("TIME",))
    pres = create_netCDF_var(ncOut, "PRES", "f4", "pres", "dbar", ("TIME",))
    bat = create_netCDF_var(ncOut, "BATT", "f4", "battery voltage", "V", ("TIME",))
    itemp = create_netCDF_var(ncOut, "ITEMP", "f4", "instrument temperature", "degrees_Celsius", ("TIME",))
    sspeed = create_netCDF_var(ncOut, "SSPEED", "f4", "sound speed or analog input 2", "m/s", ("TIME",))
    analog1 = create_netCDF_var(ncOut, "ANALOG1", "f4", "analog input 1", "counts", ("TIME",))

    if coord_system == 2:
        vel1 = create_netCDF_var(ncOut, "VEL_B1", "f4", "velocity beam 1", "m/s", ("TIME", "CELL"))
        vel2 = create_netCDF_var(ncOut, "VEL_B2", "f4", "velocity beam 2", "m/s", ("TIME", "CELL"))
        vel3 = create_netCDF_var(ncOut, "VEL_B3", "f4", "velocity beam 3", "m/s", ("TIME", "CELL"))
    elif coord_system == 1:
        vel1 = create_netCDF_var(ncOut, "VEL_X", "f4", "velocity X", "m/s", ("TIME", "CELL"))
        vel2 = create_netCDF_var(ncOut, "VEL_Y", "f4", "velocity Y", "m/s", ("TIME", "CELL"))
        vel3 = create_netCDF_var(ncOut, "VEL_Z", "f4", "velocity Z", "m/s", ("TIME", "CELL"))
    else:
        vel1 = create_netCDF_var(ncOut, "ECUR_MAG", "f4", "current east", "m/s", ("TIME", "CELL"))
        vel2 = create_netCDF_var(ncOut, "NCUR_MAG", "f4", "current north", "m/s", ("TIME", "CELL"))
        vel3 = create_netCDF_var(ncOut, "UCUR", "f4", "current up", "m/s", ("TIME", "CELL"))

    absci1 = create_netCDF_var(ncOut, "ABSIC1", "i2", "amplitude beam 1", "counts", ("TIME", "CELL"))
    absci2 = create_netCDF_var(ncOut, "ABSIC2", "i2", "amplitude beam 2", "counts", ("TIME", "CELL"))
    absci3 = create_netCDF_var(ncOut, "ABSIC3", "i2", "amplitude beam 3", "counts", ("TIME", "CELL"))

    sample = 0
    time_id = d['time_bcd']
    pred_msb_id = d['presMSB']
    pred_lsb_id = d['presLSW']

    imu_vec_list = []
    imu_vec_list.append((vel1, d['vel_b1[0]'], 1000, np.empty([number_samples, number_bins], dtype=np.float32)))
    imu_vec_list.append((vel2, d['vel_b2[0]'], 1000, np.empty([number_samples, number_bins], dtype=np.float32)))
    imu_vec_list.append((vel3, d['vel_b3[0]'], 1000, np.empty([number_samples, number_bins], dtype=np.float32)))

    var_list = []
    var_list.append((head, d['head'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((pitch, d['pitch'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((roll, d['roll'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((bat, d['battery'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((itemp, d['temp'], 100, np.empty([number_samples], dtype=np.float32)))
    var_list.append((sspeed, d['soundSpd_Anain2'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((analog1, d['AnaIn1'], 1, np.empty([number_samples], dtype=np.float32)))

    imu_vec_list.append((absci1, d['amp1[0]'], 1, np.zeros([number_samples, number_bins], dtype=int)))
    imu_vec_list.append((absci2, d['amp2[0]'], 1, np.zeros([number_samples, number_bins], dtype=int)))
    imu_vec_list.append((absci3, d['amp3[0]'], 1, np.zeros([number_samples, number_bins], dtype=int)))

    # NaN fill any float variables
    for v in var_list:
        if v[3].dtype == 'float32':
            v[3][:] = np.NaN
    for v in imu_vec_list:
        if v[3].dtype == 'float32':
            v[3][:] = np.NaN

    while sample < len(pkt_pos_list):
        binary_file.seek(pkt_pos_list[sample])
        packet_data = binary_file.read(pkt_len[pkt_id] * 2 - 4)
        packetDecode = struct.unpack(unpack, packet_data)

        dt = bcd_time_to_datetime(packetDecode[time_id])
        times_array[sample] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

        for v in var_list:
            v[3][sample] = packetDecode[v[1]] / v[2]
        for v in imu_vec_list:
            for i in range(number_bins):
                v[3][sample, i] = packetDecode[v[1] + i] / v[2]

        pres_array[sample] = ((packetDecode[pred_msb_id] * 65536) + packetDecode[pred_lsb_id]) * 0.001

        sample += 1

    print('read-data took', datetime.datetime.now() - time_start)

    ncTimesOut[:] = times_array
    for v in var_list:
        v[0][:] = v[3]
    for v in imu_vec_list:
        v[0][:] = v[3]
    pres[:] = pres_array

    return ncTimesOut


def build_wave_data(ncOut, binary_file, pkt_pos, pkt_pos_list, pkt_len, pkt_id, d, unpack, wave_cells):
    time_start = datetime.datetime.now()

    wave_samples = len(pkt_pos_list)

    print('wave records, cells', wave_samples, wave_cells)

    ncOut.createDimension("WAVE_CELL", wave_cells)

    tDim = ncOut.createDimension("WAVE_TIME", wave_samples)
    nc_wave_times = ncOut.createVariable("WAVE_TIME", "d", ("WAVE_TIME",), zlib=True)
    nc_wave_times.long_name = "wave time"
    nc_wave_times.units = "days since 1950-01-01 00:00:00 UTC"
    nc_wave_times.calendar = "gregorian"
    nc_wave_times.axis = "T"
    times_array = np.zeros([wave_samples])

    time_id = d['time_bcd']

    # TODO: check head_config for magnetometer, tilt and pressure sensor
    head = create_netCDF_var(ncOut, "WAVE_HEADING_MAG", "f4", "heading magnetic", "degrees", ("WAVE_TIME",))
    pitch = create_netCDF_var(ncOut, "WAVE_PITCH", "f4", "pitch", "degrees", ("WAVE_TIME",))
    roll = create_netCDF_var(ncOut, "WAVE_ROLL", "f4", "roll", "degrees", ("WAVE_TIME",))
    bat = create_netCDF_var(ncOut, "WAVE_BATT", "f4", "battery voltage", "V", ("WAVE_TIME",))
    itemp = create_netCDF_var(ncOut, "WAVE_ITEMP", "f4", "instrument temperature", "degrees_Celsius", ("WAVE_TIME",))

    wave_pres = create_netCDF_var(ncOut, "WAVE_PRES", "f4", "pres", "dbar", ("WAVE_TIME", "WAVE_CELL"))

    var_list = []
    var_list.append({'netCDF': head, 'ids': [d['heading']], 'scale': 0.1, 'array': np.empty([wave_samples], dtype=np.float32)})
    var_list.append({'netCDF': pitch, 'ids': [d['pitch']], 'scale': 0.1,  'array': np.empty([wave_samples], dtype=np.float32)})
    var_list.append({'netCDF': roll, 'ids': [d['roll']], 'scale': 0.1, 'array': np.empty([wave_samples], dtype=np.float32)})
    var_list.append({'netCDF': bat, 'ids': [d['battery']], 'scale': 0.1, 'array': np.empty([wave_samples], dtype=np.float32)})
    var_list.append({'netCDF': itemp, 'ids': [d['temperature']], 'scale': 0.01,'array': np.empty([wave_samples], dtype=np.float32)})

    # vector data
    w_vel1 = create_netCDF_var(ncOut, "WAVE_VELOCITY1", "f4", "wave sample velocity", "m/s", ("WAVE_TIME", "WAVE_CELL"))
    w_vel2 = create_netCDF_var(ncOut, "WAVE_VELOCITY2", "f4", "wave sample velocity", "m/s", ("WAVE_TIME", "WAVE_CELL"))
    w_vel3 = create_netCDF_var(ncOut, "WAVE_VELOCITY3", "f4", "wave sample velocity", "m/s", ("WAVE_TIME", "WAVE_CELL"))

    wave_keys = packet_decoder[packet_id['AWAC Wave Data']]['keys']
    wave_d = dict(zip(wave_keys, range(len(wave_keys))))

    vec_list = []
    vec_list.append({'netCDF': wave_pres, 'ids': [wave_d['pressure']], 'scale': 0.001, 'array': np.empty([wave_samples, wave_cells], dtype=np.float32)})
    vec_list.append({'netCDF': w_vel1, 'ids': [wave_d['vel1']], 'scale': 0.001, 'array': np.empty([wave_samples, wave_cells], dtype=np.float32)})
    vec_list.append({'netCDF': w_vel2, 'ids': [wave_d['vel2']], 'scale': 0.001, 'array': np.empty([wave_samples, wave_cells], dtype=np.float32)})
    vec_list.append({'netCDF': w_vel3, 'ids': [wave_d['vel3']], 'scale': 0.001, 'array': np.empty([wave_samples, wave_cells], dtype=np.float32)})

    for v in var_list:
        if v['array'].dtype == 'float32':
            v['array'][:] = np.NaN
    for v in vec_list:
        if v['array'].dtype == 'float32':
            v['array'][:] = np.NaN

    # process all the wave packets
    wave_pkt_n = 0
    sample = 0
    while sample < wave_samples:
        binary_file.seek(pkt_pos_list[sample])
        packet_data = binary_file.read(pkt_len[pkt_id] * 2 - 4)
        packetDecode = struct.unpack(unpack, packet_data)
        # print(dict(zip(keys, packetDecode)))

        dt = bcd_time_to_datetime(packetDecode[time_id])
        times_array[sample] = date2num(dt, calendar='gregorian',
                                       units="days since 1950-01-01 00:00:00 UTC")
        for v in var_list:
            v['array'][sample] = packetDecode[v['ids'][0]] * v['scale']

        # loop through 'AWAC Wave Data'
        wave_pos = pkt_pos[packet_id['AWAC Wave Data']]
        if (sample + 1) < len(pkt_pos_list):
            last_pos = pkt_pos_list[sample + 1]
        else:
            last_pos = wave_pos[-1]

        wave_unpack = packet_decoder[packet_id['AWAC Wave Data']]['unpack']
        wave_len = pkt_len[packet_id['AWAC Wave Data']] * 2 - 4
        wave_sample = 0
        # skip to the next position after this wave data header
        while wave_pos[wave_pkt_n] < pkt_pos_list[sample]:
            print('skipping packet', wave_pkt_n)
            wave_pkt_n += 1
        # process all save samples, for all wave_calls, till we get past the next data header, or we run out of same samples
        while wave_sample < wave_cells and wave_pkt_n < len(wave_pos) and wave_pos[
            wave_pkt_n] < last_pos:
            binary_file.seek(wave_pos[wave_pkt_n])
            packet_data = binary_file.read(wave_len)
            packetDecode = struct.unpack(wave_unpack, packet_data)

            # print('wave sample', sample, wave_sample)

            for v in vec_list:
                v['array'][sample, wave_sample] = packetDecode[v['ids'][0]] * v['scale']

            wave_sample += 1
            wave_pkt_n += 1

        sample += 1

    print('read-data took', datetime.datetime.now() - time_start)

    nc_wave_times[:] = times_array
    for v in var_list:
        v['netCDF'][:] = v['array']
    for v in vec_list:
        v['netCDF'][:] = v['array']

    return


def build_aquadopp_diag_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack):
    time_start = datetime.datetime.now()

    number_samples = len(pkt_pos_list)

    tDim = ncOut.createDimension("TIME_DIAG", number_samples)
    ncTimesOut = ncOut.createVariable("TIME_DIAG", "d", ("TIME_DIAG",), zlib=True)
    ncTimesOut.long_name = "time diagnostics"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"

    times_array = np.zeros([number_samples])

    pres_array = np.empty([number_samples], dtype=np.float32)
    pres_array[:] = np.NaN

    if coord_system == 2:
        vel1 = create_netCDF_var(ncOut, "VEL_B1_DIAG", "f4", "velocity beam 1", "m/s", ("TIME_DIAG",))
        vel2 = create_netCDF_var(ncOut, "VEL_B2_DIAG", "f4", "velocity beam 2", "m/s", ("TIME_DIAG",))
        vel3 = create_netCDF_var(ncOut, "VEL_B3_DIAG", "f4", "velocity beam 3", "m/s", ("TIME_DIAG",))
    elif coord_system == 1:
        vel1 = create_netCDF_var(ncOut, "VEL_X_DIAG", "f4", "velocity X", "m/s", ("TIME_DIAG",))
        vel2 = create_netCDF_var(ncOut, "VEL_Y_DIAG", "f4", "velocity Y", "m/s", ("TIME_DIAG",))
        vel3 = create_netCDF_var(ncOut, "VEL_Z_DIAG", "f4", "velocity Z", "m/s", ("TIME_DIAG",))
    else:
        vel1 = create_netCDF_var(ncOut, "ECUR_MAG_DIAG", "f4", "current east", "m/s", ("TIME_DIAG",))
        vel2 = create_netCDF_var(ncOut, "NCUR_MAG_DIAG", "f4", "current north", "m/s", ("TIME_DIAG",))
        vel3 = create_netCDF_var(ncOut, "UCUR_DIAG", "f4", "current up", "m/s", ("TIME_DIAG",))

    head = create_netCDF_var(ncOut, "HEADING_MAG_DIAG", "f4", "heading magnetic", "degrees", ("TIME_DIAG",))
    pitch = create_netCDF_var(ncOut, "PITCH_DIAG", "f4", "pitch", "degrees", ("TIME_DIAG",))
    roll = create_netCDF_var(ncOut, "ROLL_DIAG", "f4", "roll", "degrees", ("TIME_DIAG",))
    pres = create_netCDF_var(ncOut, "PRES_DIAG", "f4", "pres", "dbar", ("TIME_DIAG",))
    bat = create_netCDF_var(ncOut, "BATT_DIAG", "f4", "battery voltage", "V", ("TIME_DIAG",))

    absci1 = create_netCDF_var(ncOut, "ABSIC1_DIAG", "i2", "amplitude beam 1", "counts", ("TIME_DIAG",))
    absci2 = create_netCDF_var(ncOut, "ABSIC2_DIAG", "i2", "amplitude beam 2", "counts", ("TIME_DIAG",))
    absci3 = create_netCDF_var(ncOut, "ABSIC3_DIAG", "i2", "amplitude beam 3", "counts", ("TIME_DIAG",))

    error = create_netCDF_var(ncOut, "ERROR_DIAG", "i2", "error code", "1", ("TIME_DIAG",))
    status = create_netCDF_var(ncOut, "STATUS_DIAG", "i2", "status code", "1", ("TIME_DIAG",))

    sample = 0
    time_id = d['time_bcd']
    pred_msb_id = d['presMSB']
    pred_lsb_id = d['presLSW']

    var_list = []
    var_list.append((vel1, d['vel_b1'], 1000, np.empty([number_samples], dtype=np.float32)))
    var_list.append((vel2, d['vel_b2'], 1000, np.empty([number_samples], dtype=np.float32)))
    var_list.append((vel3, d['vel_b3'], 1000, np.empty([number_samples], dtype=np.float32)))
    var_list.append((head, d['head'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((pitch, d['pitch'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((roll, d['roll'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((bat, d['battery'], 10, np.empty([number_samples], dtype=np.float32)))
    var_list.append((absci1, d['amp1'], 1, np.zeros([number_samples], dtype=int)))
    var_list.append((absci2, d['amp2'], 1, np.zeros([number_samples], dtype=int)))
    var_list.append((absci3, d['amp3'], 1, np.zeros([number_samples], dtype=int)))

    var_list.append((error, d['error'], 1, np.zeros([number_samples], dtype=int)))
    var_list.append((status, d['status'], 1, np.zeros([number_samples], dtype=int)))

    # NaN fill any float variables
    for v in var_list:
        if v[3].dtype == 'float32':
            v[3][:] = np.NaN

    while sample < len(pkt_pos_list):
        binary_file.seek(pkt_pos_list[sample])
        packet_data = binary_file.read(pkt_len[pkt_id] * 2 - 4)
        packetDecode = struct.unpack(unpack, packet_data)

        dt = bcd_time_to_datetime(packetDecode[time_id])
        times_array[sample] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")
        for v in var_list:
            v[3][sample] = packetDecode[v[1]] / v[2]
        pres_array[sample] = ((packetDecode[pred_msb_id] * 65536) + packetDecode[pred_lsb_id]) * 0.001

        sample += 1

    print('read-data took', datetime.datetime.now() - time_start)

    ncTimesOut[:] = times_array
    for v in var_list:
        v[0][:] = v[3]
    pres[:] = pres_array

    return


def parse_file(files, include_diag):

    output_files = []
    for filepath in files:
        checksum_errors = 0
        no_sync = 0
        pkts_read = 0
        file_size = os.path.getsize(filepath)
        time_start = datetime.datetime.now()

        # sample_count = 0
        #
        # first_time = None
        #
        # create the netCDF file
        outputName = filepath + ".nc"

        print("output file : %s" % outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4')
        #
        # # add time variable
        #
        # #     TIME:axis = "T";
        # #     TIME:calendar = "gregorian";
        # #     TIME:long_name = "time";
        # #     TIME:units = "days since 1950-01-01 00:00:00 UTC";
        #
        # tDim = ncOut.createDimension("TIME")
        # ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
        # ncTimesOut.long_name = "time"
        # ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
        # ncTimesOut.calendar = "gregorian"
        # ncTimesOut.axis = "T"
        #
        # # instrument types
        # aquadopp = False
        # aquadoppHR = False
        # aquapro = False
        # vector = False
        # vectorWithImu = False
        # awac_wave_data = False
        #
        # wave_sample_time = -1

        pkt_count = {}
        pkt_len = {}
        pkt_pos = {}

        attribute_list = []

        with open(filepath, "rb") as binary_file:
            current_pos_pkt_start = binary_file.tell()
            data = binary_file.read(2)
            #print('tell', bad_ck_pos)

            while data:
                # print("sync : ", data[0])

                if data[0] == 0xa5:  # sync
                    checksum = 0xb58c
                    id = data[1]
                    #print('id_data, id', data, id)
                    checksum += 0xa5 + (id << 8)
                    #print("id = ", id)
                    if id == 16: # Vector Velocity Data is fixed size
                        pkt_size = 13 # pkt is 24 bytes, 12 words, less the extra 2 we need as we read packet_size*2-4
                    else:
                        size_data = binary_file.read(2)
                        pkt_size = (size_data[1] << 8) + size_data[0]
                        checksum += pkt_size

                    packet_data_start = binary_file.tell()
                    packet_data = binary_file.read(pkt_size*2 - 4)  # size in words, less the 4 we already read
                    if len(packet_data) != (pkt_size*2 - 4):  # did not read enough
                        break

                    #print("id =", id, "len =", len(packet_data), pkt_size*2)

                    # calculate the checksum
                    for pkt_id in range(0, len(packet_data)-2, 2):
                        checksum += (packet_data[pkt_id+1] << 8) + packet_data[pkt_id]

                    # check check sum
                    if checksum & 0xffff == (packet_data[-1] << 8) + packet_data[-2]:
                        pkts_read += 1
                        if (pkts_read % 100000) == 0:
                            print('packets', pkts_read, id, 'file remaining', file_size - binary_file.tell())
                            for pkt_id in pkt_count:
                                print('id=', pkt_id, "'"+packet_decoder[pkt_id]['name']+"'", 'count=', pkt_count[pkt_id])

                        if id in pkt_count:
                            pkt_count[id] += 1
                            pkt_pos[id].append(packet_data_start)
                        else:
                            pkt_count[id] = 1
                            pkt_pos[id] = array("L", [packet_data_start])
                            pkt_len[id] = pkt_size
                    else:
                        print("check sum error ", current_pos_pkt_start, checksum & 0xffff, (packet_data[-1] << 8) + packet_data[-2])
                        checksum_errors += 1
                        if checksum_errors > 10:
                            print("too many errors, maybe not a nortek file")
                            break
                        binary_file.seek(current_pos_pkt_start + 1, os.SEEK_SET)  # seek back to before packet

                else:
                    binary_file.seek(current_pos_pkt_start+1, os.SEEK_SET)  # seek back to before packet
                    no_sync += 1
                    #print('no sync', data[0], bad_ck_pos)
                    #if no_sync > 1000:
                    #    print("no sync found in first 1000 bytes, maybe not a nortek file")
                    #    break

                current_pos_pkt_start = binary_file.tell()
                data = binary_file.read(2)
                #print('tell', bad_ck_pos)

            #
            # print('samples', sample_count)
            #
            print()
            print('read took', datetime.datetime.now() - time_start)

            time_start = datetime.datetime.now()

            print('total packets', pkts_read, id)
            number_beams = 0

            # look though all packet types, decode each type to arrays and save to netCDF variables
            for pkt_id in pkt_count:
                pkt_pos_list = pkt_pos[pkt_id]
                print('id=', pkt_id, "'"+packet_decoder[pkt_id]['name']+"'", 'count=', pkt_count[pkt_id], len(pkt_pos_list), pkt_pos_list[0], 'len=', pkt_len[pkt_id])
                unpack = packet_decoder[pkt_id]['unpack']
                keys = packet_decoder[pkt_id]['keys']

                binary_file.seek(pkt_pos_list[0])
                packet_data = binary_file.read(pkt_len[pkt_id]*2-4)

                # deal with the variable length packets, format the decoder
                if packet_id['Aquadopp Profiler Velocity Data'] == pkt_id:
                    #print(unpack.format(number_bins * number_beams))
                    unpack = unpack.format(number_bins * number_beams)
                    keys = fill_var_keys(number_bins, keys)

                if packet_id['HR Aquadopp Profiler Velocity Data'] == pkt_id:
                    #print(unpack.format(number_bins * number_beams))
                    (beams, cells) = struct.unpack("<BB", packet_data[34-4:36-4])
                    number_bins = beams*cells
                    #print('beams, cells', beams, cells)
                    unpack = unpack.format(cells * beams)
                    keys = fill_var_keys(number_bins, keys)

                if packet_id['Vector and Vectrino Probe Check Data'] == pkt_id:
                    (number_bins,) = struct.unpack("<H", packet_data[0:2])
                    #print("probe check", number_bins)
                    unpack = unpack.format(number_bins)
                    keys = fill_var_keys(number_bins, keys)

                if packet_id['Vector With IMU'] == pkt_id:
                    (AHRS_id,) = struct.unpack("<B", packet_data[1:2])
                    #print("probe check", number_samples)
                    if AHRS_id == 0xD2:
                        keys = ['EnsCnt', 'AHRSid', 'accelX', 'accelY', 'accelZ', 'angRateX', 'angRateY', 'angRateZ', 'MagX', 'MagY', 'MagZ', 'timer', 'IMUchSum', 'checksum']
                        unpack = "<BB9fIHH"

                #print('unpack', unpack)

                d = dict(zip(keys, range(len(keys))))
                #print('dict', d)

                # decode the packet
                #print("packet size", packet_decoder[id]['name'], len(packet_data), unpack, 'read', pkt_size)
                packetDecode = struct.unpack(unpack, packet_data)
                # for i in range(len(keys)):
                #     print(keys[i], packetDecode[i])

                if packet_id['Hardware Configuration'] == pkt_id:
                    sn = packetDecode[d['serial']]
                    instrument_serialnumber = clean_serial(sn)
                    print('instrument serial number', instrument_serialnumber)
                    record_size = packetDecode[d['RecSize']]
                    system_frequency = packetDecode[d['frequency']]
                    nortek_firmware_version= packetDecode[d['FWversion']]
                    nortek_hardware_version= packetDecode[d['HWrevision']]

                if packet_id['Head Configuration'] == pkt_id:
                    sn = packetDecode[d['head_serial']]
                    instrument_head_serialnumber = clean_serial(sn)
                    print('instrument head serial number', instrument_head_serialnumber)
                    sd_raw = packetDecode[d['system']]
                    sd = struct.unpack('88h', sd_raw)
                    print('system data', sd)
                    head_frequency = packetDecode[d['head_frequency']]

                    head_config_reg = packetDecode[d['head_config']]
                    head_config = "pressure:yes," if (head_config_reg & 0x01) != 0 else "pressure:no,"
                    head_config += "magnetometer:yes," if (head_config_reg & 0x02) != 0 else "magnetometer:no,"
                    head_config += "tilt:yes," if (head_config_reg & 0x04) != 0 else "tilt:no,"
                    head_config += "tilt:down" if (head_config_reg & 0x08) == 0 else "tilt:up"

                    print("head_config", head_config_reg, head_config)

                    attribute_list.append(('head_config', head_config))
                    attribute_list.append(('head_serial_number', instrument_head_serialnumber))
                    attribute_list.append(('head_system_transform_matrix', sd[4:13]))

                    number_beams = packetDecode[d['NBeam']]

                if packet_id['User Configuration'] == pkt_id:
                    coord_system = packetDecode[d['CoordSys']]
                    print("coord system", coord_systems[coord_system])

                    tim_ctrl_reg = packetDecode[d['TimCtrlReg']]
                    timing_controller_mode = "profile:continuous," if (tim_ctrl_reg & 0x02) != 0 else "profile:single,"
                    timing_controller_mode += "mode:continuous" if (tim_ctrl_reg & 0x04) != 0 else "mode:burst"

                    print("tim_ctrl_reg", tim_ctrl_reg, timing_controller_mode)

                    attribute_list.append(('timing_control_mode', timing_controller_mode))

                    number_bins = packetDecode[d['NBins']]

                if packet_id['Aquadopp Velocity Data'] == pkt_id:
                    # add global attributes
                    instrument_model = 'Aquadopp ' + si_format(head_frequency*1000, precision=0) + 'Hz'

                    ncTimesOut = build_aquadopp_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack)

                if include_diag:
                    if packet_id['Aquadopp Diagnostics Data'] == pkt_id:

                        build_aquadopp_diag_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack)

                if packet_id['Aquadopp Diagnostics Data Header'] == pkt_id:
                    diag_records = packetDecode[d['records']]
                    diag_cells = packetDecode[d['cell']]

                    print('diag header, records', diag_records, 'cells', diag_cells)

                if packet_id['Vector Velocity Data Header'] == pkt_id:
                    instrument_model = 'Vector ' + si_format(head_frequency*1000, precision=0) + 'Hz'
                    number_data_samples = packetDecode[d['NRecords']]

                    ncTimesOut = build_vector_velocity_data(ncOut, binary_file, pkt_pos, pkt_pos_list, pkt_len, pkt_id, d, unpack, number_data_samples)

                if packet_id['Vector System Data'] == pkt_id:
                    build_vector_system_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack)

                if packet_id['Vector Velocity Data'] == pkt_id:
                    velocity_data_samples = len(pkt_pos_list)

                if packet_id['HR Aquadopp Profiler Velocity Data'] == pkt_id:
                    # add global attributes
                    instrument_model = 'AquaProHR ' + si_format(head_frequency*1000, precision=0) + 'Hz'

                    ncTimesOut = build_aquaprohr_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack, beams, cells)

                if packet_id['Aquadopp Profiler Velocity Data'] == pkt_id:
                    # add global attributes
                    instrument_model = 'AquaPro ' + si_format(head_frequency*1000, precision=0) + 'Hz'

                    ncTimesOut = build_aquapro_data(ncOut, binary_file, pkt_pos_list, pkt_len, pkt_id, d, unpack, number_bins)

                if packet_id['AWAC wave Data Header'] == pkt_id:
                    wave_cells = packetDecode[d['NRecords']]

                    build_wave_data(ncOut, binary_file, pkt_pos, pkt_pos_list, pkt_len, pkt_id, d, unpack, wave_cells)

                # copy in all attribues from the list
                for att in packet_decode2netCDF:
                    if packet_decode2netCDF[att]["decode"] in keys:
                        attribute_list.append((packet_decode2netCDF[att]["attrib"], float(packetDecode[d[packet_decode2netCDF[att]["decode"]]])))

                        # print('packet', packet_decoder[id]['name'])

                        # include all settings as attributes in file
                        # if 'User Configuration' == packet_decoder[id]['name'] or 'Head Configuration' == packet_decoder[id]['name'] or 'Hardware Configuration' == packet_decoder[id]['name']:
                        #     for k in d:
                        #         print("dict ", k, " = " , d[k])
                        #         attribute_list.append(('nortek_' + packet_decoder[id]['name'].replace(" ", "_").lower() + '-' + k, str(d[k])))


        ncOut.instrument = 'Nortek ; ' + instrument_model
        ncOut.instrument_model = instrument_model
        ncOut.instrument_serial_number = instrument_serialnumber
        ncOut.instrument_head_serial_number = instrument_head_serialnumber
        ncOut.coord_system = coord_systems[coord_system]
        ncOut.timing_controller_mode = timing_controller_mode
        ncOut.head_config = head_config

        print()
        print('instrument', instrument_model)
        # add any attributes we collected from the file
        for pkt_pos_list in attribute_list:
            print('attribute: ', pkt_pos_list)
            ncOut.setncattr('instrument_setup_'+pkt_pos_list[0], pkt_pos_list[1])

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        ts_start = num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar)
        ts_end = num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar)
        ncOut.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", ts_end.strftime(ncTimeFormat))

        # add creating and history entry
        ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
        ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

        ncOut.close()

        print('file time range', ts_start, 'to', ts_end)

        print()
        print('write took', datetime.datetime.now() - time_start)

        output_files.append(outputName)


    return output_files


if __name__ == "__main__":

    files = []
    include_diag = False
    for f in sys.argv[1:]:
        if f == '-include-diag':
            include_diag = True
        else:
            files.extend(glob.glob(f))

    parse_file(files, include_diag)

