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

import datetime
from datetime import timedelta
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct

from si_prefix import si_format

import ctypes

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

#packet_decoder[5] = {'keys': , 'unpack': ""}

packet_decoder = {}

packet_decoder[0] = {'name': 'User Configuration', 'keys': ['T1', 'T2', 'T3', 'T4', 'T5', 'NPing', 'AvgInt', 'NBeam', 'TimCtrlReg', 'PwrCtrlReg',
                            'A1', 'B0', 'B1', 'CompassUpdRate', 'CoordSys', 'NBins', 'BinLen', 'MeasInterval',
                            'DeplyName', 'WrapMode', 'clockDeploy', 'DiagInterval', 'Mode', 'AdjSoundSpeed',
                            'NSampDiag', 'NBeamsCellDiag', 'NPingsDiag', 'ModeTest', 'AnaInAddr', 'SWVersion',
                            'salinity', 'VelAdjTable', 'comments', 'spare1', 'Proc', 'spare2', 'Mode', 'DynPercPos',
                            'wT1', 'wT2', 'wT3', 'NSamp', 'wA1', 'wB0', 'wB1', 'spare3', 'AnaOutScale', 'CorrThresh',
                            'spare3', 'TiLag2', 'spare4', 'QualConst', 'checksum'], 'unpack': "<18H6sH6sI9H180s80s48sH50s14H22s24sH"}

packet_decoder[4] = {'name': 'Head Configuration', 'keys': ['head_config', 'head_frequency', 'head_type', 'head_serial', 'system', 'spare', 'NBeam', 'checksum'], 'unpack': "<HHH12s176s22sHH"}

packet_decoder[5] = {'name': 'Hardware Configuration', 'keys': ['serial', 'config', 'frequency', 'PICversion', 'HWversion', 'RecSize', 'status', 'spare', 'FWversion', 'checksum'], 'unpack': "<14s6H12s4sH"}

packet_decoder[1] = {'name': 'Aquadopp Velocity Data', 'keys': ['time_bcd', 'error', 'AnaIn1', 'battery', 'soundSpd_Anain2', 'head', 'pitch', 'roll',
                            'presMSB', 'status', 'presLSW', 'temp', 'vel_b1', 'vel_b2', 'vel_b3', 'amp1', 'amp2', 'amp3', 'fill', 'checksum'], 'unpack': "<6s7hBBH4h4BH"}

packet_decoder[128] = {'name': 'Aquadopp Diagnostics Data', 'keys': ['time_bcd', 'error', 'AnaIn1', 'battery', 'soundSpd_Anain2', 'head', 'pitch', 'roll',
                            'presMSB', 'status', 'presLSW', 'temp', 'vel_b1', 'vel_b2', 'vel_b3', 'amp1', 'amp2', 'amp3', 'fill', 'checksum'], 'unpack': "<6s7hBB5h4BH"}

packet_decoder[6] = {'name': 'Aquadopp Diagnostics Data Header', 'keys': ['records', 'cell', 'noise1', 'noise2', 'noise3', 'noise4', 'proc1', 'proc2',
                            'proc3', 'proc4', 'dis1', 'dis2', 'dist3', 'dist4', 'spare', 'checksum'], 'unpack': "<2H4B8H6sH"}

packet_decoder[7] = {'name': 'Vector and Vectrino Probe Check Data', 'keys': ['samples', 'firstsample', 'AmpB1...', 'AmpB2...', 'AmpB3...', 'checksum'], 'unpack': "<HH{0}B{0}B{0}BH"}

packet_decoder[18] = {'name': 'Vector Velocity Data Header', 'keys': ['time_bcd', 'NRecords', 'noise1', 'noise2', 'noise3', 'spare', 'corr1', 'corr2', 'corr3', 'spare1', 'spare3', 'checksum'], 'unpack': "<6sH3BB3B1B20BH"}
packet_decoder[17] = {'name': 'Vector System Data', 'keys': ['time_bcd', 'battery', 'soundSpeed', 'heading', 'pitch', 'roll', 'temp', 'error', 'status', 'anain', 'checksum'], 'unpack': "<6s6HBBHH"}
packet_decoder[16] = {'name': 'Vector Velocity Data', 'keys': ['anaIn2LSB', 'count', 'presMSB', 'anaIn2MSB', 'presLSW', 'anaIn1', 'vel1', 'vel2', 'vel3', 'amp1', 'amp2', 'amp3', 'corr1', 'corr2', 'corr3', 'checksum'], 'unpack': "<BBBB5H3B3BH"}
packet_decoder[113] = {'name': 'Vector With IMU', 'keys': ['EnsCnt', 'AHRSid', 'accelX', 'accelY', 'accelZ', 'angRateX', 'angRateY', 'angRateZ', 'MagX', 'MagY', 'MagZ', 'M11', 'M12', 'M13', 'M21', 'M22', 'M23', 'M31', 'M32', 'M33', 'timer', 'IMUchSum', 'checksum'], 'unpack': "<BB18fIHH"}

packet_decoder[33] = {'name': 'Aquadopp Profiler Velocity Data', 'keys': ['time_bcd', 'error', 'AnaIn1', 'battery', 'soundSpd_Anain2', 'head', 'pitch', 'roll',
                              'presMSB', 'status', 'presLSW', 'temp', 'vel_b1...', 'vel_b2...', 'vel_b3...', 'amp1...', 'amp2...', 'amp3...', 'checksum' ], 'unpack': '<6s7hBBHH{0}h{0}BH'}

packet_decoder[48] = {'name': 'AWAC Wave Data', 'keys': ['pressure', 'distance1', 'anaIn', 'vel1', 'vel2', 'vel3', 'dist1_vel4', 'amp1', 'amp2', 'amp3', 'amp4', 'checksum' ], 'unpack': '<7h4BH'}

packet_decoder[49] = {'name': 'AWAC wave Data Header', 'keys': ['time_bcd', 'NRecords', 'blanking', 'battery', 'sound_speed', 'heading', 'pitch', 'roll', 'minPres', 'maxPres',
                                                                'temperature', 'cell_size', 'noise1', 'noise2', 'noise3', 'noise4', 'progmagn1', 'progmagn2', 'progmagn3', 'progmagn4', 'spare', 'checksum'], 'unpack': "<6s11H4B4H14sH"}


# TODO: how to map the above into netCDF attributes....

packet_decode2netCDF = {}
packet_decode2netCDF[0] = {'decode': 'head_frequency', 'attrib': 'nortek_head_frequency_kHz'}
packet_decode2netCDF[1] = {'decode': 'T1', 'attrib': 'nortek_tx_pulse_length'}
packet_decode2netCDF[2] = {'decode': 'T2', 'attrib': 'nortek_blank_distance'}
packet_decode2netCDF[3] = {'decode': 'T3', 'attrib': 'nortek_receive_length'}
packet_decode2netCDF[4] = {'decode': 'T4', 'attrib': 'nortek_time_between_pings'}
packet_decode2netCDF[5] = {'decode': 'T5', 'attrib': 'nortek_time_bewteen_bursts'}
packet_decode2netCDF[6] = {'decode': 'NBeam', 'attrib': 'nortek_number_beams'}
packet_decode2netCDF[7] = {'decode': 'MeasInterval', 'attrib': 'nortek_mesurement_interval'}
packet_decode2netCDF[8] = {'decode': 'AvgInt', 'attrib': 'nortek_averaging_interval'}
packet_decode2netCDF[9] = {'decode': 'FWversion', 'attrib': 'nortek_firmware_version'}

attribute_list = []

velocity_data = []
vector_velocity_data = []
vector_imu_data = []
aquapro_data = []

coord_system = None

coord_systems = ['ENU', 'XYZ', 'BEAM']


def parse_file(filepath):

    checksum_errors = 0
    no_sync = 0
    sample_count = 0

    first_time = None

    with open(filepath, "rb") as binary_file:
        data = binary_file.read(1)
        bad_ck_pos = binary_file.tell()
        #print('tell', bad_ck_pos)

        while data:
            #print("sync : ", data)

            if data == b'\xa5':  # sync
                checksum = 0xb58c
                id = binary_file.read(1)
                id = struct.unpack("B", id)
                id = id[0]
                checksum += 0xa5 + (id << 8)
                #print("id = ", id)
                if id == 16:
                    l = 13
                else:
                    size = binary_file.read(2)
                    l = struct.unpack("<H", size)
                    l = l[0]
                    checksum += l

                packet = binary_file.read(l*2 - 4)  # size in words, less the 4 we already read
                #print("len = ", l, len(packet))
                if len(packet) != l*2 - 4:  # did not read enough
                    break

                for i in range(0, (l-3)):
                    checksum += (struct.unpack("<H", packet[i*2:i*2+2]))[0]
                if checksum & 0xffff != (struct.unpack("<H", packet[-2:]))[0]:
                    print("check sum error ", bad_ck_pos, checksum & 0xffff, (struct.unpack("<H", packet[-2:])[0]))
                    checksum_errors += 1
                    if checksum_errors > 10:
                        print("too many errors, maybe not a nortek file")
                        return None
                    binary_file.seek(bad_ck_pos, 0)  # seek back to before packet
                else:
                    try:
                        #print(packet_decoder[id]['unpack'])
                        unpack = packet_decoder[id]['unpack']

                        # deal with the variable length packets
                        if 'Aquadopp Profiler Velocity Data' == packet_decoder[id]['name']:
                            #print(unpack.format(number_bins * number_beams))
                            unpack = unpack.format(number_samples * number_beams)

                        if 'Vector and Vectrino Probe Check Data' == packet_decoder[id]['name']:
                            unpack = unpack.format(300)
                            number_samples = 300

                        keys = packet_decoder[id]['keys']
                        #print(type(keys))
                        keys_out = []
                        for k in keys:
                            if k.endswith("..."):
                                kn = k.replace("...", "")
                                for i in range(0, number_samples):
                                    keys_out.append(kn + "[" + str(i) + "]")
                            else:
                                keys_out.append(k)
                        #print(keys_out)

                        # decode the packet
                        #print("packet size", packet_decoder[id]['name'], len(packet))
                        packetDecode = struct.unpack(unpack, packet)
                        d = dict(zip(keys_out, packetDecode))

                        #print(packet_decoder[id]['name'])
                        #for k in d:
                        #     print("dict ", k, " = " , d[k])

                        # decode and capture any datacodes
                        if 'time_bcd' in d:
                            ts_bcd = struct.unpack("<6B", d['time_bcd'])
                            y = []
                            for x in ts_bcd:
                                y.append(int((((x & 0xf0)/16) * 10) + (x & 0xf)))
                            dt = datetime.datetime(y[4]+2000, y[5], y[2], y[3], y[0], y[1])
                            #print('time ', packet_decoder[id]['name'], dt, sample_count)

                        if 'serial' in d:
                            sn = d['serial']
                            snx = bytearray(sn)
                            for x in range(0,len(sn)):
                                #print("byte ", x , sn[x])
                                if snx[x] in [0xc0, 0x07]:
                                    snx[x] = 32

                            instrument_serialnumber = snx.decode("utf-8", errors='ignore').strip()
                            print('instrument serial number ', instrument_serialnumber)

                        if 'head_serial' in d:
                            instrument_head_serialnumber = d['head_serial'].decode("utf-8").strip()
                            print('instrument head serial number ', instrument_head_serialnumber)

                        if 'CoordSys' in d:
                            coord_system = d['CoordSys']

                        if 'NBins' in d:
                            number_bins = d['NBins']
                            number_samples = number_bins

                        if 'NBeam' in d:
                            number_beams = d['NBeam']

                        if 'head_frequency' in d:
                            system_frequency = d['head_frequency']

                        for att in packet_decode2netCDF:
                            #print(packet_decode2netCDF[att])
                            if packet_decode2netCDF[att]['decode'] in d:
                                attribute_list.append((packet_decode2netCDF[att]["attrib"], float(d[packet_decode2netCDF[att]["decode"]])))

                        # include all settings as attributes in file
                        # if 'User Configuration' == packet_decoder[id]['name'] or 'Head Configuration' == packet_decoder[id]['name'] or 'Hardware Configuration' == packet_decoder[id]['name']:
                        #     for k in d:
                        #         print("dict ", k, " = " , d[k])
                        #         attribute_list.append(('nortek_' + packet_decoder[id]['name'].replace(" ", "_").lower() + '-' + k, str(d[k])))

                        if 'Vector System Data' == packet_decoder[id]['name']:
                            if not first_time:
                                first_time = dt
                                sample_count = 0

                        # create an array of all the data packets (this does copy them into memory)
                        if 'Aquadopp Velocity Data' == packet_decoder[id]['name']:
                            #print('velocity data')
                            velocity_data.append((dt, d))
                            #print(dt, d)

                        # create an array of all the data packets (this does copy them into memory)
                        if 'Aquadopp Profiler Velocity Data' == packet_decoder[id]['name']:
                            #print('velocity data')
                            aquapro_data.append((dt, d))
                            #print(dt, d)

                        if 'Vector Velocity Data' == packet_decoder[id]['name']:
                            # calculate the sample timestamp
                            ts = first_time + timedelta(microseconds=int(sample_count*63000)) # a sample every 63 ms, where does this come from?

                            vector_velocity_data.append((ts, d))
                            sample_count += 1

                            #print(dt, d)
                        if 'Vector With IMU' == packet_decoder[id]['name']:
                            print('Vector With IMU')

                            # use same timestamp as the last 'Vector Velocity Data' (from sample_count)
                            vector_imu_data.append((ts, d))

                            if len(vector_imu_data) % 1000 == 0:
                                print("samples read ", len(vector_imu_data), ts, dt, (dt - ts).total_seconds(), (dt-first_time).total_seconds())

                            print(dt, d)

                    except KeyError:
                        print('packet_decode not found ', id)
            else:
                no_sync += 1
                #if no_sync > 100:
                #    print("no sync found in first 100 bytes, maybe not a nortek file")
                #    return None

            data = binary_file.read(1)
            bad_ck_pos = binary_file.tell()
            #print('tell', bad_ck_pos)

    print('aquadopp velocity data samples ', len(velocity_data))
    print('aquapro velocity data samples ', len(aquapro_data))
    print('vector velocity data samples ', len(vector_velocity_data))
    print('vector IMU data samples ', len(vector_imu_data))

    number_samples_read = len(velocity_data) + len(vector_velocity_data) + len(aquapro_data)

    aquadopp = len(velocity_data) > 0
    vector = len(vector_velocity_data) > 0
    aquapro = len(aquapro_data) > 0
    vectImu = len(vector_imu_data) > 0

    if number_samples_read == 0:
        print("no samples, probably not a nortek aquadopp or vector file")
        return None

    # create the netCDF file
    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    if aquadopp:
        # add global attributes
        instrument_model = 'Aquadopp ' + si_format(system_frequency * 1000, precision=0) + 'Hz'

        # create an array to store data in (creates another memory copy)
        data_array = np.zeros((11, number_samples_read))
        data_array.fill(np.nan)
        byte_array = np.zeros((3, number_samples_read), 'byte')
        i = 0
        for d in velocity_data:
            data_array[0][i] = date2num(d[0], calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")
            data_array[1][i] = d[1]['vel_b1']/1000
            data_array[2][i] = d[1]['vel_b2']/1000
            data_array[3][i] = d[1]['vel_b3']/1000

            data_array[4][i] = d[1]['head']/10
            data_array[5][i] = d[1]['pitch']/10
            data_array[6][i] = d[1]['roll']/10

            data_array[7][i] = ((d[1]['presMSB'] * 65536) + d[1]['presLSW']) * 0.001

            data_array[8][i] = d[1]['battery']/10

            data_array[9][i] = d[1]['temp'] * 0.01

            data_array[10][i] = d[1]['soundSpd_Anain2'] * 0.1

            byte_array[0][i] = d[1]['amp1']
            byte_array[1][i] = d[1]['amp2']
            byte_array[2][i] = d[1]['amp3']

            i = i + 1

        # output variable structures
        var_names = []
        var_names.append({'data_n':  1, 'name': 'UCUR_MAG', 'comment': "current east", 'unit': 'm/s'})
        var_names.append({'data_n':  2, 'name': 'VCUR_MAG', 'comment': "current north", 'unit': 'm/s'})
        var_names.append({'data_n':  3, 'name': 'WCUR', 'comment': "current up", 'unit': 'm/s'})
        var_names.append({'data_n':  4, 'name': 'HEADING_MAG', 'comment': "heading", 'unit': 'degrees'})
        var_names.append({'data_n':  5, 'name': 'PITCH', 'comment': "pitch", 'unit': 'degrees'})
        var_names.append({'data_n':  6, 'name': 'ROLL', 'comment': "roll", 'unit': 'degrees'})
        var_names.append({'data_n':  7, 'name': 'PRES', 'comment': "pressure", 'unit': 'dbar'})
        var_names.append({'data_n':  8, 'name': 'BATT', 'comment': "battery voltage", 'unit': 'V'})
        var_names.append({'data_n':  9, 'name': 'TEMP', 'comment': "temperature", 'unit': 'degrees_Celsius'})
        var_names.append({'data_n': 10, 'name': 'SSPEED', 'comment': "sound speed", 'unit': 'm/s'})

        var_names.append({'byte_n':  0, 'name': 'ABSIC1', 'comment': "amplitude beam 1", 'unit': 'counts'})
        var_names.append({'byte_n':  1, 'name': 'ABSIC2', 'comment': "amplitude beam 2", 'unit': 'counts'})
        var_names.append({'byte_n':  2, 'name': 'ABSIC3', 'comment': "amplitude beam 3", 'unit': 'counts'})

    if aquapro:
        # add global attributes
        instrument_model = 'AquaPro ' + si_format(system_frequency * 1000, precision=0) + 'Hz'

        # create an array to store data in (creates another memory copy)
        data_array = np.zeros((8, number_samples_read))
        data_array.fill(np.nan)
        profile_array = np.zeros((number_beams, number_samples_read, number_bins))
        profile_array.fill(np.nan)

        i = 0
        for d in aquapro_data:
            data_array[0][i] = date2num(d[0], calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")
            data_array[1][i] = d[1]['head']/10
            data_array[2][i] = d[1]['pitch']/10
            data_array[3][i] = d[1]['roll']/10

            data_array[4][i] = ((d[1]['presMSB'] * 65536) + d[1]['presLSW']) * 0.001

            data_array[5][i] = d[1]['battery']/10

            data_array[6][i] = d[1]['temp'] * 0.01

            data_array[7][i] = d[1]['soundSpd_Anain2'] * 0.1

            i = i + 1

        # output variable structures
        var_names = []
        var_names.append({'data_n':  1, 'name': 'HEADING_MAG', 'comment': "heading", 'unit': 'degrees'})
        var_names.append({'data_n':  2, 'name': 'PITCH', 'comment': "pitch", 'unit': 'degrees'})
        var_names.append({'data_n':  3, 'name': 'ROLL', 'comment': "roll", 'unit': 'degrees'})
        var_names.append({'data_n':  4, 'name': 'PRES', 'comment': "pressure", 'unit': 'dbar'})
        var_names.append({'data_n':  5, 'name': 'BATT', 'comment': "battery voltage", 'unit': 'V'})
        var_names.append({'data_n':  6, 'name': 'TEMP', 'comment': "temperature", 'unit': 'degrees_Celsius'})
        var_names.append({'data_n':  7, 'name': 'SSPEED', 'comment': "sound speed", 'unit': 'm/s'})

        ncOut.createDimension("CELL", number_bins)

    if vector:
        # add global attributes
        instrument_model = 'Vector'

        # create an array to store data in (creates another memory copy)
        data_array = np.zeros((3, number_samples_read))
        data_array.fill(np.nan)
        vector_array = np.zeros((3, number_samples_read, 3))
        vector_array.fill(np.nan)
        mat_array = np.zeros((1, number_samples_read, 9))
        mat_array.fill(np.nan)

        for i in range(0, number_samples_read):
            #print(vector_velocity_data[i])
            data_array[0][i] = date2num(vector_velocity_data[i][0], calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")
            data_array[1][i] = ((vector_velocity_data[i][1]['presMSB'] * 65536) + vector_velocity_data[i][1]['presLSW']) * 0.001
            data_array[2][i] = vector_velocity_data[i][1]['anaIn1']

            if vectImu:
                vector_array[0][i][0] = vector_imu_data[i][1]['accelX']
                vector_array[0][i][1] = vector_imu_data[i][1]['accelY']
                vector_array[0][i][2] = vector_imu_data[i][1]['accelZ']

                vector_array[1][i][0] = vector_imu_data[i][1]['angRateX']
                vector_array[1][i][1] = vector_imu_data[i][1]['angRateY']
                vector_array[1][i][2] = vector_imu_data[i][1]['angRateZ']

                vector_array[2][i][0] = vector_imu_data[i][1]['MagX']
                vector_array[2][i][1] = vector_imu_data[i][1]['MagY']
                vector_array[2][i][2] = vector_imu_data[i][1]['MagZ']

                mat_array[0][i][0] = vector_imu_data[i][1]['M11']
                mat_array[0][i][1] = vector_imu_data[i][1]['M12']
                mat_array[0][i][2] = vector_imu_data[i][1]['M13']
                mat_array[0][i][3] = vector_imu_data[i][1]['M21']
                mat_array[0][i][4] = vector_imu_data[i][1]['M22']
                mat_array[0][i][5] = vector_imu_data[i][1]['M23']
                mat_array[0][i][6] = vector_imu_data[i][1]['M31']
                mat_array[0][i][7] = vector_imu_data[i][1]['M32']
                mat_array[0][i][8] = vector_imu_data[i][1]['M33']

        var_names = []
        var_names.append({'data_n':  1, 'name': 'PRES', 'comment': "pressure", 'unit': 'dbar'})
        var_names.append({'data_n':  2, 'name': 'ANALOG1', 'comment': "voltage", 'unit': 'V'})

        if vectImu:
            var_names.append({'vector_n':  0, 'name': 'ACCEL', 'comment': "acceleration", 'unit': 'm/s'})
            var_names.append({'vector_n':  1, 'name': 'ANG_RATE', 'comment': "angular rate", 'unit': 'deg/s'})
            var_names.append({'vector_n':  2, 'name': 'MAG', 'comment': "magnetic", 'unit': 'gauss'})

            var_names.append({'mat_n':  0, 'name': 'ORIENTATION', 'comment': "matrix", 'unit': '1'})

            ncOut.createDimension("VECTOR", 3)
            ncOut.createDimension("MATRIX", 9)

    ncOut.instrument = 'Nortek ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber
    ncOut.instrument_head_serial_number = instrument_head_serialnumber
    ncOut.coord_system = coord_systems[coord_system]

    for a in attribute_list:
        ncOut.setncattr(a[0], a[1])

    # add time variable

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME", number_samples_read)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = data_array[0]

    # add data variables
    for v in var_names:
        if 'data_n' in v:
            ncVarOut = ncOut.createVariable(v['name'], "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
            ncVarOut.comment = v['comment']
            ncVarOut.units = v['unit']
            ncVarOut[:] = data_array[v['data_n']]
        elif 'byte_n' in v:
            ncVarOut = ncOut.createVariable(v['name'], "u1", ("TIME",), fill_value=0, zlib=True)  # fill_value=0 otherwise defaults to max
            ncVarOut.comment = v['comment']
            ncVarOut.units = v['unit']
            ncVarOut[:] = byte_array[v['byte_n']]
        elif 'vector_n' in v:
            ncVarOut = ncOut.createVariable(v['name'], "f4", ("TIME", "VECTOR"), fill_value=0, zlib=True)  # fill_value=0 otherwise defaults to max
            ncVarOut.comment = v['comment']
            ncVarOut.units = v['unit']
            ncVarOut[:] = vector_array[v['vector_n']]
        elif 'mat_n' in v:
            ncVarOut = ncOut.createVariable(v['name'], "f4", ("TIME", "MATRIX"), fill_value=0, zlib=True)  # fill_value=0 otherwise defaults to max
            ncVarOut.comment = v['comment']
            ncVarOut.units = v['unit']
            ncVarOut[:] = mat_array[v['mat_n']]

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse_file(sys.argv[1])

