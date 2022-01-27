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

packet_decoder[42] = {'name': 'HR Aquadopp Profiler Velocity Data', 'keys': ['time_bcd', 'ms', 'error', 'battery', 'soundSpd', 'head', 'pitch', 'roll',
                                                                             'presMSB', 'status', 'presLSW', 'temp', 'AnaIn1', 'AnaIn2', 'beams', 'cells',
                                                                             'VelLag2_b1', 'VelLag2_b2', 'VelLag2_b3',
                                                                             'AmpLag2_b1', 'AmpLag2_b3', 'AmpLag2_b3',
                                                                             'CorrLag2_b1', 'CorrLag2_b2', 'CorrLag2_b3',
                                                                             'spare1', 'spare2', 'spare3',
                                                                             'vel...', 'amp...', 'corr...', 'checksum' ], 'unpack': '<6s7hBBHHHHBB3H3B3B3H{0}h{0}B{0}BH'}

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
packet_decode2netCDF[5] = {'decode': 'T5', 'attrib': 'nortek_time_between_bursts'}
packet_decode2netCDF[6] = {'decode': 'NBeam', 'attrib': 'nortek_number_beams'}
packet_decode2netCDF[7] = {'decode': 'MeasInterval', 'attrib': 'nortek_mesurement_interval'}
packet_decode2netCDF[8] = {'decode': 'AvgInt', 'attrib': 'nortek_averaging_interval'}
packet_decode2netCDF[9] = {'decode': 'FWversion', 'attrib': 'nortek_firmware_version'}

attribute_list = []

coord_system = None

coord_systems = ['ENU', 'XYZ', 'BEAM']


def create_netCDF_var(ncfile, name, type, comment, units, dims):

    if type.startswith('f'):
        fill = np.nan
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


def parse_file(files):

    output_files = []
    for filepath in files:
        checksum_errors = 0
        no_sync = 0
        sample_count = 0

        first_time = None

        # create the netCDF file
        outputName = filepath + ".nc"

        print("output file : %s" % outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4')

        # add time variable

        #     TIME:axis = "T";
        #     TIME:calendar = "gregorian";
        #     TIME:long_name = "time";
        #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

        tDim = ncOut.createDimension("TIME")
        ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
        ncTimesOut.long_name = "time"
        ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
        ncTimesOut.calendar = "gregorian"
        ncTimesOut.axis = "T"

        # instrument types
        aquadopp = False
        aquadoppHR = False
        aquapro = False
        vector = False
        vectorWithImu = False
        awac_wave_data = False

        wave_sample_time = -1

        with open(filepath, "rb") as binary_file:
            data = binary_file.read(1)
            bad_ck_pos = binary_file.tell()
            #print('tell', bad_ck_pos)

            while data:
                #print("sync : ", data)

                if data == b'\xa5':  # sync
                    checksum = 0xb58c
                    id_data = binary_file.read(1)
                    (id,) = struct.unpack("B", id_data)
                    checksum += 0xa5 + (id << 8)
                    #print("id = ", id)
                    if id == packet_id["Vector Velocity Data"]: # Vector Velocity Data is fixed size
                        pkt_size = 13
                    else:
                        size_data = binary_file.read(2)
                        (pkt_size,) = struct.unpack("<H", size_data)
                        checksum += pkt_size

                    packet_data = binary_file.read(pkt_size*2 - 4)  # size in words, less the 4 we already read
                    #print(id, "len = ", l, len(packet))
                    if len(packet_data) != pkt_size*2 - 4:  # did not read enough
                        break

                    # check check sum
                    for i in range(0, (pkt_size-3)):
                        checksum += (struct.unpack("<H", packet_data[i*2:i*2+2]))[0]
                    if checksum & 0xffff != (struct.unpack("<H", packet_data[-2:]))[0]:
                        print("check sum error ", bad_ck_pos, checksum & 0xffff, (struct.unpack("<H", packet_data[-2:])[0]))
                        checksum_errors += 1
                        if checksum_errors > 10:
                            print("too many errors, maybe not a nortek file")
                            return None
                        binary_file.seek(bad_ck_pos, 0)  # seek back to before packet
                    else:
                        try:
                            #print(packet_decoder[id]['unpack'])
                            unpack = packet_decoder[id]['unpack']

                            keys = packet_decoder[id]['keys']

                            # deal with the variable length packets, format the decoder
                            if packet_id['Aquadopp Profiler Velocity Data'] == id:
                                #print(unpack.format(number_bins * number_beams))
                                unpack = unpack.format(number_samples * number_beams)
                                keys = fill_var_keys(number_samples, keys)

                            if packet_id['HR Aquadopp Profiler Velocity Data'] == id:
                                #print(unpack.format(number_bins * number_beams))
                                (beams, cells) = struct.unpack("<BB", packet_data[34-4:36-4])
                                number_samples = beams*cells
                                #print('beams, cells', beams, cells)
                                unpack = unpack.format(cells * beams)
                                keys = fill_var_keys(number_samples, keys)

                            if packet_id['Vector and Vectrino Probe Check Data'] == id:
                                (number_samples,) = struct.unpack("<H", packet_data[0:2])
                                #print("probe check", number_samples)
                                unpack = unpack.format(number_samples)
                                keys = fill_var_keys(number_samples, keys)

                            # decode the packet
                            #print("packet size", packet_decoder[id]['name'], len(packet))
                            packetDecode = struct.unpack(unpack, packet_data)
                            d = dict(zip(keys, packetDecode))

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
                                for x in range(len(snx)):
                                    #print("byte ", x , sn[x])
                                    if snx[x] in [0xc0, 0x07, 0x83, 0x06, 0x11, 0x02]:
                                        snx[x] = 32
                                    if not ((0x2e <= snx[x] <= 0x39) or (0x40 < snx[x] <= 0x7a)):
                                        snx[x] = 32

                                sn_string = snx.decode("utf-8", errors='ignore').strip()
                                instrument_serialnumber = sn_string # .split(" ")[0]
                                print('serial1', sn)
                                print('instrument serial number', instrument_serialnumber)

                            if 'head_serial' in d:
                                #print('head_serial', d['head_serial'])
                                sn = d['head_serial']
                                snx = bytearray(sn)
                                for x in range(len(snx)):
                                    #print("byte ", x , sn[x])
                                    if snx[x] in [0xc0, 0x07, 0x83, 0x06, 0x11, 0x02]:
                                        snx[x] = 32
                                    if not ((0x2e <= snx[x] <= 0x39) or (0x40 < snx[x] <= 0x7a)):
                                        snx[x] = 32

                                sn_string = snx.decode("utf-8", errors='ignore').strip()

                                instrument_head_serialnumber = sn_string
                                print('instrument head serial number', instrument_head_serialnumber)

                            if 'CoordSys' in d:
                                coord_system = d['CoordSys']
                                print("coord system", coord_systems[coord_system])

                            if 'TimCtrlReg' in d:
                                tim_ctrl_reg = d['TimCtrlReg']
                                timing_controller_mode = "profile:continuous," if (tim_ctrl_reg & 0x02) != 0 else "profile:single,"
                                timing_controller_mode += "mode:continuous" if (tim_ctrl_reg & 0x04) != 0 else "mode:burst"

                                print("tim_ctrl_reg", tim_ctrl_reg, timing_controller_mode)

                            if 'head_config' in d:
                                head_config_reg = d['head_config']
                                head_config = "pressure:yes," if (head_config_reg & 0x01) != 0 else "pressure:no,"
                                head_config += "magnetometer:yes" if (head_config_reg & 0x02) != 0 else "magnetometer:no,"
                                head_config += "tilt:yes" if (head_config_reg & 0x04) != 0 else "tilt:no,"
                                head_config += "tilt:down" if (head_config_reg & 0x08) != 0 else "tilt:up"

                                print("head_config", head_config_reg, head_config)

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

                            # print('packet', packet_decoder[id]['name'])

                            # include all settings as attributes in file
                            # if 'User Configuration' == packet_decoder[id]['name'] or 'Head Configuration' == packet_decoder[id]['name'] or 'Hardware Configuration' == packet_decoder[id]['name']:
                            #     for k in d:
                            #         print("dict ", k, " = " , d[k])
                            #         attribute_list.append(('nortek_' + packet_decoder[id]['name'].replace(" ", "_").lower() + '-' + k, str(d[k])))

                            if packet_id['Vector System Data'] == id:
                                if not first_time:
                                    first_time = dt
                                    vector_sample_count = 0

                            # create an array of all the data packets (this does copy them into memory)
                            if packet_id['Aquadopp Velocity Data'] == id:
                                if not aquadopp:
                                    aquadopp = True
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

                                    head = create_netCDF_var(ncOut, "HEADING_MAG", "f4", "heading magnetic", "degrees", ("TIME", ))
                                    pitch = create_netCDF_var(ncOut, "PITCH", "f4", "pitch", "degrees", ("TIME", ))
                                    roll = create_netCDF_var(ncOut, "ROLL", "f4", "roll", "degrees", ("TIME", ))
                                    pres = create_netCDF_var(ncOut, "PRES", "f4", "pres", "dbar", ("TIME", ))
                                    bat = create_netCDF_var(ncOut, "BATT", "f4", "battery voltage", "V", ("TIME", ))
                                    itemp = create_netCDF_var(ncOut, "ITEMP", "f4", "instrument temperature", "degrees_Celsius", ("TIME", ))
                                    sspeed = create_netCDF_var(ncOut, "SSPEED", "f4", "sound speed", "m/s", ("TIME", ))

                                    absci1 = create_netCDF_var(ncOut, "ABSIC1", "i2", "amplitude beam 1", "counts", ("TIME", ))
                                    absci2 = create_netCDF_var(ncOut, "ABSIC2", "i2", "amplitude beam 2", "counts", ("TIME", ))
                                    absci3 = create_netCDF_var(ncOut, "ABSIC3", "i2", "amplitude beam 3", "counts", ("TIME", ))

                                    # add global attributes
                                    instrument_model = 'Aquadopp ' + si_format(system_frequency * 1000, precision=0) + 'Hz'

                                vel1[sample_count] = d['vel_b1'] / 1000
                                vel2[sample_count] = d['vel_b2'] / 1000
                                vel3[sample_count] = d['vel_b3'] / 1000

                                head[sample_count] = d['head'] / 10
                                pitch[sample_count] = d['pitch'] / 10
                                roll[sample_count] = d['roll'] / 10

                                pres[sample_count] = ((d['presMSB'] * 65536) + d['presLSW']) * 0.001
                                bat[sample_count] = d['battery'] / 10
                                itemp[sample_count] = d['temp'] * 0.01
                                sspeed[sample_count] = d['soundSpd_Anain2'] * 0.1

                                absci1[sample_count] = d['amp1']
                                absci2[sample_count] = d['amp2']
                                absci3[sample_count] = d['amp3']

                                ncTimesOut[sample_count] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

                                sample_count += 1

                                #print(dt, d)

                            if packet_id['HR Aquadopp Profiler Velocity Data'] == id:
                                if not aquadoppHR:
                                    aquadoppHR = True

                                    ncOut.createDimension("CELL", cells)
                                    ncOut.createDimension("BEAMS", beams)

                                    head = create_netCDF_var(ncOut, "HEADING_MAG", "f4", "heading magnetic", "degrees", ("TIME", ))
                                    pitch = create_netCDF_var(ncOut, "PITCH", "f4", "pitch", "degrees", ("TIME", ))
                                    roll = create_netCDF_var(ncOut, "ROLL", "f4", "roll", "degrees", ("TIME", ))
                                    pres = create_netCDF_var(ncOut, "PRES", "f4", "pres", "dbar", ("TIME", ))
                                    bat = create_netCDF_var(ncOut, "BATT", "f4", "battery voltage", "V", ("TIME", ))
                                    itemp = create_netCDF_var(ncOut, "ITEMP", "f4", "instrument temperature", "degrees_Celsius", ("TIME", ))
                                    sspeed = create_netCDF_var(ncOut, "SSPEED", "f4", "sound speed", "m/s", ("TIME", ))
                                    analog1 = create_netCDF_var(ncOut, "ANALOG1", "f4", "analog input 1", "counts", ("TIME", ))
                                    analog2 = create_netCDF_var(ncOut, "ANALOG2", "f4", "analog input 1", "counts", ("TIME", ))

                                    vel = create_netCDF_var(ncOut, "VELOCITY", "f4", "velocity", "m/s", ("TIME", "BEAMS", "CELL"))
                                    amp = create_netCDF_var(ncOut, "ABSC", "i2", "velocity", "counts", ("TIME", "BEAMS", "CELL"))
                                    corr = create_netCDF_var(ncOut, "CORR", "i2", "velocity", "1", ("TIME", "BEAMS", "CELL"))

                                    # add global attributes
                                    instrument_model = 'Aquadopp HR ' + si_format(system_frequency * 1000, precision=0) + 'Hz'

                                ncTimesOut[sample_count] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

                                head[sample_count] = d['head'] / 10
                                pitch[sample_count] = d['pitch'] / 10
                                roll[sample_count] = d['roll'] / 10

                                pres[sample_count] = ((d['presMSB'] * 65536) + d['presLSW']) * 0.001
                                bat[sample_count] = d['battery'] / 10
                                itemp[sample_count] = d['temp'] * 0.01
                                sspeed[sample_count] = d['soundSpd'] * 0.1

                                analog1[sample_count] = d['AnaIn1']
                                analog2[sample_count] = d['AnaIn2']

                                for i in range(beams):
                                    for j in range(cells):
                                        vel[sample_count, i, j] = d['vel['+str(i*cells+j)+']'] / 1000
                                        amp[sample_count, i, j] = d['amp['+str(i*cells+j)+']']
                                        corr[sample_count, i, j] = d['corr['+str(i*cells+j)+']']

                                sample_count += 1

                                #print(dt, d)

                            # create an array of all the data packets (this does copy them into memory)
                            if packet_id['Aquadopp Profiler Velocity Data'] == id:
                                #print('aqua pro data')
                                if not aquapro:

                                    ncOut.createDimension("CELL", number_bins)

                                    head = create_netCDF_var(ncOut, "HEADING_MAG", "f4", "heading magnetic", "degrees", ("TIME", ))
                                    pitch = create_netCDF_var(ncOut, "PITCH", "f4", "pitch", "degrees", ("TIME", ))
                                    roll = create_netCDF_var(ncOut, "ROLL", "f4", "roll", "degrees", ("TIME", ))
                                    pres = create_netCDF_var(ncOut, "PRES", "f4", "pres", "dbar", ("TIME", ))
                                    bat = create_netCDF_var(ncOut, "BATT", "f4", "battery voltage", "V", ("TIME", ))
                                    itemp = create_netCDF_var(ncOut, "ITEMP", "f4", "instrument temperature", "degrees_Celsius", ("TIME", ))
                                    sspeed = create_netCDF_var(ncOut, "SSPEED", "f4", "sound speed or analog input 2", "m/s", ("TIME", ))
                                    analog1 = create_netCDF_var(ncOut, "ANALOG1", "f4", "analog input 1", "counts", ("TIME", ))

                                    if coord_system == 2:
                                        vel1 = create_netCDF_var(ncOut, "VEL_B1", "f4", "velocity beam 1", "m/s", ("TIME", "CELL"))
                                        vel2 = create_netCDF_var(ncOut, "VEL_B2", "f4", "velocity beam 2", "m/s", ("TIME", "CELL"))
                                        vel3 = create_netCDF_var(ncOut, "VEL_B3", "f4", "velocity beam 3", "m/s", ("TIME", "CELL" ))
                                    elif coord_system == 1:
                                        vel1 = create_netCDF_var(ncOut, "VEL_X", "f4", "velocity X", "m/s", ("TIME", "CELL"))
                                        vel2 = create_netCDF_var(ncOut, "VEL_Y", "f4", "velocity Y", "m/s", ("TIME", "CELL"))
                                        vel3 = create_netCDF_var(ncOut, "VEL_Z", "f4", "velocity Z", "m/s", ("TIME", "CELL" ))
                                    else:
                                        vel1 = create_netCDF_var(ncOut, "UCUR_MAG", "f4", "current east", "m/s", ("TIME", "CELL"))
                                        vel2 = create_netCDF_var(ncOut, "VCUR_MAG", "f4", "current north", "m/s", ("TIME", "CELL"))
                                        vel3 = create_netCDF_var(ncOut, "WCUR", "f4", "current up", "m/s", ("TIME", "CELL" ))

                                    absci1 = create_netCDF_var(ncOut, "ABSIC1", "i2", "amplitude beam 1", "counts", ("TIME", "CELL"))
                                    absci2 = create_netCDF_var(ncOut, "ABSIC2", "i2", "amplitude beam 2", "counts", ("TIME", "CELL"))
                                    absci3 = create_netCDF_var(ncOut, "ABSIC3", "i2", "amplitude beam 3", "counts", ("TIME", "CELL"))

                                    # add global attributes
                                    instrument_model = 'Aquadopp ' + si_format(system_frequency * 1000, precision=0) + 'Hz'

                                    aquapro = True

                                head[sample_count] = d['head'] / 10
                                pitch[sample_count] = d['pitch'] / 10
                                roll[sample_count] = d['roll'] / 10

                                pres[sample_count] = ((d['presMSB'] * 65536) + d['presLSW']) * 0.001
                                bat[sample_count] = d['battery'] / 10
                                itemp[sample_count] = d['temp'] * 0.01
                                sspeed[sample_count] = d['soundSpd_Anain2'] * 0.1

                                for i in range(number_beams):
                                    vel1[sample_count, i] = d['vel_b1['+str(i)+']'] / 1000
                                    vel2[sample_count, i] = d['vel_b2['+str(i)+']'] / 1000
                                    vel3[sample_count, i] = d['vel_b3['+str(i)+']'] / 1000

                                    absci1[sample_count, i] = d['amp1['+str(i)+']']
                                    absci2[sample_count, i] = d['amp2['+str(i)+']']
                                    absci3[sample_count, i] = d['amp3['+str(i)+']']

                                ncTimesOut[sample_count] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

                                sample_count += 1

                            if packet_id['Vector Velocity Data'] == id:
                                # calculate the sample timestamp
                                ts = first_time + timedelta(microseconds=int(vector_sample_count*63000)) # a sample every 63 ms, where does this come from?
                                vector_sample_count += 1
                                if not vector:
                                    # add global attributes
                                    instrument_model = 'Vector'
                                    pres = create_netCDF_var(ncOut, "PRES", "f4", "pressure", "dbar", ("TIME", ))
                                    analog1 = create_netCDF_var(ncOut, "ANALOG1", "f4", "analog input 1", "V", ("TIME", ))
                                    analog2 = create_netCDF_var(ncOut, "ANALOG2", "f4", "analog input 2", "V", ("TIME", ))

                                    if coord_system == 2:
                                        vel1 = create_netCDF_var(ncOut, "VEL_B1", "f4", "velocity beam 1", "m/s", ("TIME", ))
                                        vel2 = create_netCDF_var(ncOut, "VEL_B2", "f4", "velocity beam 2", "m/s", ("TIME", ))
                                        vel3 = create_netCDF_var(ncOut, "VEL_B3", "f4", "velocity beam 3", "m/s", ("TIME", ))
                                    else:
                                        vel1 = create_netCDF_var(ncOut, "VEL_X", "f4", "velocity X", "m/s", ("TIME", ))
                                        vel2 = create_netCDF_var(ncOut, "VEL_Y", "f4", "velocity Y", "m/s", ("TIME", ))
                                        vel3 = create_netCDF_var(ncOut, "VEL_Z", "f4", "velocity Z", "m/s", ("TIME", ))

                                    amp1 = create_netCDF_var(ncOut, "AMP_B1", "i1", "amplitude B1", "counts", ("TIME", ))
                                    amp2 = create_netCDF_var(ncOut, "AMP_B2", "i1", "amplitude B2", "counts", ("TIME", ))
                                    amp3 = create_netCDF_var(ncOut, "AMP_B3", "i1", "amplitude B3", "counts", ("TIME", ))

                                    corr1 = create_netCDF_var(ncOut, "CORR_B1", "i1", "correlation B1", "%", ("TIME", ))
                                    corr2 = create_netCDF_var(ncOut, "CORR_B2", "i1", "correlation B2", "%", ("TIME", ))
                                    corr3 = create_netCDF_var(ncOut, "CORR_B3", "i1", "correlation B3", "%", ("TIME", ))

                                    vector = True

                                ncTimesOut[sample_count] = date2num(ts, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

                                pres[sample_count] = ((d['presMSB'] * 65536) + d['presLSW']) * 0.001
                                analog1[sample_count] = d['anaIn1']
                                analog2[sample_count] = d['anaIn2MSB'] * 256 + d['anaIn2LSB']
                                vel1[sample_count] = d['vel1']
                                vel2[sample_count] = d['vel2']
                                vel3[sample_count] = d['vel3']
                                amp1[sample_count] = d['amp1']
                                amp2[sample_count] = d['amp2']
                                amp3[sample_count] = d['amp3']
                                corr1[sample_count] = d['corr1']
                                corr2[sample_count] = d['corr2']
                                corr3[sample_count] = d['corr3']

                                # should increment sample_count if the vector does not and IMU data

                            if packet_id['Vector With IMU'] == id:
                                # print('Vector With IMU')

                                # use same timestamp as the last 'Vector Velocity Data' (from sample_count)
                                if not vectorWithImu:
                                    ncOut.createDimension("vector", 3)
                                    ncOut.createDimension("matrix", 9)

                                    accel = create_netCDF_var(ncOut, "ACCEL", "f4", "acceleration", "m/s^2", ("TIME", "vector"))
                                    ang_rate = create_netCDF_var(ncOut, "ANG_RATE", "f4", "angular rate", "deg/s", ("TIME", "vector"))
                                    mag = create_netCDF_var(ncOut, "MAG", "f4", "magnetic", "gauss", ("TIME", "vector"))
                                    orient = create_netCDF_var(ncOut, "ORIENTATION", "f4", "orientation matrix", "1", ("TIME", "matrix"))

                                    vectorWithImu = True

                                accel[sample_count, 0] = d['accelX']
                                accel[sample_count, 1] = d['accelX']
                                accel[sample_count, 2] = d['accelY']

                                ang_rate[sample_count, 0] = d['angRateX']
                                ang_rate[sample_count, 1] = d['angRateY']
                                ang_rate[sample_count, 2] = d['angRateZ']

                                mag[sample_count, 0] = d['MagX']
                                mag[sample_count, 1] = d['MagY']
                                mag[sample_count, 2] = d['MagZ']

                                orient[sample_count, 0] = d['M11']
                                orient[sample_count, 1] = d['M12']
                                orient[sample_count, 2] = d['M13']
                                orient[sample_count, 3] = d['M21']
                                orient[sample_count, 4] = d['M22']
                                orient[sample_count, 5] = d['M23']
                                orient[sample_count, 6] = d['M31']
                                orient[sample_count, 7] = d['M32']
                                orient[sample_count, 8] = d['M33']

                                sample_count += 1

                            if packet_id['AWAC wave Data Header'] == id:
                                wave_sample_time += 1

                                if not awac_wave_data:

                                    wave_cells = d['NRecords']

                                    ncOut.createDimension("WAVE_CELL", wave_cells)

                                    tDim = ncOut.createDimension("WAVE_TIME")
                                    nc_wave_times = ncOut.createVariable("WAVE_TIME", "d", ("WAVE_TIME",), zlib=True)
                                    nc_wave_times.long_name = "wave time"
                                    nc_wave_times.units = "days since 1950-01-01 00:00:00 UTC"
                                    nc_wave_times.calendar = "gregorian"
                                    nc_wave_times.axis = "T"

                                    wave_pres = create_netCDF_var(ncOut, "WAVE_PRES", "f4", "pres", "dbar", ("WAVE_TIME", "WAVE_CELL"))

                                    w_vel1 = create_netCDF_var(ncOut, "VELOCITY1", "f4", "velocity", "m/s", ("WAVE_TIME", "WAVE_CELL"))
                                    w_vel2 = create_netCDF_var(ncOut, "VELOCITY2", "f4", "velocity", "m/s", ("WAVE_TIME", "WAVE_CELL"))
                                    w_vel3 = create_netCDF_var(ncOut, "VELOCITY3", "f4", "velocity", "m/s", ("WAVE_TIME", "WAVE_CELL"))

                                    awac_wave_data = True

                                print(dt, 'wave cells', wave_cells)

                                nc_wave_times[wave_sample_time] = date2num(dt, calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC")

                                wave_cell = 0

                            if packet_id['AWAC Wave Data'] == id:
                                    # print('wave data', sample_count, wave_cell)

                                    if wave_cell < wave_cells:
                                        wave_pres[wave_sample_time, wave_cell] = d['pressure']/1000
                                        w_vel1[wave_sample_time, wave_cell] = d['vel1']/1000
                                        w_vel2[wave_sample_time, wave_cell] = d['vel2']/1000
                                        w_vel3[wave_sample_time, wave_cell] = d['vel3']/1000

                                    wave_cell += 1

                        except KeyError as err:
                            print(traceback.format_exc())
                            print('packet_decode not found ', id)

                        if sample_count > 0 and sample_count % 1000 == 0:
                            print(sample_count, dt, num2date(ncTimesOut[sample_count-1], calendar='gregorian', units="days since 1950-01-01 00:00:00 UTC"), packet_decoder[id]["name"] )

                        #if sample_count > 10000:
                        #    break
                else:
                    no_sync += 1
                    #if no_sync > 100:
                    #    print("no sync found in first 100 bytes, maybe not a nortek file")
                    #    return None

                data = binary_file.read(1)
                bad_ck_pos = binary_file.tell()
                #print('tell', bad_ck_pos)


        print('samples', sample_count)

        ncOut.instrument = 'Nortek ; ' + instrument_model
        ncOut.instrument_model = instrument_model
        ncOut.instrument_serial_number = instrument_serialnumber
        ncOut.instrument_head_serial_number = instrument_head_serialnumber
        ncOut.coord_system = coord_systems[coord_system]
        ncOut.timing_controller_mode = timing_controller_mode
        ncOut.head_config = head_config

        # add any attributes we collected from the file
        for a in attribute_list:
            ncOut.setncattr(a[0], a[1])

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

        # add creating and history entry
        ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
        ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

        ncOut.close()

        output_files.append(outputName)

    return output_files


if __name__ == "__main__":
    parse_file(sys.argv[1:])

