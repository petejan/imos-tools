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
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct

import ctypes
c_uint8 = ctypes.c_uint8
c_uint16 = ctypes.c_uint16


class Flags_bits( ctypes.LittleEndianStructure ):
    _fields_ = [
                ("profile",     c_uint8, 1),     # bit 1
                ("mode_burst", c_uint8, 1),      # bin 2
                ("not used", c_uint8, 2),        # bin 3, 4
                ("power",    c_uint8, 2),        # bit 5, 6
                ("sync_out",       c_uint8, 1),  # bit 7
                ("sample_on_sync", c_uint8, 1),  # bit 8
                ("start_on_sync", c_uint8, 1),   # bit 9
    ]

class Flags( ctypes.Union ):
    _anonymous_ = ("bit",)
    _fields_ = [
                ("bit",    Flags_bits ),
                ("asByte", c_uint16    )
               ]

time_ctrl_reg = Flags()
time_ctrl_reg.asByte = 0x2  # ->0010

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

# TODO: how to map the above into netCDF attributes....

packet_decode2netCDF = {}
packet_decode2netCDF[0] = {'decode': 'head_frequency', 'attrib': 'nortek_head_frequency'}
packet_decode2netCDF[1] = {'decode': 'T1', 'attrib': 'nortek_tx_pulse_length'}
packet_decode2netCDF[2] = {'decode': 'T2', 'attrib': 'nortek_blank_distance'}
packet_decode2netCDF[3] = {'decode': 'T3', 'attrib': 'nortek_receive_length'}
packet_decode2netCDF[4] = {'decode': 'T4', 'attrib': 'nortek_time_between_pings'}
packet_decode2netCDF[5] = {'decode': 'T5', 'attrib': 'nortek_time_bewteen_bursts'}
packet_decode2netCDF[6] = {'decode': 'NBeam', 'attrib': 'nortek_number_beams'}
packet_decode2netCDF[7] = {'decode': 'MeasInterval', 'attrib': 'nortek_mesurement_interval'}
packet_decode2netCDF[8] = {'decode': 'AvgInt', 'attrib': 'nortek_averaging_interval'}

attribute_list = []

velocity_data = []
coord_system = None

coord_systems = ['ENU', 'XYZ', 'BEAM']


def main(files):

    filepath = files[1]
    checksum_errors = 0
    no_sync = 0

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
                size = binary_file.read(2)
                l = struct.unpack("<H", size)
                l = l[0]
                checksum += l

                packet = binary_file.read(l*2 - 4)  # size in words, less the 4 we already read
                #print("len = ", l, len(packet))
                for i in range(0, (l-3)):
                    checksum += (struct.unpack("<H", packet[i*2:i*2+2]))[0]
                if checksum & 0xffff != (struct.unpack("<H", packet[-2:]))[0]:
                    print("check sum error ", bad_ck_pos, checksum & 0xffff, (struct.unpack("<H", packet[-2:])[0]))
                    checksum_errors += 1
                    if checksum_errors > 10:
                        print("too many errors, maybe not a nortek file")
                        exit(-1)
                    binary_file.seek(bad_ck_pos, 0)  # seek back to before packet
                else:
                    try:
                        #print(packet_decoder[id]['unpack'])
                        packetDecode = struct.unpack(packet_decoder[id]['unpack'], packet)
                        d = dict(zip(packet_decoder[id]['keys'], packetDecode))
                        #print(packet_decoder[id]['name'], d)

                        # decode and capture any datacodes
                        if 'time_bcd' in d:
                            ts_bcd = struct.unpack("<6B", d['time_bcd'])
                            y = []
                            for x in ts_bcd:
                                y.append(int((((x & 0xf0)/16) * 10) + (x & 0xf)))
                            dt = datetime.datetime(y[4]+2000, y[5], y[2], y[3], y[0], y[1])

                        if 'serial' in d:
                            instrument_serialnumber = d['serial'].decode("utf-8").strip()
                            print('instrument serial number ', instrument_serialnumber)

                        if 'head_serial' in d:
                            instrument_head_serialnumber = d['head_serial'].decode("utf-8").strip()
                            print('instrument head serial number ', instrument_head_serialnumber)

                        if 'CoordSys' in d:
                            coord_system = d['CoordSys']

                        for att in packet_decode2netCDF:
                            #print(packet_decode2netCDF[att])
                            if packet_decode2netCDF[att]['decode'] in d:
                                attribute_list.append((packet_decode2netCDF[att]["attrib"], float(d[packet_decode2netCDF[att]["decode"]])))

                        # create an array of all the data packets (this does copy them into memory)
                        if 'Aquadopp Velocity Data' == packet_decoder[id]['name']:
                            #print('velocity data')
                            velocity_data.append((dt, d))
                            #print(dt, d)

                    except KeyError:
                        print('packet_decode not found ', id)
            else:
                no_sync += 1
                if no_sync > 100:
                    print("no sync found in first 100 bytes, maybe not a nortek file")
                    exit(-1)

            data = binary_file.read(1)
            bad_ck_pos = binary_file.tell()
            #print('tell', bad_ck_pos)

    number_samples_read = len(velocity_data)
    print('data samples ', number_samples_read)

    if number_samples_read == 0:
        print("no samples, probably not a nortek aquadopp file")
        exit(-1)

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

        data_array[7][i] = ((d[1]['presMSB'] * 65563) + d[1]['presLSW']) * 0.001

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

    # create the netCDF file
    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    # add global attributes
    instrument_model = 'Aquadopp'

    ncOut.instrument = 'Nortek - ' + instrument_model
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
    ncTimesOut[:] = data_array[0] + 0.001/3600/24   # WTF

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

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    return outputName


if __name__ == "__main__":
    main(sys.argv)

