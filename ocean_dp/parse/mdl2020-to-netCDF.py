#!/usr/bin/python3

# gps_imu_2020
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
import pynmea2

from ubxtranslator import core
from ubxtranslator.predefined import NAV_CLS, ACK_CLS

import sys
import struct
import io

import threading
import time
from queue import Queue

from ubxtranslator.core import Message, Cls, PadByte, Field, Flag, BitField, RepeatedBlock

# #define SENSOR_TYPE 		    0
# #define GYRO_DATA_TYPE 		1
# #define ACCEL_MAG_DATA_TYPE   2
# #define ADC_DATA_TYPE 		3
# #define TIME_DATA_TYPE 		4
# #define SERIAL_DATA_TYPE 	    5
#
# //------------------------------------------------------------------------------
# // data buffers
# //------------------------------------------------------------------------------
#
# #define BUF_SAMPLES 246
#
# #define ADC_MAGIC 		0xADC0
# #define GYRO_MAGIC 		0xFACE
# #define ACCEL_MAGIC 	    0xACC1
# #define SENSOR_MAGIC 	    0x1234
# #define SERIAL_MAGIC 	    0xBEAF

# struct hdr_t
# {
# 	uint16_t magic;
# 	uint16_t len;
# 	uint16_t type;
# };
#
# struct hdr_ts_t
# {
# 	hdr_t hdr;
# 	time_t rtc;
# 	uint16_t samples;
# 	unsigned long time;
# 	unsigned long time1;
# };
#
# #define SER_SAMPLES 2048
#
# struct SerBuf
# {
# 	hdr_ts_t hdr;
# 	uint8_t samples[SER_SAMPLES];
# 	uint32_t dirty;
# } ;
#
# SerBuf serBuf[2];
# uint8_t serBufN;
# uint16_t serBufSample;
#
#
# struct AdcBuf
# {
# 	hdr_ts_t hdr;
# 	uint8_t ch0;
# 	long samples[BUF_SAMPLES*sizeof(adc_list)/sizeof(adc_list[0])];
# 	uint32_t dirty;
# } ;
#
# AdcBuf adcBuf[2];
# uint8_t adcBufN;
# uint16_t adcBufSample;
#
# struct GyroBuf
# {
# 	hdr_ts_t hdr;
# 	float samples[BUF_SAMPLES*3];
# 	uint32_t dirty;
# } ;
#
# GyroBuf gyroBuf[2];
# uint8_t gyroBufN;
# uint16_t gyroBufSample;
#
# struct AccelMagBuf
# {
# 	hdr_ts_t hdr;
# 	float accel[BUF_SAMPLES*3];
# 	float mag[BUF_SAMPLES*3];
# 	uint32_t dirty;
# } ;


def eeprom_crc(p):
    crc_table = (0x00000000, 0x1db71064, 0x3b6e20c8, 0x26d930ac, 0x76dc4190, 0x6b6b51f4, 0x4db26158, 0x5005713c,
                 0xedb88320, 0xf00f9344, 0xd6d6a3e8, 0xcb61b38c, 0x9b64c2b0, 0x86d3d2d4, 0xa00ae278, 0xbdbdf21c)

    crc = 0xffffffff

    for index in range(0, len(p)):
        crc = crc_table[(crc ^ p[index]) & 0x0f] ^ (crc >> 4)
        crc = crc_table[(crc ^ (p[index] >> 4)) & 0x0f] ^ (crc >> 4)
        crc = ~crc & 0xffffffff

        #print("CRC %s %d %s %d" % (hex(p[index]), p[index] & 0x0f, hex(crc), index))

    return crc


def mdl_2020(netCDFfiles):

    fn = netCDFfiles[0]
    error_count = 0
    bad_header = 0

    serial_data_file = open(fn + "-serial_data.bin", "wb")

    # adc output file
    outputName = fn + '-MDL-ADC.nc'
    print("adc output file : %s" % outputName)
    adc_nc = Dataset(outputName, 'w', format='NETCDF4')

    adc_nc.createDimension("TIME")
    adc_times = adc_nc.createVariable("TIME", "d", ("TIME",))
    adc_times.long_name = "time"
    adc_times.units = "days since 1950-01-01 00:00:00 UTC"
    adc_times.calendar = "gregorian"
    adc_times.axis = "T"

    adc_micros_var = adc_nc.createVariable("MICROS", "i4", ("TIME",))
    adc_pos_var = adc_nc.createVariable("FILE_POS", "i8", ("TIME",))

    adc_nc.createDimension("SAMPLE", 246)
    adc_var = adc_nc.createVariable("ADC", "f4", ("TIME", "SAMPLE"))

    adc_samples = 0

    # accel netcdf file
    outputName = fn + '-MDL-ACCEL.nc'
    print("accel output file : %s" % outputName)
    accel_nc = Dataset(outputName, 'w', format='NETCDF4')

    accel_nc.createDimension("TIME")
    accel_times = accel_nc.createVariable("TIME", "d", ("TIME",))
    accel_times.long_name = "time"
    accel_times.units = "days since 1950-01-01 00:00:00 UTC"
    accel_times.calendar = "gregorian"
    accel_times.axis = "T"

    accel_micros_var = accel_nc.createVariable("MICROS", "i4", ("TIME",))
    accel_pos_var = accel_nc.createVariable("FILE_POS", "i8", ("TIME",))

    accel_ser_pos_var = accel_nc.createVariable("SERIAL_POS", "i8", ("TIME",))

    accel_nc.createDimension("SAMPLE", 1476/2)
    accel_var = accel_nc.createVariable("ACCEL", "f4", ("TIME", "SAMPLE"))
    mag_var = accel_nc.createVariable("MAG", "f4", ("TIME", "SAMPLE"))

    accel_samples = 0

    # gyro netcdf file
    outputName = fn + '-MDL-GYRO.nc'
    print("gyro output file : %s" % outputName)
    gyro_nc = Dataset(outputName, 'w', format='NETCDF4')

    gyro_nc.createDimension("TIME")
    gyro_times = gyro_nc.createVariable("TIME", "d", ("TIME",))
    gyro_times.long_name = "time"
    gyro_times.units = "days since 1950-01-01 00:00:00 UTC"
    gyro_times.calendar = "gregorian"
    gyro_times.axis = "T"

    gyro_micros_var = gyro_nc.createVariable("MICROS", "i4", ("TIME",))
    gyro_pos_var = gyro_nc.createVariable("FILE_POS", "i8", ("TIME",))

    gyro_nc.createDimension("SAMPLE", 738)
    gyro_var = gyro_nc.createVariable("GYRO", "f4", ("TIME", "SAMPLE"))

    gyro_samples = 0

    t_micros_sync = None

    for fn in netCDFfiles:
        try:
            with open(fn, "rb") as f:
                # hdr = f.read(1024) # read the text header
                # hdr_lines = hdr.split(b'\n')
                # for l in hdr_lines:
                #     if l[0] != 0:
                #         print (l.decode("utf-8"))

                while True:
                    pos = f.tell()

                    print("pos ", pos)

                    byte = f.read(6)
                    if not byte:
                        break
                    (magic, plen, type) = struct.unpack("<HHH", byte)
                    print('magic 0x%04x type %d N %d' % (magic, type, plen))
                    if magic == 0x4441 and type == 20300:
                        hdr = f.read(1024-6)
                        hdr_lines = hdr.split(b'\n')
                        for l in hdr_lines:
                            if l[0] != 0:
                                print (l.decode("utf-8"))
                        continue

                    if magic in (0xface, 0xadc0, 0xacc1, 0xBEAF) and plen > 0:
                        # read the packet data
                        pkt = f.read(plen - 6)
                        #print("len ", plen, " read ", len(pkt) + 6)

                        # read the packet CRC
                        check_bytes = f.read(4)
                        (check,) = struct.unpack("<I", check_bytes)
                        calc_crc = eeprom_crc(byte + pkt)
                        #print("check ", hex(check), " calc ", hex(calc_crc))
                        if check != calc_crc:
                            f.seek(pos + 1)
                            #print('skipping', pos)

                            error_count += 1
                            continue

                        # decode the header
                        hdr_t = struct.unpack("<HIIHHIII", pkt[0:26])
                        no_samples = hdr_t[3]
                        time_sync = hdr_t[5]
                        time_sample = hdr_t[6]
                        #print(hdr_t)
                        t_utc = datetime.utcfromtimestamp(hdr_t[1])

                        packet_type = 'unknown'
                        # decode the packet data
                        if magic == 0xface:
                            #print("GYRO packet",len(pkt[32-6:32-6+no_samples*4]))
                            gyro_raw = struct.unpack("<"+str(no_samples)+"f", pkt[32-6:32-6+no_samples*4])
                            #print(gyro_raw[0:3])
                            (dirty,) = struct.unpack("<I", pkt[-8:-4])
                            #print('dirty ', hex(dirty))
                            #print("rtc ", hex(rtc), " no_samples ", no_samples, " time ", hex(time), " time1 ", hex(time1))
                            packet_type = 'GYRO'
                            #print(time_sync, 'gyro no_samples', len(gyro_raw), gyro_raw[0:3])

                            gyro_times[gyro_samples] = date2num(t_utc, gyro_times.units, gyro_times.calendar)
                            gyro_var[gyro_samples] = gyro_raw
                            gyro_micros_var[gyro_samples] = time_sync
                            gyro_pos_var[gyro_samples] = pos
                            gyro_samples += 1

                        elif magic == 0xadc0:
                            #print("ADC packet",len(pkt[32-6:32-6+no_samples*4]))
                            (adc_ch,) = struct.unpack("<B", pkt[32-6:32-6+1])
                            adc_raw = struct.unpack("<"+str(no_samples)+"i", pkt[32-6+4:32-6+4+no_samples*4])
                            #print(adc_ch, adc_raw[0:10])
                            (dirty,) = struct.unpack("<I", pkt[-4:])
                            #print('dirty ', hex(dirty))
                            packet_type = 'ADC'

                            #print(time_sync, 'adc no_samples', len(adc_raw), adc_raw[0:3])

                            adc_times[adc_samples] = date2num(t_utc, adc_times.units, adc_times.calendar)
                            adc_var[adc_samples] = adc_raw
                            adc_micros_var[adc_samples] = time_sync
                            adc_pos_var[adc_samples] = pos
                            adc_samples += 1

                            #for i in pkt:
                            #    print(hex(i))

                        elif magic == 0xacc1:
                            #print("accel packet",len(pkt[32-6:32-6+no_samples*4*2]))
                            accel_raw = struct.unpack("<"+str(no_samples*2)+"f", pkt[32-6:32-6+no_samples*4*2])
                            #print(accel_raw[0:3])
                            (dirty,) = struct.unpack("<I", pkt[-8:-4])
                            #print('dirty ', hex(dirty))
                            packet_type = 'ACCEL'
                            mid = int(no_samples)
                            #print(time_sync, 'accel no_samples', no_samples, len(accel_raw), accel_raw[0:3], accel_raw[mid:mid+3])

                            accel_times[accel_samples] = date2num(t_utc, adc_times.units, adc_times.calendar)
                            accel_var[accel_samples] = accel_raw[0:mid]
                            mag_var[accel_samples] = accel_raw[mid:no_samples+mid]
                            accel_micros_var[accel_samples] = time_sync
                            accel_pos_var[accel_samples] = pos
                            accel_ser_pos_var[accel_samples] = serial_data_file.tell()
                            accel_samples += 1

                        elif magic == 0xBEAF:
                            #print("serial packet")
                            print("serial pos", serial_data_file.tell())
                            serial_data_file.write(pkt[32-6:32-6+no_samples])
                            packet_type = 'SERIAL'
                            (dirty,) = struct.unpack("<I", pkt[-4:])

                        print(t_utc.strftime("%Y-%m-%d %H:%M:%S"), packet_type, "no_samples", no_samples,"micros_sync", time_sync, "micros_sample", time_sample, "dt", time_sync - time_sample)

                    else:
                        f.seek(pos+1)
                        #print('skipping', pos)
                        bad_header += 1
                        continue
        except struct.error as e:
            print(e)

        print("file errors", error_count)

    serial_data_file.close()
    adc_nc.close()
    accel_nc.close()
    gyro_nc.close()

    print('number samples: accel, gyro, adc', accel_samples, gyro_samples, adc_samples)

if __name__ == "__main__":
    mdl_2020(sys.argv[1:])
