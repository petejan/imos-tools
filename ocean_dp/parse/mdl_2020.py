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

RXM = Cls(0x02, 'RXM', [
    Message(0x10, 'RAW', [
        Field('iTOW', 'U4'),
        Field('week', 'U2'),
        Field('numSV', 'U1'),
        PadByte(),
        RepeatedBlock('RB', [
            Field('cpMes', 'R8'),
            Field('prMes', 'R8'),
            Field('doMes', 'R4'),
            Field('sv', 'U1'),
            Field('messQI', 'I1'),
            Field('cno', 'I1'),
            Field('lli', 'U1')
            ])
     ]),
    Message(0x15, 'RAWX', [
        Field('iTOW', 'U4'),
        Field('week', 'U2'),
        Field('leapS', 'I1'),
        Field('numMeas', 'U1'),
        BitField('recStat', 'X1', [Flag('data', 0, 7)]),
        PadByte(),
        PadByte(),
        PadByte(),
        RepeatedBlock('RB', [
            Field('prMes', 'R8'),
            Field('cpMes', 'R8'),
            Field('doMes', 'R4'),
            Field('gnssId', 'U1'),
            Field('sv', 'U1'),
            PadByte(),
            Field('freqId', 'U1'),
            Field('locktime', 'U2'),
            Field('cno', 'I1'),
            BitField('prStdev', 'X1', [Flag('data', 0, 4)]),
            BitField('cpStdev', 'X1', [Flag('data', 0, 4)]),
            BitField('doStdev', 'X1', [Flag('data', 0, 4)]),
            BitField('trkStat', 'X1', [Flag('data', 0, 4)]),
            PadByte()
            ])
     ]),
    Message(0x11, 'SFRB', [
        Field('chn', 'U1'),
        Field('svid', 'U1'),
        BitField('dwrd0', 'X4', [Flag('data', 0, 23)]),
        BitField('dwrd1', 'X4', [Flag('data', 0, 23)]),
        BitField('dwrd2', 'X4', [Flag('data', 0, 23)]),
        BitField('dwrd3', 'X4', [Flag('data', 0, 23)]),
        BitField('dwrd4', 'X4', [Flag('data', 0, 23)]),
        BitField('dwrd5', 'X4', [Flag('data', 0, 23)]),
        BitField('dwrd6', 'X4', [Flag('data', 0, 23)]),
        BitField('dwrd7', 'X4', [Flag('data', 0, 23)]),
        BitField('dwrd8', 'X4', [Flag('data', 0, 23)]),
        BitField('dwrd9', 'X2', [Flag('data', 0, 16)]),
    ]),
    Message(0x13, 'SFRBX', [
        Field('gnssId', 'U1'),
        Field('svid', 'U1'),
        PadByte(),
        Field('numWords', 'U1'),
        PadByte(),
        Field('version', 'U1'),
        PadByte(),
        RepeatedBlock('RB', [
            Field('dwrd', 'U4'),
            ])
    ])
    ])

NAV_S = Cls(0x01, 'NAV', [
    Message(0x03, 'STATUS', [
        Field('iTOW', 'U4'),
        Field('gpsFix', 'U1'),
        BitField('flags', 'X1', [Flag('gpsFixOk', 0, 1 ), Flag('diffSoln', 1, 2 ), Flag('wknSet', 2, 3 ), Flag('towSet', 3, 4 )]),
        BitField('flagStat', 'X1', [Flag('data', 0, 8 )]),
        BitField('flags2', 'X1', [Flag('data', 0, 8 )]),
        Field('ttf', 'U4'),
        Field('msss', 'U4'),
    ]),
    Message(0x06, 'SOL', [
        Field('iTOW', 'U4'),
        Field('fTOW', 'I4'),
        Field('week', 'I2'),
        Field('gpsFix', 'U1'),
        BitField('flags', 'X1',
                 [Flag('gpsFixOk', 0, 1), Flag('diffSoln', 1, 2), Flag('wknSet', 2, 3), Flag('towSet', 3, 4)]),
        Field('ecefX', 'I4'),
        Field('ecefY', 'I4'),
        Field('ecefZ', 'I4'),
        Field('pAcc', 'U4'),
        Field('ecefVX', 'I4'),
        Field('ecefVY', 'I4'),
        Field('ecefVZ', 'I4'),
        Field('sAcc', 'U4'),
        Field('pDOP', 'U2'),
        PadByte(),
        Field('numSV', 'U1'),
        PadByte(repeat=3),
    ]),
    Message(0x13, 'HPPOSECEF', [
        Field('version', 'U1'),
        PadByte(),
        PadByte(),
        PadByte(),
        Field('iTOW', 'U4'),
        Field('ecefX', 'I4'),
        Field('ecefY', 'I4'),
        Field('ecefZ', 'I4'),
        Field('ecefPX', 'I1'),
        Field('ecefPY', 'I1'),
        Field('ecefPZ', 'I1'),
        BitField('flags', 'X1',
                 [Flag('invalid_ecef', 0, 1)]),
        Field('pAcc', 'U4'),
    ]),
    Message(0x07, 'PVT', [
        Field('iTOW', 'U4'),
        Field('year', 'U2'),
        Field('month', 'U1'),
        Field('day', 'U1'),
        Field('hour', 'U1'),
        Field('min', 'U1'),
        Field('sec', 'U1'),
        BitField('valid', 'X1',
                 [Flag('valid_date', 0, 1), Flag('valid_time', 1, 2), Flag('fully_resolved', 2, 3), Flag('valid_mag', 3, 4)]),
        Field('tAcc', 'U4'),
        Field('nano', 'I4'),
        Field('fixType', 'U1'),
        BitField('flags', 'X1',
                 [Flag('invalid_ecef', 0, 1)]),
        BitField('flags2', 'X1',
                 [Flag('invalid_ecef', 0, 1)]),
        Field('numSV', 'U1'),
        Field('lon', 'I4'),
        Field('lat', 'I4'),
        Field('height', 'I4'),
        Field('hMSL', 'I4'),
        Field('hACC', 'U4'),
        Field('vACC', 'U4'),
        Field('velN', 'I4'),
        Field('velE', 'I4'),
        Field('velD', 'I4'),
        Field('gSpeed', 'I4'),
        Field('headMot', 'I4'),
        Field('sACC', 'U4'),
        Field('headAcc', 'U4'),
        Field('pDOP', 'U2'),
        BitField('flags3', 'X1',
                 [Flag('invalidLlh', 0, 1)]),
        PadByte(),
        PadByte(),
        PadByte(),
        PadByte(),
        PadByte(),
        Field('headVeh', 'I4'),
        Field('magDec', 'I2'),
        Field('magAcc', 'U2'),
    ]),
    Message(0x21, 'TIMEUTC', [
        Field('iTOW', 'U4'),
        Field('tAcc', 'U4'),
        Field('nano', 'I4'),
        Field('year', 'U2'),
        Field('month', 'U1'),
        Field('day', 'U1'),
        Field('hour', 'U1'),
        Field('min', 'U1'),
        Field('sec', 'U1'),
        BitField('valid', 'X1',
                 [Flag('valid_tow', 0, 1), Flag('valid_WKN', 1, 2), Flag('valid_UTC', 2, 3),
                  Flag('utc_standard', 7, 8)]),
    ])
])

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


class BytesLoop:
    def __init__(self, s=b''):
        self.buffer = s

    def read(self, n=-1):
        chunk = self.buffer[:n]
        self.buffer = self.buffer[n:]
        return chunk

    def write(self, s):
        self.buffer += s

    def remaining(self):
        return len(self.buffer)


def worker(port, ubx_parser, q):
    while True:
        try:
            cls_name, msg_name, payload = ubx_parser.receive_from(port)
            print(cls_name, msg_name, payload)
            q.put_nowait(payload)
        except (ValueError, IOError) as err:
            print(err)


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

    ubx_parser = core.Parser([
        ACK_CLS,
        NAV_CLS,
        RXM,
        NAV_S
    ])

    serial_data_file = open(fn + ".serial_data.bin", "wb")

    stream_io_w = io.BytesIO()
    stream_io_r = io.BytesIO()
    stream_write = io.BufferedWriter(stream_io_w)
    stream_read = io.BufferedReader(stream_io_r)

    stream_ubx = io.BufferedRWPair(stream_read, stream_write)

    stream_loop = BytesLoop()

    #q = Queue()
    #thread = threading.Thread(target=worker, args=(bytes_stream, ubx_parser, q))

    #print('Starting the worker thread to listen for UBX packets.')
    #thread.start()

    with open(fn, "rb") as f:
        hdr = f.read(1024) # read the text header
        hdr_lines = hdr.split(b'\n')
        for l in hdr_lines:
            if l[0] != 0:
                print (l.decode("utf-8"))

        while True:
            pos = f.tell()

            #print("pos ", pos)

            byte = f.read(6)
            if not byte:
                break
            (magic, plen, type) = struct.unpack("<HHH", byte)
            print('magic 0x%04x type %d N %d' % (magic, type, plen))

            if magic in (0xface, 0xadc0, 0xacc1, 0xBEAF) and plen > 0:
                # read the packet data
                pkt = f.read(plen - 6)
                print("len ", plen, " read ", len(pkt) + 6)

                # read the packet CRC
                check_bytes = f.read(4)
                (check,) = struct.unpack("<I", check_bytes)
                calc_crc = eeprom_crc(byte + pkt)
                #print("check ", hex(check), " calc ", hex(calc_crc))
                if check != calc_crc:
                    f.seek(pos + 1)
                    print('skipping', pos)

                    error_count += 1
                    continue

                # decode the header
                hdr_t = struct.unpack("<HIIHHIII", pkt[0:26])
                samples = hdr_t[3]
                time_sync = hdr_t[5]
                time_sample = hdr_t[6]
                print(hdr_t)
                t_utc = datetime.utcfromtimestamp(hdr_t[1])

                packet_type = 'unknown'
                # decode the packet data
                if magic == 0xface:
                    print("GYRO packet",len(pkt[32-6:32-6+samples*4]))
                    gyro_raw = struct.unpack("<"+str(samples)+"f", pkt[32-6:32-6+samples*4])
                    print(gyro_raw[0:3])
                    (dirty,) = struct.unpack("<I", pkt[-8:-4])
                    print('dirty ', hex(dirty))
                    #print("rtc ", hex(rtc), " samples ", samples, " time ", hex(time), " time1 ", hex(time1))
                    packet_type = 'GYRO'

                elif magic == 0xadc0:
                    print("ADC packet",len(pkt[32-6:32-6+samples*4]))
                    (adc_ch,) = struct.unpack("<B", pkt[32-6:32-6+1])
                    adc_raw = struct.unpack("<"+str(samples)+"i", pkt[32-6+4:32-6+4+samples*4])
                    print(adc_ch, adc_raw[0:10])
                    (dirty,) = struct.unpack("<I", pkt[-4:])
                    print('dirty ', hex(dirty))
                    packet_type = 'ADC'
                    #for i in pkt:
                    #    print(hex(i))

                elif magic == 0xacc1:
                    print("accel packet",len(pkt[32-6:32-6+samples*4*2]))
                    accel_raw = struct.unpack("<"+str(samples*2)+"f", pkt[32-6:32-6+samples*4*2])
                    print(accel_raw[0:3])
                    (dirty,) = struct.unpack("<I", pkt[-8:-4])
                    print('dirty ', hex(dirty))
                    packet_type = 'ACCEL'

                elif magic == 0xBEAF:
                    print("serial packet")
                    serial_data_file.write(pkt[32-6:32-6+samples])
                    (dirty,) = struct.unpack("<I", pkt[-4:])
                    print('dirty ', hex(dirty))
                    packet_type = 'SERIAL'
                    # for i in pkt[26:80]:
                    #     print(hex(i))

                    # stream_loop.write(pkt[26:])
                    # #print(stream_ubx.tell())
                    # count = stream_loop.remaining()
                    # while count > 1024:
                    #     try:
                    #         cls_name, msg_name, payload = ubx_parser.receive_from(stream_loop)
                    #         print(cls_name, msg_name, payload)
                    #         print(stream_loop.remaining())
                    #         count = stream_loop.remaining()
                    #     except (ValueError, IOError) as err:
                    #         print(err)
                    #     #print ("stream tell ", bytes_stream.tell())

                print(t_utc.strftime("%Y-%m-%d %H:%M:%S"), packet_type, " micros_sync ", time_sync, " micros_sample ", time_sample, " dt ", time_sync - time_sample)

            else:
                f.seek(pos+1)
                print('skipping', pos)
                bad_header += 1
                continue

    print("file errors", error_count)

    serial_data_file.close()


if __name__ == "__main__":
    mdl_2020(sys.argv[1:])
