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

import io
import threading
import time
from datetime import datetime, timedelta
from queue import Queue

import sys

import serial
from cftime import date2num
from netCDF4 import Dataset

from ubxtranslator.core import Parser
from ubxtranslator import core

from ubxtranslator.predefined import NAV_CLS, ACK_CLS

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


def run(fn):
    #port = serial.Serial('[your port here]', baudrate=9600, timeout=0.1)
    #port = open(fn, "rb")

    parser = core.Parser([
        ACK_CLS,
        NAV_CLS,
        RXM,
        NAV_S
    ])

    # TIMEUTC output file
    outputName = fn + '-MDL-TIME.nc'
    print("TIME output file : %s" % outputName)
    time_nc = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    time_nc.createDimension("TIME")
    utc_times = time_nc.createVariable("TIME", "d", ("TIME",))
    utc_times.long_name = "time"
    utc_times.units = "days since 1950-01-01 00:00:00 UTC"
    utc_times.calendar = "gregorian"
    utc_times.axis = "T"

    utc_pos_var = time_nc.createVariable("FILE_POS", "i8", ("TIME",))
    utc_iTOW_var = time_nc.createVariable("iTOW", "i8", ("TIME",))
    utc_nano_var = time_nc.createVariable("nano", "i8", ("TIME",))

    utc_samples = 0

    # SOL output file
    outputName = fn + '-MDL-SOL.nc'
    print("SOL output file : %s" % outputName)
    sol_nc = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    sol_nc.createDimension("TIME")
    sol_times = sol_nc.createVariable("TIME", "d", ("TIME",))
    sol_times.long_name = "time"
    sol_times.units = "days since 1950-01-01 00:00:00 UTC"
    sol_times.calendar = "gregorian"
    sol_times.axis = "T"

    sol_iTOW_var = sol_nc.createVariable("iTOW", "i8", ("TIME",))
    sol_fTOW_var = sol_nc.createVariable("fTOW", "i8", ("TIME",))
    sol_week_var = sol_nc.createVariable("week", "i2", ("TIME",))
    sol_fpos_var = sol_nc.createVariable("FILE_POS", "i8", ("TIME",))

    sol_nc.createDimension("XYZ", 3)

    sol_pos_var = sol_nc.createVariable("POS", "i8", ("TIME", "XYZ"))

    sol_samples = 0

    dt = None
    try:
        with open(fn, "rb") as f:
            cont = True
            while cont:
                try:
                    msg = parser.receive_from(f)
                    if msg:
                        if msg[1] == 'TIMEUTC':
                            print(f.tell(), msg)
                            utc_pos_var[utc_samples] = f.tell()
                            utc_iTOW_var[utc_samples] = msg[2].iTOW
                            utc_nano_var[utc_samples] = msg[2].nano
                            dt = datetime(msg[2].year, msg[2].month, msg[2].day, msg[2].hour, msg[2].min, msg[2].sec)
                            utc_times[utc_samples] = date2num(dt, utc_times.units, utc_times.calendar)
                            utc_samples += 1
                        if msg[1] == 'SOL':
                            dt = datetime(1980,1,6,0,0,0) + timedelta(days=msg[2].week*7) + timedelta(seconds=msg[2].iTOW*1e-3 + msg[2].fTOW*1e-9)
                            print('SOL', dt)
                            sol_times[sol_samples] = date2num(dt, sol_times.units, sol_times.calendar)
                            sol_iTOW_var[sol_samples] = msg[2].iTOW
                            sol_fTOW_var[sol_samples] = msg[2].fTOW
                            sol_week_var[sol_samples] = msg[2].week
                            sol_pos_var[sol_samples] = [msg[2].ecefX, msg[2].ecefY, msg[2].ecefZ]
                            sol_fpos_var[sol_samples] = f.tell()
                            sol_samples += 1
                    else:
                        cont = False
                except ValueError as ex:
                    print(ex)
    except OSError as ex:
        print(ex)

    finally:
        f.close()
        time_nc.close()
        print('samples utc, sol', utc_samples, sol_samples)


if __name__ == "__main__":
    run(sys.argv[1])

