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

from ubxtranslator import core
from ubxtranslator.predefined import NAV_CLS, ACK_CLS

import sys
import struct
import pynmea2

from ubxtranslator.core import Parser
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
                 [Flag('gnssFixOK', 0, 1), Flag('diffSoln', 1, 2), Flag('psmState', 4, 5), Flag('headVehValid', 5, 6), Flag('carrSoln', 7, 8)]),
        BitField('flags2', 'X1',
                 [Flag('confirmedAvail', 5, 6), Flag('confirmedDate', 6, 7), Flag('confirmedTime', 7, 8)]),
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

def parse(ubx_file):

    fn = ubx_file[0]

    parser = Parser([
        NAV_CLS, ACK_CLS, RXM, NAV_S
    ])

    sync = False
    sync_count = 0
    nmea_hdr = 0
    nmea = bytearray()

    ubx_errors = 0
    nmea_errors = 0

    with open(fn, "rb") as f:
        b = f.read(1)
        while b:
            print(f.tell(), hex(b[0]), sync_count, sync, nmea_hdr)
            if sync_count == 0 and b[0] == 0xb5:
                sync_count += 1
            elif sync_count == 1 and b[0] == 0x62:
                sync_count += 1
                sync = True
                f.seek(-2, 1)
                b = f.read(6)
                (sync, cl, id, len) = struct.unpack("<HBBH", b)
                print("sync found bytes, start ", hex(sync), "cl=", cl, "id=", id, "len=", len, ' file pos ', f.tell())
#                b = f.read(10)
#                for i in b:
#                    print(hex(i))
#                f.seek(-10, 1)

                f.seek(-6, 1)
            elif sync_count == 0 and nmea_hdr == 0 and b[0] == 0x24:
                nmea.append(0x24)
                nmea_hdr = 1
            elif nmea_hdr >= 1 and (b[0] == 0x0a or b[0] == 0x0d):
                if b[0] == 0x0a:
                    nmea_hdr = 0
                    print(nmea.decode("utf-8"))
                    try:
                        msg = pynmea2.parse(nmea.decode("utf-8"))
                        print("NMEA sentence", msg.sentence_type)
                    except pynmea2.nmea.ParseError:
                        print ("NMEA Parse error")
                        nmea_errors += 1
                    nmea.clear()
                else:
                    nmea_hdr += 1
            elif nmea_hdr >= 1 and (b[0] > 0x20 and b[0] < 127):
                    nmea.append(b[0])
                    nmea_hdr += 1
            else:
                sync_count = 0
                nmea_hdr = 0

            if sync:
                try:
                    ppos = f.tell()
                    cls_name, msg_name, payload = parser.receive_from(f)
                    print(cls_name, msg_name, payload)
                except (ValueError, IOError) as err:
                    print(err, " pos was ", ppos, " now ", f.tell())
                    f.seek(ppos+1, 0)
                    if str(err).find("unsuppored") >= 0:
                        print("unsuported")
                        f.seek(len+2, 1)
                    ubx_errors += 1

                #print("sync bytes, start ", hex(sync), "cl=", cl, "id=", id, "len=", len, ' file pos ', f.tell())
                sync = False
                sync_count = 0

            b = f.read(1)

    print("file Errors ", ubx_errors, " nmea ", nmea_errors)

if __name__ == "__main__":
    parse(sys.argv[1:])

