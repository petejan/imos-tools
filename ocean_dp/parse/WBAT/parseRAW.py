import struct
import sys
from datetime import datetime, timedelta


def parseRAW(file):

    dt = '01cb17701e9c885a'

    us = int(dt, 16) / 10
    print()

    print(file)

    f = open(file, 'rb')

    hdr_len = 4*4
    d = f.read(hdr_len)
    while d:

        len, type, ts_l, ts_h = struct.unpack('<l4sll', d)

        us = ((ts_h * 2**32) + ts_l)/10
        dt = datetime(1601, 1, 1) + timedelta(microseconds=us)

        print(len, type, dt)

        d = f.read(len-8-4)

        if type == b'XML0':
            xml = d.decode('utf-8')
            print(xml)
        if type == b'RAW3':
            ChannelID, Datatype, SP, Offset, Count = struct.unpack('<128sh2sll', d[0:140])
            print('RAW3: ', ChannelID.decode('utf-8'), Datatype, Offset, Count)
        if type == b'FIL1':
            Stage, SP1, FilterType, ChannelID, NoOfCoeff, DecimationFactor = struct.unpack('<h1cb128shh', d[0:136])
            print(Stage, ChannelID)
            print('FIL1:', ChannelID.decode('utf-8'), FilterType, NoOfCoeff, DecimationFactor)

        d_end = f.read(4)
        len_end, = struct.unpack('<l', d_end)

        print('len_end', len_end)

        d = f.read(hdr_len)


if __name__ == "__main__":

    parseRAW(sys.argv[1])
