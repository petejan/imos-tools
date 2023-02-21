import struct
import sys
from datetime import datetime, timedelta

def unpack_float(d):
    dx = bytearray(d)
    dx.append(0)
    dx.append(0)
    #print(' '.join('{:02x}'.format(x) for x in dx))
    (data,) = struct.unpack('>f', dx)

    return data

def parseRAW(file):

    print()

    print(file)

    f = open(file, 'rb')

    hdr_len = 4*4
    d = f.read(hdr_len)
    while d:

        pkt_len, type, ts_l, ts_h = struct.unpack('<l4sll', d)

        us = ((ts_h * 2**32) + ts_l)/10
        dt = datetime(1601, 1, 1) + timedelta(microseconds=us)

        print(pkt_len, type, dt)

        d = f.read(pkt_len - 8 - 4)

        if type == b'XML0':
            xml = d.decode('utf-8')
            print(xml)
        if type == b'RAW3':
            ChannelID, Datatype, SP, Offset, Count = struct.unpack('<128sh2sll', d[0:140])
            data_t = "Power" if (Datatype & 0x01) else ""
            data_t += "Angle" if (Datatype & 0x02) else ""
            data_t += "ComplexFloat16" if (Datatype & 0x04) else ""
            data_t += "ComplexFloat32" if (Datatype & 0x08) else ""

            print('RAW3: ', '"' + ChannelID.decode('utf-8').strip('\x00') + '"', Datatype, 'offset', Offset, 'count', Count, data_t, 'n compex samples', Datatype >> 8)
            data_samples = Datatype >> 8
            pos = 141

            total_filter_delay = (N1/2/D1 + N2/2) / D2
            print('total filter delay', total_filter_delay)
            for i in range(1, 20):
                for j in range(0, data_samples):
                    i_real = unpack_float(d[pos:pos+2])
                    pos += 2
                    v_real = unpack_float(d[pos:pos+2])
                    pos += 2
                    i_imag = unpack_float(d[pos:pos+2])
                    pos += 2
                    v_imag = unpack_float(d[pos:pos+2])
                    pos += 2
                    print(i, i_real, v_real, i_imag, v_imag)

        if type == b'FIL1':
            stage, SP1, FilterType, ChannelID, NoOfCoeff, DecimationFactor = struct.unpack('<h1cb128shh', d[0:136])
            #print(Stage, ChannelID)
            print('FIL1:', stage, ChannelID.decode('utf-8'), FilterType, 'NoOfCoeff', NoOfCoeff, 'DecimationFactor', DecimationFactor)
            if stage == 1:
                N1 = NoOfCoeff
                D1 = DecimationFactor
            if stage == 2:
                N2 = NoOfCoeff
                D2 = DecimationFactor

        d_end = f.read(4)
        len_end, = struct.unpack('<l', d_end)

        print('len_end', len_end)

        d = f.read(hdr_len)


if __name__ == "__main__":

    parseRAW(sys.argv[1])
