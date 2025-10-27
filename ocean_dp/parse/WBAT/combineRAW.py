import struct
import sys
from datetime import datetime, timedelta

import numpy as np
from glob2 import glob
from netCDF4 import Dataset, date2num


# https://www.kongsbergdiscovery.online/ek80/interface/int_specs_en_a4.pdf

def unpack_float(d):
    dx = bytearray(d[:2])
    dx.append(0)
    dx.append(0)
    print(' '.join('{:02x}'.format(x) for x in dx))
    (data,) = struct.unpack('>f', dx)

    return data


# long Length;
# struct DatagramHeader
# {
#   long DatagramType;
#   struct {
#           long LowDateTime;
#           long HighDateTime;
#       } DateTime;
# };
# - -
# datagram content
# - -
# long Length;
# };

def read_packet(file):
    pkt_len_bytes = file.read(4)
    d = None

    if pkt_len_bytes:
        pkt_len, = struct.unpack('<l', pkt_len_bytes)

        print('packet len', pkt_len)

        d = file.read(pkt_len)

        if len(d) != pkt_len:
            return None
        
        pkt_len_end = file.read(4)

        if pkt_len_end != pkt_len:
            return None

    return d


def write_packet(file, length, data):
    len_long = struct.pack('<l', length)
    file.write(len_long)
    file.write(data)
    file.write(len_long)


def decode_hdr(packet):

    if len(packet) < 12:
        return None

    datagram_type, ts = struct.unpack('<4sq', packet[0:12])
    content = packet[12:]

    # decode the time into a datetime
    # us = ((ts_h * 2**32) + ts_l)/10
    us = ts / 10
    dt = datetime(1601, 1, 1) + timedelta(microseconds=us)

    print(' datagram', datagram_type, ts, dt)

    return {'time': dt, 'ts': ts, 'datagram_type': datagram_type, 'content': content}


def pack_datagram(ts, datagram_type, content):

    # ts = int((dt - datetime(1601, 1, 1)).total_seconds() * 1e6 * 10)
    # td = dt - datetime(1601, 1, 1)
    # total_microseconds = (td.days * 24 * 60 * 60 * 1_000_000) + (td.seconds * 1_000_000) + td.microseconds
    # print('total_microseconds', total_microseconds)

    print(' pack datagram', datagram_type, ts)

    packet = struct.pack('<4sq', datagram_type, ts)
    packet += content

    print(' pack length', len(packet))
    return packet


def decode_XML(datagram):

    datagram_content = datagram.decode('utf-8')
    print('  XML')
    print()
    print(datagram_content)

    return {'type': 'XML0', 'datagram_content': datagram_content}


def decode_FIL(datagram):
    stage, spare, filter_type, channel_id, no_of_coeff, decimation_factor = struct.unpack('<h1cb128shh', datagram[0:136])
    # print(Stage, ChannelID)
    print('  FIL1:', stage, channel_id.decode('utf-8').strip('\x00'), filter_type, 'NoOfCoeff', no_of_coeff, 'DecimationFactor', decimation_factor)

    pos = 136
    #print('   FIL1 data len', len(datagram[pos:]))

    filter_coeff = struct.unpack('<' + str(no_of_coeff*2) + 'f', datagram[pos:])

    #for i in range(0, no_of_coeff):
    #    print('    filter', i, 'coeff', filter_coeff[i])

    return {'type': 'FIL1', 'stage': stage, 'no_coeff': no_of_coeff, 'decimation_factor': decimation_factor}


# Gordon Keith:
#  My current understanding of Datatype & 0xf
#  1 = power values 2 bytes per sample (short)
#  3 = power and angle, 4 bytes per sample (short;byte,byte)
#  4 = 16 bit complex, (Datatype >> 8) * 4 bytes per sample (float16,float16)
#  8 = 32 bit complex, (Datatype >> 8) * 8 bytes per sample (float32,float32)
#  otherwise - I don't know


def decode_RAW3(datagram):
    channel_id, datatype, spare, offset, count = struct.unpack('<128sh2sll', datagram[0:140])
    data_t = "Power" if (datatype & 0x01) else ""
    data_t += "Angle" if (datatype & 0x02) else ""
    data_t += "ComplexFloat16" if (datatype & 0x04) else ""
    data_t += "ComplexFloat32" if (datatype & 0x08) else ""

    data_samples = datatype >> 8
    print('  RAW3: ', '"' + channel_id.decode('utf-8').strip('\x00') + '"', datatype, 'offset', offset, 'count', count, data_t, 'n_complex_samples', data_samples)

    pos = 140
    # tx_data = np.zeros(16*data_samples*4)
    # p = pos
    # for i in range(0, 16):
    #     for n in range(0, data_samples*4):
    #         tx_data[i*data_samples*2+n] = unpack_float(datagram[p:p+2])
    #         print('   RAW3: TX data', i, n, tx_data[i*data_samples*2+n])
    #         p += 2
    #
    # print('  RAW3: pos', pos, p)
    # # NB first samples are TX samples, they are 2 x 16 bit float current/voltage
    # #pos = pos + (16 * data_samples * 2 * 4)  # 16 samples are TX pulse (for 2 us pulse), each channel, complex 32 bit data
    # pos = p

    print('  RAW3 data len', len(datagram[pos:]))

    samples = None
    if datatype & 0x08:
        samples = struct.unpack('<' + str(data_samples * count * 2) + 'f', datagram[pos:])

    print('  RAW3: ', samples[0:4])

    # for i in range(0, 20):
    #     if datatype & 0x08:
    #         for j in range(0, data_samples):
    #             print('  samples', i, samples[i*2], samples[i*2+1])

    return {'type': 'RAW3', 'channel': channel_id.decode('utf-8').strip('\x00'), 'offset': offset, 'count': count, 'n_complex_samples': data_samples, 'samples': samples}


decode_pkt = {b'XML0': decode_XML, b'RAW3': decode_RAW3, b'FIL1': decode_FIL}

def decode_datagram(datagram_type, datagram):

    datagram_content = decode_pkt[datagram_type](datagram)

    return datagram_content


first_config = False
first_env = False


def parseRAW(file, out_file):

    global first_config, first_env

    print('file name', file)

    f = open(file, 'rb')
    config_xml = None
    env_xml = None
    par_xml = None
    first_time = None

    resample_3_4 = False

    raw_samples = 0

    packet = read_packet(f)
    while packet:
        datagram = decode_hdr(packet)

        data = decode_datagram(datagram['datagram_type'], datagram['content'])
        if data['type'] == 'FIL1':
            stage = data['stage']
            no_of_coeff = data['no_coeff']
            decimation_factor = data['decimation_factor']
            if stage == 1:
                N1 = no_of_coeff
                D1 = decimation_factor
            if stage == 2:
                N2 = no_of_coeff
                D2 = decimation_factor

                total_filter_delay = (N1 / 2 / D1 + N2 / 2) / D2
                print('   total filter delay', total_filter_delay)

        data_out = datagram['content']

        # re-write data
        if resample_3_4:
            if data['type'] == 'RAW3':
                old_samples = int((len(data_out) - 140) / 4 / 4 / 2)
                new_samples = int(old_samples * 3 / 4)
                print('   RAW3: new_samples, old_samples', new_samples, old_samples)
                new_len = (new_samples * 4 * 4 * 2) + 140
                data_out = bytearray(new_len)

                # copy the header
                data_out[0:140] = datagram['content'][0:140]

                # modify the data_type pos from 1032 (complex32 4 channel to complex32 3 channel)
                data_type_pos = 128
                old_dt = struct.unpack('<h', data_out[data_type_pos:data_type_pos+2])
                print(' new n_complex', 776, old_dt)
                data_out[data_type_pos:data_type_pos+2] = struct.pack('<h', 776)

                # now shuffle samples
                #  channel old 4 : new 3
                #    samples 2 (real imag)
                #       complex 4 bytes
                samples_pos = 140

                for i in range(0, old_samples):
                    #print('   RAW3: shuffle', i)
                    data_out[samples_pos + (i*4*3*2):samples_pos + (i*4*3*2)+8*3] = datagram['content'][samples_pos + (i*4*4*2):samples_pos + (i*4*4*2)+8*3]

        # remove deplicate XML0 packets
        if data['type'] == 'XML0':
            if 'Configuration' in data['datagram_content']:
                print('   XML0: ', data['datagram_content'])
                if first_config:
                    data_out = None
                first_config = True
            elif 'Environment' in data['datagram_content']:
                print('   XML0: ', data['datagram_content'])
                if first_env:
                    data_out = None
                first_env = True
            elif 'Parameter' in data['datagram_content']:
                print('   XML0: ', data['datagram_content'])

        # write out to combined file
        if data_out is not None:
            packet_out = pack_datagram(datagram['ts'], datagram['datagram_type'], data_out)

            write_packet(out_file, len(packet_out), packet_out)

        #print(' write', out_file.tell(), f.tell())

        # get next packet
        packet = read_packet(f)

    print()


if __name__ == "__main__":


    out_file = open('outfile.raw', 'wb')

    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    for f in files:
        parseRAW(f, out_file)

    out_file.close()

