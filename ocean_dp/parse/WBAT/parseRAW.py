import struct
import sys
from datetime import datetime, timedelta

import numpy as np
from cftime import date2num
from glob2 import glob
from netCDF4 import Dataset


# https://www.kongsbergdiscovery.online/ek80/interface/int_specs_en_a4.pdf

def unpack_float(d, data_type):
    dx = bytearray(d[:4])
    #print(' '.join('{:02x}'.format(x) for x in dx))
    (data,) = struct.unpack('>f', dx)

    return data

size_tuple_set = list()
datagroup = None

def parseRAW(file, dataset, summary_file):

    print('file name', file)

    f = open(file, 'rb')

    summary_file.write(file)

    first_time = None

    sample_size_n = 1

    samples_in_file = 0

    dt = None
    N1 = None
    N2 = None

    hdr_len = 4*4
    hdr_data = f.read(hdr_len)
    note = ""
    while hdr_data:

        if len(hdr_data) < 16:
            note = 'not enough bytes to read header'
            break

        pkt_len, type, ts = struct.unpack('<l4sq', hdr_data)

        us = ts/10
        dt = datetime(1601, 1, 1) + timedelta(microseconds=us)
        if first_time is None:
            first_time = dt

        print(' packet', pkt_len, type, dt)

        d = f.read(pkt_len - 8 - 4)
        if not d:
            note = 'no more bytes'
            break

        d_end = f.read(4)
        # if len(d) != 4:
        #     print('not enough bytes to read end length')
        #     break

        len_end, = struct.unpack('<l', d_end)

        print(' len_end', len_end)

        if len_end != pkt_len:
            note = 'lengths dont match'
            break

        if type == b'XML0':
            xml = d.decode('utf-8')
            print('  XML')
            print()
            print(xml)

            if 'Configuration' in xml:
                config_xml = xml
            elif 'Environment' in xml:
                env_xml = xml
            elif 'Parameter' in xml:
                par_xml = xml


        if type == b'RAW3':
            channel_id, datatype, spare, offset, count = struct.unpack('<128sh2sll', d[0:140])
            data_t = "Power" if (datatype & 0x01) else ""
            data_t += "Angle" if (datatype & 0x02) else ""
            data_t += "ComplexFloat16" if (datatype & 0x04) else ""
            data_t += "ComplexFloat32" if (datatype & 0x08) else ""

            print('  RAW3: ', '"' + channel_id.decode('utf-8').strip('\x00') + '"', datatype, 'offset', offset, 'count', count, data_t, 'n_complex_samples', datatype >> 8)
            data_samples = datatype >> 8
            pos = 140

            size_tuple = (count, data_samples, data_t)
            print('  RAW3: this tuple', size_tuple)
            try:
                sample_size_n = size_tuple_set.index(size_tuple)
                datagroup = dataset.groups["ensemble"+str(sample_size_n)]

            except ValueError:
                size_tuple_set.append(size_tuple)
                sample_size_n = size_tuple_set.index(size_tuple)

                datagroup = dataset.createGroup("ensemble"+str(sample_size_n))

                if 'TIME' not in datagroup.dimensions:
                    time_dim = datagroup.createDimension('TIME')
                if 'COMPLEX' not in datagroup.dimensions:
                    complex_dim = datagroup.createDimension('COMPLEX', 2)

                # create the time array
                if 'TIME' not in datagroup.variables:
                    times = datagroup.createVariable('TIME', np.float64, ('TIME',))
                    times.units = 'days since 1950-01-01 00:00:00 UTC'
                    times.calendar = 'gregorian'

            time_var = datagroup.variables['TIME']
            raw_samples = time_var.size
            print('raw_samples', raw_samples)

            print('  RAW3: size, tuples', sample_size_n, size_tuple_set)

            if N1 is not None and N2 is not None:
                total_filter_delay = (N1/2/D1 + N2/2) / D2
                print('   total filter delay', total_filter_delay)

            samples = struct.unpack('<' + str(data_samples * count * 2) + 'f', d[pos:])

            for i in range(0, 5, 2):
                print('  samples', i, samples[i], samples[i+1])

            if 'SECTORS' not in datagroup.dimensions:
                sectors_dim = datagroup.createDimension('SECTORS', data_samples)

            if 'SAMPLES' not in datagroup.dimensions:

                samples_dim = datagroup.createDimension('SAMPLES', count)
                samples_var = datagroup.createVariable('SAMPLE', np.float64, ('TIME', 'SAMPLES', 'SECTORS', 'COMPLEX'))
            else:
                samples_var = datagroup.variables['SAMPLE']

            time_var[raw_samples] = date2num(dt, time_var.units, time_var.calendar)

            samples_var[raw_samples] = samples

            raw_samples += 1
            samples_in_file += 1

        if type == b'FIL1':
            stage, spare, filter_type, channel_id, no_of_coeff, decimation_factor = struct.unpack('<h1cb128shh', d[0:136])
            #print(Stage, ChannelID)
            print('  FIL1:', stage, channel_id.decode('utf-8').strip('\x00'), filter_type, 'NoOfCoeff', no_of_coeff, 'DecimationFactor', decimation_factor)
            if stage == 1:
                N1 = no_of_coeff
                D1 = decimation_factor
            if stage == 2:
                N2 = no_of_coeff
                D2 = decimation_factor
            pos = 136
            for i in range(0, no_of_coeff):
                filter_coeff, = struct.unpack('<f', d[pos:pos + 4])
                pos += 4
                #print('    filter', i, 'coeff', filter_coeff)

        hdr_data = f.read(hdr_len)

        print(' len_next', len(hdr_data))

    summary_file.write(',first_time,' + str(first_time) + ',last_time,' + str(dt) + ',samples_in_file,' + str(samples_in_file) + ',' + note + '\n')

    print()


if __name__ == "__main__":

    text_file = open("summary.txt", "w")
    dataset = Dataset('WBAT.nc', 'w', format='NETCDF4')

    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    for f in files:
        parseRAW(f, dataset, text_file)

    dataset.close()
    text_file.close()

