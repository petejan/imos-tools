import os
import struct
import sys
from datetime import datetime, timedelta

import numpy as np
from cftime import date2num
from glob2 import glob
from netCDF4 import Dataset
import xmltodict


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
    filters1 = {}
    filters2 = {}

    hdr_len = 4*4
    hdr_data = f.read(hdr_len)
    note = ""
    while hdr_data:

        if len(hdr_data) < 16:
            note = 'not enough bytes to read header'
            break

        pkt_len, pkt_type, ts = struct.unpack('<L4sQ', hdr_data)

        # some error checking
        if pkt_len - 8 - 4 < 0:
            note = 'negative packet length'
            break

        if ts > 2000000000000000000:
            note = 'timestamp out of range'
            break

        d = f.read(pkt_len - 8 - 4)
        if not d:
            note = 'no more bytes'
            break

        d_end = f.read(4)
        if len(d_end) != 4:
            print('not enough bytes to read end length')
            break

        len_end, = struct.unpack('<L', d_end)

        print(' len_end', len_end)

        if len_end != pkt_len:
            note = 'lengths dont match'
            break

        # convert timestamp to a datetime
        print(ts)
        us = ts/10
        dt = datetime(1601, 1, 1) + timedelta(microseconds=us)
        if first_time is None:
            first_time = dt

        print(' packet', pkt_len, pkt_type, dt)

        if pkt_type == b'XML0':
            xml = d.decode('utf-8')
            #print('  XML')
            #print()
            #print(xml)

            if 'Configuration' in xml:
                config_xml = xml
            elif 'Environment' in xml:
                env_xml = xml
            elif 'Parameter' in xml:
                par_xml = xml

        if pkt_type == b'RAW3':
            channel_id_raw, datatype, spare, offset, count = struct.unpack('<128sh2sll', d[0:140])
            data_t = "Power" if (datatype & 0x01) else ""
            data_t += "Angle" if (datatype & 0x02) else ""
            data_t += "ComplexFloat16" if (datatype & 0x04) else ""
            data_t += "ComplexFloat32" if (datatype & 0x08) else ""

            channel_id = channel_id_raw.decode('utf-8').strip('\x00')
            print('  RAW3: ', '"' + channel_id + '"', datatype, 'offset', offset, 'count', count, data_t, 'n_complex_samples', datatype >> 8)
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
            #print('raw_samples', raw_samples)

            #print('  RAW3: size, tuples', sample_size_n, size_tuple_set)

            samples = struct.unpack('<' + str(data_samples * count * 2) + 'f', d[pos:])

            #for i in range(0, 5, 2):
            #    print('  samples', i, samples[i], samples[i+1])

            if 'SECTORS' not in datagroup.dimensions:
                sectors_dim = datagroup.createDimension('SECTORS', data_samples)

            if 'SAMPLES' not in datagroup.dimensions:
                samples_dim = datagroup.createDimension('SAMPLES', count)
                samples_var = datagroup.createVariable('SAMPLE', np.float64, ('TIME', 'SAMPLES', 'SECTORS', 'COMPLEX'), fill_value=np.nan)
            else:
                samples_var = datagroup.variables['SAMPLE']

            # parse parameter group XML
            ordered_dict = xmltodict.parse(par_xml)
            #print('ordered_dict', ordered_dict)
            NumberSamplesPerPulse = float(ordered_dict['Parameter']['Channel']['@PulseDuration'])/float(ordered_dict['Parameter']['Channel']['@SampleInterval'])
            datagroup.FrequencyStart = float(ordered_dict['Parameter']['Channel']['@FrequencyStart'])
            datagroup.FrequencyEnd = float(ordered_dict['Parameter']['Channel']['@FrequencyEnd'])
            datagroup.PulseDuration = float(ordered_dict['Parameter']['Channel']['@PulseDuration'])
            datagroup.SampleInterval = float(ordered_dict['Parameter']['Channel']['@SampleInterval'])
            datagroup.NumberSamplesPerPulse = NumberSamplesPerPulse
            datagroup.TransmitPower = float(ordered_dict['Parameter']['Channel']['@TransmitPower'])
            datagroup.Slope = float(ordered_dict['Parameter']['Channel']['@Slope'])
            datagroup.ChannelID = ordered_dict['Parameter']['Channel']['@ChannelID']

            ns = int(np.ceil(NumberSamplesPerPulse))+18

            if 'TX_SAMPLES' not in datagroup.dimensions:
                datagroup.createDimension('TX_SAMPLES', ns)
                tx_v_samples_var = datagroup.createVariable('TX_V_SAMPLES', np.float32, ('TIME', 'TX_SAMPLES', 'SECTORS', 'COMPLEX'), fill_value=np.nan)
                tx_i_samples_var = datagroup.createVariable('TX_I_SAMPLES', np.float32, ('TIME', 'TX_SAMPLES', 'SECTORS', 'COMPLEX'), fill_value=np.nan)
            else:
                tx_v_samples_var = datagroup.variables['TX_V_SAMPLES']
                tx_i_samples_var = datagroup.variables['TX_I_SAMPLES']

            fx1 = filters1[channel_id]
            fx2 = filters2[channel_id]

            datagroup.filter_stage1_no_of_coeff = np.int32(fx1[0])
            datagroup.filter_stage2_no_of_coeff = np.int32(fx2[0])

            datagroup.filter_stage1_decimation_factor = np.int32(fx1[1])
            datagroup.filter_stage2_decimation_factor = np.int32(fx2[1])

            # if N1 is not None and N2 is not None:
            #     total_filter_delay = (N1/2/D1 + N2/2) / D2
            #     print('   total filter delay', total_filter_delay)

            if 'FILTER1' not in datagroup.dimensions:
                filter_dim = datagroup.createDimension('FILTER1', fx1[0])
                filter1_var = datagroup.createVariable('FILTER1', np.float64, ('FILTER1',), fill_value=np.nan)
            else:
                filter1_var = datagroup.variables['FILTER1']

            #print('  FILTER1: ', filter1_var, len(fx1[2]))
            filter1_var[:] = fx1[2]

            if 'FILTER2' not in datagroup.dimensions:
                filter_dim = datagroup.createDimension('FILTER2', fx2[0])
                filter2_var = datagroup.createVariable('FILTER2', np.float64, ('FILTER2',), fill_value=np.nan)
            else:
                filter2_var = datagroup.variables['FILTER2']

            #print('  FILTER2: ', filter2_var, len(fx2[2]))
            filter2_var[:] = fx2[2]

            time_var[raw_samples] = date2num(dt, time_var.units, time_var.calendar)

            samples_var[raw_samples] = samples

            # extract the transmit measurements
            #  tx voltage and tx current are packed in the 32 bit as 16 bit floating points
            tx_v = np.zeros(ns * data_samples * 2, dtype=np.float32)
            tx_i = np.zeros(ns * data_samples * 2, dtype=np.float32)
            for i in range(ns):
                for j in range(data_samples):
                    for k in range(2):
                        #print(i, j, k, samples[k + j * 2 + i * data_samples])
                        s = samples[k + j * 2 + i * data_samples * 2]
                        packed_float = struct.pack('>f', s)
                        padded_float = packed_float[0:2] + b'\00\00' + packed_float[2:4] + b'\00\00'
                        floats32 = struct.unpack('>ff', padded_float)
                        tx_v[k + j * 2 + i * data_samples * 2] = floats32[0]
                        tx_i[k + j * 2 + i * data_samples * 2] = floats32[1]
                        #tx_i_samples_var[raw_samples][i][j][k] = floats32[1]
                        #print(tx_v)
                        #print(raw_samples, i, j, k, s, floats32[0], floats32[1], tx_v_samples_var[raw_samples][i][j][k], tx_i_samples_var[raw_samples][i][j][k])

            tx_v_samples_var[raw_samples] = tx_v
            tx_i_samples_var[raw_samples] = tx_i
            #print(tx_v_samples_var[:])
            raw_samples += 1
            samples_in_file += 1

        # extract the filters
        if pkt_type == b'FIL1':
            stage, spare, filter_type, channel_id_raw, no_of_coeff, decimation_factor = struct.unpack('<h1cb128shh', d[0:136])
            #print(Stage, ChannelID)
            channel_id = channel_id_raw.decode('utf-8').strip('\x00')
            print('  FIL1:', stage, channel_id, filter_type, 'NoOfCoeff', no_of_coeff, 'DecimationFactor', decimation_factor)
            if stage == 1:
                N1 = no_of_coeff
                D1 = decimation_factor
                filter_coeff1 = np.zeros((N1,))
            if stage == 2:
                N2 = no_of_coeff
                D2 = decimation_factor
                filter_coeff2 = np.zeros((N2,))
            pos = 136
            for i in range(0, no_of_coeff):
                if stage == 1:
                    fc1, = struct.unpack('<f', d[pos:pos + 4])
                    filter_coeff1[i] = fc1
                if stage == 2:
                    fc2, = struct.unpack('<f', d[pos:pos + 4])
                    filter_coeff2[i] = fc2
                pos += 4
                #print('    filter', i, 'coeff', filter_coeff)
            if stage == 1:
                filters1[channel_id] = (N1, D1, filter_coeff1)
            if stage == 2:
                filters2[channel_id] = (N2, D2, filter_coeff2)

        hdr_data = f.read(hdr_len)

        print(' len_next', len(hdr_data))

    summary_file.write(',first_time,' + str(first_time) + ',last_time,' + str(dt) + ',samples_in_file,' + str(samples_in_file) + ',' + note + '\n')

    print()


if __name__ == "__main__":

    text_file = open("summary.txt", "w")

    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    out_filename = os.path.splitext(os.path.basename(files[0]))[0] + '.nc'
    dataset = Dataset(out_filename, 'w', format='NETCDF4')

    for f in files:
        parseRAW(f, dataset, text_file)

    dataset.close()
    text_file.close()

