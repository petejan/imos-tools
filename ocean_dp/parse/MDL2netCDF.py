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
import struct
import datetime
import binascii

from netCDF4 import Dataset
from netCDF4 import date2num, num2date
import numpy as np

# parsers need to output
#  instrument
#  instrument_serial_number
#  time_coverage_start
#  time_coverage_end
# optional
#  date_created
#  history
#
# convert time to netCDF cf-timeformat (double days since 1950-01-01 00:00:00 UTC)

def eeprom_crc(p):
    crc_table = (0x00000000, 0x1db71064, 0x3b6e20c8, 0x26d930ac, 0x76dc4190, 0x6b6b51f4, 0x4db26158, 0x5005713c,
                 0xedb88320, 0xf00f9344, 0xd6d6a3e8, 0xcb61b38c, 0x9b64c2b0, 0x86d3d2d4, 0xa00ae278, 0xbdbdf21c)

    crc = 0xffffffff

    for index in range(0, len(p)):
        crc = crc_table[(crc ^ p[index]) & 0x0f] ^ (crc >> 4)
        crc = crc_table[(crc ^ (p[index] >> 4)) & 0x0f] ^ (crc >> 4)
        crc = ~crc & 0xffffffff
        # print("CRC %s %d %s %d" % (hex(p[index]), p[index] & 0x0f, hex(crc), index))

    return crc


types = ['sensor', 'gyro', 'accel/mag', 'adc', 'TIME', 'serial']
hdr_len = 20  # header + dirty flag

def readpacket(f):

    b = f.read(4)  # read rest of header
    if len(b) != 4:
        return (f.tell()+1, 'end of file')

    (packet_len, packet_type) = struct.unpack('<HH', b)

    print("type %d len %d %s" % (packet_type, packet_len, types[packet_type]))

    f.seek(-6, 1)
    # print("file pos %d" % f.tell())
    pos = f.tell()

    b = f.read(packet_len)
    if len(b) != packet_len:
        return (f.tell(), 'end of file')

    # print("file pos %d" % f.tell())

    ccrc = binascii.crc32(b)
    acrc = eeprom_crc(b)

    # print("packet len %d" % len(b))
    crc_bytes = f.read(4) # read CRC
    if len(crc_bytes) != 4:
        return (pos+1, 'end of file')

    crc = struct.unpack('<I', crc_bytes)

    if crc[0] != acrc:
        print("************* CRC error **************")
        return (pos + 1, 'crc error')

    # print("crc %s" % hex(crc[0]))
    # print(hex(ccrc))
    # print(hex(acrc))

    packet = None

    if packet_type == 0:
        # typedef struct
        # {
        #     char     name[12];                        /**< sensor name */
        #     int32_t  version;                         /**< version of the hardware + driver */
        #     int32_t  sensor_id;                       /**< unique sensor identifier */
        #     int32_t  type;                            /**< this sensor's type (ex. SENSOR_TYPE_LIGHT) */
        #     float    max_value;                       /**< maximum value of this sensor's value in SI units */
        #     float    min_value;                       /**< minimum value of this sensor's value in SI units */
        #     float    resolution;                      /**< smallest difference between two values reported by this sensor */
        #     int32_t  min_delay;                       /**< min delay in microseconds between events. zero = not a constant rate */
        # } sensor_t;

        sensor_packet = struct.unpack('<HHHH12siiiffii', b)
        sensor_dict = dict(zip(['magic', 'type', 'len', 'unsed', 'name', 'version', 'sensor_id', 'type', 'max_v', 'min_v', 'resolution', 'delay'], sensor_packet))

        sensor_dict['name'] = (sensor_dict['name'].decode('utf8').rstrip('\0'))

        print("sensor name %s" % sensor_dict['name'])

        packet = (f.tell(), 'sensor', sensor_dict)

    elif packet_type == 1:
        # struct gyro
        # {
        #       hdr_ts_t hdr;
        #       time_t rtc; // this is written when the sample is written to file
        #       uint32_t time; // this is the time at the end of the first sample
        #       float samples[BUF_SAMPLES*3];
        #       uint32_t dirty;
        # } ;

        samples = int((len(b) - hdr_len) / 4)
        #print("packet len %d gyro samples %d" % (len(b), samples))

        hdr_packet = struct.unpack('<HHHHLL', b[0:16])
        hdr_dict = dict(zip(['magic', 'pt', 'len', 'unused', 'rtc', 'us'], hdr_packet))

        ts = datetime.datetime.utcfromtimestamp(hdr_dict['rtc'])

        #print("type %d %-10s : time %s : samples %d us %d" % (t, types[t], ts, samples, hdr_dict['us']))

        data = struct.unpack("<%df" % samples, b[16:-4])

        #print("gyro %f %f %f" % (data[0], data[1], data[2]))

        nSample = int((samples) / 3)

        d = np.array(data)
        dataMat = d.reshape((nSample, 3))

        packet = (f.tell(), 'gyro', ts, samples, hdr_dict, dataMat)

    elif packet_type == 2:
        # struct Acceleration
        # {
        #       hdr_ts_t hdr;
        #       time_t rtc; // this is written when the sample is written to file
        #       uint32_t time; // this is the time at the end of the first sample
        #       	float accel[BUF_SAMPLES*3];
        # 	        float mag[BUF_SAMPLES*3];
        #       uint32_t dirty;
        # } ;

        samples = int((len(b) - hdr_len) / 4)
        #print("packet len %d accel samples %d" % (len(b), samples))

        hdr_packet = struct.unpack('<HHHHLL', b[0:16])
        hdr_dict = dict(zip(['magic', 'pt', 'len', 'unused', 'rtc', 'us'], hdr_packet))

        ts = datetime.datetime.utcfromtimestamp(hdr_dict['rtc'])
        #print("type %d %-10s : time %s : samples %d us %d" % (t, types[t], ts, samples, hdr_dict['us']))

        data = struct.unpack("<%df" % samples, b[16:-4])
        #print("accel %f %f %f" % (data[0], data[1], data[2]))

        nSample = int((samples) / 3 / 2)

        d = np.array(data)
        dataMat = d.reshape((nSample * 2, 3))

        packet = (f.tell(), 'accel', ts, samples, hdr_dict, dataMat[0:nSample, :], dataMat[nSample:nSample*2, :])

    elif packet_type == 3:
        # struct AdcBuf
        # {
        #       hdr_ts_t hdr;
        #       time_t rtc; // this is written when the sample is written to file
        #       uint32_t time; // this is the time at the end of the first sample
        #           long samples[BUF_SAMPLES+1]; // first sample has channel number
        #       uint32_t dirty;
        # } ;

        #        ADC  offset   data  samples: 0x10   dirty: 0x4C4, 16 decimal to samples, dirty 1220

        samples = int((len(b) - hdr_len) / 4)
        #print("packet len %d adc samples %d" % (len(b), samples))

        hdr_packet = struct.unpack('<HHHHLL', b[0:16])
        hdr_dict = dict(zip(['magic', 'pt', 'len', 'unused', 'rtc', 'us'], hdr_packet))

        ts = datetime.datetime.utcfromtimestamp(hdr_dict['rtc'])
        #print("type %d %-10s : time %s : samples %d us %d" % (t, types[t], ts, samples, hdr_dict['us']))

        channel = struct.unpack("<L", b[16:20])
        data = struct.unpack("<%dL" % (samples - 1), b[20:-4])

        nSample = int((samples - 1) / 3)

        d = np.array(data[0:samples - 1])

        dataMat = d.reshape((nSample, 3))

        dirty = struct.unpack("<L", b[len(b) - 4:len(b)])

        #print("channel %d nsampled %d sample %d" % (channel[0], len(data), data[0]))
        #print("dirty %x" % dirty)

        packet = (f.tell(), 'adc', ts, samples, hdr_dict, dataMat)

        # packetS = struct.unpack('<LL', packet[0:8])
        # d0 = 8
        # ch = -1
        # # if samples >= 300:
        # ch = struct.unpack('<l', packet[d0:d0 + 4])
        # d0 = d0 + 4
        # samples = samples - 1
        # print("Channel %d" % ch)
        #
        # fmt = '<%dl' % samples
        # print("Len %d samples %d " % (len(packet[d0:((samples * 4) + d0)]), samples))
        # data = struct.unpack(fmt, packet[d0:((samples * 4) + d0)])
        # print("data length %d" % len(data))
        #
        # d = numpy.array(data, dtype=float).reshape(samples / 3, 3)
        # # d = d * 2.5 / 2**23  # scale to 2.048 V / 23 bits
        #
        # print(d.shape)
        # print(d)
        #
        # mean = d.mean(axis=0)
        # std = d.std(axis=0) / 1e-6  # std in uV
        # used = struct.unpack('<I', packet[((samples * 4) + d0):((samples * 4) + d0 + 4)])
        #
        # ts = datetime.utcfromtimestamp(packetS[0])
        #
        # print("type %d %-10s : time %s : samples %d " % (dataType, types[dataType], ts, samples))
    elif packet_type == 4:
        # time data, now used
        print("Unused type 4")
    elif packet_type == 5:
        # struct serial
        # {
        #       hdr_ts_t hdr;
        #       time_t rtc; // this is written when the sample is written to file
        #       uint32_t time; // this is the time at the end of the first sample
        #           char samples[2028]; // makes entire buffer 2048 bytes
        #       uint32_t dirty;
        # } ;

        samples = int((len(b) - hdr_len))
        #print("packet len %d serial samples %d" % (len(b), samples))

        hdr_packet = struct.unpack('<HHHHLL', b[0:16])
        hdr_dict = dict(zip(['magic', 'pt', 'len', 'unused', 'rtc', 'us'], hdr_packet))

        ts = datetime.datetime.utcfromtimestamp(hdr_dict['rtc'])
        #print("type %d %-10s : time %s : samples %d us %d" % (packet_type, types[packet_type], ts, samples, hdr_dict['us']))

        f_serial_data = open('serial.bin', 'ab+')
        f_serial_data.write(b[16:-4])
        f_serial_data.close()

        packet = (f.tell(), 'serial', ts, samples, hdr_dict, b[16:-4])

    #print(packet[1], packet[2])

    return packet


def parse(files):

    outputName = 'mooringDataLogger.nc'

    dataset = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    time = dataset.createDimension('TIME', None)
    v = dataset.createDimension('vector', 3)

    times = dataset.createVariable('TIME', np.float64, ('TIME',), fill_value=None)
    times.long_name = "time"
    times.units = "days since 1950-01-01 00:00:00 UTC"
    times.calendar = "gregorian"
    times.axis = "T"

    acc = dataset.createVariable('accel', np.float32, ('TIME', 'vector', ), fill_value=np.nan)
    mag = dataset.createVariable('mag', np.float32, ('TIME', 'vector', ), fill_value=np.nan)
    gryo = dataset.createVariable('gyro', np.float32, ('TIME', 'vector', ), fill_value=np.nan)

    adc1 = dataset.createVariable('adc1', np.double, ('TIME', ), fill_value=np.nan)
    adc2 = dataset.createVariable('adc2', np.double, ('TIME', ), fill_value=np.nan)

    sensor_n = 1
    tidx = -100
    n_times = 0

    for file_name in files:
        n = 0
        next_byte = 1
        print("file ", file_name)

        p = 0

        with open(file_name, "rb") as f:
            b = f.read(1)
            while b:
                # print("byte(1) %s %d %d" % (b.hex(), n, f.tell()))
                next_byte = 1

                if b == b'\xaf':  # SERIAL
                    b = f.read(1)
                    # print("byte(2) ", b.hex())
                    if b == b'\xbe':  # SERIAL
                        # print("Serial magic number")
                        p = readpacket(f)
                    else:
                        next_byte = 0

                elif b == b'\xC0':  # ADC
                    b = f.read(1)
                    if b == b'\xAD':  # ADC
                        # print("ADC magic number")
                        p = readpacket(f)
                    else:
                        next_byte = 0

                elif b == b'\xCE':  # GYRO
                    b = f.read(1)
                    if b == b'\xFA':  # GYRO
                        # print("GYRO magic number")
                        p = readpacket(f)
                    elif b == b'\xAC':  # ACCEL
                        # print("ACCEL magic number")
                        p = readpacket(f)
                    else:
                        next_byte = 0

                elif b == b'\xC1':  # ACCEL
                    b = f.read(1)
                    if b == b'\xAC':  # ACCEL
                        # print("ACCEL1 magic number")
                        p = readpacket(f)
                    else:
                        next_byte = 0

                elif b == b'\x34':  # SENSOR
                    b = f.read(1)
                    if b == b'\x12':  # SENSOR
                        # print("Sensor magic number")
                        p = readpacket(f)
                    else:
                        next_byte = 0
                else:
                    next_byte = 1

                # print(p)
                #print(type(p))
                if p[1] == 'crc error':
                    break

                if isinstance(p, tuple) and len(p) > 2:

                    if p[1] == 'sensor':
                        dataset.setncattr('sensor_' + str(sensor_n), p[2]['name'])
                        dataset.setncattr('sensor_' + str(sensor_n) + '_max_v', p[2]['max_v'])
                        sensor_n += 1
                    else:
                        if p[2] > datetime.datetime(2000,1,1):
                            ts_num = date2num(p[2], units=times.units, calendar=times.calendar)
                            n_times += 1
                            if n_times % 500 == 0:
                                print("time ", file_name, p[2])
                            #print('data shape', p[5].shape)
                            if ts_num not in times[:]:

                                time_samples = [p[2] + datetime.timedelta(milliseconds=d * 5/100 * 1000) for d in np.arange(0, 100, 1)]  # sampling is at 20 Hz -> 5 seconds for 100 samples
                                #print(time_samples)

                                tidx += 100
                                #print('new time', p[2])
                                times[tidx:tidx+100] = date2num(time_samples, units=times.units, calendar=times.calendar)

                            if p[1] == 'accel':
                                #print(p[5].shape, acc[tidx:tidx+100].shape)
                                acc[tidx:tidx+100] = p[5]
                                mag[tidx:tidx+100] = p[6]
                            if p[1] == 'gyro':
                                #print(p[5].shape, gryo[tidx:tidx+100].shape)
                                gryo[tidx:tidx+100] = p[5]
                            if p[1] == 'adc':
                                #print(p[5].shape)
                                adc1[tidx:tidx+100] = p[5][:,0]
                                adc2[tidx:tidx+100] = p[5][:,1]

                if next_byte:
                    n = n + 1
                    f.seek(p[0])
                    b = f.read(1)
                    p = (f.tell(), 'next byte')

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    dataset.setncattr("time_coverage_start", num2date(times[0], units=times.units, calendar=times.calendar).strftime(ncTimeFormat))
    dataset.setncattr("time_coverage_end", num2date(times[-1], units=times.units, calendar=times.calendar).strftime(ncTimeFormat))
    dataset.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    dataset.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + files[0])

    dataset.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])

