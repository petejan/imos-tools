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

import datetime
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct
import csv
from os.path import isfile, isdir, join
from os import listdir, walk

import re
import xml.etree.ElementTree as ET



def parse_azfp(files):
    output_name = "SOFS-3-2012-AZFP.nc"

    print("output file : %s" % output_name)

    config_xml_tree = ET.parse(files[1])
    root = config_xml_tree.getroot()
    print(root)

    for sn in root.findall('.//AZFP_Version/SerialNumber'):
        print (sn.tag, sn.text)

    instrument_serialnumber = sn.text
    instrument_model = 'AZFP'

    frequencies = []
    for f in root.findall('.//LogAcousticCoefficients/Frequencies/Frequency'):
        print (f.tag)
        fn = f.findall('kHz')[0].text
        print(fn)
        tvr = float(f.findall('TVR')[0].text)
        vtx = float(f.findall('VTX0')[0].text)
        bp = float(f.findall('BP')[0].text)
        el = float(f.findall('EL')[0].text)
        ds = float(f.findall('DS')[0].text)
        frequencies.append({'f': fn, 'tvr': tvr, 'vtx': vtx, 'bp': bp, 'el': el, 'ds': ds})

    print (frequencies)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut = Dataset(output_name, 'w', format='NETCDF4')

    ncOut.instrument = 'ASL Environmental Sciences - ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME") # create unlimited
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"

    # read the data, and write to netCDF file
    sample_n = 0

    with open(files[2], "rb") as binary_file:
        data = binary_file.read(2)
        while data:
            print("hdr ", data)
            if data == b'\xfd\x02':
                #print(data)
                data = binary_file.read(10)
                packet = struct.unpack(">hHhI", data)
                d = ['burst', 'serial', 'status', 'interval']
                pack_dict = dict(zip(d, packet))
                print(pack_dict)

                data = binary_file.read(14)
                packet = struct.unpack(">hhhhhhh", data)
                d = ['year', 'month', 'day', 'hour', 'min', 'second', 'hun_seconds']
                time_dict = dict(zip(d, packet))
                print(time_dict)

                data = binary_file.read(32)
                packet = struct.unpack(">4H4h4h4h", data)
                d = ['rate1', 'rate2', 'rate3', 'rate4', 'lock1', 'lock2', 'lock3', 'lock4',
                     'bin1', 'bin2', 'bin3', 'bin4', 'samples1', 'sample2', 'samples3', 'samples4']
                samples_dict = dict(zip(d, packet))
                print(samples_dict)

                data = binary_file.read(5*2)
                packet = struct.unpack((">hhhHH"), data)
                d = ['pings', 'npings', 'seconds', 'first_ping', 'last_ping']
                ping_dict = dict(zip(d, packet))
                print(ping_dict)

                data = binary_file.read(9)
                packet = struct.unpack((">ihbbb"), data)
                d = ['data_type', 'data_error', 'phase', 'overrun', 'channels']
                data_type_dict = dict(zip(d, packet))
                print(data_type_dict)

                data = binary_file.read(7+12*2)
                packet = struct.unpack((">4B3b4h4h4h"), data)
                d = ['gain_1', 'gain_2', 'gain_3', 'gain_4', 'spare_1', 'spare_2', 'spare_3',
                     'pulse_len_1', 'pulse_len_2', 'pulse_len_3', 'pulse_len_4',
                     'board_1', 'board_2', 'board_3', 'board_4',
                     'freq_1', 'freq_2', 'freq_3', 'freq_4']
                pings_dict = dict(zip(d, packet))
                print(pings_dict)

                data = binary_file.read(8*2)
                packet = struct.unpack((">8H"), data)
                d = ['sensors', 'tilt_x', 'tilt_y', 'battery', 'pressure', 'temperature', 'ad6', 'ad7']
                sensors_dict = dict(zip(d, packet))
                print(sensors_dict)

                data = binary_file.read(2 * samples_dict['bin1'])
                data_raw = []
                data_raw.append(struct.unpack(">%dH" % samples_dict['bin1'], data))
                data = binary_file.read(2 * samples_dict['bin2'])
                data_raw = []
                data_raw.append(struct.unpack(">%dH" % samples_dict['bin2'], data))
                data = binary_file.read(2 * samples_dict['bin3'])
                data_raw = []
                data_raw.append(struct.unpack(">%dH" % samples_dict['bin3'], data))
                data = binary_file.read(2 * samples_dict['bin4'])
                data_raw = []
                data_raw.append(struct.unpack(">%dH" % samples_dict['bin4'], data))

                t = datetime.datetime(time_dict['year'], time_dict['month'], time_dict['day'],
                                      time_dict['hour'], time_dict['min'], time_dict['second'],
                                      time_dict['hun_seconds'] * 10 * 1000)
                ts = date2num(t, calendar=ncTimesOut.calendar, units=ncTimesOut.units)
                ncTimesOut[sample_n] = ts

                #  EL = ELmax – 2.5/a + N/(26214·a)
                #  Sv = ELmax – 2.5/a + N/(26214·a) – SL + 20·logR + 2·α·R – 10log(½c·τ·ψ)
                #  SL = sound transmission level
                #  α = absorption coefficient (dB/m)
                #  τ = transmit pulse length (s)
                #  c = sound speed (m/s)
                #  ψ = equivalent solid angle of the transducer beam pattern (sr).

    # public double getVolts(int i)
    # {
    #     return 2.5 * ((double)i) / 65536;
    # }
    # public double getBattery()
    # {
    #     return 6.5 * getVolts(battery);
    # }
    # public double getTemp()
    # {
    #     double v = getVolts(temperature);
    #     double r = (calTempK[0] + calTempK[1] * v ) / (calTempK[2] - v);
    #     double lnr = Math.log(r);
    #     return (1/(calTemp[0] + calTemp[1] * lnr + calTemp[2] * Math.pow(lnr, 3))) - 273.15;
    # }
    #
    # public double getTiltX()
    # {
    #     double v = tiltx; // manual says volts, but the AD counts gives the correct value (?)
    #     return calTiltX[0] + calTiltX[1] * v + calTiltX[2] * Math.pow(v, 2) + calTiltX[3] * Math.pow(v, 3);
    # }
    # public double getTiltY()
    # {
    #     double v = tilty; // manual says volts, but the AD counts gives the correct value (?)
    #     return calTiltX[0] + calTiltY[1] * v + calTiltY[2] * Math.pow(v, 2) + calTiltY[3] * Math.pow(v, 3);
    # }
    #
    # public double[] getSV(int ch)
    # {
    # 	// Sv = ELmax –2.5/ds + N/(26214.ds) – TVR – 20.logVTX + 20.logR + 2.α.R – 10log(1/2c.t.Ψ)
    #     double[] sv = new double[bins[ch]];
    #     double r;
    #
    #     double c = el[ch] - 2.5/ds[ch] - tvr[ch] - 20 * Math.log10(vtx[ch]) - 10 * Math.log10(bp[ch] * sos * (pulseLen[ch]*1.e-6) / 2.0);
    #
    #     for(int i=0;i<bins[ch];i++)
    #     {
    #         r = sos * (i + 1) / rate[ch] / 2; // range
    #         // sv = const + data (dB) + range_spread + range_water_attenuation
    #         sv[i] = c + data[ch][i]/(26214 * ds[ch])  + 20 * Math.log10(r) + 2 * alpha[ch] * r ; // 26214 = 65536/2.5
    #
    #     }
    #     return sv;
    # }

                sample_n += 1

                # print(data_raw)
                # break

            data = binary_file.read(2)

    return output_name


if __name__ == "__main__":

    # arguments are <mooring> <xml file> <files....(or zip file)>
    parse_azfp(sys.argv)
