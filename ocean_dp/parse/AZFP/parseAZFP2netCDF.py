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

    with open(files[2], "rb") as binary_file:
        data = binary_file.read(2)
        while data:
            print("hdr ", data)
            if data == b'\xfd\x02':
                #print(data)
                data = binary_file.read(10)
                (burst, serial, status, interval) = struct.unpack(">hHhI", data)
                print("burst, serial, status, interval", burst, serial, status, interval)

                data = binary_file.read(14)
                (year, mon, day, hour, min, sec, hsec) = struct.unpack(">hhhhhhh", data)
                print("year, mon, day", year, mon, day)

                data = binary_file.read(32)
                packet = struct.unpack(">4H4h4h4h", data)
                d = ['rate1', 'rate2', 'rate3', 'rate4', 'lock1', 'lock2', 'lock3', 'lock4',
                     'bin1', 'bin2', 'bin3', 'bin4', 'samples1', 'sample2', 'samples3', 'samples4']
                pack_dict = dict(zip(d, packet))
                print(pack_dict)

                break

            data = binary_file.read(2)

    # idx_sort = np.argsort(times)
    #
    # #
    # # build the netCDF file
    # #
    #
    # ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
    #
    # ncOut = Dataset(output_name, 'w', format='NETCDF4')
    #
    # ncOut.instrument = 'ASL Environmental Sciences - ' + instrument_model
    # ncOut.instrument_model = instrument_model
    # ncOut.instrument_serial_number = instrument_serialnumber
    #
    # #     TIME:axis = "T";
    # #     TIME:calendar = "gregorian";
    # #     TIME:long_name = "time";
    # #     TIME:units = "days since 1950-01-01 00:00:00 UTC";
    #
    # tDim = ncOut.createDimension("TIME", number_samples_read)
    # ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    # ncTimesOut.long_name = "time"
    # ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    # ncTimesOut.calendar = "gregorian"
    # ncTimesOut.axis = "T"
    # # sort the times
    # t_unsorted = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)
    # ncTimesOut[:] = t_unsorted[idx_sort]
    #

    return output_name


if __name__ == "__main__":

    # arguments are <mooring> <xml file> <files....(or zip file)>
    parse_azfp(sys.argv)
