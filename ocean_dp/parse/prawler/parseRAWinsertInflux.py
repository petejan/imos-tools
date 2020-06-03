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
from datetime import timedelta, datetime

import glob
from influxdb import InfluxDBClient
import json

profile = []
eng = []
locations = []

def dm(x):
    degrees = int(x) // 100
    minutes = x - 100*degrees

    return degrees, minutes


def decimal_degrees(degrees, minutes):

    return degrees + minutes/60


def parse(files):

    fn = []
    for f in files:
        fn.extend(glob.glob(f))

    times_out = []
    depth_out = []
    temp_out = []
    cndc_out = []
    dox2_out = []
    dox2_temp_out = []
    flu_out = []
    bs_out = []

    profile_n_out = []
    profile_sample_out = []

    n_profile = 0
    line_number = 0
    sample_n = 0
    loc_sample = {}
    client = InfluxDBClient(host='144.6.225.199', port=8086)
    client.switch_database('mooringdata')

    last_gps = None
    for filepath in fn:
        nv = -1
        print('file name', filepath)

        f = open(filepath)
        line = f.readline()
        while line:
            line = line.strip()
            # print(line_number, line)
            if len(line) > 1:
                if line.startswith("%%"):
                    line_number = 0
                    site = line[line.index('_')+1:]

                if line.startswith('%%PRAWC'):
                    type = 'PRAWC'
                    sample_n = 0
                    n_profile += 1
                elif line.startswith('%%GPS'):
                    type = 'GPS'
                elif line.startswith('%%PRAWE'):
                    type = 'PRAWE'

                if line_number == 1:
                    hdr = line.split(',')
                    print("Header ", hdr)
                elif line_number > 1:
                    line_split = line.split(',')
                    if len(line_split) > 1:
                        point = {}

                        # print("line split ", line_split)
                        dictionary = dict(zip(hdr, line_split))
                        dictionary['type'] = type
                        dictionary['site'] = site

                        if type == 'PRAWC':
                            sec = int(dictionary['EP'], 16)
                            dictionary['time'] = timedelta(seconds=sec) + datetime(1970,1,1)
                            dictionary['sample'] = sample_n
                            dictionary['profile'] = n_profile
                            profile.append(dictionary)
                            sample_n += 1

                        if type == 'GPS':
                            dictionary['time'] = datetime.strptime(dictionary['DT'], "%Y-%m-%dT%H:%M:%SZ")
                            dictionary['latitude'] = float(dictionary['LAT'])/100
                            dictionary['longitude'] = float(dictionary['LON'])/100

                            locations.append(dictionary)

                        if type == 'PRAWE':
                            dictionary['time'] = datetime.strptime(dictionary['DT'], "%Y-%m-%dT%H:%M:%SZ")

                            eng.append(dictionary)

                        #print(dictionary)
                        point['measurement'] = dictionary['type']
                        point['time'] = datetime.strftime(dictionary['time'], "%Y-%m-%dT%H:%M:%SZ")
                        point['tags'] = {'site': dictionary['site']}
                        fields = {}
                        for i in dictionary:
                            if (i in ['longitude', 'latitude', 'CD', 'CT', 'CC', 'OT', 'O2', 'CH', 'TB', 'T', 'LAT', 'LON', 'Q', 'S', 'H', 'M', 'MM', 'PN', 'SR', 'IR', 'UL', 'LL', 'IP', 'ET', 'TS', 'EC', 'VM', 'PT', 'WD', 'TM', 'CM', 'CL']):
                                try:
                                    fl = float(dictionary[i])
                                    fields[i] = fl
                                except ValueError:
                                    pass
                                except TypeError:
                                    pass

                        point['fields'] = fields

                        #print(point)

                        client.write_points([point], database='mooringdata', protocol='json')

                line_number += 1

            line = f.readline()

    if n_profile == 0:
        print('no profiles found, returning')
        return None

if __name__ == "__main__":
    parse(sys.argv[1:])
