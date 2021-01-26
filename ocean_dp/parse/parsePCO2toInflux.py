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
from datetime import timedelta, datetime

import glob
from influxdb import InfluxDBClient
import json
import csv

profile = []
eng = []
locations = []


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
    client = InfluxDBClient(host='144.6.230.0', port=8086)
    client.switch_database('rtdp')

    last_gps = None
    for filepath in fn:
        nv = -1
        print('file name', filepath)

        with open(filepath, newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            for row in spamreader:
                #print(line_number, ', '.join(row))
                if line_number >= 9:
                    point = {}
                    point['measurement'] = 'pCO2'
                    point['time'] = datetime.strptime(row[0].strip(), "%Y-%m-%d %H:%M:%S")
                    point['tags'] = {'site': 'SOFS'}
                    point['fields'] = {'pressure': float(row[1]), 'xCO2_sw': float(row[2]), 'xCO2_air': float(row[3])}
                    print(point)

                    try:
                        client.write_points([point], database='rtdp', protocol='json')
                    except:
                        pass

                line_number += 1


if __name__ == "__main__":
    parse(sys.argv[1:])
