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

from datetime import datetime, timedelta
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct
import xml.etree.ElementTree as ET
import untangle
import xmltodict

from collections import OrderedDict

def parse_xml(files):

    instrument_model = 'ISUS'
    instrument_serialnumber = '0829'

    with open(files[0]) as fd:
        doc = xmltodict.parse(fd.read())

    instrument_manufacture = doc['InstrumentPackage']['Instrument']['@manufacturer']
    instrument_serialnumber = doc['InstrumentPackage']['Instrument']['@serialNumber']
    instrument_model = doc['InstrumentPackage']['Instrument']['@identifier']

    output_name = files[1] + ".nc"
    print("output file : %s" % output_name)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut = Dataset(output_name, 'w', format='NETCDF4')

    ncOut.instrument = instrument_manufacture + ' ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

    tDim = ncOut.createDimension("TIME") # create unlimited
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=False)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"

    ncFrameOut = ncOut.createVariable('FRAME_TYPE', "f4", ("TIME",), fill_value=np.nan, zlib=False)  # fill_value=nan otherwise defaults to max
    ncFrameOut.units = '1'
    ncFrameOut.comment = '0 = dark frame, 1 = light frame'

    fields = []
    # create netCDF variables for each sensorField in frame
    frames = doc['InstrumentPackage']['Instrument']['VarAsciiFrame']
    for f in frames:
        if f['@identifier'].startswith('SATNLF') or f['@identifier'].startswith('SATSLF'):
            for sfg in f['SensorFieldGroup']:
                sensor_field = sfg['SensorField']
                #print('type', sensor_field.__class__, 'len', len(sensor_field))
                if isinstance(sensor_field, dict):
                    print(sensor_field['Sequence'], sfg['Name'])  # , sfg['SensorField'])
                    var_name = sfg['Name']
                    if var_name not in ('TIME', 'DATE', 'CHECK'):
                        ncVarOut = ncOut.createVariable(sfg['Name'], "f4", ("TIME",), fill_value=np.nan, zlib=False)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = sfg['Units']
                        fields.append({'seq': int(sensor_field['Sequence']), 'var': ncVarOut})
                else:
                    var_name = sfg['Name']
                    print(var_name, 'length sensor field', len(sensor_field))
                    ncOut.createDimension(var_name+'_DIM', len(sensor_field))
                    ncVarOutDim = ncOut.createVariable(var_name+'_DIM', "f4", (var_name+'_DIM', ), fill_value=np.nan, zlib=False)  # fill_value=nan otherwise defaults to max
                    ncVarOut = ncOut.createVariable(var_name, "f4", ("TIME", var_name+'_DIM'), fill_value=np.nan, zlib=False)  # fill_value=nan otherwise defaults to max
                    ncVarOut.units = sfg['Units']
                    idx = 0
                    for sf in sensor_field:
                        print(sfg['Name'], sf['Sequence'], sf['Identifier'])  # , sfg['SensorField'])
                        fields.append({'seq': int(sf['Sequence']), 'var': ncVarOut, 'idx': idx})
                        ncVarOutDim[idx] = float(sf['Identifier'])
                        idx += 1

    #print('fields', fields)

    num_samples = 0
    array_idx = np.zeros([256])
    for f in files[1:]:
        print('reading file', f)
        with open(f, 'r', errors='ignore') as fp:
            line = fp.readline()
            while line:
                if line.startswith('SATNLF') or line.startswith('SATNDF') or line.startswith('SATSLF') or line.startswith('SATSDF'):
                    frame_type = 0
                    if line.startswith('SATNLF') or line.startswith('SATSLF'):
                        frame_type = 1
                    split = line.split(',')
                    dt = datetime.strptime(split[1], '%Y%j') + timedelta(hours=float(split[2]))
                    print(dt)
                    ncTimesOut[num_samples] = date2num(dt, calendar=ncTimesOut.calendar, units=ncTimesOut.units)
                    ncFrameOut[num_samples] = frame_type
                    for field in fields:
                        v = field['var']
                        if len(split[field['seq']]) > 0:
                            if 'idx' in field.keys():
                                #print(field['idx'])
                                array_idx[field['idx']] = float(split[field['seq']])
                                if field['idx'] == 255:
                                    print(array_idx[0])
                                    v[num_samples] = array_idx
                            else:
                                v[num_samples] = float(split[field['seq']])

                    num_samples += 1
                line = fp.readline()

    ncOut.close()

    return output_name


if __name__ == "__main__":

    # arguments are <mooring> <xml file> <files....(or zip file)>
    parse_xml(sys.argv[1:])