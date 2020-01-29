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

from netCDF4 import Dataset
import sys
import numpy as np
import csv
from datetime import datetime
from dateutil import parser
from fuzzywuzzy import fuzz

match_threshold = 90

def parseTypeValue(att_type, v):
    if att_type == 'float64':
        value = np.float64(v)
    elif att_type == 'float32':
        value = np.float32(v)
    elif att_type == 'int16':
        value = np.float16(v)
    else:
        value = v

    return value


def add(netCDFfile, metadatafiles):

    ds = Dataset(netCDFfile, 'a')

    time_start = parser.parse(ds.time_coverage_start, ignoretz=True, yearfirst=True, dayfirst=False)
    time_end = parser.parse(ds.time_coverage_end, ignoretz=True,  yearfirst=True, dayfirst=False)
    instrument_model = ds.instrument_model
    instrument_serial_number = ds.instrument_serial_number

    #print(time_start)

    ds_variables = ds.variables

    added_attribute = False
    fuzz_best_model = -1
    fuzz_model = None
    fuzz_best_serial = -1
    fuzz_serial = None

    #print(ds_variables)
    dict1 = {}

    for filepath in metadatafiles:
        with open(filepath, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
            headers = next(csv_reader)
            for row in csv_reader:
                dict1 = {key: value for key, value in zip(headers, row)}
                #print(dict1)

                try:
                    deployment_code = ds.deployment_code
                except AttributeError:
                    deployment_code = None

                #print("deployment_code ", deployment_code)

                if len(dict1) >= 5:
                    # match deployment_code, time_in, time_end
                    match = True
                    # match deployment if we have one
                    if 'deployment_code' in dict1:
                        if len(dict1['deployment_code']) > 0 and dict1['deployment_code'] != deployment_code:
                            match = False
                            #print("deployment not match : ", dict1['deployment_code'])

                    # match time
                    if len(dict1['time_deployment']) > 0:
                        if time_end < parser.parse(dict1['time_deployment'], ignoretz=True, dayfirst=False):
                            match = False
                            #print("Time end before deployment ", time_end, dict1['time_deployment'])
                    if len(dict1['time_recovery']) > 0:
                        if time_start > parser.parse(dict1['time_recovery'], ignoretz=True, dayfirst=False):
                            match = False
                            #print("Time start after recovery ", time_start, dict1['time_recovery'], parser.parse(dict1['time_recovery'], ignoretz=True, dayfirst=False))

                    # match instrument
                    # fuzzie matching : https://medium.com/@categitau/fuzzy-string-matching-in-python-68f240d910fe
                    if len(dict1['model']) > 0:
                        fuz = fuzz.ratio(instrument_model.lower(), dict1['model'].lower())
                        #print("model fuzz", fuz, instrument_model, dict1['model'])
                        if fuz > fuzz_best_model:
                            fuzz_best_model = fuz
                            fuzz_model = dict1
                        if fuz < match_threshold:
                            match = False
                            #print("instrument_model not match : ", dict1['model'])
                    if len(dict1['serial_number']) > 0:
                        fuz = fuzz.ratio(instrument_serial_number.lower(), dict1['serial_number'].lower())
                        #print("fuzz serial", fuz, instrument_serial_number, dict1['serial_number'])
                        if fuz > fuzz_best_serial:
                            fuzz_best_serial = fuz
                            fuzz_serial = dict1
                        if fuz < match_threshold:
                            match = False
                            #print("serial_number not match : ", dict1['serial_number'])

                    if match:
                        #print("match ", dict1)

                        # global attributes
                        if dict1['rec_type'] == 'GLOBAL':
                            name = dict1['attribute_name']
                            att_type = dict1['type']
                            value = parseTypeValue(att_type, dict1['value'])
                            #print("add global %s (%s) = %s" % (name, att_type, value))
                            ds.setncattr(name, value)
                            added_attribute = True

                        # create any scalar variables (LATITUDE, LONGITUDE, NOMINAL_DEPTH)
                        if dict1['rec_type'] == 'VAR':
                            if dict1["variable_shape"] == "()":
                                print("Create Variable : %s shape %s" % (dict1["variable_name"], dict1["variable_shape"]))

                                newVar = ds.createVariable(dict1["variable_name"], "d")
                                newVar[:] = float(dict1["value"])

                        # variable attributes
                        elif dict1['rec_type'] == 'VAR_ATT':
                            var_name = dict1['var_name']
                            #print(var_name)
                            if var_name in ds_variables:
                                name = dict1['attribute_name']
                                att_type = dict1['type']
                                value = parseTypeValue(att_type, dict1['value'])
                                #print("add variable %s attribute %s (%s) = %s" % (var_name, name, att_type, value))
                                ds_variables[var_name].setncattr(name, value)
                                added_attribute = True

    if not added_attribute:
        if fuzz_best_model > 0:
            print("best model match", fuzz_best_model, fuzz_model)
        if fuzz_best_serial > 0:
            print("best serial match", fuzz_best_serial, fuzz_serial)

        print('no attributes added, file not changed')
        ds.close()

        return None

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : attributes added from file(s) [" + format(', '.join(metadatafiles)) + "]")

    ds.close()

    return netCDFfile


if __name__ == "__main__":
    add(sys.argv[1], sys.argv[2:])


