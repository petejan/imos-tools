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
import os
import sys
from datetime import datetime

import numpy as np
from cftime import num2date
from netCDF4 import Dataset


def resample(netCDF_file, sample_file, vars):

    ds_sample = Dataset(sample_file, 'r')
    sample_time = ds_sample.variables["TIME"]

    ds = Dataset(netCDF_file, 'a')

    time_var = ds.variables["TIME"]
    qc_in_level = 1

    for v in ds_sample.variables:
        var = ds_sample.variables[v]
        if 'TIME' in (var.dimensions) and v != 'TIME' and v.find('_quality_control') == -1:
            if vars is None or v in vars:
                print('time dimension in', v)
                qc = np.ones_like(sample_time)
                if v in ds.variables:
                    new_var = ds.variables[v]
                else:
                    new_var = ds.createVariable(v, var.datatype, var.dimensions)

                #print('var', v, 'qc', v + '_quality_control', 'in file', v + '_quality_control' in ds_sample.variables)
                if v + '_quality_control' in ds_sample.variables:
                    print('using qc : ', v + "_quality_control")
                    qc = ds_sample.variables[v + "_quality_control"][:]

                new_data = np.interp(time_var[:], sample_time[qc <= qc_in_level], var[qc <= qc_in_level])
                #print(new_data)
                new_var.sensor_model = ds_sample.instrument_model
                new_var.sensor_serial_number = ds_sample.instrument_serial_number
                new_var[:] = new_data

                new_var_qc = ds.createVariable(v+"_quality_control", "i1", var.dimensions, fill_value=99)
                new_var_qc[:] = 8
                new_var_qc.quality_control_conventions = "IMOS standard flags"
                new_var_qc.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
                new_var_qc.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'

                new_var.ancillary_variables = v+"_quality_control"

                for a in var.ncattrs():
                    #print(a)
                    if a not in ('_FillValue', 'ancillary_variables'):
                        new_var.setncattr(a, var.getncattr(a))

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " added data from " + os.path.basename(sample_file) + " interpolated to this time")

    ds.close()

    ds_sample.close()


if __name__ == "__main__":
    # TODO: add argument for list of variables to add, eg --VARS TEMP,PSAL ....
    file = None
    data_file = None
    v = None
    i = 1
    while i < len(sys.argv):
        print('merge_resample: args', sys.argv[i])
        if sys.argv[i] == '-VARS':
            v = sys.argv[i+1].split(',')
            print (' VARS:', v)
            i += 1
        elif file is None:
            file = sys.argv[i]
        elif data_file is None:
            data_file = sys.argv[i]
        i += 1

    resample(file, data_file, v)
