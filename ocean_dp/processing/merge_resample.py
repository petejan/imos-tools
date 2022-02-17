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


def resample(netCDF_file, sample_file):

    ds_sample = Dataset(sample_file, 'r')
    sample_time = ds_sample.variables["TIME"]

    ds = Dataset(netCDF_file, 'a')

    time_var = ds.variables["TIME"]
    qc_in_level = 1

    for v in ds_sample.variables:
        var = ds_sample.variables[v]
        if 'TIME' in (var.dimensions) and v != 'TIME' and v.find('_quality_control') == -1:
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
    resample(sys.argv[1], sys.argv[2])
