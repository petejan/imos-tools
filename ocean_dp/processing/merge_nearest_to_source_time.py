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
from netCDF4 import Dataset, stringtochar
from scipy.interpolate import interp1d

# merge from date from source_file to time in merge_file using nearest point (within 1 hr window)

def resample(merge_file, source_files):

    merge_ds = Dataset(merge_file, 'a')

    time_var = merge_ds.variables["TIME"]
    time = time_var[:]

    qc_in_level = 1

    for source_file in source_files:
        source_ds = Dataset(source_file, 'r')
        source_time_var = source_ds.variables["TIME"]


        # create source_file and source_time variables
        if 'SOURCE_FILE' not in merge_ds.dimensions:
            fDim = merge_ds.createDimension("SOURCE_FILE")
            sDim = merge_ds.createDimension("strlen", 256)
            merge_ds_source_file = merge_ds.createVariable("SOURCE_FILE", "S1", ('SOURCE_FILE', 'strlen'))
            merge_ds_source_file_id = merge_ds.createVariable("SOURCE_FILE_IDX", "i2", ('TIME', ), fill_value=-1)
            merge_ds_source_time = merge_ds.createVariable("SOURCE_TIME", "f4", ('SOURCE_FILE', 'TIME'))

            for a in time_var.ncattrs():
                print('source time attribute', a)
                if a not in ('_FillValue', 'ancillary_variables', 'axis', 'long_name'):
                    merge_ds_source_time.setncattr(a, time_var.getncattr(a))
            merge_ds_source_time.long_name = 'source_file time of sample used'
            merge_ds_source_file.long_name = 'source file name'

        else:
            merge_ds_source_file = merge_ds.variables['SOURCE_FILE']
            merge_ds_source_file_id = merge_ds.variables['SOURCE_FILE_IDX']
            merge_ds_source_time = merge_ds.variables['SOURCE_TIME']
            fDim = merge_ds.dimensions['SOURCE_FILE']

        print('file name dim :', os.path.basename(source_file), fDim.size)

        file_idx = fDim.size
        source_file_str = np.empty(1, dtype='S256')
        source_file_str[0] = os.path.basename(source_file)
        merge_ds_source_file[file_idx,:] = stringtochar(source_file_str)

        # for all variables in the source_file
        for v in source_ds.variables:
            var = source_ds.variables[v]
            # select only variables with TIME dimension, don't copy TIME and _quality_control variables
            if 'TIME' in (var.dimensions) and v != 'TIME' and v.find('_quality_control') == -1 and var.datatype == 'float32':
                #print('time dimension in', v)
                qc = np.ones_like(source_time_var)

                # do we need to create the variable
                if v in merge_ds.variables:
                    new_var = merge_ds.variables[v]
                else:
                    #print("data type : ", var.datatype)
                    new_var = merge_ds.createVariable(v, var.datatype, var.dimensions, fill_value=np.nan)

                # get the QC of the source variable
                if v + '_quality_control' in merge_ds.variables:
                    #print('using qc : ', v + "_quality_control")
                    qc = merge_ds.variables[v + "_quality_control"][:]

                # find the timestamps of the source_file
                source_time_np = np.array(source_time_var[qc <= qc_in_level])
                f_times = interp1d(source_time_np, source_time_np, kind='nearest', bounds_error=False, fill_value=np.nan)

                # TODO: might be an easier way, only create the data for times where data is good
                new_times = f_times(time_var)
                time_msk = abs(new_times - time) > 1 / 24

                # find the nearest data point to the output times
                f = interp1d(source_time_np, np.array(var[qc <= qc_in_level]), kind='nearest', bounds_error=False, fill_value=np.nan)
                new_data = f(time_var)

                new_data[time_msk] = np.nan

                #print(new_data)

                msk = ~np.isnan(new_data)
                print(v, 'msk count', sum(msk))
                if sum(msk) > 0:

                    merge_ds_source_file_id[msk] = file_idx
                    merge_ds_source_time[file_idx, msk] = new_times[msk]

                    # copy data where new_data is not nan
                    # TODO: only copy when the data is nan
                    new_var[msk] = new_data[msk]

                    # copy over the source variable attributes
                    for a in var.ncattrs():
                        #print('source attribute', a)
                        if a not in ('_FillValue', 'ancillary_variables') and not a.startswith('calibration_'):
                            new_var.setncattr(a, var.getncattr(a))


                # add the source sensor information
                #new_var.sensor_model = source_ds.instrument_model
                #new_var.sensor_serial_number = source_ds.instrument_serial_number


        # update the history attribute
        try:
            hist = merge_ds.history + "\n"
        except AttributeError:
            hist = ""

        merge_ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " added data from " + os.path.basename(source_file) + " interpolated to this time")

        source_ds.close()

    merge_ds.close()


if __name__ == "__main__":
    resample(sys.argv[1], sys.argv[2:])