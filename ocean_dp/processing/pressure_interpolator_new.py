#!/usr/bin/python3

# add_qc_flags
# Copyright (C) 2020 Ben Weeding
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

from datetime import datetime, timedelta, UTC
import numpy as np
from netCDF4 import Dataset
import glob
import pandas as pd
import shutil
import os
import sys

# Supply netCDFfiles as a ['list'] of files, agg as a 'string'


def pressure_interpolator(netCDFfiles = None, agg_file = None):
    
    if not netCDFfiles:
        print('netCDFfiles = none')
        # Load the filenames of the fv01 files in the current folder
        netCDFfiles = glob.glob('*FV01*.nc')
            
    if not agg_file:
        print('agg = none')
        # Extract the aggregate file data
        agg_file = glob.glob('*Aggregate*.nc')

    agg = Dataset(agg_file, mode="r")

    out_file_list = []

    # Loop through each of the fv01 files
    for fn in netCDFfiles:
        
        print(datetime.now(UTC), 'processing file : ', fn)

        # Change the creation date in the filename to today
        now = datetime.now(UTC)
            
        fn_new_split = os.path.basename(fn).split('_')
        fn_new_split[-1] = "C-" + now.strftime("%Y%m%d") + ".nc"
        try:
            fn_new_split[2].index("Z")
        except ValueError:
            fn_new_split[2] += 'Z'

        fn_new = os.path.join(os.path.dirname(fn), '_'.join(fn_new_split))

        # If a new (different) filename has been successfully generated, make 
        # a copy of the old file with the new name
        if fn_new != fn:
            print('copying file to ', fn_new)
            # copy file
            shutil.copy(fn, fn_new)

        out_file_list.append(fn_new)

        # Open and work in the new copy
        fv01_contents = Dataset(fn_new, mode='a')
        num_times = len(fv01_contents.variables["TIME"])
        num_depths = len(agg.variables["NOMINAL_DEPTH"])

        print('variables ', fv01_contents.variables.keys())
        print('aggregate variables', agg.variables.keys())

        # Create a NaN array to fill with interpolated pressure values
        interp_agg_pres = np.full((num_depths + 1, num_times), np.nan)

        # Set the first row as zeros to set 0m as 0dbar
        interp_agg_pres[0, :] = 0

        nominal_depths = np.array(agg.variables["NOMINAL_DEPTH"][:])
        agg_nominal_depths = np.insert(nominal_depths, 0, 0)
        nominal_depth_sort_idx = np.argsort(agg_nominal_depths)

        print(datetime.now(UTC), 'interpolating aggregate to file times')

        # new coding

        print("aggregate nominal_depths", agg_nominal_depths)
        nominal_depth = fv01_contents.variables["NOMINAL_DEPTH"][0]
        above_nom_depths = np.where(nominal_depth > agg_nominal_depths)

        print("above nominal_depths", above_nom_depths)

        msk = np.where(agg.variables["instrument_index"][:] == 1)
        print("mask", msk)
        time_selection = agg.variables["TIME"][msk]
        pres_selection = agg.variables["PRES"][msk]
        pres_qc_selection = agg.variables["PRES_quality_control"][msk]
        pres_selection[pres_qc_selection != 1] = np.nan
        above_interp_agg_pres = np.interp(fv01_contents.variables["TIME"][:], time_selection, pres_selection)

        print("above pressure", above_interp_agg_pres)

        for j in range(1, len(agg_nominal_depths)):
            print(datetime.now(UTC), ' selecting data ', j)

            msk = agg.variables["instrument_index"][:] == (j - 1)
            time_selection = agg.variables["TIME"][msk]
            pres_selection = agg.variables["PRES"][msk]
            pres_qc_selection = agg.variables["PRES_quality_control"][msk]
            pres_selection[pres_qc_selection != 1] = np.nan

            print(datetime.now(UTC), ' interpolating ', j)

            interp_agg_pres[j, :] = np.interp(fv01_contents.variables["TIME"][:], time_selection, pres_selection)

        interp_agg_pres = interp_agg_pres[nominal_depth_sort_idx, :]
        print(interp_agg_pres)

        print("max")
        print(np.nanargmax(interp_agg_pres[0:2, :], axis=0))
        print("min")
        print(np.nanargmin(interp_agg_pres[2:, :], axis=0))

        # # For each nominal depth, interpolate the agg data at the fv01 times
        # # TODO: look at only calculating pressures for sensors above and below the NOMINAL_DEPTH that we're interested in
        # for j in range(1, len(agg_nominal_depths)):
        #     print(datetime.now(UTC), ' selecting data ', j)
        #
        #     msk = agg.variables["instrument_index"][:] == (j - 1)
        #     time_selection = agg.variables["TIME"][msk]
        #     pres_selection = agg.variables["PRES"][msk]
        #     pres_qc_selection = agg.variables["PRES_quality_control"][msk]
        #     pres_selection[pres_qc_selection != 1] = np.nan
        #
        #     print(datetime.now(UTC), ' interpolating ', j)
        #
        #     interp_agg_pres[j, :] = np.interp(fv01_contents.variables["TIME"][:], time_selection, pres_selection)
        #
        # # Sort the nominal depths and pressures according to nominal depth
        # interp_agg_pres = interp_agg_pres[nominal_depth_sort_idx, :]
        #
        # agg_nominal_depths.sort()
        #
        # print(agg_nominal_depths)
        # print(interp_agg_pres)
        #
        # # Create a NaN array to receive the fv01 interpolated pressures
        # interp_fv01_pres = np.full((np.shape(fv01_contents.variables["TIME"][:])), np.nan, dtype=agg.variables["PRES"].dtype)
        #
        # interp_fv01_pres_qc = np.full_like(fv01_contents.variables["TIME"], 7, dtype=np.int8)
        #
        # new_pressure = False
        # add_pressure = False
        #
        # # Check the current file doesn't contain pressure to run the following interpolator
        # if not 'PRES' in fv01_contents.variables:
        #     add_pressure = True
        #     new_pressure = True
        #     print("file doesn't contain pressure")
        # else:
        #     interp_fv01_pres = fv01_contents.variables['PRES'][:]
        #     interp_fv01_pres_qc = fv01_contents.variables['PRES_quality_control']
        #     if any(np.isnan(np.array(interp_fv01_pres))):
        #         new_pressure = True
        #         history_comment = 'replaced NANs with interpolated pressure.'
        #
        # interp_at = np.where(np.isnan(interp_fv01_pres))
        # print(datetime.now(UTC), 'interpolating pressure at depth=', fv01_contents.variables["NOMINAL_DEPTH"][0])
        #
        # # At each timestamp, interpolate pressure for the fv01 data
        # for j in range(len(fv01_contents.variables["TIME"])):
        #     interp_fv01_pres[j] = np.interp(fv01_contents.variables["NOMINAL_DEPTH"][0], agg_nominal_depths, interp_agg_pres[:, j])
        #
        #     # # Find indices where the netcdf data and interpolated data don't match (where the NaNs are in the netcdf)
        #     # nan_rep_idx = np.where(interp_fv01_pres != fv01_contents.variables['PRES'][:])[1]
        #     #
        #     # # update the quality flags
        #     # interp_fv01_pres_qc[nan_rep_idx] = 7
        #
        # if add_pressure:
        #     # Create the PRES and PRES_quality_control variables, and their attributes
        #     pres_var = fv01_contents.createVariable('PRES', 'f4', fv01_contents.variables['TIME'].dimensions, fill_value = np.nan, zlib=True)
        #
        #     pres_var.setncattr('standard_name', 'sea_water_pressure_due_to_sea_water')
        #     pres_var.setncattr('long_name', 'sea_water_pressure_due_to_sea_water')
        #     pres_var.setncattr('units', 'dbar')
        #     pres_var.setncattr('coordinates', 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH')
        #     pres_var.setncattr('valid_max', np.float64(12000))
        #     pres_var.setncattr('valid_min', np.float64(-15))
        #     pres_var.ancillary_variables = "PRES_quality_control"
        #     pres_var.setncattr('comments', 'interpolated from surounding pressure sensors on mooring')
        #
        #     pres_qc_var = fv01_contents.createVariable('PRES_quality_control', 'i1', fv01_contents.variables['TIME'].dimensions, fill_value=99, zlib=True)
        #     pres_qc_var.long_name = "quality_code for PRES"
        #     pres_qc_var.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
        #     pres_qc_var.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
        #
        #     history_comment = 'added interpolated pressure.'
        #
        # if new_pressure:
        #
        #     # Insert pressure value back to the netCDF
        #     fv01_contents.variables['PRES'][:] = interp_fv01_pres
        #     fv01_contents.variables['PRES_quality_control'][:] = interp_fv01_pres_qc
        #
        #     # update the history attribute
        #     try:
        #         hist = fv01_contents.history + "\n"
        #     except AttributeError:
        #         hist = ""
        #
        #     fv01_contents.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " : " + history_comment)
        #
        print(datetime.now(UTC), 'close file')
        fv01_contents.close()
            
    agg.close()

    return out_file_list


if __name__ == "__main__":
    pressure_interpolator(netCDFfiles=sys.argv[2:], agg_file=sys.argv[1])
