#!/usr/bin/python3

# add_qc_flags
# Copyright (C) 2020 Peter Jansen
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
import glob
import shutil

import numpy as np
import pandas as pd
from netCDF4 import Dataset
from datetime import datetime

plot = False
write_file = True

# pressure interpolator, from a aggregate pressure file interpolate the pressure to a target instrument
def interpolator(target_files=None, pres_file=None):

    # get the target file, default to all FV01 files
    if not target_files:
        print('target_files = none')
        # Load the filenames of the fv01 files in the current folder
        target_files = glob.glob('*FV01*.nc')
        #target_files = ['/Users/pete/cloudstor/SOTS-Temp-Raw-Data/Pulse-7-2010/netCDF/IMOS_ABOS-SOTS_T_20100908_SOFS_FV00_Pulse-7-2010-Minilog-T-6098-45m_END-20110222_C-20200129.nc']
        # target_files = '../../pres_interp_test_no_pres.nc'

    # get the pressure aggregate file, default to the first Aggregate file
    if not pres_file:
        print('pres_file = none')
        # Extract the aggregate file data
        pres_file = glob.glob('*Aggregate*.nc')[0]
        #pres_file = '/Users/pete/cloudstor/SOTS-Temp-Raw-Data/Pulse-7-2010/netCDF/IMOS_ABOS-SOTS_PT_20100817_SOFS_FV02_Pulse-Aggregate-P_END-20110430_C-20200130.nc'
        # pres_file='../../pres_interp_test.nc'

    out_file_list = []

    print(datetime.utcnow(), 'load pressure file', os.path.basename(pres_file))

    # read the pressure aggregate file
    agg_ds = Dataset(pres_file, mode="r")

    time_var = agg_ds.variables["TIME"]
    time = time_var[:]
    inst_idx_var = agg_ds.variables["instrument_index"]
    pres_var = agg_ds.variables["PRES"]
    pres = pres_var[:]

    nominal_depths = np.array(agg_ds.variables["NOMINAL_DEPTH"][:])

    inst_idx = inst_idx_var[:]

    agg_ds.close()

    inst_set = set(inst_idx)

    # load the target file
    for target_file in target_files:

        print(datetime.utcnow(), 'target file', os.path.basename(target_file))

        # Change the creation date in the filename to today
        now = datetime.utcnow()

        fn_new = target_file
        if os.path.basename(target_file).startswith("IMOS"):
            fn_new_split = os.path.basename(target_file).split('_')
            fn_new_split[-1] = "C-" + now.strftime("%Y%m%d") + ".nc"
            try:
                fn_new_split[2].index("Z")
            except ValueError:
                fn_new_split[2] += 'Z'

            fn_new = os.path.join(os.path.dirname(target_file), '_'.join(fn_new_split))

            # If a new (different) filename has been successfully generated, make
            # a copy of the old file with the new name
            if fn_new != target_file:
                # copy file
                shutil.copy(target_file, fn_new)

        print(datetime.utcnow(), 'output file', os.path.basename(fn_new))

        # read the TIME, and nominal depth from the target file
        interp_ds = Dataset(fn_new, 'r')

        interp_times_var = interp_ds.variables["TIME"]
        interp_times = interp_times_var[:]
        interp_nom_depth = float(interp_ds.variables['NOMINAL_DEPTH'][:])

        interp_ds.close()

        # add an entry for the nominal depth of the target sensor
        nominal_depths = np.append(nominal_depths, interp_nom_depth)
        # add an entry for the surface, where we assume pressure = 0 dbar
        nominal_depths = np.append(nominal_depths, -0.1)

        # create an array to hold all the target time pressures
        interp_pres = np.zeros([len(interp_times), len(nominal_depths)])
        # all nans except the 0 dbar pressure, which is filled with zero
        interp_pres[:, 0:-1] = np.nan

        # TODO: only use data where the QC flag is 1
        # interpolate each pressure to the target time
        i = 0
        for j in inst_set:
            print(datetime.utcnow(), ' selecting data ', j)

            msk = np.where(inst_idx == j)
            time_msk = time[msk]
            pres_msk = pres[msk]

            interp_pres[:, i] = np.interp(interp_times, time_msk, pres_msk, left=np.nan, right=np.nan)

            i += 1

        # TODO: if the target file already has pressure, read the data into the last column, then interpolate

        print(datetime.utcnow(), 'create data frame')

        # create a dataframe of all the data, including the target ones, filled with nan
        df = pd.DataFrame(interp_pres, index=interp_times, columns=nominal_depths)
        # sort the columns by depth
        df = df.reindex(sorted(df.columns), axis=1)

        if plot:
            plt = df.plot()
            plt.invert_yaxis()

        # fill any time nan's
        print(datetime.utcnow(), 'time interpolate data frame')
        df_interp = df.interpolate(method='index', axis=0, limit=1)

        # fill the target column with interpolated value
        print(datetime.utcnow(), 'depth interpolate data frame')
        df_interp = df_interp.interpolate(method='index', axis=1, limit=1)

        # extract the target pressure
        interp_pres_target = df_interp[interp_nom_depth].values

        if write_file:
            print(datetime.utcnow(), 'write to file', os.path.basename(fn_new))

            interp_msk = df[interp_nom_depth].isna().values # values which were NaNs before the interpolation
            interp_ds = Dataset(fn_new, 'a')

            # Create the PRES and PRES_quality_control variables, and their attributes
            if 'PRES' in interp_ds.variables:
                pres_var = interp_ds['PRES']
            else:
                pres_var = interp_ds.createVariable('PRES', 'f4', interp_ds.variables['TIME'].dimensions, fill_value = np.nan, zlib=True)
            pres_var.setncattr('standard_name', 'sea_water_pressure_due_to_sea_water')
            pres_var.setncattr('long_name', 'sea_water_pressure_due_to_sea_water')
            pres_var.setncattr('units', 'dbar')
            pres_var.setncattr('coordinates', 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH')
            pres_var.setncattr('valid_max', np.float64(12000))
            pres_var.setncattr('valid_min', np.float64(-15))
            pres_var.ancillary_variables = "PRES_quality_control"
            pres_var.setncattr('comments', 'interpolated from surounding pressure sensors on mooring')
            pres_var[interp_msk] = interp_pres_target[interp_msk]

            if 'PRES_quality_control' in interp_ds.variables:
                pres_qc_var = interp_ds['PRES_quality_control']
            else:
                pres_qc_var = interp_ds.createVariable('PRES_quality_control', 'i1', interp_ds.variables['TIME'].dimensions, fill_value=99, zlib=True)
            pres_qc_var.long_name = "quality_code for PRES"
            pres_qc_var.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
            pres_qc_var.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
            pres_qc_var[interp_msk] = 7

            history_comment = 'added interpolated pressure from ' + os.path.basename(pres_file)
            try:
                hist = interp_ds.history + "\n"
            except AttributeError:
                hist = ""

            interp_ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : " + history_comment)

            interp_ds.close()

        out_file_list.append(fn_new)

    return out_file_list


if __name__ == "__main__":
    if len(sys.argv) == 1:
        interpolator(None, None)
    else:
        interpolator(target_files=sys.argv[2:], pres_file=sys.argv[1])
