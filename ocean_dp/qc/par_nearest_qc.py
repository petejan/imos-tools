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
import glob

import xarray as xr
from datetime import timedelta
from datetime import datetime

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import sys

from netCDF4 import Dataset

# TODO: there will be a way of generating this programmatically, this works for now
qc_map2 = {
            tuple([1, 0]): [1, 2],
            tuple([0, 1]): [3, 3],
         }

qc_map3 = {
            tuple([2, 1, 0]): [1, 1, 2],
            tuple([1, 2, 0]): [3, 3, 2],
            tuple([2, 0, 1]): [1, 3, 3],
            tuple([1, 0, 2]): [4, 1, 2],
            tuple([0, 2, 1]): [3, 3, 3],
            tuple([0, 1, 2]): [4, 3, 3]
         }

qc_map4 = {
            tuple([3, 2, 1, 0]): [1, 1, 1, 2],
            tuple([3, 1, 2, 0]): [1, 4, 1, 2], # checked Pulse-8
            tuple([3, 2, 0, 1]): [1, 1, 3, 3],
            tuple([3, 1, 0, 2]): [1, 4, 1, 2],
            tuple([3, 0, 2, 1]): [1, 3, 3, 3],
            tuple([3, 0, 1, 2]): [1, 4, 3, 3],

            tuple([2, 3, 1, 0]): [1, 1, 4, 2],
            tuple([2, 3, 0, 1]): [1, 4, 3, 3],
            tuple([1, 3, 0, 2]): [4, 4, 1, 2],
            tuple([1, 3, 2, 0]): [1, 4, 1, 2],
            tuple([0, 3, 2, 1]): [3, 3, 3, 3],
            tuple([0, 3, 1, 2]): [4, 3, 3, 3],

            tuple([2, 1, 3, 0]): [1, 1, 4, 2],
            tuple([1, 2, 3, 0]): [1, 3, 4, 2],
            tuple([2, 0, 3, 1]): [1, 3, 4, 3],
            tuple([1, 0, 3, 2]): [4, 1, 4, 2],
            tuple([0, 2, 3, 1]): [3, 3, 4, 3],
            tuple([0, 1, 3, 2]): [4, 3, 4, 3],

            tuple([2, 1, 0, 3]): [1, 1, 1, 4],
            tuple([1, 2, 0, 3]): [3, 3, 2, 4],
            tuple([2, 0, 1, 3]): [1, 3, 3, 4],
            tuple([1, 0, 2, 3]): [4, 1, 2, 4],
            tuple([0, 2, 1, 3]): [3, 3, 3, 4],
            tuple([0, 1, 2, 3]): [4, 3, 3, 3]
}

plot = False

def add_qc(dataDIR):

    spherical_scale_factor = 1.0
    daily_means = []
    daily_mean_days = []
    par_data = []

    depths = []
    sensor_types = []
    sensor_spherical = []
    # scan each file for its depth and sensor type
    for f in dataDIR:
        ds = Dataset(f, 'r')

        # get the nominal depths
        depths.append(float(ds.variables['NOMINAL_DEPTH'][:]))
        # get if the sensor is spherical
        sensor_type = ds.variables['PAR'].comment_sensor_type
        spherical = 'spherical' in sensor_type
        sensor_types.append(sensor_type)
        sensor_spherical.append(spherical)

        start_date = datetime.strptime(ds.time_deployment_start, '%Y-%m-%dT%H:%M:%SZ')
        end_date = datetime.strptime(ds.time_deployment_end, '%Y-%m-%dT%H:%M:%SZ')
        lon = ds.variables['LONGITUDE'][:]
        print("longitude", lon)

        print('depth', depths[-1], 'sensor type', sensor_type, ' : spherical = ', spherical)

        ds.close()

    hr_offset = (-lon * 24 / 360)
    # generate a index for the days in the between deployment_start and deployment_end centred on the local noon
    td = pd.date_range(start_date, end_date, freq='1d').round('1d') + timedelta(hours=hr_offset)

    files = np.array(dataDIR)
    depth_order = np.argsort(depths)

    # process files in depth order
    # create a daily mean for each file
    for f in files[depth_order]:
        print("file", f)
        DS = xr.open_dataset(f)

        df = DS.to_dataframe()

        par_data.append(df.PAR)

        # only data from deployment_start to deployment_end and day time
        mask = (df.index > start_date) & (df.index <= end_date) & (df.ePAR > 1)

        daily_mean = df.PAR[mask].resample(timedelta(hours=24), loffset=timedelta(hours=hr_offset)).mean()
        daily_mean_days.append(daily_mean)

        daily_mean_epar = df.ePAR[mask].resample(timedelta(hours=24), loffset=timedelta(hours=hr_offset)).mean()

        # save a the means in a list
        sensor_type = DS.PAR.comment_sensor_type
        spherical = 'spherical' in sensor_type
        if spherical:
            print('appling spherial_factor / ', spherical_scale_factor)
            daily_means.append(daily_mean.reindex(td, method='ffill')/spherical_scale_factor)
        else:
            daily_means.append(daily_mean.reindex(td, method='ffill'))

        DS.close()

    # create an array time x depth
    if depth_order.shape[0] == 2:
        array = np.array([daily_means[0].values, daily_means[1].values])
        qc_map = qc_map2
    elif depth_order.shape[0] == 3:
        array = np.array([daily_means[0].values, daily_means[1].values, daily_means[2].values])
        qc_map = qc_map3
    elif depth_order.shape[0] == 4:
        array = np.array([daily_means[0].values, daily_means[1].values, daily_means[2].values, daily_means[3].values])
        qc_map = qc_map4

    # plot data
    if plot:
        fig, axs = plt.subplots(2, 1, sharex=True)
        axs[0].plot(daily_means[0].index, array.transpose())
        #axs[0].plot(array.transpose())
        axs[0].set_yscale('log')

    # sort every time by value
    array[np.isnan(array)] = 0  # make NANs sort lowest (first)
    idx = np.argsort(array, axis=0)

    # assign QC flags based on the sorted order of PAR, cf depth
    qc = []
    for i in range(idx.shape[1]):
        qc.append(qc_map[tuple(idx[:, i])])

    # plot QC flags
    if plot:
        axs[1].plot(daily_means[0].index, qc)
        #axs[1].plot(qc)
        axs[1].legend(np.array(depths)[depth_order], loc='upper right', fontsize='small')
        axs[1].set_ylim([0, 9])

    if plot:
        plt.show()

    # write QC flags back to the source file
    qc_flag_n = 0
    for f in files[depth_order]:
        ds = Dataset(f, 'a')

        df_qc = pd.DataFrame(qc, index=td)
        qc_final = df_qc.reindex(par_data[qc_flag_n].index, method='ffill')[qc_flag_n]
        qc_flag_n += 1

        nc_var = ds.variables['PAR']

        # create a qc variable just for this test flags
        qc_var_name = nc_var.name + "_quality_control_nn"
        if qc_var_name in ds.variables:
            ncVarOut = ds.variables[qc_var_name]
        else:
            ncVarOut = ds.createVariable(qc_var_name, "i1", nc_var.dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max

            ncVarOut.long_name = "quality flag for " + nc_var.long_name
            if 'standard_name' in nc_var.ncattrs():
                ncVarOut.standard_name = nc_var.standard_name + " status_flag"

            ncVarOut.quality_control_conventions = "IMOS standard flags"
            ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
            ncVarOut.comment_spherical_scale_applied = spherical_scale_factor

            ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'

        ncVarOut[:] = np.zeros(nc_var.shape)

        new_qc_flags = qc_final.values
        new_qc_flags[np.isnan(new_qc_flags)] = 0
        ncVarOut[:] = new_qc_flags

        ncVarOut = ds.variables[nc_var.name + "_quality_control"]
        existing_qc_flags = ncVarOut[:]
        existing_qc_flags = np.max([new_qc_flags, existing_qc_flags], axis=0)
        ncVarOut[:] = existing_qc_flags

        # add new variable to list of aux variables
        nc_var.ancillary_variables = nc_var.ancillary_variables + " " + qc_var_name

        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""
        ds.setncattr("history", hist + datetime.utcnow().strftime("%Y-%m-%d") + " nearest neighbour test")

        try:
            # find the existing quality_control variable in the auxillary variables list
            aux_vars = nc_var.ancillary_variables
            aux_var = aux_vars.split(" ")
            qc_vars = [i for i in aux_var if i.endswith("_quality_control")]
            qc_var = qc_vars[0]
            print("QC var name ", qc_var)
            var_qc = ds.variables[qc_var]
            # read existing quality_control flags
            qc_existing = var_qc[:]
        except (KeyError, AttributeError):
            print("no QC variable found")

        ds.close()


if __name__ == "__main__":
    add_qc(sys.argv[1:])
    # data = [
    #     '../../data/PAR/raw_files/netCDF/IMOS_DWM-SOTS_FZX_20110802_SOFS_FV01_Pulse-8-2011-MDS-MKVL-200341-50m_END-20120105_C-20200427.nc',
    #     '../../data/PAR/raw_files/netCDF/IMOS_DWM-SOTS_FZX_20110802_SOFS_FV01_Pulse-8-2011-MDS-MKVL-200664-27m_END-20120722_C-20200427.nc',
    #     '../../data/PAR/raw_files/netCDF/IMOS_DWM-SOTS_FZX_20110802_SOFS_FV01_Pulse-8-2011-MDS-MKVL-200665-0m_END-20120722_C-20200427.nc',
    #     '../../data/PAR/raw_files/netCDF/IMOS_DWM-SOTS_RZXF_20110725_SOFS_FV01_Pulse-8-2011-ECO-PARS-PARS-134-34m_END-20120727_C-20200427.nc']
    # add_qc(data)
