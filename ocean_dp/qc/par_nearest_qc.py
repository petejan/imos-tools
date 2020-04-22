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

from netCDF4 import Dataset

dataDIR = glob.glob(os.path.join('/Users/pete/cloudStor/PAR/raw_files/netCDF', 'IMOS_ABOS-SOTS*FV01*Pulse-6-2009-*.nc'))

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
            tuple([3, 1, 2, 0]): [1, 4, 1, 2],
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
    daily_means.append(daily_mean.reindex(td, method='ffill'))

    DS.close()


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
axs[1].plot(daily_means[0].index, qc)
#axs[1].plot(qc)
axs[1].legend(np.array(depths)[depth_order], loc='upper right', fontsize='small')
axs[1].set_ylim([0, 9])

# write QC flags back to the source file
qc_flag_n = 0
for f in files[depth_order]:
    ds = Dataset(f, 'a')

    df_qc = pd.DataFrame(qc, index=td)
    qc_final = df_qc.reindex(par_data[qc_flag_n].index, method='ffill')[qc_flag_n]
    qc_flag_n += 1

    nc_var = ds.variables['PAR']

    # create a qc variable just for this test flags
    if nc_var.name + "_quality_control_nn" in ds.variables:
        ncVarOut = ds.variables[nc_var.name + "_quality_control_nn"]
    else:
        ncVarOut = ds.createVariable(nc_var.name + "_quality_control_nn", "i1", nc_var.dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
        ncVarOut[:] = np.zeros(nc_var.shape)
        ncVarOut.long_name = "quality flag for " + nc_var.name
        ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
        ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'

    qc_array = qc_final.values
    qc_array[np.isnan(qc_array)] = 0
    ncVarOut[:] = qc_array

    # add new variable to list of aux variables
    #nc_var.ancillary_variables = nc_var.ancillary_variables + " " + nc_var.name + "_quality_control_nn"

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
