import os
import shutil
import sys

from netCDF4 import Dataset, date2num, num2date

from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import numpy as np
from scipy.interpolate import interp1d
import statsmodels.api as sm

np.set_printoptions(linewidth=256)


def mld(files):

    mix_layer_depth_temp_diff = 0.3

    ds = Dataset(files[0], 'a')
    ds.set_auto_mask(False)

    if 'NOMINAL_DEPTH_TEMP' in ds.variables:
        n_depths_var = ds.variables['NOMINAL_DEPTH_TEMP']
    elif 'DEPTH_TEMP' in ds.variables:
        n_depths_var = ds.variables['DEPTH_TEMP']
    else:
        n_depths_var = ds.variables['NOMINAL_DEPTH']

    temp_var = ds.variables['TEMP']
    pres_var = ds.variables['PRES']

    temp = temp_var[:]
    pres = pres_var[:]
    n_depths = n_depths_var[:]

    msk = (n_depths > 15) & (n_depths < 800)
    print('temp shape', temp.shape)

    n_depth_touse = n_depths[msk]
    temp_touse = temp[msk, :]
    pres_touse = pres[msk, :]

    print('depth to use', n_depth_touse)

    print('MLD, temp to use shape', temp_touse.shape)

    mld = np.empty(temp_touse.shape[1])
    mld_pres = np.empty(temp_touse.shape[1])

    # for each timestep, calculate the mix layer depth
    for i in range(temp_touse.shape[1]):
        first_non_nan = np.where(~np.isnan(temp_touse[:, i]))[0][0]
        diff = np.abs(temp_touse[first_non_nan, i] - temp_touse[:, i])
        #print(diff)
        idx = np.where(diff > mix_layer_depth_temp_diff)
        #print(len(idx[0]))
        if len(idx[0]) > 0:
            #print(n_depth_touse[idx[0][0]])
            # use the first temp which is not nan above the difference of mix_layer_depth_temp_diff
            mld_idx = idx[0][0]-1
            mld_idx = max(mld_idx, first_non_nan)
            mld[i] = n_depth_touse[mld_idx]
        else:
            # if no temperature difference is > the limit, use the last one
            mld[i] = n_depth_touse[-1]

        pres_msk = ~np.isnan(pres_touse[:, i])
        #print('shape', pres_msk.shape, pres_touse.shape)
        mld_pres[i] = np.interp(mld[i], n_depth_touse[pres_msk], pres_touse[pres_msk, i])

        #print(mld[i], mld_pres[i])

    if 'MLD' not in ds.variables:
        mld_out_var = ds.createVariable("MLD", 'f4', 'TIME', fill_value=np.NaN, zlib=True)
    else:
        mld_out_var = ds.variables['MLD']

    mld_out_var[:] = mld_pres

    mld_out_var.comment_depth = "Mix Layer Depth using PRES to convert to pressure"
    mld_out_var.units = pres_var.units

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added mix layer depth, temperature difference = " + str(mix_layer_depth_temp_diff))

    ds.close()


if __name__ == "__main__":

    mld(sys.argv[1:])
