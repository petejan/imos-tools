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

    ds = Dataset(files[0], 'a')

    n_depths_var = ds.variables['NOMINAL_DEPTH_TEMP']
    temp_var = ds.variables['TEMP']


    temp = temp_var[:]
    n_depths = n_depths_var[:]

    msk = (n_depths > 15) & (n_depths < 800)
    print(n_depths[msk])
    print(temp.shape)

    n_depth_touse = n_depths[msk]
    temp_touse = temp[msk]

    mld = np.empty(temp_touse.shape[1])
    for i in range(temp_touse.shape[1]):
        diff = np.abs(temp_touse[0, i] - temp_touse[:, i])
        #print(diff)
        idx = np.where(diff > 0.2)
        #print(len(idx[0]))
        if (len(idx[0]) > 0):
            #print(n_depth_touse[idx[0][0]])
            mld[i]=n_depth_touse[idx[0][0]]
        else:
            mld[i]=n_depth_touse[-1]

        print(mld[i])

    if 'MLD' not in ds.variables:
        mld_out_var = ds.createVariable("MLD", 'f4', 'TIME', fill_value=np.NaN, zlib=True)
    else:
        mld_out_var = ds.variables['MLD']

    mld_out_var[:] = mld

    ds.close()


if __name__ == "__main__":

    mld(sys.argv[1:])
