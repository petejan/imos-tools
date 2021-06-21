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

import pandas as pd
import xarray as xr
import numpy as np
import glob
import os

ds = xr.open_dataset('pCO2/mooring_SOFS-all_xco2_pres-xco2seadryair-xco2airdryair-ph-sss-sst-chl-ntu-sc_o2-sc_o2_mgl-sc_o2_umolkg-sigmatheta.dat.nc')
df = ds.to_dataframe()

ncFiles = glob.glob(os.path.join('pCO2', 'IMOS*FV01*.nc'))

ds1 = xr.open_dataset(ncFiles[0], drop_variables=['LONGITUDE', 'LATITUDE', 'NOMINAL_DEPTH'])
df1 = ds1.to_dataframe()

# df1.PSAL[df1.PSAL_quality_control != 1] = np.nan
# df1.TEMP[df1.TEMP_quality_control != 1] = np.nan

df1_reindex = df1.reindex(df.index, method='bfill', limit=2)

df_comb = df.merge(df1_reindex, how='left', left_index=True, right_index=True)

for f in ncFiles[1:2]:
    print('processing file :', f)
    ds1 = xr.open_dataset(f, drop_variables=['LONGITUDE', 'LATITUDE', 'NOMINAL_DEPTH'])
    df1 = ds1.to_dataframe()

    #df1.PSAL[df1.PSAL_quality_control != 1] = np.nan
    #df1.TEMP[df1.TEMP_quality_control != 1] = np.nan

    df1_reindex = df1.reindex(df_comb.index, method='bfill', limit=2)

    take_non_nan = lambda s1, s2: s1 if np.isnan(s2).item else s2
    df_comb = df_comb.combine(df1_reindex, take_non_nan)

    print(df_comb.dropna())

# create xray Dataset from Pandas DataFrame
xr = xr.Dataset.from_dataframe(df_comb)
#xr['TIME'].attrs={'units':'days since 1950-01-01 00:00:00 UTC', 'long_name':'gregorian'}
# save to netCDF
xr.to_netcdf('pCO2/pCO2-merged.nc')