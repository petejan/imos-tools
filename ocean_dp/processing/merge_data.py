import sys
import os

sys.path.extend(['.'])

import xarray as xr
import pandas as pd
import numpy as np
from netCDF4 import Dataset

import ocean_dp.file_name.find_file_with

ds_swr = xr.open_dataset('/Volumes/PJ-SSD-512/ASIMET/SWR-Aggregate.nc', drop_variables=['LONGITUDE', 'LATITUDE', 'NOMINAL_DEPTH'])
df_swr = ds_swr.to_dataframe()
df_sort = df_swr.sort_values(by=['TIME'])

path = sys.argv[1] + "/"

print ('file path : ', path)

pulse_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "IMOS*FV01*.nc"))
par_files = ocean_dp.file_name.find_file_with.find_variable(pulse_files, 'PAR')

for f in par_files:
    print('file', f)

    #file_par = '/Users/pete/cloudstor/PAR/raw_files/netCDF/IMOS_ABOS-SOTS_CPSTR_20090922_SOFS_FV02_Pulse-Aggregate-PAR_END-20190328_C-20200616.nc'
    ds_par = xr.open_dataset(f, drop_variables=['LONGITUDE', 'LATITUDE', 'NOMINAL_DEPTH'])
    df_par = ds_par.to_dataframe()
    df_par_sort = df_par.sort_values(by=['TIME'])

    #df_m = pd.merge_asof(df_par, df_sort, left_index=True, right_index=True, tolerance=pd.Timedelta('1min'), direction='nearest')
    df_m = pd.merge_asof(df_par_sort, df_sort, left_on='TIME', right_on='TIME', tolerance=pd.Timedelta('1min'), direction='nearest')
    ds_par.close()

    ds = Dataset(f, 'a')
    ncVarOut = ds.createVariable("SWR", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    ncVarOut[:] = df_m.SWR.to_numpy()

    ds.close()
