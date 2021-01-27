import os
import shutil
import sys
import glob

from netCDF4 import Dataset, date2num, num2date

from datetime import datetime
import matplotlib.pyplot as plt

import numpy as np

def merge(files):
    output_names = []

    now = datetime.utcnow()

    for filepath in files:

        fn_new = 'pCO2/pCO2-merge.nc'
        print('output file : ', fn_new)

        ds = Dataset(filepath, 'r')
        varList = ds.variables
        var_time = varList["TIME"]

        # output data to new file
        ds_new = Dataset(fn_new, 'w')

        #  copy global attributes
        attr_dict = {}
        for a in ds.ncattrs():
            attr_dict[a] = ds.getncattr(a)
        ds_new.setncatts(attr_dict)

        ds_new.date_created = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        #  copy dimension
        ds_new.createDimension(ds.dimensions['TIME'].name, len(var_time[:]))

        #  create new time
        time_var = ds_new.createVariable('TIME', 'f8', 'TIME', fill_value=np.NaN, zlib=True)
        #   copy times attributes and data
        attr_dict = {}
        for a in var_time.ncattrs():
            attr_dict[a] = var_time.getncattr(a)
        time_var.setncatts(attr_dict)
        time_var[:] = var_time[:]

        # # copy the NOMINAL_DEPTH, LATITUDE, and LONGITUDE
        # # TODO: check _FillValue
        # varList = ds.variables
        # for v in ['NOMINAL_DEPTH', 'LATITUDE', 'LONGITUDE']:
        #     maVariable = ds.variables[v][:]  # get the data
        #     varDims = varList[v].dimensions
        #
        #     ncVariableOut = ds_new.createVariable(v, varList[v].dtype, varDims, zlib=True)
        #
        #     for a in varList[v].ncattrs():
        #         ncVariableOut.setncattr(a, varList[v].getncattr(a))
        #
        #     ncVariableOut[:] = maVariable  # copy the data

        for v in ['pressure', 'xCO2_SW', 'xCO2_AIR']:
            maVariable = ds.variables[v][:]  # get the data
            varDims = varList[v].dimensions

            ncVariableOut = ds_new.createVariable(v, varList[v].dtype, varDims, zlib=True)

            for a in varList[v].ncattrs():
                ncVariableOut.setncattr(a, varList[v].getncattr(a))

            ncVariableOut[:] = maVariable  # copy the data

        #  create history
        ds_new.history += '\n' + now.strftime("%Y-%m-%d : ") + 'merged data created from ' + os.path.basename(filepath)

        ncFiles = glob.glob(os.path.join('pCO2', 'IMOS*FV01*.nc'))
        for fn in ncFiles:
            print('merging  :', fn)
            ds_merge = Dataset(fn, 'r')
            time_merge = ds_merge.variables['TIME']

            for v_to_merge in ['PSAL', 'TEMP']:
                psal_merge = ds_merge.variables[v_to_merge]
                psal_qc = ds_merge.variables[v_to_merge+'_quality_control']
                psal_merge_vals = psal_merge[:]
                psal_merge_vals[psal_qc[:] != 1] = np.nan

                psal_interp = np.interp(varList['TIME'][:], time_merge[:], psal_merge_vals, left=np.nan, right=np.nan)
                if v_to_merge not in ds_new.variables:
                    psal_var = ds_new.createVariable(v_to_merge, varList['xCO2_SW'].dtype, varList['xCO2_SW'].dimensions, zlib=True, fill_value=np.nan)
                    for a in ds_merge.variables[v_to_merge].ncattrs():
                        if a != '_FillValue' and a != 'ancillary_variables':
                            ds_new.variables[v_to_merge].setncattr(a, ds_merge.variables[v_to_merge].getncattr(a))
                else:
                    psal_var = ds_new.variables[v_to_merge]

                psal_vals = psal_var[:]
                msk = ~np.isnan(psal_interp)
                psal_vals[msk] = psal_interp[msk]

                psal_var[:] = psal_vals

                print(psal_vals[msk])

            ds_new.history += '\n' + now.strftime("%Y-%m-%d : ") + 'merged ' + os.path.basename(fn)

        ds_new.close()

        ds.close()

    return output_names


if __name__ == "__main__":

    merge(sys.argv[1:])
