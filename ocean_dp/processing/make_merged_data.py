import os
import shutil
import sys

from netCDF4 import Dataset, date2num, num2date, stringtochar

from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import numpy as np
from scipy.interpolate import interp1d
import statsmodels.api as sm


# this assumes that all datasets have the same TIME values


np.set_printoptions(linewidth=256)

def make(files):

    now = datetime.utcnow()

    nominal_depth = np.empty(len(files))

    for fn in range(len(files)):
        filepath = files[fn]
        ds = Dataset(filepath, 'r')

        nominal_depth[fn] = ds.variables['NOMINAL_DEPTH'][:]

        ds.close()

    nominal_depth_order = np.argsort(nominal_depth)

    print(nominal_depth[nominal_depth_order])

    var_list_dict = {}
    file_basenames = []
    for fn in range(len(files)):
        filepath = files[nominal_depth_order[fn]]
        file_basenames.append(os.path.basename(filepath))
        ds = Dataset(filepath, 'r')

        for v in ds.variables:
            if v != 'TIME':
                if 'TIME' in ds.variables[v].dimensions:
                    print(filepath, v, ds.variables[v].dimensions)
                    if v not in var_list_dict.keys():
                        var_list_dict[v] = []
                    var_list_dict[v].append(nominal_depth_order[fn])

        ds.close()
    print(var_list_dict)

    ds = Dataset(files[nominal_depth_order[0]], 'r')

    var_time = ds.variables["TIME"]
    sample_datetime = num2date(var_time[:], units=var_time.units)

    # IMOS_DWM-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
    # 0    1         2   3        4    5    6                                    7            8
    fn_split = ['IMOS', ds.institution, 'PTSO', sample_datetime[0].strftime('%Y%m%d'), ds.platform_code, 'FV02', ds.deployment_code + '-gridded', sample_datetime[-1].strftime('END-%Y%m%d'), now.strftime("C-%Y%m%d.nc")]
    fn_new = "_".join(fn_split)

    ds_new = Dataset(fn_new, 'w', format='NETCDF4_CLASSIC')

    # create the TIME, LATITUDE, LONGITUDE from first file

    ds_new.createDimension("TIME", len(sample_datetime))
    ncTimesOut = ds_new.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = var_time[:]

    # copy the LATITUDE, and LONGITUDE
    # TODO: check _FillValue
    varList = ds.variables
    for v in ['LATITUDE', 'LONGITUDE']:
        maVariable = ds.variables[v][:]  # get the data
        varDims = varList[v].dimensions

        ncVariableOut = ds_new.createVariable(v, varList[v].dtype, varDims, zlib=True)

        for a in varList[v].ncattrs():
            ncVariableOut.setncattr(a, varList[v].getncattr(a))

        ncVariableOut[:] = maVariable  # copy the data

    #  copy global attributes
    attr_dict = {}
    delete_list = ['deployment_code', 'instrument', 'instrument_model', 'instrument_nominal_depth', 'instrument_serial_number']
    for a in ds.ncattrs():
        if a not in delete_list:
            attr_dict[a] = ds.getncattr(a)
    ds_new.setncatts(attr_dict)
    ds_new.date_created = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    ds.close()

    # some metadata about the source files
    ds_new.createDimension("strlen256", 256)
    ds_new.createDimension("NOMINAL_DEPTHS", len(nominal_depth))
    file_names_var = ds_new.createVariable("source_file", "S1", ("NOMINAL_DEPTHS", "strlen256",), zlib=True)
    nds_var = ds_new.createVariable("NOMINAL_DEPTHS", "f4", ("NOMINAL_DEPTHS", ), zlib=True)

    files_basename_array = np.array(file_basenames, dtype='S256')
    file_names_var[:] = stringtochar(files_basename_array)
    nds_var[:] = nominal_depth[nominal_depth_order]

    # for each variable in the source files create a output variable with an instance of each input file
    for v in var_list_dict:
        ds_new.createDimension("INSTANCE_"+v, len(var_list_dict[v]))
        nd_out = ds_new.createVariable("NOMINAL_DEPTH_"+v, "f4", ("INSTANCE_"+v), zlib=True)
        nd_out[:] = nominal_depth[var_list_dict[v]]
        fn_out = ds_new.createVariable("SOURCE_FILE_"+v, "i2", ("INSTANCE_"+v), zlib=True)

        temp_out = ds_new.createVariable(v, "f4", ("INSTANCE_" + v, "TIME"), fill_value=np.nan, zlib=True)
        # for each file copy the data
        for fn in range(len(var_list_dict[v])):
            filepath = files[var_list_dict[v][fn]]
            print(v, 'processing', os.path.basename(filepath))
            fn_idx = file_basenames.index(os.path.basename(filepath))
            fn_out[fn] = fn_idx
            ds = Dataset(filepath, 'r')

            temp_out[fn, :] = ds.variables['TEMP'][:]

            ds.close()

    ds_new.history = now.strftime("%Y-%m-%d") + ' : merged data'

    ds_new.file_version = 'Level 2 – Derived Products'
    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
    ds_new.time_coverage_start = sample_datetime[0].strftime(ncTimeFormat)
    ds_new.time_coverage_end = sample_datetime[-1].strftime(ncTimeFormat)

    ds_new.close()


if __name__ == "__main__":
    make(sys.argv[1:])
