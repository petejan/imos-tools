import os
import shutil
import sys

from netCDF4 import Dataset, date2num, num2date

from datetime import datetime
import matplotlib.pyplot as plt

import numpy as np
from scipy import interpolate


def bisection(array, value):
    '''Given an ``array`` , and given a ``value`` , returns an index j such that ``value`` is between array[j]
    and array[j+1]. ``array`` must be monotonic increasing. j=-1 or j=len(array) is returned
    to indicate that ``value`` is out of range below and above respectively.'''
    n = len(array)
    if (value < array[0]):
        return -1
    elif (value > array[n-1]):
        return n
    jl = 0# Initialize lower
    ju = n-1# and upper limits.
    while (ju-jl > 1):# If we are not yet done,
        jm=(ju+jl) >> 1# compute a midpoint with a bitshift
        if (value >= array[jm]):
            jl=jm# and replace either the lower limit
        else:
            ju=jm# or the upper limit, as appropriate.
        # Repeat until the test condition is satisfied.
    if (value == array[0]):# edge cases at bottom
        return 0
    elif (value == array[n-1]):# and top
        return n-1
    else:
        return jl


def resample(files):
    output_names = []

    now = datetime.utcnow()

    for filepath in files:

        fn_new = filepath
        dirname = os.path.dirname(fn_new)
        basename = os.path.basename(fn_new)
        ds = Dataset(filepath, 'r')
        print('input file : ', filepath)

        # deal with TIME
        var_time = ds.variables["TIME"]

        date_time_start = datetime.strptime(ds.getncattr('time_deployment_start'), '%Y-%m-%dT%H:%M:%SZ')
        date_time_end = datetime.strptime(ds.getncattr('time_deployment_end'), '%Y-%m-%dT%H:%M:%SZ')

        time = var_time[:]
        # create mask for deployment time
        msk = (time > date2num(date_time_start, units=var_time.units)) & (time < date2num(date_time_end, units=var_time.units))

        time_masked = var_time[msk]

        t0 = time_masked[0]
        tend = time_masked[-1]

        # create the new time array to sample to
        d0 = np.ceil(t0*24)
        dend = np.floor(tend*24)
        resample_times = np.arange(d0, dend, 1) / 24
        #resample_date_time = num2date(resample_times, units=var_time.units)
        resample_start = num2date(resample_times[0], units=var_time.units)
        resample_end = num2date(resample_times[-1], units=var_time.units)
        print('first time', resample_start, 'end time', resample_end)

        if basename.startswith("IMOS"):
            fn_split = basename.split('_')

            # IMOS_DWM-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
            # 0    1         2   3        4    5    6                                    7            8
            fn_split[5] = "FV02"
            fn_split[6] = fn_split[6] + "-resample"

            fn_split[3] = resample_start.strftime("%Y%m%d")
            fn_split[7] = resample_end.strftime("END-%Y%m%d")

            # Change the creation date in the filename to today
            fn_split[8] = now.strftime("C-%Y%m%d.nc")
            fn_new = os.path.join(dirname, 'resample', "_".join(fn_split))

        # Add the new file name to the list of new file names
        output_names.append(fn_new)

        print('output file : ', fn_new)

        # output data to new file
        ds_new = Dataset(fn_new, 'w')

        #  copy global attributes
        attr_dict = {}
        for a in ds.ncattrs():
            attr_dict[a] = ds.getncattr(a)
        ds_new.setncatts(attr_dict)
        ds_new.date_created = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        #  copy dimension
        ds_new.createDimension(ds.dimensions['TIME'].name, len(resample_times))

        #  create new time
        time_var = ds_new.createVariable('TIME', 'f8', 'TIME', fill_value=np.NaN, zlib=True)
        #   copy times attributes and data
        attr_dict = {}
        for a in var_time.ncattrs():
            attr_dict[a] = var_time.getncattr(a)
        time_var.setncatts(attr_dict)
        time_var[:] = resample_times

        # copy the NOMINAL_DEPTH, LATITUDE, and LONGITUDE
        # TODO: check _FillValue
        varList = ds.variables
        for v in ['NOMINAL_DEPTH', 'LATITUDE', 'LONGITUDE']:
            maVariable = ds.variables[v][:]  # get the data
            varDims = varList[v].dimensions

            ncVariableOut = ds_new.createVariable(v, varList[v].dtype, varDims, zlib=True)

            for a in varList[v].ncattrs():
                ncVariableOut.setncattr(a, varList[v].getncattr(a))

            ncVariableOut[:] = maVariable  # copy the data

        # variable to smooth
        in_vars = set([x for x in ds.variables])
        # print('input file vars', in_vars)
        z = in_vars.intersection(['TEMP', 'PSAL', 'DENSITY', 'DOX2', 'PRES'])
        print ('vars to smooth', z)
        for var_name in z:

            var_to_resample = ds.variables[var_name]
            qc = np.zeros_like(var_to_resample)
            if var_name + '_quality_control' in ds.variables:
                qc = ds.variables[var_name + '_quality_control'][:]

            var_msk = qc < 2
            # need to use QC variable as mask also
            resample_data = var_to_resample[var_msk]

            print(var_name, 'input data : ', var_to_resample[var_msk])
            x = np.array(var_time[var_msk])
            y = np.array(resample_data)

            y_resample = np.zeros_like(resample_times)*np.nan

            # resample to new times (resample_times), by
            # 1. find the nearest point to the new time
            # 2. find points within +/-45 min range of the resample time
            # 3. if there are more than 3 points in this range
            # 4. fit a linear line between the points in the window
            # 5. calculate the new value along the line at the new time
            # this time search must be done for each variable as the QC fags maybe different
            for idx in range(0, len(resample_times)):
                resample_time = resample_times[idx]
                idx_resample = bisection(x, resample_time) # find the index of the nearest point to the new sample time
                if (idx_resample >= 0) and (idx_resample < len(x)):
                    #print(idx_resample, y[idx_resample], num2date(x[idx_resample], units=var_time.units), 'resample time', num2date(resample_time, units=var_time.units))

                    if abs(resample_time - x[idx_resample]) > 120/60/24:
                        print('nearest point', num2date(resample_time, units=var_time.units)-num2date(x[idx_resample], units=var_time.units), 'is more than 120 mins away')
                        continue

                    # find indexes of window which is +/- 30 mins wide
                    window_width = 30
                    idx_start_window = idx_resample
                    while (idx_start_window > 1) and (x[idx_start_window-1] - resample_time) > -window_width/60/24:
                        idx_start_window -= 1
                    idx_end_window = idx_resample
                    while (idx_end_window < len(x)-1-1) and (x[idx_end_window+1] - resample_time) < window_width/60/24:
                        idx_end_window += 1

                    # TODO: might be better to check that the 'next point' is within 2x window than number of points
                    if (idx_end_window - idx_start_window) < 3:
                        #print('back offset', (x[idx_end_window-1]-resample_time)*60*24, 'forward offset', (x[idx_end_window+1]-resample_time)*60*24)
                        if (x[idx_end_window+1]-resample_time) < 240/60/24:
                            #print((idx_end_window - idx_start_window), "points, widening window forward")
                            idx_end_window += 1
                        if (x[idx_start_window-1]-resample_time) > -240/60/24:
                            #print((idx_end_window - idx_start_window), "points, widening window back")
                            idx_start_window -= 1

                    #print('window points', idx_end_window-idx_start_window, 'for', num2date(resample_time, units=var_time.units),
                    #      'nearest_offset', num2date(resample_time, units=var_time.units)-num2date(x[idx_resample], units=var_time.units),
                    #      'back_offset', num2date(resample_time, units=var_time.units)-num2date(x[idx_start_window], units=var_time.units),
                    #      'forward_offset', num2date(x[idx_end_window], units=var_time.units)-num2date(resample_time, units=var_time.units))

                    # if we have more than 3 points in window, interpolate to centre time
                    if (idx_end_window - idx_start_window) >= 2:
                        if (resample_time > x[idx_start_window]) and (resample_time < x[idx_end_window]):
                            f = interpolate.interp1d(x[idx_start_window:idx_end_window+1], y[idx_start_window:idx_end_window+1])
                            try:
                                y_resample[idx] = f(resample_time)
                                #print(num2date(resample_time, units=var_time.units), y_resample[idx])
                            except ValueError:
                                print('value error', idx_start_window, idx_resample, idx_end_window, len(x), num2date(resample_time, units=var_time.units))
                                for z in range(idx_start_window, idx_end_window+1):
                                    print('ts', num2date(x[z], units=var_time.units), '=', y[z])
                    else:
                        print('less than 3 sample around', num2date(resample_time, units=var_time.units), num2date(x[idx_resample], units=var_time.units))
                else:
                    print('no sample for', num2date(resample_time, units=var_time.units))

                    #print(num2date(resample_time, units=var_time.units), 'points in resample', idx_end_window - idx_start_window, 'new sample', y_resample[idx])

            # do the resample

            print('output data : ', y)

            #  create output variables
            var_resample_out = ds_new.createVariable(var_name, 'f4', 'TIME', fill_value=np.NaN, zlib=True)
            attr_dict = {}
            for a in var_to_resample.ncattrs():
                attr_dict[a] = var_to_resample.getncattr(a)
            var_resample_out.setncatts(attr_dict)
            var_resample_out[:] = y_resample

        ds_new.time_coverage_start = resample_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        ds_new.time_coverage_end = resample_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        #  create history
        ds_new.history += '\n' + now.strftime("%Y-%m-%d : ") + 'resampled data created from ' + os.path.basename(filepath)

        ds_new.close()

        ds.close()

    return output_names

def plot():
    #plt.plot(t_dt[msk], psal)
    #plt.plot(d_dt, y)

    plt.grid()

    plt.show()


if __name__ == "__main__":
    # netCDFfile = '../../data/TEMP/netcdf-surface/IMOS_DWM-SOTS_CST_20130328_SOFS_FV00_SOFS-4-2013-SBE37SM-RS485-03707409-1m_END-20131028_C-20200317.nc'
    # netCDFfile = '../../data/TEMP/netCDF-upper/IMOS_DWM-SOTS_CFPST_20100817_SOFS_FV00_Pulse-7-2010-SBE16plus-01606331-31m_END-20110430_C-20200428.nc'
    #netCDFfile = '../../data/TEMP/netCDF-upper/IMOS_DWM-SOTS_CPT_20110729_SOFS_FV00_Pulse-8-2011-SBE16plusV2-01606330-34m_END-20120711_C-20200427.nc'

    # netCDFfile = sys.argv[1]

    resample(sys.argv[1:])
