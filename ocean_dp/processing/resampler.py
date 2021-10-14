import os
import shutil
import sys

from glob2 import glob
from netCDF4 import Dataset, date2num, num2date

from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import numpy as np
from scipy.interpolate import interp1d
import statsmodels.api as sm

np.set_printoptions(linewidth=256)


def resample(files, method, resample='True', hours=12):
    output_names = []

    now = datetime.utcnow()

    for filepath in files:

        fn_new = filepath
        dirname = os.path.dirname(fn_new)
        basename = os.path.basename(fn_new)

        ds = Dataset(filepath, 'r')

        # deal with TIME
        var_time = ds.variables["TIME"]

        # create the time window around the time_deployment_start and time_deployment_end
        datetime_deploy_start = datetime.strptime(ds.getncattr('time_deployment_start'), '%Y-%m-%dT%H:%M:%SZ')
        datetime_deploy_end = datetime.strptime(ds.getncattr('time_deployment_end'), '%Y-%m-%dT%H:%M:%SZ')

        datetime_start = datetime_deploy_start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        datetime_end = datetime_deploy_end.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        timedelta_hours = (datetime_end - datetime_start).total_seconds()/3600
        #print('datetime_start, datetime_end', datetime_start, datetime_end, timedelta_hours)

        sample_datetime = [(datetime_start + timedelta(hours=h)) for h in range(np.int(timedelta_hours+1))]
        print('sample_datetime_start, sample_datetime_end', sample_datetime[0], sample_datetime[-1])

        num_deploy_start = date2num(datetime_deploy_start, units=var_time.units)
        num_deploy_end = date2num(datetime_deploy_end, units=var_time.units)

        # read existing times, find sample rate
        time = var_time[:]

        # create mask for deployment time
        deployment_msk = (time > num_deploy_start) & (time < num_deploy_end)

        datetime_time = num2date(time, units=var_time.units)
        datetime_time_deployment = datetime_time[deployment_msk]
        time_deployment = time[deployment_msk]

        # use the mid point sample rate, as it may change at start/end
        n_mid = np.int(len(time_deployment)/2)
        t_mid0 = datetime_time_deployment[n_mid]
        t_mid1 = datetime_time_deployment[n_mid+1]

        sample_rate_mid = t_mid1 - t_mid0
        print('sample rate mid', sample_rate_mid.total_seconds(), '(seconds)')

        # find number of samples to make 2.2 hrs of data
        i = n_mid
        while (datetime_time_deployment[i] - t_mid0) < timedelta(hours=hours):
            i = i + 1
        i = i - n_mid

        window = np.max([i, 30])
        print('window (points)', window)
        frac = window/len(time_deployment)

        # create the new time array to sample to
        if resample:
            sample_datenum = date2num(sample_datetime, units=var_time.units)
        else:
            sample_datenum = date2num(datetime_time_deployment, units=var_time.units)

        print('len sample_datenum', len(sample_datenum))
        if basename.startswith("IMOS"):
            fn_split = basename.split('_')

            # IMOS_ABOS-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
            # 0    1         2   3        4    5    6                                    7            8
            # rename the file FV00 to FV01
            fn_split[3] = sample_datetime[0].strftime('%Y%m%d')
            fn_split[7] = sample_datetime[-1].strftime('END-%Y%m%d')
            fn_split[5] = "FV02"
            fn_split[6] = fn_split[6] + "-" + method

            # Change the creation date in the filename to today
            fn_split[8] = now.strftime("C-%Y%m%d.nc")
            fn_new = os.path.join(dirname, 'resample', "_".join(fn_split))

        # Add the new file name to the list of new file names
        output_names.append(fn_new)

        print('output file : ', fn_new)

        # output data to new file
        ds_new = Dataset(fn_new, 'w', format='NETCDF4_CLASSIC')

        #  copy global attributes
        attr_dict = {}
        for a in ds.ncattrs():
            attr_dict[a] = ds.getncattr(a)
        ds_new.setncatts(attr_dict)
        ds_new.comment_original_file_sample_rate_sec = str(sample_rate_mid)
        ds_new.date_created = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        #  copy dimension
        ds_new.createDimension(ds.dimensions['TIME'].name, len(sample_datenum))

        #  create new time
        time_var = ds_new.createVariable('TIME', 'f8', 'TIME', zlib=True)

        #   copy times attributes
        attr_dict = {}
        for a in var_time.ncattrs():
            if a != '_FillValue':
                attr_dict[a] = var_time.getncattr(a)

        time_var.setncatts(attr_dict)
        time_var[:] = sample_datenum

        # copy the NOMINAL_DEPTH, LATITUDE, and LONGITUDE
        # TODO: check _FillValue
        varList = ds.variables
        in_vars = set([x for x in ds.variables])

        z = in_vars.intersection(['NOMINAL_DEPTH', 'LATITUDE', 'LONGITUDE'])
        for v in z:
            maVariable = ds.variables[v][:]  # get the data
            varDims = varList[v].dimensions

            ncVariableOut = ds_new.createVariable(v, varList[v].dtype, varDims, zlib=True)

            for a in varList[v].ncattrs():
                ncVariableOut.setncattr(a, varList[v].getncattr(a))

            ncVariableOut[:] = maVariable  # copy the data

        # variable to smooth
        # print('input file vars', in_vars)
        z = in_vars.intersection(['PRES', 'TEMP', 'PSAL', 'CNDC', 'DENSITY', 'SIGMA_T0', 'DOX2', 'ATMP', 'AIRT', 'WSPD', 'SW', 'LW', 'UWIND', 'VWIND', 'CPHL', 'BB'])
        print ('vars to smooth', z)
        qc = np.ones_like(datetime_time)
        lowess = sm.nonparametric.lowess

        qc_in_level = 2
        for resample_var in z:

            var_to_resample_in = ds.variables[resample_var]
            only_qc = False
            if resample_var + '_quality_control' in ds.variables:
                print('using qc : ', resample_var + "_quality_control")
                qc = ds.variables[resample_var + "_quality_control"][:]
                only_qc = True

            data_in = var_to_resample_in[deployment_msk & (qc <= qc_in_level)]

            if only_qc and len(data_in) > 0:
                time_deployment = var_time[deployment_msk & (qc <= qc_in_level)]

                print(resample_var, 'input data : ', data_in)
                #print('times', time_deployment)
                #print('new times', new_times)

                # do the smoothing
                if method == 'lowess':
                    y = lowess(np.array(data_in), np.array(time_deployment), frac=frac, it=2, is_sorted=True, xvals=sample_datenum)
                    print('isnan', sum(np.isnan(y)))
                elif method == 'interp':
                    f = interp1d(np.array(time_deployment), np.array(data_in), bounds_error=False, kind='linear')
                    y = f(sample_datenum)
                else: # assume nearest
                    method = 'nearest'
                    f = interp1d(np.array(time_deployment), np.array(data_in), kind='nearest', bounds_error=False, fill_value=np.nan)
                    y = f(sample_datenum)

                print(resample_var, 'interpolated data', y)

                #  create output variables
                var_resample_out = ds_new.createVariable(resample_var, 'f4', 'TIME', fill_value=np.NaN, zlib=True)
                attr_dict = {}
                for a in var_to_resample_in.ncattrs():
                    if a != 'ancillary_variables' and a != '_FillValue' : # don't copy these for now
                        attr_dict[a] = var_to_resample_in.getncattr(a)

                var_resample_out.setncatts(attr_dict)

                # interpolate the time to get the distance to the nearest point
                f = interp1d(np.array(time_deployment), np.array(time_deployment), kind='nearest', bounds_error=False, fill_value=np.nan)
                sample_time_dist = (f(sample_datenum) - sample_datenum) * 24 * 3600

                print('sample distance', sample_time_dist, '(seconds)')
                # create a variable to save distance to nearest point
                var_resample_dist_out = ds_new.createVariable(resample_var + '_SAMPLE_TIME_DIFF', 'f4', 'TIME', fill_value=np.NaN, zlib=True)
                var_resample_dist_out.comment = 'seconds to actual sample timestamp, abs max='+str(max(abs(y)))
                var_resample_dist_out[:] = sample_time_dist

                # only use data where sample time is less than 3 hrs from the gridded time
                sample_time_dist_msk = abs(sample_time_dist) < 3 * 60 * 60
                var_resample_out[sample_time_dist_msk] = y[sample_time_dist_msk]

        #  create history
        ds_new.history += '\n' + now.strftime("%Y-%m-%d : ") + 'resampled data created from ' + os.path.basename(filepath) + ' window=' + str(window) + ' method=' + method

        ds_new.file_version = 'Level 2 - Derived Products'
        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
        ds_new.time_coverage_start = sample_datetime[0].strftime(ncTimeFormat)
        ds_new.time_coverage_end = sample_datetime[-1].strftime(ncTimeFormat)

        ds_new.close()

        ds.close()

    return output_names


def plot():
    #plt.plot(t_dt[msk], psal)
    #plt.plot(d_dt, y)

    plt.grid()

    plt.show()


if __name__ == "__main__":
    method = 'nearest'
    hours = 1
    files = []
    for f in sys.argv[1:]:
        if f.startswith('--method='):
            method = f.replace('--method=', '')
        if f.startswith('--hours='):
            hours = float(f.replace('--hours=', ''))
        files.extend(glob(f))

    resample(files, method, resample=True, hours=hours)
