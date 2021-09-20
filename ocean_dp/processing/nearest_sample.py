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

def smooth(files):
    output_names = []

    now = datetime.utcnow()

    for filepath in files:

        fn_new = filepath
        dirname = os.path.dirname(fn_new)
        basename = os.path.basename(fn_new)

        ds = Dataset(filepath, 'r')

        # deal with TIME
        var_time = ds.variables["TIME"]

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
        msk = (time > num_deploy_start) & (time < num_deploy_end)

        datetime_time = num2date(time, units=var_time.units)
        datetime_time_masked = datetime_time[msk]
        time_masked = time[msk]

        # use the mid point sample rate, as it may change at start/end
        n_mid = np.int(len(time_masked)/2)
        t_mid0 = datetime_time_masked[n_mid]
        t_mid1 = datetime_time_masked[n_mid+1]

        sample_rate_mid = t_mid1 - t_mid0
        print('sample rate mid', sample_rate_mid.total_seconds(), '(seconds)')

        # create the new time array to sample to
        sample_datenum = date2num(sample_datetime, units=var_time.units )

        print('len sample_datenum', len(sample_datenum))
        if basename.startswith("IMOS"):
            fn_split = basename.split('_')

            # IMOS_ABOS-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
            # 0    1         2   3        4    5    6                                    7            8
            # rename the file FV00 to FV01
            fn_split[3] = sample_datetime[0].strftime('%Y%m%d')
            fn_split[7] = sample_datetime[-1].strftime('END-%Y%m%d')
            fn_split[5] = "FV02"
            fn_split[6] = fn_split[6] + "-nearest"

            # Change the creation date in the filename to today
            fn_split[8] = now.strftime("C-%Y%m%d.nc")
            fn_new = os.path.join(dirname, 'resample', "_".join(fn_split))
        else:
            fn_new = fn_new.replace('.nc', '-resample.nc')

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

        ds_new.comment_original_file_sample_rate_sec = str(sample_rate_mid)
        ds_new.date_created = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        #  copy dimension
        ds_new.createDimension(ds.dimensions['TIME'].name, len(sample_datenum))

        #  create new time
        time_var = ds_new.createVariable('TIME', 'f8', 'TIME', zlib=True)

        #   copy times attributes and data
        attr_dict = {}
        for a in var_time.ncattrs():
            if a != '_FillValue':
                attr_dict[a] = var_time.getncattr(a)

        time_var.setncatts(attr_dict)
        time_var[:] = sample_datenum

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

        # variable to resample
        in_vars = set([x for x in ds.variables])
        # print('input file vars', in_vars)
        z = in_vars.intersection(['PRES', 'TEMP', 'PSAL', 'DENSITY', 'SIGMA_T0', 'DOX2', 'AIRT', 'RELH', 'SW', 'LW', 'WSPD', 'CPHL', 'TURB', 'BB', 'ATMP', 'xCO2_AIR', 'xCO2_SW'])
        print ('vars to smooth', z)
        qc = np.ones_like(datetime_time)

        # interpolate the time to get the distance to the nearest point
        f = interp1d(np.array(time_masked), np.array(time_masked), kind='nearest', bounds_error=False, fill_value=np.nan)
        sample_time_diff = (f(sample_datenum) - sample_datenum) * 24 * 3600

        print('sample distance', sample_time_diff, '(seconds)')
        var_resample_out = ds_new.createVariable('SAMPLE_TIME_DIFF', 'f4', 'TIME', fill_value=np.NaN, zlib=True)
        var_resample_out.comment = 'seconds to actual sample timestamp'
        var_resample_out[:] = sample_time_diff

        for resample_var in z:

            qc = np.ones_like(datetime_time)
            var_to_smooth_in = ds.variables[resample_var]
            if resample_var + '_quality_control' in ds.variables:
                print('using qc : ', resample_var + "_quality_control")
                qc = ds.variables[resample_var + "_quality_control"][:]

            data_in = var_to_smooth_in[msk & (qc <= 2)]
            time_masked = var_time[msk & (qc <= 2)]
            if len(time_masked) == 0:
                continue

            print(resample_var, 'input data : ', data_in)

            # do the resample

            f = interp1d(np.array(time_masked), np.array(data_in), kind='nearest', bounds_error=False, fill_value=np.nan)
            y = f(sample_datenum)

            print('interpolated data', resample_var, y)

            # mark time cells bad where there are less than 3 samples in +/- 2.2 hours
            bad = 0
            # for v in range(0, len(sample_datenum)):
            #     time_cell_min = np.where(time_masked > (sample_datenum[v] - 24/24)) # TODO: only works when time.units='days since .....'
            #     time_cell_max = np.where(time_masked < (sample_datenum[v] + 24/24))
            #     # print(np.shape(time_cell_max), np.shape(time_cell_min))
            #     if np.shape(time_cell_min)[1] > 0 and np.shape(time_cell_max)[1] > 0:
            #         #print(v, time_cell_max[0][-1], time_cell_min[0][0], time_cell_max[0][-1]-time_cell_min[0][0])
            #         #print(dy[0][-1]-dx[0][0])
            #         if (time_cell_max[0][-1]-time_cell_min[0][0]) < 1:
            #             y[v] = np.nan
            #             bad += 1
            #     else:
            #         y[v] = np.nan
            #         bad += 1
            too_far_msk = sample_time_diff > 3600*1.5
            bad = sum(too_far_msk)
            y[too_far_msk] = np.nan
            print('number bad', bad)

            #  create output variables
            var_resample_out = ds_new.createVariable(resample_var, 'f4', 'TIME', fill_value=np.NaN, zlib=True)
            attr_dict = {}
            for a in var_to_smooth_in.ncattrs():
                if a != 'ancillary_variables': # don't copy these for now, did we want the aux (quality_code) vars copied
                    attr_dict[a] = var_to_smooth_in.getncattr(a)

            var_resample_out.setncatts(attr_dict)
            var_resample_out[:] = y

        #  create history
        ds_new.history += '\n' + now.strftime("%Y-%m-%d : ") + 'resampled data created from ' + os.path.basename(filepath)

        ds_new.file_version = "Level 2 - Derived Products"
        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
        ds_new.time_coverage_start = sample_datetime[0].strftime(ncTimeFormat)
        ds_new.time_coverage_end = sample_datetime[-1].strftime(ncTimeFormat)

        ds_new.close()

        ds.close()

        print()

    return output_names

def plot():
    #plt.plot(t_dt[msk], psal)
    #plt.plot(d_dt, y)

    plt.grid()

    plt.show()


if __name__ == "__main__":
    # netCDFfile = '../../data/TEMP/netcdf-surface/IMOS_ABOS-SOTS_CST_20130328_SOFS_FV00_SOFS-4-2013-SBE37SM-RS485-03707409-1m_END-20131028_C-20200317.nc'
    # netCDFfile = '../../data/TEMP/netCDF-upper/IMOS_ABOS-SOTS_CFPST_20100817_SOFS_FV00_Pulse-7-2010-SBE16plus-01606331-31m_END-20110430_C-20200428.nc'
    #netCDFfile = '../../data/TEMP/netCDF-upper/IMOS_ABOS-SOTS_CPT_20110729_SOFS_FV00_Pulse-8-2011-SBE16plusV2-01606330-34m_END-20120711_C-20200427.nc'

    # netCDFfile = sys.argv[1]

    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    smooth(files)
