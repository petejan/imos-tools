import os
import sys

from glob2 import glob
from netCDF4 import Dataset, date2num, num2date

from datetime import datetime, timedelta, UTC

import numpy as np
from scipy.interpolate import interp1d
from scipy import stats

np.set_printoptions(linewidth=256)


def down_sample(files, method):
    output_names = []

    now = datetime.now(UTC)

    for filepath in files:

        fn_new = filepath
        dirname = os.path.dirname(fn_new)
        basename = os.path.basename(fn_new)

        ds = Dataset(filepath, 'r')
        ds.set_auto_mask(False)

        # deal with TIME
        var_time = ds.variables["TIME"]

        # create the time window around the time_deployment_start and time_deployment_end
        datetime_deploy_start = datetime.strptime(ds.getncattr('time_deployment_start'), '%Y-%m-%dT%H:%M:%SZ')
        datetime_deploy_end = datetime.strptime(ds.getncattr('time_deployment_end'), '%Y-%m-%dT%H:%M:%SZ')

        # create an array of sample points between deployment start and deployment end
        datetime_start = datetime_deploy_start.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        datetime_end = datetime_deploy_end.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        timedelta_hours = (datetime_end - datetime_start).total_seconds()/3600

        sample_datetime = [(datetime_start + timedelta(hours=h)) for h in range(int(timedelta_hours+1))]
        print('sample_datetime_start, sample_datetime_end', sample_datetime[0], sample_datetime[-1])

        num_deploy_start = date2num(datetime_deploy_start, units=var_time.units)
        num_deploy_end = date2num(datetime_deploy_end, units=var_time.units)

        bin_datetime = [(datetime_start + timedelta(hours=(h-0.5))) for h in range(int(timedelta_hours+2))]
        bins = date2num(bin_datetime, units=var_time.units)

        # read existing times, find sample rate
        time = var_time[:]

        # create mask for deployment time
        deployment_msk = (time > num_deploy_start) & (time < num_deploy_end)

        datetime_time = num2date(time, units=var_time.units)
        datetime_time_deployment = datetime_time[deployment_msk]
        time_deployment = time[deployment_msk]

        # use the mid point sample rate, as it may change at start/end
        n_mid = int(len(time_deployment)/2)
        t_mid0 = datetime_time_deployment[n_mid]
        t_mid1 = datetime_time_deployment[n_mid+1]

        sample_rate_mid = t_mid1 - t_mid0
        print('sample rate mid', sample_rate_mid.total_seconds(), '(seconds)')

        # # find number of samples to make sample interval hrs of data
        # window = n_mid
        # while (datetime_time_deployment[window] - t_mid0) < timedelta(hours=hours):
        #     window = window + 1
        # window = window - n_mid
        #

        # create the new time array to sample to
        sample_datenum = date2num(sample_datetime, units=var_time.units)

        print('len sample_datenum', len(sample_datenum))
        if basename.startswith("IMOS"):
            fn_split = basename.split('_')

            # IMOS_DWM-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
            # 0    1         2   3        4    5    6                                    7            8
            # rename the file FV00 to FV01
            fn_split[3] = sample_datetime[0].strftime('%Y%m%d')
            fn_split[7] = sample_datetime[-1].strftime('END-%Y%m%d')
            fn_split[5] = "FV02"
            fn_split[6] = fn_split[6] + "-" + method

            # Change the creation date in the filename to today
            fn_split[8] = now.strftime("C-%Y%m%d.nc")
            fn_new = os.path.join(dirname, 'resample', "_".join(fn_split))
        else:
            fn_new = basename + "-resample.nc"

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

        # copy the coordinate variables
        # TODO: check _FillValue
        varList = ds.variables
        in_vars = set([x for x in ds.variables])

        # find list of coordinate variables
        z = []
        for v in ds.variables:
            dims = ds.variables[v].dimensions
            print('checking for coordinate var', v, dims, len(dims))
            if (v != 'TIME') and (len(dims) == 0 or v == dims[0] and len(dims) == 1):
                z.append(v)
        for v in z:
            print("processing coord", v)
            maVariable = ds.variables[v][:]  # get the data
            varDims = varList[v].dimensions

            ncVariableOut = ds_new.createVariable(v, varList[v].dtype, varDims, zlib=True)

            for a in varList[v].ncattrs():
                if a not in ['_FillValue']:
                    ncVariableOut.setncattr(a, varList[v].getncattr(a))

            print('shape', maVariable.shape, ncVariableOut.shape)
            if len(maVariable.shape) and maVariable.shape[0] != 1:
                f = interp1d(np.array(time_deployment), np.array(maVariable[deployment_msk]), kind='nearest', bounds_error=False, fill_value=np.nan)
                ncVariableOut[:] = f(sample_datenum)
            else:
                ncVariableOut[:] = maVariable  # copy the data

        # variable to smooth
        # print('input file vars', in_vars)
        z = in_vars.intersection(['PRES', 'PRES_REL', 'TEMP', 'PSAL', 'CNDC', 'DENSITY', 'SIGMA_T0', 'DOX2', 'DOXS', 'ATMP', 'AIRT', 'AIRT2_0M', 'AIRT1_5M',
                                  'RELH', 'RELH1_5M', 'RELH2_0M', 'HL', 'HS', 'PL_CMP', 'RAIN_AMOUNT',
                                  'H_RAIN', 'TAU', 'SST', 'HEAT_NET', 'MASS_NET', 'LW_NET', 'SW_NET',
                                  'SW', 'LW', 'UWIND', 'VWIND', 'CPHL', 'BB',
                                  'VAVH', 'SWH', 'Hm0', 'Tz',
                                  'UCUR', 'VCUR', 'WCUR',
                                  'xCO2_SW', 'xCO2_AIR', 'xCO2_SW_WET', 'xCO2_AIR_WET',
                                  'PAR',
                                  'NTRI_CONC', 'ALKA_CONC', 'PHOS_CONC', 'SLCA_CONC', 'TCO2',
                                  'WEIGHT', 'NTRI', 'PHOS', 'SLCA', 'TALK', 'pHt'])
        print ('vars to smooth', z)

        qc_in_level = 2
        for resample_var in sorted(z):
            qc = np.ones_like(datetime_time)

            var_to_resample_in = ds.variables[resample_var]
            if ds.file_version.startswith("Level 1"):

                if resample_var + '_quality_control' in ds.variables:
                    print('using qc : ', resample_var + "_quality_control")
                    qc = ds.variables[resample_var + "_quality_control"][:]
                else:
                    print(resample_var, 'no QC, skipping')
                    continue  # only include variables that have quality_control

            if any(qc) == 7:  # only include variables which are not interpolated
                print('only interpolated data, not including in resampled data')
                continue

            data_in = var_to_resample_in[qc <= qc_in_level]

            print()
            print('len data', len(data_in))
            if len(data_in) > 0:
                time_deployment = var_time[qc <= qc_in_level]

                print(resample_var, 'input data : ', data_in)

                y = stats.binned_statistic(np.array(time_deployment), np.array(data_in), statistic='mean', bins=bins)
                sd = stats.binned_statistic(np.array(time_deployment), np.array(data_in), statistic='std', bins=bins)
                n = stats.binned_statistic(np.array(time_deployment), np.array(data_in), statistic='count', bins=bins)

                qc_out = stats.binned_statistic(np.array(time_deployment), np.array(qc[qc <= qc_in_level]), statistic='max', bins=bins)

                #print('sampled data', y)

                #  create output variables
                var_resample_out = ds_new.createVariable(resample_var, 'f4', 'TIME', fill_value=np.nan, zlib=True)
                attr_dict = {}
                for a in var_to_resample_in.ncattrs():
                    if a != 'ancillary_variables' and a != '_FillValue' : # don't copy these for now
                        attr_dict[a] = var_to_resample_in.getncattr(a)

                var_resample_out.setncatts(attr_dict)

                # # interpolate the time to get the distance to the nearest point
                # f = interp1d(np.array(time_deployment), np.array(time_deployment), kind='nearest', bounds_error=False, fill_value=np.nan)
                # sample_time_dist = (f(sample_datenum) - sample_datenum) * 24 * 3600
                #
                # print('sample distance', sample_time_dist, '(seconds)')
                # create a variable to save distance to nearest point
                # var_resample_dist_out = ds_new.createVariable(resample_var + '_SAMPLE_TIME_DIFF', 'f4', 'TIME', fill_value=np.nan, zlib=True)
                # var_resample_dist_out.comment = 'seconds to actual sample timestamp, abs max='+str(max(abs(sample_time_dist)))
                # var_resample_dist_out[:] = sample_time_dist
                #
                # # only use data where sample time is less than 3 hrs from the gridded time
                # sample_time_dist_msk = abs(sample_time_dist) < 0.75 * 60 * 60

                aux_vars = []
                if resample_var + "_quality_control" in ds.variables:
                    var_resample_dist_out = ds_new.createVariable(resample_var + '_quality_control', 'i1', 'TIME', fill_value=99, zlib=True)
                    try:
                        var_resample_dist_out.standard_name = ds.variables[resample_var + "_quality_control"].standard_name
                    except:
                        pass
                    var_resample_dist_out.long_name = ds.variables[resample_var + "_quality_control"].long_name
                    var_resample_dist_out.comment = 'maximum of quality flags of input data'
                    var_resample_dist_out.quality_control_conventions = 'IMOS standard flags'
                    var_resample_dist_out.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
                    var_resample_dist_out.flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value"

                    var_resample_dist_out[:] = qc_out.statistic

                    aux_vars.append(resample_var + '_quality_control')

                var_resample_dist_out = ds_new.createVariable(resample_var + '_standard_error', 'f4', 'TIME', fill_value=np.nan, zlib=True)
                var_resample_dist_out.long_name = ds.variables[resample_var].long_name + ' standard error'
                var_resample_dist_out.units = var_resample_out.units
                var_resample_dist_out.comment = 'sample bin standard deviation'
                var_resample_dist_out[:] = sd.statistic
                aux_vars.append(resample_var + '_standard_error')

                var_resample_dist_out = ds_new.createVariable(resample_var + '_number_of_observations', 'f4', 'TIME', fill_value=np.nan, zlib=True)
                var_resample_dist_out.long_name = ds.variables[resample_var].long_name + ' number of observations'
                var_resample_dist_out.units = '1'
                var_resample_dist_out.comment = 'number of samples'
                var_resample_dist_out[:] = n.statistic
                aux_vars.append(resample_var + '_number_of_observations')

                var_resample_out.ancillary_variables = " ".join(aux_vars)

                print("shape", var_resample_out.shape, y.statistic.shape)
                var_resample_out[:] = y.statistic

        #  create history
        # update the history attribute
        try:
            hist = ds_new.history + "\n"
        except AttributeError:
            hist = ""

        hist += now.strftime("%Y-%m-%d") + ' resampled data created from ' + os.path.basename(filepath) + ' points, method ' + method
        ds_new.history = hist

        ds_new.file_version = 'Level 2 - Derived Products'
        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
        ds_new.time_coverage_start = sample_datetime[0].strftime(ncTimeFormat)
        ds_new.time_coverage_end = sample_datetime[-1].strftime(ncTimeFormat)

        ds_new.close()

        ds.close()

    return output_names


if __name__ == "__main__":
    method = 'mean'
    hours = 1
    files = []
    for f in sys.argv[1:]:
        if f.startswith('--method='):
            method = f.replace('--method=', '')
        if f.startswith('--hours='):
            hours = float(f.replace('--hours=', ''))
        files.extend(glob(f))

    down_sample(files, method)
