import os
import shutil
import sys

from netCDF4 import Dataset, date2num, num2date

from datetime import datetime
import matplotlib.pyplot as plt

from pyloess import Loess
import numpy as np

def smooth(files):
    output_names = []

    now = datetime.utcnow()

    for filepath in files:

        fn_new = filepath
        dirname = os.path.dirname(fn_new)
        basename = os.path.basename(fn_new)
        if basename.startswith("IMOS"):
            fn_split = basename.split('_')

            # IMOS_ABOS-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
            # 0    1         2   3        4    5    6                                    7            8
            # rename the file FV00 to FV01
            fn_split[6] = fn_split[6] + "-Smooth"

            # Change the creation date in the filename to today
            fn_split[8] = now.strftime("C-%Y%m%d.nc")
            fn_new = os.path.join(dirname, "_".join(fn_split))

        # Add the new file name to the list of new file names
        output_names.append(fn_new)

        print()
        print("output", fn_new)

        ds = Dataset(filepath, 'r')

        # deal with TIME
        var_time = ds.variables["TIME"]

        date_time_start = datetime.strptime(ds.getncattr('time_deployment_start'), '%Y-%m-%dT%H:%M:%SZ')
        date_time_end = datetime.strptime(ds.getncattr('time_deployment_end'), '%Y-%m-%dT%H:%M:%SZ')

        time = var_time[:]
        # create mask for deployment time
        msk = (time > date2num(date_time_start, units=var_time.units)) & (time < date2num(date_time_end, units=var_time.units))

        t_dt = num2date(time, units=var_time.units)
        time_masked = var_time[msk]

        t0 = time_masked[0]
        t1 = time_masked[1]
        tend = time_masked[-1]

        sample_rate = np.round(24*3600*(tend-t0)/len(time_masked))
        print('dt ', (t1 - t0)*24*3600, 'sec, sample_rate', sample_rate)

        # fine number of samples to make 3 hrs of data
        i = 0
        while time_masked[i] < (t0 + 3/24):
            i = i + 1

        window = np.max([i, 3])
        print('window (points)', window)

        # create a new time array to sample to
        d0 = np.ceil(t0*24) / 24
        dend = np.floor(tend*24) / 24
        d = np.arange(d0, dend, 1/24)
        d_dt = num2date(d, units=var_time.units)

        # output data to new file
        ds_new = Dataset(fn_new, 'w')

        #  copy global vars
        attr_dict = {}
        for a in ds.ncattrs():
            attr_dict[a] = ds.getncattr(a)
        ds_new.setncatts(attr_dict)

        #  copy dimension
        ds_new.createDimension(ds.dimensions['TIME'].name, len(d_dt))

        #  create new time
        time_var = ds_new.createVariable('TIME', 'f8', 'TIME', fill_value=np.NaN, zlib=True)

        # variable to smooth

        degree = 3
        in_vars = set([x for x in ds.variables])
        # print('input file vars', in_vars)
        z = in_vars.intersection(['TEMP', 'PSAL', 'DENSITY', 'DOX2'])
        print ('vars to smooth', z)
        for smooth_var in z:

            var_to_smooth_in = ds.variables[smooth_var]

            smooth_in = var_to_smooth_in[msk]

            print(var_to_smooth_in[msk])

            # do the smoothing
            loess = Loess.Loess(np.array(time_masked), np.array(smooth_in))
            y = [loess.estimate(x, window=int(window), use_matrix=False, degree=degree) for x in d]

            print(t0, t1)

            #y = loess.estimate(t_msk, window=7, use_matrix=False, degree=1)

            #print(y)

            #   copy times attributes
            attr_dict = {}
            for a in var_time.ncattrs():
                attr_dict[a] = var_time.getncattr(a)
            time_var.setncatts(attr_dict)
            time_var[:] = d

            #  create output variables
            var_smooth_out = ds_new.createVariable(smooth_var, 'f4', 'TIME', fill_value=np.NaN, zlib=True)
            attr_dict = {}
            for a in var_to_smooth_in.ncattrs():
                attr_dict[a] = var_to_smooth_in.getncattr(a)
            var_smooth_out.setncatts(attr_dict)
            var_smooth_out[:] = y

        #  create history
        ds_new.history += '\n' + now.strftime("%Y-%m-%d : ") + 'resampled data created from ' + os.path.basename(filepath) + ' window=' + str(window) + ' degree=' + str(degree)

        ds_new.close()

        ds.close()

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

    smooth(sys.argv[1:])
