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
            now = datetime.utcnow()

            fn_split[8] = now.strftime("C-%Y%m%d.nc")
            fn_new = os.path.join(dirname, "_".join(fn_split))

        # Add the new file name to the list of new file names
        output_names.append(fn_new)

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

        # fine number of samples to make 5 hrs of data
        i = 0
        while time_masked[i] < (t0 + 5/24):
            i = i + 1

        #window = 5 * 3600 / sample_rate
        window = i
        print('5 hr in samples', window)

        # create a new time array to sample to
        d0 = np.ceil(t0*24) / 24
        dend = np.floor(tend*24) / 24
        d = np.arange(d0, dend, 1/24)
        d_dt = num2date(d, units=var_time.units)


        # variable to smooth
        var_psal = ds.variables["PSAL"]

        psal = var_psal[msk]

        print(var_psal[msk])

        loess = Loess.Loess(np.array(time_masked), np.array(psal))

        y = [loess.estimate(x, window=int(window), use_matrix=False, degree=1) for x in d]

        print(t0, t1)

        #y = loess.estimate(t_msk, window=7, use_matrix=False, degree=1)

        #print(y)

        ds_new = Dataset(fn_new, 'w')
        attr_dict = {}
        for a in ds.ncattrs():
            attr_dict[a] = ds.getncattr(a)
        ds_new.setncatts(attr_dict)

        ds_new.createDimension(ds.dimensions['TIME'].name, len(d_dt))

        time_var = ds_new.createVariable('TIME', 'f8', 'TIME', fill_value=np.NaN, zlib=True)
        attr_dict = {}
        for a in var_time.ncattrs():
            attr_dict[a] = var_time.getncattr(a)
        time_var.setncatts(attr_dict)

        time_var[:] = d

        psal_var = ds_new.createVariable('PSAL', 'f4', 'TIME', fill_value=np.NaN, zlib=True)
        psal_var[:] = y

        ds_new.history += '\n' + now.strftime("%Y-%m-%d : ") + 'resampled data created from ' + filepath

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
