import os
import shutil
import sys

from netCDF4 import Dataset, date2num, num2date

from datetime import datetime, UTC
import matplotlib.pyplot as plt

from pyloess import Loess
import numpy as np


def smooth(files):
    output_names = []

    now = datetime.now(UTC)
    #files = ['../../prawler.nc']

    for filepath in files:

        fn_new = filepath.replace('.nc', '-loess-smooth-2dbar.nc')

        dirname = os.path.dirname(fn_new)
        basename = os.path.basename(fn_new)

        if basename.startswith("IMOS"):
            fn_split = basename.split('_')

            # IMOS_DWM-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
            # 0    1         2   3        4    5    6                                    7            8
            # rename the file FV00 to FV01
            fn_split[6] = fn_split[6] + "-smooth"

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

        time = var_time[:]

        t_dt = num2date(time, units=var_time.units)

        # profile number from netCDF file
        profile_var = ds.variables['PROFILE']
        profile = profile_var[:]

        # pressure from netCDF file
        pres_var = ds.variables['PRES']
        pres = pres_var[:]

        # output data to new file
        ds_new = Dataset(fn_new, 'w')

        #  copy global attributes
        attr_dict = {}
        for a in ds.ncattrs():
            attr_dict[a] = ds.getncattr(a)
        ds_new.setncatts(attr_dict)

        d = np.arange(5, 90, 2)
        window = 4
        degree = 3

        #  copy dimension
        ds_new.createDimension('TIME', len(set(profile)))

        # the new pres sample dimension
        ds_new.createDimension('PRES', len(d))

        #  create new time
        new_time_var = ds_new.createVariable('TIME', 'f8', 'TIME', fill_value=np.NaN, zlib=True)

        new_pres_var = ds_new.createVariable('PRES', 'f8', 'PRES', fill_value=np.NaN, zlib=True)
        new_pres_var[:] = d

        #   copy times attributes
        attr_dict = {}
        for a in var_time.ncattrs():
            attr_dict[a] = var_time.getncattr(a)
        new_time_var.setncatts(attr_dict)

        for var_name in ['TEMP', 'PSAL', 'DOX2']:
            # temp from netCDF file
            temp_var = ds.variables[var_name]
            temp = temp_var[:]

            #  create output variables
            var_smooth_out = ds_new.createVariable(var_name, 'f4', ['TIME', 'PRES'], fill_value=np.NaN, zlib=True)
            attr_dict = {}
            for a in temp_var.ncattrs():
                attr_dict[a] = temp_var.getncattr(a)
            var_smooth_out.setncatts(attr_dict)

            j = 0
            for n in set(profile):
                msk = profile == n

                print(n, num2date(time[msk][0], units=var_time.units))
                #print (pres[msk])
                new_time_var[j] = time[msk][0]

                pres_profile = pres[msk]

                if len(pres_profile) > window:
                    # do the smoothing
                    loess = Loess.Loess(pres[msk], temp[msk])
                    y = np.array([loess.estimate(x, window=int(window), use_matrix=False, degree=degree) for x in d])

                    y[d > np.max(pres_profile)] = np.nan
                    y[d < min(pres_profile)] = np.nan

                    # print(y)
                    var_smooth_out[j] = y
                else:
                    var_smooth_out[j] = np.nan

                j = j + 1


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
    smooth(sys.argv[1:])
