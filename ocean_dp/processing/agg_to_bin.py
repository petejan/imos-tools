#!/usr/bin/python3

# readDSGfile
# Copyright (C) 2019 Peter Jansen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.


from netCDF4 import Dataset, num2date, chartostring
from dateutil import parser
from datetime import datetime
from datetime import timedelta

import numpy as np
import matplotlib.pyplot as plt

import sys


def agg_to_bin(netCDFfiles):
    ds = Dataset(netCDFfiles[1], 'r')

    vs = ds.get_variables_by_attributes(standard_name='sea_water_pressure_due_to_sea_water')
    print("number sea_water_pressure_due_to_sea_water", len(vs))

    vs = ds.get_variables_by_attributes(long_name='actual depth')
    print("number actual depth", len(vs))

    pres_var = vs[0]
    pres = pres_var[:]

    #plt.plot(pres)
    #plt.show()

    temp_var = ds.variables["TEMP"]
    temp = temp_var[:]

    print("Read and convert time")
    time_var = ds.variables["TIME"]
    #time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)
    #first_hour = time[0].replace(minute=0, second=0, microsecond=0)

    t = time_var[:]
    hours = t * 24

    hours_min = np.min(hours)
    hours_max = np.max(hours)

    print("time max, min", hours_max, hours_min)
    print("time max, min", num2date(hours_min/24, units=time_var.units, calendar=time_var.calendar), num2date(hours_max/24, units=time_var.units, calendar=time_var.calendar))

    time_bins = np.arange(np.ceil(hours_min+0.5), np.floor(hours_max-0.5))
    nt_points = len(time_bins)
    print("time bin range ", num2date(time_bins[0]/24, units=time_var.units, calendar=time_var.calendar), num2date(time_bins[-1]/24, units=time_var.units, calendar=time_var.calendar))

    # make pressure bins

    pres_min = np.min(pres)
    pres_max = np.max(pres)
    print("pres min, max", pres_min, pres_max)

    pres_bins = np.arange(0, np.round(pres_max + 10, -1), 10)
    print("pres bin range ", pres_bins[0], pres_bins[-1])

    nd_points = len(pres_bins)

    print(nt_points, nd_points)

    bin = np.full([nt_points, nd_points], np.nan)
    count = np.zeros([nt_points, nd_points])

    # bin data, looping over input array
    for i in range(0, len(temp)):
        # compute the location of this data point
        h = int(np.round(hours[i] - hours_min + 0.5)) - 1
        d = int(np.round(pres[i] - pres_min + 5, -1)/ 10)
        #print (i, hours[i], pres[i], temp[i], h, d)
        if np.isnan(bin[h, d]):
            bin[h, d] = temp[i]
        else:
            bin[h, d] += temp[i]
        # count number of points
        count[h, d] += 1

    print("count ", np.sum(count), len(hours))
    only_time = ~np.isnan(bin).all(axis=1)
    only_depth = ~np.isnan(bin).all(axis=0)
    print("axis=0", only_time)
    print("axis=1", only_depth)
    print("Shape only_time, only_depth ", bin.shape, only_time.shape, only_depth.shape)
    #s = bin[only_time, :]
    #print("Shape ", s)

    ncOut = Dataset(netCDFfiles[1].replace("aggregated", "binned"), 'w', format='NETCDF4')

    # add time
    tDim = ncOut.createDimension("TIME", nt_points)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = time_bins / 24

    bin_dim = ncOut.createDimension("BIN", nd_points)
    bin_var = ncOut.createVariable("BIN", "d", ("BIN",), zlib=True)
    bin_var[:] = pres_bins

    # add variables

    nc_var_out = ncOut.createVariable("TEMP", "f4", ("TIME", "BIN"), fill_value=np.nan, zlib=True)
    print("shape ", bin.shape, nc_var_out.shape)

    mean = bin/count
    nc_var_out[:] = mean

    # add some summary metadata
    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    for att in ds.ncattrs():
        print("copying attribute ", att, "value", ds.getncattr(att))
        ncOut.setncattr(att, ds.getncattr(att))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + netCDFfiles[1])

    ncOut.close()

    # close the netCDF file
    ds.close()


if __name__ == "__main__":
    agg_to_bin(sys.argv)
