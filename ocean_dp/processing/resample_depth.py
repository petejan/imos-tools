#!/usr/bin/python3

# raw2netCDF
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


from netCDF4 import Dataset, num2date, date2num
import numpy as np
import sys
from scipy.interpolate import interp1d
import datetime


def resample(file):
    nc = Dataset(file)

    # get time variable
    time_var = nc.variables['TIME']
    t_unit = time_var.units  # get unit  "days since 1950-01-01T00:00:00Z"

    try:
        t_cal = time_var.calendar
    except AttributeError:  # Attribute doesn't exist
        t_cal = u"gregorian"  # or standard

    time = time_var[:]
    dt_time = [num2date(t, units=t_unit, calendar=t_cal) for t in time]

    temp_var = nc.variables['TEMP']
    temp = temp_var[:]
    pres_var = nc.variables['PRES']
    pres = pres_var[:]

    profile_var = nc.variables['PROFILE']
    profile = profile_var[:]

    # resample the profile to common depths

    pres_resample = np.linspace(2, 100, num=100, endpoint=True)
    profile_range = range(min(profile), max(profile)+1)
    print("new shape, n_profile, n_pres", len(profile_range), len(pres_resample))
    profile_temp_resampled = np.zeros([len(profile_range), len(pres_resample)])
    profile_time = np.zeros([len(profile_range)])

    print("profile range", profile_range, len(profile_range))

    for profile_n in profile_range:

        time_n = time[profile == profile_n]

        #print (profile_n, num2date(time_n[0], units=t_unit, calendar=t_cal))

        profile_time[profile_n] = time_n[0]
        pres_n = pres[profile == profile_n]
        pres_n_sorted, pres_n_sort_idx = np.unique(pres_n, return_index=True)
        temp_n_sorted = temp[profile == profile_n][pres_n_sort_idx]

        #print(pres_n_sorted)
        temp_resample = interp1d(pres_n_sorted, temp_n_sorted, kind='cubic', fill_value=np.nan, bounds_error=False)

        #print(temp_resample(pres_resample))
        profile_temp_resampled[profile_n] = temp_resample(pres_resample)

    #print(profile_temp_resampled)
    #print(profile_time)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = file.replace(".nc", "-resample.nc")

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    ncOut.instrument = nc.instrument
    ncOut.instrument_model = nc.instrument_model
    ncOut.instrument_serial_number = nc.instrument_serial_number

    tDim = ncOut.createDimension("TIME", len(profile_time))
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = profile_time

    ncOut.createDimension("PRES", len(pres_resample))
    ncPresOut = ncOut.createVariable("PRES", "d", ("PRES",), zlib=True)
    ncPresOut[:] = pres_resample
    ncPresOut.long_name = "resampled pressure"

    ncVarOut = ncOut.createVariable("TEMP", "f4", ("TIME", "PRES"), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
    #print ("var TEMP", profile_temp_resampled)
    ncVarOut[:] = profile_temp_resampled

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + file)

    ncOut.close()

    nc.close()

    return outputName


if __name__ == "__main__":
    resample(sys.argv[1])
