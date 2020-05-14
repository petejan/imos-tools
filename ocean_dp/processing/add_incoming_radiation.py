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
import pytz
from netCDF4 import Dataset, num2date
import sys

import numpy as np
from datetime import datetime

from pysolar.solar import get_altitude_fast
from pysolar.util import extraterrestrial_irrad

import os
import shutil


def add_solar(netCDFfiles):
    # add incoming radiation

    out_files = []

    for fn in netCDFfiles:
        # Change the creation date in the filename to today
        now = datetime.utcnow()

        fn_new = fn
        if os.path.basename(fn).startswith("IMOS_"):
            fn_new_split = os.path.basename(fn).split('_')
            fn_new_split[-1] = "C-" + now.strftime("%Y%m%d") + ".nc"

            fn_new = os.path.join(os.path.dirname(fn), '_'.join(fn_new_split))

        # If a new (different) filename has been successfully generated, make
        # a copy of the old file with the new name
        if fn_new != fn:
            print('copying file to ', fn_new)
            # copy file
            shutil.copy(fn, fn_new)

        out_files.append(fn_new)

        ds = Dataset(fn_new, 'a')

        lat = ds.variables['LATITUDE'][:]
        lon = ds.variables['LONGITUDE'][:]
        ndepth = ds.variables['NOMINAL_DEPTH'][:]

        print('lat ', lat, ' lon ', lon)

        time_var = ds.variables['TIME']

        print('number of points ', len(time_var))
        dt = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar, only_use_cftime_datetimes=False)

        dt_utc = [d.replace(tzinfo=pytz.UTC) for d in dt]

        print('time start ', dt_utc[0])

        altitude_deg = get_altitude_fast(lat, lon, dt)
        rad = extraterrestrial_irrad(lat, lon, dt, 1361)

        if ndepth > 0:
            #depth_var = ds.variables['PRES']
            #depth = depth_var[:]
            depth = np.ones_like(rad) * ndepth
            par = rad * np.exp(-0.04 * depth) * 2.114
        else:
            par = rad * 2.114

        print("altitude", altitude_deg[0], " rad ", rad[0])

        if 'ALT' in ds.variables:
            ncVarOut = ds.variables["ALT"]
        else:
            ncVarOut = ds.createVariable("ALT", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

        ncVarOut[:] = altitude_deg
        ncVarOut.units = "degree"
        ncVarOut.long_name = 'sun_altitude'
        ncVarOut.coordinates = 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH'
        ncVarOut.comment = "using http://docs.pysolar.org/en/latest/ v0.8 get_altitude"

        if 'SOLAR' in ds.variables:
            ncVarOut = ds.variables["SOLAR"]
        else:
            ncVarOut = ds.createVariable("SOLAR", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

        ncVarOut[:] = rad
        ncVarOut.units = "W/m2"
        ncVarOut.long_name = 'incoming_solar_radiation'
        ncVarOut.coordinates = 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH'
        ncVarOut.comment = "using http://docs.pysolar.org/en/latest/ v0.8 extraterrestrial_irrad() with incoming = 1361 W/m^2"

        if 'ePAR' in ds.variables:
            ncVarOut = ds.variables["ePAR"]
        else:
            ncVarOut = ds.createVariable("ePAR", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

        ncVarOut[:] = par
        ncVarOut.units = "umol/m^2/s"
        ncVarOut.long_name = 'incoming_solar_radiation converted to PAR (x2.114) attenuated by depth'
        ncVarOut.coordinates = 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH'
        ncVarOut.comment = "using http://docs.pysolar.org/en/latest/ v0.8 extraterrestrial_irrad() with incoming = 1361 W/m^2, x 2.114, kd = 0.04"

        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""

        ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added incoming radiation")

        ds.close()

    return out_files


if __name__ == "__main__":
    add_solar(sys.argv[1:])
