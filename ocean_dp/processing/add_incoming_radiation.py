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

from pysolar.solar import get_altitude
from pysolar.util import extraterrestrial_irrad
from pysolar.util import global_irradiance_overcast

import pysolar

# add incoming radiation

def add_solar(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    lat = ds.variables['LATITUDE'][:]
    lon = ds.variables['LONGITUDE'][:]

    print('lat ', lat, ' lon ', lon)

    time = ds.variables['TIME']

    print('number of points ', len(time))
    dt = num2date(time[:], units=time.units, calendar=time.calendar)

    dt_utc = [d.replace(tzinfo=pytz.UTC) for d in dt]

    print('time ', dt_utc[0])

    altitude_deg = [get_altitude(lat, lon, d) for d in dt_utc]
    rad = [extraterrestrial_irrad(lat, lon, d, 1370) for d in dt_utc]

    print("altitude", altitude_deg[0], " rad ", rad[0])

    ncVarOut = ds.createVariable("SOLAR", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = rad
    ncVarOut.units = "W/m2"
    ncVarOut.setncattr('name', 'extraterrestrial_irrad celestial incoming solar radiation')
    ncVarOut.long_name = 'incoming_solar_radiation'
    ncVarOut.comment = "using http://docs.pysolar.org/en/latest/ v0.8 extraterrestrial_irrad() with incoming = 1370 W/m^2"

    rad = [global_irradiance_overcast(lat, lon, d, 1370) for d in dt_utc]

    ncVarOut = ds.createVariable("SURFACE", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = rad
    ncVarOut.units = "W/m2"
    ncVarOut.setncattr('name', 'extraterrestrial_irrad celestial incoming solar radiation')
    ncVarOut.long_name = 'incoming_solar_radiation'
    ncVarOut.comment = "using http://docs.pysolar.org/en/latest/ v0.8 global_irradiance_overcast() with incoming = 1370 W/m^2"


    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added incoming radiation")

    ds.close()


if __name__ == "__main__":
    add_solar(sys.argv[1])
