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

from netCDF4 import Dataset, num2date
import sys
import geomag

import datetime

def main(netCDFfile):

    print(netCDFfile)

    ds = Dataset(netCDFfile, 'a')

    lat = ds.variables['LATITUDE'][:]
    lon = ds.variables['LONGITUDE'][:]
    nom_depth = ds.variables['NOMINAL_DEPTH'][:]

    time = ds.variables['TIME']
    dt = num2date(time[:], units=time.units, calendar=time.calendar)

    len_time = len(dt)
    time_mid = dt[int(len_time/2)]
    date = time_mid.date()  # geomag wants date object not datetime

    dec = geomag.declination(lat, lon, time=date)

    print("calc for ", time_mid, lat, lon, dec)

    ds.setncattr("geomagnetic_varition", dec)
    ds.setncattr("geomagnetic_varition_comment", "calculation for %s at LATITUDE and LONGITUDE using https://pypi.org/project/geomag/" % date)

    ds.close()


if __name__ == "__main__":
    main(sys.argv[1])
