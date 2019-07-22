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
import numpy as np
import datetime

# find the pressure where its half the median pressure

def main(netCDFfile):

    print(netCDFfile)

    ds = Dataset(netCDFfile, 'r')

    if "PRES" in ds.variables:
        var_pres = ds.variables["PRES"]
        p = var_pres[:]
        var_time = ds.variables["TIME"]
        unit = var_time.units

        t = var_time[:]

        median = np.median(p.compressed())
        print("pressure median", median)
        find = np.where(p.compressed() > median/2)
        #print("find ", find)
        dep_recovery = [find[0][0], find[0][-1]]
        print("deployment ", find[0][0], " recovery ", find[0][-1])
        print(p[dep_recovery])
        ts = num2date(t[dep_recovery], units=unit)

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        print('time deployment ', ts[0].strftime(ncTimeFormat))
        print('time recovery ', ts[-1].strftime(ncTimeFormat))
    else:
        print("no pressure variable in file")

    ds.close()


if __name__ == "__main__":
    main(sys.argv[1])
