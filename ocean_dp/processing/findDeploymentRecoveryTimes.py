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
from dateutil import parser

overwrite = False

# find the pressure where its half the median pressure

def main(netCDFfile):

    print(netCDFfile)

    ds = Dataset(netCDFfile, 'a')

    if "PRES" in ds.variables:
        var_pres = ds.variables["PRES"]
        p = var_pres[:]
        var_time = ds.variables["TIME"]
        unit = var_time.units

        t = var_time[:]

        median = np.median(p.compressed())
        print("pressure median", median)

        if median > 1:
            mid = int(len(p)/2)
            print("mid", mid)

            for i in range(mid, 0, -1):
                if p[i] < median/2:
                    break
            for j in range(mid, len(p)):
                if p[j] < median/2:
                    break

            dep_recovery = [i, j]
            # find = np.where(p.compressed() > median/2)
            # #print("find ", find)
            # dep_recovery = [find[0][0], find[0][-1]]
            print("deployment ", dep_recovery[0], " recovery ", dep_recovery[1])
            print(p[dep_recovery])
            ts = num2date(t[dep_recovery], units=unit)

            ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

            print('time deployment ', ts[0].strftime(ncTimeFormat))
            print('time recovery   ', ts[-1].strftime(ncTimeFormat))

            t_deploy = None
            t_recovery = None
            if not overwrite:
                if "time_deployment_start" in ds.ncattrs():
                    t_deploy = ds.time_deployment_start
                if "time_deployment_end" in ds.ncattrs():
                    t_recovery = ds.time_deployment_end

            if t_deploy:
                t_diff = parser.parse(t_deploy, ignoretz=True) - ts[0]
                print("time deploy to estimated   ", t_diff, "should be positive")
            else:
                ds.time_deployment_start = ts[0].strftime(ncTimeFormat)
            if t_recovery:
                t_diff = ts[-1] - parser.parse(t_recovery, ignoretz=True)
                print("time recovery to estimated ", t_diff, "should be positive")
            else:
                ds.time_deployment_end = ts[-1].strftime(ncTimeFormat)


    else:
        print("no pressure variable in file")

    ds.close()

    return netCDFfile


if __name__ == "__main__":
    for f in sys.argv[1:]:
        if f == '-overwrite':
            overwrite = True
        else:
            main(f)
