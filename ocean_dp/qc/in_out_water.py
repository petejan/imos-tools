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
from dateutil import parser
import pytz
import os

# flag out of water as QC value 7 (not_deployed), with wise leave as 0


def in_out_water(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    vars = ds.variables

    to_add = []
    for v in vars:
        #print (vars[v].dimensions)
        if v != 'TIME':
            to_add.append(v)

    time_var = vars["TIME"]
    time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

    time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
    time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)

    print(time_deploy)

    print(to_add)
    for v in to_add:
        if "TIME" in vars[v].dimensions:

            if v.endswith("_quality_control"):

                print("QC time dim ", v)

                ncVarOut = vars[v]
                mask = (time <= time_deploy) | (time >= time_recovery)
                ncVarOut[mask] = np.ones(vars[v].shape)[mask] * 7


    ds.file_version = "Level 1 - Quality Controlled Data"

    ds.close()

    return netCDFfile


if __name__ == "__main__":
    in_out_water(sys.argv[1])
