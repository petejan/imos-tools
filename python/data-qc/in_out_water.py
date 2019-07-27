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
import gsw
import numpy as np
from dateutil import parser
import pytz
import os

# add QC variables to file, flag out of water as QC value 8, withwise leave as 0

def add_qc(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    vars = ds.variables

    to_add = []
    for v in vars:
        #print (vars[v].dimensions)
        if v != 'TIME':
            to_add.append(v)

    time_var = vars["TIME"]
    time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

    timez = {z.replace(tzinfo=pytz.UTC) for z in time}

    time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
    time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)

    print(time_deploy)

    print(to_add)
    for v in to_add:
        if "TIME" in vars[v].dimensions:
            print("time dim ", v)

            ncVarOut = ds.createVariable(v+"_quality_control", "i1", ("TIME",), fill_value=127, zlib=True)  # fill_value=nan otherwise defaults to max
            ncVarOut[:] = np.zeros(vars[v].shape)
            ncVarOut[(time <= time_deploy) | (time >= time_recovery)] = np.ones(vars[v].shape)[(time <= time_deploy) | (time >= time_recovery)] * 8
            ncVarOut.long_name = "quality_code for " + v

            vars[v].ancillary_variables = v + "_quality_control"

    ds.close()

    os.rename(netCDFfile, netCDFfile.replace("FV00", "FV01"))


if __name__ == "__main__":
    add_qc(sys.argv[1])
