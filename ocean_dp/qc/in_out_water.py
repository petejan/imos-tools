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

# flag out of water as QC value 6 (not_deployed), with wise leave as 0


def in_out_water(netCDFfile, var_name=None):

    out_file = []

    for fn in netCDFfile:
        ds = Dataset(fn, 'a')

        nc_vars = ds.variables
        to_add = []
        if var_name:
            to_add.append(var_name)
        else:
            for v in nc_vars:
                #print (vars[v].dimensions)
                if v != 'TIME':
                    to_add.append(v)

        time_var = nc_vars["TIME"]
        time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

        time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
        time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)

        print('deployment time', time_deploy)

        print(to_add)

        # create a mask for the time range
        mask = (time <= time_deploy) | (time >= time_recovery)

        for v in to_add:
            if "TIME" in nc_vars[v].dimensions:
                if v.endswith("_quality_control"):
                    print("QC time dim ", v)

                    ncVarOut = nc_vars[v]
                    ncVarOut[mask] = 7
                else:
                    # create a qc variable just for this test flags
                    if v + "_quality_control_io" in ds.variables:
                        ncVarOut = ds.variables[v + "_quality_control_io"]
                    else:
                        ncVarOut = ds.createVariable(v + "_quality_control_io", "i1", nc_vars[v].dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
                    ncVarOut[:] = np.zeros(nc_vars[v].shape)
                    ncVarOut.long_name = "quality flag for " + v
                    ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
                    ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'

                    nc_vars[v].ancillary_variables = nc_vars[v].ancillary_variables + " " + v + "_quality_control_io"
                    ncVarOut[mask] = 6

        ds.file_version = "Level 1 - Quality Controlled Data"

        ds.close()

        out_file.append(fn)

    return out_file


if __name__ == "__main__":
    if len(sys.argv) > 2 & sys.argv[1].startswith('-'):
        in_out_water(sys.argv[2:], var_name=sys.argv[1][1:])
    else:
        in_out_water(sys.argv[1:])
