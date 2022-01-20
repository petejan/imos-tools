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
from datetime import datetime
import numpy as np
from dateutil import parser
import pytz
import os

# flag out of water as QC value 6 (not_deployed), with wise leave as 0

create_sub_qc = True

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
                if "TIME" in nc_vars[v].dimensions:
                    #print (vars[v].dimensions)
                    if v != 'TIME':
                        to_add.append(v)
            # remove any anx variables from the list
            for v in nc_vars:
                if 'ancillary_variables' in nc_vars[v].ncattrs():
                    remove = nc_vars[v].getncattr('ancillary_variables').split(' ')
                    print("in/out water remove ", remove)
                    for r in remove:
                        to_add.remove(r)

        time_var = nc_vars["TIME"]
        time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

        time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
        time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)

        print('in/out water file', fn)
        print('deployment time', time_deploy)

        print('var to add', to_add)

        # create a mask for the time range
        mask = (time <= time_deploy) | (time >= time_recovery)
        count = -1
        for v in to_add:
            if v in nc_vars:
                print("qc for var", v, ' dimensions ', nc_vars[v].dimensions)

                ncVarOut = nc_vars[v + "_quality_control"]
                ncVarOut[mask] = 6

                if create_sub_qc:
                    # create a qc variable just for this test flags
                    if v + "_quality_control_io" in ds.variables:
                        ncVarOut = ds.variables[v + "_quality_control_loc"]
                        ncVarOut[:] = 1
                    else:
                        ncVarOut = ds.createVariable(v + "_quality_control_loc", "i1", nc_vars[v].dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
                        nc_vars[v].ancillary_variables = nc_vars[v].ancillary_variables + " " + v + "_quality_control_loc"

                    ncVarOut[:] = 1
                    if 'long_name' in nc_vars[v].ncattrs():
                        ncVarOut.long_name = "in/out of water flag for " + nc_vars[v].long_name
                    #if 'standard_name' in nc_vars[v].ncattrs():
                    #    ncVarOut.standard_name = nc_vars[v].standard_name + " in_out_water_flag"

                    #ncVarOut.quality_control_conventions = "IMOS standard flags"
                    #ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
                    #ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
                    ncVarOut.units = "1"

                    ncVarOut.comment = 'data flagged not deployed (6) when out of water'

                    ncVarOut[mask] = 6
                # calculate the number of points marked as bad_data
                marked = np.zeros_like(ncVarOut)
                marked[mask] = 1
                count = sum(marked)

        ds.file_version = "Level 1 - Quality Controlled Data"
        if count > 0:
            # update the history attribute
            try:
                hist = ds.history + "\n"
            except AttributeError:
                hist = ""

            ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + ' : ' + 'in/out marked ' + str(int(count)))

        ds.close()

        out_file.append(fn)

    return out_file


if __name__ == "__main__":
    if (len(sys.argv) > 2) & sys.argv[1].startswith('-'):
        in_out_water(sys.argv[2:], var_name=sys.argv[1][1:])
    else:
        in_out_water(sys.argv[1:])
