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

from datetime import datetime

from netCDF4 import Dataset, num2date
import sys

import numpy as np
from dateutil import parser

# flag data after a date


def maunal(netCDFfile, var_name=None, after_str=None, flag=4, reason=None):

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
                    print("remove ", remove)
                    for r in remove:
                        to_add.remove(r)

        time_var = nc_vars["TIME"]
        time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

        print('file', fn)

        print('var to add', to_add)

        # create a mask for the time range
        after = None
        mask = True
        if after_str:
            after = parser.parse(after_str, ignoretz=True)
            mask = (time >= after)

        for v in to_add:
            print("var", v, ' dimensions ', nc_vars[v].dimensions)

            var_qc = nc_vars[v + "_quality_control"]
            # read existing quality_control flags
            existing_qc_flags = var_qc[:]

            # create a qc variable just for this test flags
            if v + "_quality_control_man" in ds.variables:
                ncVarOut = ds.variables[v + "_quality_control_man"]
                ncVarOut[:] = 0
            else:
                ncVarOut = ds.createVariable(v + "_quality_control_man", "i1", nc_vars[v].dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
                nc_vars[v].ancillary_variables = nc_vars[v].ancillary_variables + " " + v + "_quality_control_man"

            ncVarOut[:] = 0

            if 'long_name' in nc_vars[v].ncattrs():
                ncVarOut.long_name = "manual flag for " + nc_vars[v].long_name
            #if 'standard_name' in nc_vars[v].ncattrs():
            #    ncVarOut.standard_name = nc_vars[v].standard_name + " manual flag"

            #ncVarOut.quality_control_conventions = "IMOS standard flags"
            #ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
            #ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
            ncVarOut.units = "1"
            ncVarOut.comment = 'manual'
            if after_str:
                ncVarOut.comment += ', by date, after ' + after.strftime("%Y-%m-%d")
            if reason:
                ncVarOut.comment += ', ' + reason

            new_qc_flags = np.zeros(var_qc.shape)
            new_qc_flags[mask] = flag
            ncVarOut[:] = new_qc_flags

            # update the existing qc-flags
            existing_qc_flags = np.max([existing_qc_flags, new_qc_flags], axis=0)

            # calculate the number of points marked as bad_data
            marked = np.zeros_like(existing_qc_flags)
            marked[mask] = 1
            count = sum(marked)
            print('marked records ', count, mask, existing_qc_flags)

            # write flags back to main QC variable
            var_qc[:] = existing_qc_flags

        ds.file_version = "Level 1 - Quality Controlled Data"

        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""
        if var_name:
            hist = hist + datetime.utcnow().strftime("%Y-%m-%d") + " " + var_name + " manual QC, marked " + str(int(count))
        else:
            hist = hist + datetime.utcnow().strftime("%Y-%m-%d") + " manual QC, marked " + str(int(count))
        if after_str:
            hist = hist + ", after " + after.strftime("%Y-%m-%d %H:%M:%S")
        if reason:
            hist += ', ' + reason

        ds.setncattr("history", hist)

        ds.close()

        out_file.append(fn)

    return out_file


if __name__ == "__main__":
    maunal(netCDFfile=[sys.argv[1]], after_str=sys.argv[2], var_name=sys.argv[3], flag=sys.argv[4])