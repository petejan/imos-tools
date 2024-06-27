#!/usr/bin/python3

# global_range.py
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
from datetime import datetime, UTC

# flag 4 (bad) when out of global range

create_sub_qc = True


def global_range(netCDFfiles, variable, max, min, qc_value=4):

    for netCDFfile in netCDFfiles:
        print('global range, processing', netCDFfile)

        ds = Dataset(netCDFfile, 'a')

        nc_var = ds.variables[variable]
        var_data = nc_var[:]
        #var_data.mask = False

        try:
            # find the existing quality_control variable in the auxillary variables list
            aux_vars = nc_var.ancillary_variables
            aux_var = aux_vars.split(" ")
            qc_vars = [i for i in aux_var if i.endswith("_quality_control")]
            qc_var = qc_vars[0]
            print("QC var name ", qc_var)
            var_qc = ds.variables[qc_var]
        except KeyError:
            print("no QC variable found")
            return None

        # read existing quality_control flags
        existing_qc_flags = var_qc[:]
        data_to_qc_msk = existing_qc_flags < 3
        var_data_qc = var_data[data_to_qc_msk]

        # this is where the actual QC test is done
        mask = ((var_data_qc > max) | (var_data_qc < min))
        #print('mask data ', mask)

        new_qc_flags = np.ones_like(var_data_qc)
        new_qc_flags.mask = False # incase its a masked array
        new_qc_flags[mask] = qc_value

        if create_sub_qc:
            # create a qc variable just for this test flags
            if nc_var.name + "_quality_control_gr" in ds.variables:
                ncVarOut = ds.variables[nc_var.name + "_quality_control_gr"]
            else:
                ncVarOut = ds.createVariable(nc_var.name + "_quality_control_gr", "i1", nc_var.dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
                ncVarOut[:] = np.ones(nc_var.shape)
                # add new variable to list of aux variables
                nc_var.ancillary_variables = nc_var.ancillary_variables + " " + nc_var.name + "_quality_control_gr"

            if 'long_name' in nc_var.ncattrs():
                ncVarOut.long_name = "global_range flag for " + nc_var.long_name
            #if 'standard_name' in nc_var.ncattrs():
            #    ncVarOut.standard_name = nc_var.standard_name + " global range status flag"
            ncVarOut.units = "1"

            ncVarOut.comment = 'Test 4. gross range test'

            # store new flags
            ncVarOut[data_to_qc_msk] = new_qc_flags
            ncVarOut[~data_to_qc_msk] = 2

        # update the existing qc-flags
        existing_qc_flags[data_to_qc_msk] = np.max([existing_qc_flags[data_to_qc_msk], new_qc_flags], axis=0)

        # existing_qc_flags 66666666----------------------------------------6666666666
        # var_data          ----------------------------------------------------------
        # data_to_qc_msk    00000000----------------------------------------0000000000
        # var_data_qc               -----------x----------------------------
        # new_qc_flags              1111111111141111111111111111111111111111
        # var_qc            6666666611111111111411111111111111111111111111116666666666

        # calculate the number of points marked as bad_data
        marked = np.zeros_like(new_qc_flags)
        marked[mask] = 1
        count = sum(marked)
        #print('global range marked records ', count, mask, existing_qc_flags)

        # write flags back to main QC variable
        var_qc[:] = existing_qc_flags

        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""
        ds.setncattr("history", hist + datetime.now(UTC).strftime("%Y-%m-%d") + " " + variable + " global range min = " + str(min) + " max = " + str(max) + " marked " + str(int(count)))

        ds.close()

    return netCDFfiles


if __name__ == "__main__":

    for i in range(1, len(sys.argv)-1):
        print('global range: args ', i, sys.argv[i])

    # usage is <file_name> <variable_name> <max> <min> <qc value>
    if len(sys.argv) > 5:
        global_range([sys.argv[1]], sys.argv[2], max=float(sys.argv[3]), min=float(sys.argv[4]), qc_value=int(sys.argv[5]))
    else:
        global_range([sys.argv[1]], sys.argv[2], max=float(sys.argv[3]), min=float(sys.argv[4]))
