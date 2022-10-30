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
from datetime import datetime

# flag 4 (bad) when spike

create_sub_qc = True


def rate_of_change(netCDFfiles, variable, rate, qc_value=4):

    for netCDFfile in netCDFfiles:
        ds = Dataset(netCDFfile, 'a')

        time_var = ds.variables['TIME']
        time = time_var[:] * 24 # time in hours, ok its a hack

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
        if sum(data_to_qc_msk) == 0:
            print("no good data")
            return netCDFfiles

        # this is where the actual QC test is done
        mask = np.empty_like(data_to_qc_msk[data_to_qc_msk], dtype=bool)
        mask[0] = False
        #print('shape msk ', len(mask))
        mask[1:] = np.abs(np.diff(var_data_qc)/np.diff(time[data_to_qc_msk])) > rate
        print('mask data ', mask)

        new_qc_flags = np.ones_like(var_data_qc)
        new_qc_flags.mask = False
        new_qc_flags[mask] = qc_value

        if create_sub_qc:
            # create a qc variable just for this test flags
            if nc_var.name + "_quality_control_roc" in ds.variables:
                ncVarOut = ds.variables[nc_var.name + "_quality_control_roc"]
            else:
                ncVarOut = ds.createVariable(nc_var.name + "_quality_control_roc", "i1", nc_var.dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
                ncVarOut[:] = np.zeros(nc_var.shape)

                if 'long_name' in nc_var.ncattrs():
                    ncVarOut.long_name = "rate_of_change flag for " + nc_var.long_name

                ncVarOut.units = "1"

                # add new variable to list of aux variables
                nc_var.ancillary_variables = nc_var.ancillary_variables + " " + nc_var.name + "_quality_control_roc"
                ncVarOut.comment = 'Test 7. rate of change test'

            # store new flags
            ncVarOut[data_to_qc_msk] = new_qc_flags
            ncVarOut[~data_to_qc_msk] = 2

        # update the existing qc-flags
        existing_qc_flags[data_to_qc_msk] = np.max([existing_qc_flags[data_to_qc_msk], new_qc_flags], axis=0)

        # existing_qc_flags 66666666----------------------------------------6666666666
        # var_data          ----------------------------------------------------------
        # data_to_qc_msk    00000000----------------------------------------0000000000
        # var_data_qc               -----------x----------------------------
        # new_qc_flags              -----------4----------------------------
        # var_qc            66666666-----------4----------------------------6666666666

        # calculate the number of points marked as bad_data
        marked = np.zeros_like(new_qc_flags)
        marked[mask] = 1
        count = sum(marked)
        print('marked records ', count, mask, existing_qc_flags)

        # write flags back to main QC variable
        var_qc[:] = existing_qc_flags

        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""
        ds.setncattr("history", hist + datetime.utcnow().strftime("%Y-%m-%d") + " " + variable + " max rate = " + str(rate) + " marked " + str(int(count)))

        ds.close()

    return netCDFfiles


if __name__ == "__main__":

    # usage is <file_name> <variable_name> <height> <qc value>
    if len(sys.argv) > 4:
        rate_of_change([sys.argv[1]], sys.argv[2], rate=float(sys.argv[3]), qc_value=int(sys.argv[4]))
    else:
        rate_of_change([sys.argv[1]], sys.argv[2], rate=float(sys.argv[3]))
