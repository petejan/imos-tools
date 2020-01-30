#!/usr/bin/python3

# add_qc_flags
# Copyright (C) 2020 Peter Jansen
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
import shutil

# add QC variables to file


def add_qc(netCDFfile):

    new_name = [] # list of new file names

    # loop over all file names given
    for fn in netCDFfile:

        # rename the file FV00 to FV01 (imos specific)
        fn_new = fn.replace("FV00", "FV01")
        new_name.append(fn_new)

        if fn_new != fn:
            # copy file
            shutil.copy(fn, fn_new)

        print(fn_new)

        ds = Dataset(fn_new, 'a')

        # read the variable names from the netCDF dataset
        vars = ds.variables

        # create a list of variables, don't include the 'TIME' variable
        # TODO: detect 'TIME' variable using the standard name 'time'
        to_add = []
        for v in vars:
            #print (vars[v].dimensions)
            if v != 'TIME':
                to_add.append(v)

        # for each variable, add a new ancillary variable <VAR>_quality_control to each which has 'TIME' as a dimension
        for v in to_add:
            if "TIME" in vars[v].dimensions:
                # print("time dim ", v)

                if v+"_quality_control" not in ds.variables:
                    ncVarOut = ds.createVariable(v+"_quality_control", "i1", vars[v].dimensions, fill_value=99, zlib=True)  # fill_value=99 otherwise defaults to max, imos-toolbox uses 99
                    ncVarOut[:] = np.zeros(vars[v].shape)
                    ncVarOut.long_name = "quality_code for " + v

                    vars[v].ancillary_variables = v + "_quality_control"

        # update the file version attribute
        ds.file_version = "Level 1 - Quality Controlled Data"

        ds.close()


    return new_name


if __name__ == "__main__":
    add_qc(sys.argv[1:])
