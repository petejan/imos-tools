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

from netCDF4 import Dataset
import sys
import gsw
import numpy as np
from datetime import datetime

# add OXSOL to a data file with TEMP, PSAL, PRES variables, many assumptions are made about the input file


def scale_offset(netCDFfile, var, scale, offset):
    ds = Dataset(netCDFfile, 'a')

    var_temp = ds.variables[var]

    t = var_temp[:]
    #print(t)

    var_temp[:] = t * float(scale) + float(offset)
    #print(var_temp[:])

    var_temp.comment = "rescale scale = " + str(scale) + " offset " + str(offset)

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : scale, offset variable " + var + "")

    ds.close()


if __name__ == "__main__":
    scale_offset(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
