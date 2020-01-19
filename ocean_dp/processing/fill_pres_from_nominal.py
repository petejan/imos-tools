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

# add PSAL to a data file with TEMP, CNDC, PRES variables, many assumptions are made about the input file

def fill_press(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    var_time = ds.variables["TIME"]
    var_pres = ds.variables["NOMINAL_DEPTH"]

    ncVarOut = ds.createVariable("PRES", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = var_pres[:] * np.ones(len(var_time[:]))
    ncVarOut.units = "dbar"
    ncVarOut.comment = "filled from NOMINAL_DEPTH"

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : filled PRES from NOMINAL_DEPTH")

    ds.close()


if __name__ == "__main__":
    fill_press(sys.argv[1])
