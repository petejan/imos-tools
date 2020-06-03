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

def add_psal(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    var_temp = ds.variables["TEMP"]
    var_cndc = ds.variables["CNDC"]
    comment = ""
    try:
        var_pres = ds.variables["PRES"]
    except KeyError:
        var_pres = ds.variables["NOMINAL_DEPTH"]
        comment = ", using nominal depth " + str(var_pres[:])

    t = var_temp[:]
    C = var_cndc[:] * 10
    p = var_pres[:]
    psal = gsw.SP_from_C(C, t, p)

    if "PSAL" in ds.variables:
        ncVarOut = ds.variables["PSAL"]
    else:
        ncVarOut = ds.createVariable("PSAL", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = psal
    ncVarOut.units = "1"
    ncVarOut.standard_name = "sea_water_practical_salinity"
    ncVarOut.long_name = "sea_water_practical_salinity"
    ncVarOut.valid_max = np.float32(40)
    ncVarOut.valid_min = np.float32(-1)
    ncVarOut.comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html" + comment

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added PSAL from TEMP, CNDC, PRES")

    ds.close()


if __name__ == "__main__":
    add_psal(sys.argv[1])
