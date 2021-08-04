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


def add_psal(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]

    t = var_temp[:]
    SP = var_psal[:]

    if "PRES" in ds.variables:
        var_pres = ds.variables["PRES"]
        pres_var = "PRES"
        comment = ""
        p = var_pres[:]
    elif "NOMINAL_DEPTH" in ds.variables:
        var_pres = ds.variables["NOMINAL_DEPTH"]
        pres_var = "NOMINAL_DEPTH"
        comment = ", using nominal depth of " + str(var_pres[:])
        p = var_pres[:]
    else:
        p = 0
        pres_var = "nominal depth of 0 dbar"
        comment = ", using nominal depth 0 dbar"

    lat = -47
    lon = 142
    try:
        lat = ds.variables["LATITUDE"][0]
        lon = ds.variables["LONGITUDE"][0]
    except:
        pass

    SA = gsw.SA_from_SP(SP, p, lon , lat)
    pt = gsw.pt0_from_t(SA, t, p)

    oxsol = gsw.O2sol_SP_pt(SP, pt)

    ncVarOut = ds.createVariable("OXSOL", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = oxsol
    ncVarOut.units = "umol/kg"
    ncVarOut.comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html function gsw.O2sol_SP_pt" + comment

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added oxygen solubility")

    ds.close()


if __name__ == "__main__":
    add_psal(sys.argv[1])
