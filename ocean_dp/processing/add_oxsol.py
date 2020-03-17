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
    var_pres = ds.variables["PRES"]

    t = var_temp[:]
    SP = var_psal[:]
    p = var_pres[:]

    SA = gsw.SA_from_SP(SP, p, ds.variables["LONGITUDE"][0], ds.variables["LATITUDE"][0])
    pt = gsw.pt0_from_t(SA, t, p)

    oxsol = gsw.O2sol_SP_pt(SP, pt)

    ncVarOut = ds.createVariable("OXSOL", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = oxsol
    ncVarOut.units = "umol/kg"
    ncVarOut.comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html function gsw.O2sol_SP_pt"

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added oxygen solubility")

    ds.close()


if __name__ == "__main__":
    add_psal(sys.argv[1])
