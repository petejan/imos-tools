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

# add OXSOL to a data file with TEMP, PSAL, PRES variables, many assumptions are made about the input file


def add_psal(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    B0 = -6.24097e-3
    C0 = -3.11680e-7
    B1 = -6.93498e-3
    B2 = -6.90358e-3
    B3 = -4.29155e-3

    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]
    var_pres = ds.variables["PRES"]

    t = var_temp[:]
    SP = var_psal[:]
    p = var_pres[:]

    SA = gsw.SA_from_SP(SP, p, ds.longitude, ds.latitude)
    CT = gsw.CT_from_t(SA, t, p)
    pt = gsw.pt0_from_t(SA, t, p)

    sigma_theta0 = gsw.sigma0(SA, CT)
    oxsol = var_pres = ds.variables["OXSOL"][:]

    ts = np.log((298.15 - t) / (273.15 + t))

    dox2 = ds.variables['DOX2_RAW'] * np.exp(SP *(B0 + B1 * ts + B2 * np.power(ts, 2) + B3 * np.power(ts, 3)) + np.power(SP, 2) * C0)
    dox2 = 1000 * dox2 / (sigma_theta0 + 1000)

    if 'DOX2' not in ds.variables:
        ncVarOut = ds.createVariable("DOX2", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables['DOX2']

    ncVarOut[:] = dox2
    ncVarOut.units = "umol/kg"
    ncVarOut.comment = "calculated using DOX2 = DOX2_RAW * PSAL_CORRECTION / ((sigma_theta(P=0,Theta,S) + 1000).. Sea Bird AN 64, Aanderaa TD210 Optode Manual "

    if 'DOXS' not in ds.variables:
        ncVarOut = ds.createVariable("DOXS", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables['DOXS']

    ncVarOut[:] = dox2/oxsol
    ncVarOut.units = "1"
    ncVarOut.comment = "calculated using DOX2/OXSOL"

    ds.close()


if __name__ == "__main__":
    add_psal(sys.argv[1])
