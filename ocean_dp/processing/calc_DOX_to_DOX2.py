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


def oxygen(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]
    var_pres = ds.variables["PRES"]

    t = var_temp[:]
    SP = var_psal[:]
    p = var_pres[:]

    var_dox = None
    if "DOX" in ds.variables:
        var_dox = ds.variables["DOX"]
        dox = var_dox[:]
    if "DOXY" in ds.variables:
        var_dox = ds.variables["DOXY"]
        dox = var_dox[:]/1.42903 # Sea Bird AN-64 [mg/L] = [ml/L] * 1.42903
    if var_dox is None:
        print("No DOX/DOXY in file")
        return

    lat = -47
    lon = 142
    try:
        lat = ds.variables["LATITUDE"][0]
        lon = ds.variables["LONGITUDE"][0]
    except:
        pass

    SA = gsw.SA_from_SP(SP, p, lon , lat)
    CT = gsw.CT_from_t(SA, t, p)

    sigma_theta0 = gsw.sigma0(SA, CT)

    # calculate disolved oxygen, umol/kg
    dox2 = 44660 * dox / (sigma_theta0 + 1000)

    if 'DOX2' not in ds.variables:
        ncVarOut = ds.createVariable("DOX2", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables['DOX2']

    ncVarOut[:] = dox2
    ncVarOut.units = "umol/kg"
    ncVarOut.units = "calculated from DOX using https://www.seabird.com/asset-get.download.jsa?code=251036"

    # finish off, and close file

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added derived oxygen, DOX2, DOXS, DOX_MG")

    ds.close()


if __name__ == "__main__":
    oxygen(sys.argv[1])
