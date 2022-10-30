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


def add_optode_oxygen(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]
    var_bphase = ds.variables["BPHASE"]
    var_otemp = ds.variables["OTEMP"]
    #var_otemp = ds.variables["TEMP"]

    t = var_temp[:]
    SP = var_psal[:]
    phase = var_bphase[:]
    otemp = var_otemp[:]

    try:
        var_pres = ds.variables["PRES"]
        p = var_pres[:]
    except:
        p = 0

    lat = -47
    lon = 142
    try:
        lat = ds.variables["LATITUDE"][0]
        lon = ds.variables["LONGITUDE"][0]
    except:
        pass

    SA = gsw.SA_from_SP(SP, p, lon, lat)
    pt = gsw.pt0_from_t(SA, t, p)
    sigmat = gsw.sigma0(SA, t)

    oxsol = gsw.O2sol_SP_pt(SP, pt)

    if 'OXSOL' in ds.variables:
        out_oxsol_var = ds.variables['OXSOL']
    else:
        out_oxsol_var = ds.createVariable("OXSOL", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    out_oxsol_var[:] = oxsol
    out_oxsol_var.units = "umol/kg"

    out_oxsol_var.comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html function gsw.O2sol_SP_pt"

    C0 = var_bphase.calibration_C0
    C1 = var_bphase.calibration_C1
    C2 = var_bphase.calibration_C2
    A0 = var_bphase.calibration_A0
    A1 = var_bphase.calibration_A1
    B0 = var_bphase.calibration_B0
    B1 = var_bphase.calibration_B1

    if 'DOX2_RAW' in ds.variables:
        out_ox_var = ds.variables['DOX2_RAW']
    else:
        out_ox_var = ds.createVariable("DOX2_RAW", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    oxygen = ((A0 + A1 * otemp) / (B0 + B1 * phase) - 1) / (C0 + C1 * otemp + C2 * np.power(otemp, 2))

    out_ox_var[:] = oxygen

    out_ox_var.comment_calc1 = 'calculated using ((A0 + A1 * otemp)/(B0 + B1 * phase) - 1)/(C0 + C1 * otemp + C2 * otemp * otemp)'
    out_ox_var.units = "umol"
    out_ox_var.long_name = "mole_concentration_of_dissolved_molecular_oxygen_in_sea_water (not salinity or pressure corrected)"
    out_ox_var.valid_max = np.float32(400)
    out_ox_var.valid_min = np.float32(0)

    out_ox_var.coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH"

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " calculated optode oxygen and oxygen solubility")

    ds.close()


if __name__ == "__main__":
    add_optode_oxygen(sys.argv[1])
