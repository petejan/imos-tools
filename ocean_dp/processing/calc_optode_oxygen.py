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

    SA = gsw.SA_from_SP(SP, p, lon , lat)
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

    if 'DOX2' in ds.variables:
        out_ox_var = ds.variables['DOX2']
    else:
        out_ox_var = ds.createVariable("DOX2", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    oxygen = ((A0 + A1 * otemp) / (B0 + B1 * phase) - 1) / (C0 + C1 * otemp + C2 * np.power(otemp, 2))
    ts = np.log((298.15 - t)/(273.15 + t))

    B0 = -6.24097e-3
    B1 = -6.93498e-3
    B2 = -6.90358e-3
    B3 = -4.29155e-3

    C0 = -3.11680e-7

    sal_cor = np.exp(SP*(B0 + B1 * ts + B2 * np.power(ts, 2) + B3 * np.power(ts, 3)) + np.power(SP, 2) * C0)

    out_ox_var[:] = oxygen * sal_cor / (1 + sigmat / 1000)

    out_ox_var.comment_calc1 = 'calculated using ((A0 + A1 * otemp)/(B0 + B1 * phase) - 1)/(C0 + C1 * otemp + C2 * otemp * otemp)'
    out_ox_var.comment_calc2 = 'salinity correction np.exp(SP*(SOL_B1 + SOL_B1 * ts + SOL_B2 * (ts^2) + SOL_B3 * (ts ^ 3)))+ SOL_C0 * (SP^2) (Benson and Krause, 1984)'
    out_ox_var.comment_calc3 = 'pressure correction 1 + (0.032 * pressure)/1000'
    out_ox_var.comment_calc4 = 'convert to oxygen(umol/kg) = oxygen (uM) / (1 + sigma-theta0/1000)'
    out_ox_var.comment_calc2_SolB0 = -6.24523e-3
    out_ox_var.comment_calc2_SolB1 = -7.37614e-3
    out_ox_var.comment_calc2_SolB2 = -1.03410e-2
    out_ox_var.comment_calc2_SolB3 = -8.17083e-3
    out_ox_var.comment_calc2_SolC0 = -4.88682e-7

    if 'DOXS' in ds.variables:
        out_oxs_var = ds.variables['DOXS']
    else:
        out_oxs_var = ds.createVariable("DOXS", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    out_oxs_var.units = '1'

    out_oxs_var[:] = out_ox_var[:] / oxsol

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added oxygen and oxygen solubility")

    ds.close()


if __name__ == "__main__":
    add_optode_oxygen(sys.argv[1])
