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
from datetime import datetime, UTC

# add OXSOL to a data file with TEMP, PSAL, PRES variables, many assumptions are made about the input file


def oxygen(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    if 'DOX2_RAW' not in ds.variables:
        ds.close()
        return

    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]
    if 'PRES' in ds.variables:
        var_pres = ds.variables["PRES"]
        p = var_pres[:]
    elif 'NOMINAL_DEPTH' in ds.variables:
        var_pres = ds.variables["NOMINAL_DEPTH"]
        p = var_pres[:]
    else:
        p = 1.0

    t = var_temp[:]
    SP = var_psal[:]
    lat = -47
    lon = 142

    try:
        lat = ds.variables['LATITUDE'][0]
        lon = ds.variables['LONGITUDE'][0]
    except:
        pass

    SA = gsw.SA_from_SP(SP, p, lon, lat)
    CT = gsw.CT_from_t(SA, t, p)
    pt = gsw.pt0_from_t(SA, t, p)

    sigma_theta0 = gsw.sigma0(SA, CT)

    ts = np.log((298.15 - t) / (273.15 + t))

    # psal correction, from Aanderra TD-218, Gordon and Garcia oxygaen solubility salinity coefficents, salinity coeff, col 5 combined fit (ml/l).
    B0 = -6.24097e-3
    C0 = -3.11680e-7
    B1 = -6.93498e-3
    B2 = -6.90358e-3
    B3 = -4.29155e-3

    psal_correction = np.exp(SP *(B0 + B1 * ts + B2 * np.power(ts, 2) + B3 * np.power(ts, 3)) + np.power(SP, 2) * C0)

    # get correction slope, offset
    slope = 1.0
    offset = 0.0
    try:
        slope = ds.variables['DOX2_RAW'].calibration_slope
        offset = ds.variables['DOX2_RAW'].calibration_offset
    except AttributeError:
        pass

    # calculate disolved oxygen, umol/kg
    dox2_raw = ds.variables['DOX2_RAW'] * psal_correction * slope + offset
    dox2 = 1000 * dox2_raw / (sigma_theta0 + 1000)
    #dox2 = dox2_raw

    if 'DOX2' not in ds.variables:
        ncVarOut = ds.createVariable("DOX2", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables['DOX2']

    ncVarOut[:] = dox2
    ncVarOut.standard_name = "moles_of_oxygen_per_unit_mass_in_sea_water"
    ncVarOut.long_name = "moles_of_oxygen_per_unit_mass_in_sea_water"
    ncVarOut.units = "umol/kg"
    ncVarOut.comment1 = "calculated using DOX2_uM = DOX2_RAW * PSAL_CORRECTION .. Aanderaa TD210 Optode Manual (optode/SBE63 only)"
    ncVarOut.comment2 = "DOX2 = DOX2_uM / ((sigma_theta(P=0,Theta,S) + 1000).. Sea Bird AN 64 (unit conversion umol/l -> umol/kg)"
    ncVarOut.comment_calibration = "calibration slope " + str(slope) + " offset " + str(offset) + " umol/l"
    ncVarOut.coordinates = 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH'

    # finish off, and close file

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " added derived oxygen, DOX2")

    ds.close()

    return netCDFfile


if __name__ == "__main__":
    oxygen(sys.argv[1])
