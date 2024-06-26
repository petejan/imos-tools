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
from glob2 import glob
from netCDF4 import Dataset
import sys
import gsw
import numpy as np
from datetime import datetime, UTC
import os

# add PSAL to a data file with TEMP, CNDC, PRES variables, many assumptions are made about the input file

overwrite = False

def add_psal(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    if not overwrite and "PSAL" in ds.variables:
        print("file already has salinity variable")
        return netCDFfile

    if "CNDC" not in ds.variables:
        print("no conductivity")
        return netCDFfile

    fn_new = netCDFfile
    dirname = os.path.dirname(fn_new)
    basename = os.path.basename(fn_new)
    now = datetime.now(UTC)
    if basename.startswith("IMOS"):
        fn_split = basename.split('_')

        # IMOS_DWM-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
        # 0    1         2   3        4    5    6                                    7            8
        # rename the file FV00 to FV01
        fn_split[2] += 'S'

        if len(fn_split) > 8:
            fn_split[8] = now.strftime("C-%Y%m%d.nc")
        fn_new = os.path.join(dirname, "_".join(fn_split))

    var_temp = ds.variables["TEMP"]
    var_cndc = ds.variables["CNDC"]

    # find a pressure/depth variable
    # TODO: use nc.get_variables_by_attributes(units='dbar')
    if "PRES" in ds.variables:
        var_pres = ds.variables["PRES"]
        pres_var = "PRES"
        comment = ""
        p = var_pres[:]
    elif "NOMINAL_DEPTH" in ds.variables:
        var_pres = ds.variables["NOMINAL_DEPTH"]
        pres_var = "NOMINAL_DEPTH"
        comment = ", using nominal depth " + str(var_pres[:])
        p = var_pres[:]
    else:
        p = 0
        pres_var = "nominal depth of 0 dbar"
        comment = ", using nominal depth 0 dbar"

    t = var_temp[:]
    cndc_scale = 10.0
    print('cndc unit', var_cndc.units)
    if var_cndc.units == 'mS/cm':
        cndc_scale = 1.0
    print('cndc unit', var_cndc.units, cndc_scale)
    C = var_cndc[:] * cndc_scale
    psal = gsw.SP_from_C(C, t, p)

    added = True
    if "PSAL" in ds.variables:
        ncVarOut = ds.variables["PSAL"]
        added = False
    else:
        ncVarOut = ds.createVariable("PSAL", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = psal
    ncVarOut.units = "1"
    ncVarOut.standard_name = "sea_water_practical_salinity"
    ncVarOut.long_name = "sea_water_practical_salinity"
    ncVarOut.valid_max = np.float32(40)  # this is just the limits that the file will hold
    ncVarOut.valid_min = np.float32(-1)
    ncVarOut.comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html" + comment
    ncVarOut.coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH"

    if added:
        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""

    ds.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " added PSAL from TEMP, CNDC, " + pres_var)

    ds.close()

    os.rename(netCDFfile, fn_new)

    return fn_new


if __name__ == "__main__":
    files = []

    for f in sys.argv[1:]:
        if f == '-overwrite':
            overwrite = True
        else:
            files.extend(glob(f))

    for f in files:
            add_psal(f)
