# Copyright (C) 2020 Ben Weeding and Peter Jansen
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


# add density to a data file with TEMP, PSAL, PRES variables, many assumptions are made about the input file
# based on Peter Jansen's addPSAL.py, using TEOS-10

def add_density(netCDFfile):
    # loads the netcdf file
    ds = Dataset(netCDFfile, 'a')

    # if 'DENSITY' in list(ds.variables):
    #     ds.close()
    #     return "file already contains density"

    if "PSAL" not in ds.variables:
        print("no salinity")
        return

    # extracts the variables from the netcdf
    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]

    if 'PRES' in ds.variables:
        pres_var = 'PRES'
        var_pres = ds.variables["PRES"]
    elif 'NOMINAL_DEPTH' in ds.variables:
        var_pres = ds.variables["NOMINAL_DEPTH"]
        pres_var = 'NOMINAL_DEPTH'
    else:
        print("no pressure variable, or nominal depth")
        return

    if 'LONGITUDE' in ds.variables:
        var_lon = ds.variables["LONGITUDE"]
        var_lat = ds.variables["LATITUDE"]
        lon = var_lon[:]
        lat = var_lat[:]
    else:
        lon = 142
        lat = -47

    # extracts the data from the variables
    t = var_temp[:]
    psal = var_psal[:]
    p = var_pres[:]

    # calculates absolute salinity
    SA = gsw.SA_from_SP(psal, p, lon, lat)

    # calculates conservative temperature
    CT = gsw.CT_from_t(SA, t, p)

    # TODO: copy forward the QC from PSAL

    added = True
    # calculates density
    density = gsw.rho(SA, CT, p)
    # generates a new variable 'DENSITY' in the netcdf
    if 'DENSITY' not in ds.variables:
        ncVarOut = ds.createVariable("DENSITY", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables['DENSITY']
        added = False

    # assigns the calculated densities to the DENSITY variable, sets the units as kg/m^3, and comments on the variable's origin
    ncVarOut[:] = density
    ncVarOut.units = "kg/m^3"
    ncVarOut.long_name = "sea_water_density"
    ncVarOut.standard_name = "sea_water_density"
    ncVarOut.valid_max = np.float32(1100) # https://oceanobservatories.org/wp-content/uploads/2015/09/1341-10004_Data_Product_SPEC_GLBLRNG_OOI.pdf
    ncVarOut.valid_min = np.float32(1000)
    if hasattr(var_psal, 'coordinates'):
        ncVarOut.coordinates = var_psal.coordinates

    ncVarOut.comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html"

    # calculate sigma theta at 0 dbar
    sigmatheta0 = gsw.sigma0(SA, CT)

    # generates a new variable 'SIGMAT0' in the netcdf
    if 'SIGMA_T0' not in ds.variables:
        ncVarOut = ds.createVariable("SIGMA_T0", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables['SIGMA_T0']

    # assigns the calculated densities to the DENSITY variable, sets the units as kg/m^3, and comments on the variable's origin
    ncVarOut[:] = sigmatheta0
    ncVarOut.units = "kg/m^3"
    ncVarOut.long_name = "sea_water_sigma_theta"
    ncVarOut.standard_name = "sea_water_sigma_theta"
    ncVarOut.reference_pressure = "0 dbar"
    ncVarOut.valid_max = np.float32(100)
    ncVarOut.valid_min = np.float32(0)
    if hasattr(var_psal, 'coordinates'):
        ncVarOut.coordinates = var_psal.coordinates

    ncVarOut.comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html"

    if added:
        print('added density')

        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""

        ds.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " added DENSITY and SIGMA-THETA0 from TEMP, PSAL, "+pres_var+", LAT, LON")

    ds.close()

    return [netCDFfile]


if __name__ == "__main__":
    for f in sys.argv[1:]:
        add_density(f)
