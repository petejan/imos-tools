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
from datetime import datetime

# add density to a data file with TEMP, PSAL, PRES variables, many assumptions are made about the input file
# based on Peter Jansen's addPSAL.py, using TEOS-10

def add_density(netCDFfile):
    
    # loads the netcdf file
    ds = Dataset(netCDFfile, 'a')
    
    if 'DENSITY' in list(ds.variables):
    
        ds.close()
        
        return "file already contains density"

    # extracts the variables from the netcdf
    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]
    var_pres = ds.variables["PRES"]
    var_lon = ds.variables["LONGITUDE"]
    var_lat = ds.variables["LATITUDE"]

    # extracts the data from the variables
    t = var_temp[:]
    psal = var_psal[:]
    p = var_pres[:]
    lon = var_lon[:]
    lat = var_lat[:]
    
    # calculates absolute salinity
    SA = gsw.SA_from_SP(psal, p, lon, lat)
    
    # calculates conservative temperature
    CT = gsw.CT_from_t(SA, t, p)
    
    # calculates density
    density = gsw.rho(SA, CT, p)
    
    # generates a new variable 'DENSITY' in the netcdf
    ncVarOut = ds.createVariable("DENSITY", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    
    # assigns the calculated densities to the DENSITY variable, sets the units as kg/m^3, and comments on the variable's origin
    ncVarOut[:] = density
    ncVarOut.units = "kg/m^3"
    ncVarOut.comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html"

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added DENSITY from TEMP, PSAL, PRES, LAT, LON")

    ds.close()


if __name__ == "__main__":
    add_density(sys.argv[1])
