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

# add geospatial attributes

def add_spatial_attr(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    var_lat = ds.variables["LATITUDE"]
    var_lon = ds.variables["LONGITUDE"]

    ds.geospatial_lat_max = var_lat[:]
    ds.geospatial_lat_min = var_lat[:]
    ds.geospatial_lon_max = var_lon[:]
    ds.geospatial_lon_min = var_lon[:]

    if "NOMINAL_DEPTH" in ds.variables:
        var_depth = ds.variables["NOMINAL_DEPTH"]
        ds.geospatial_vertical_max = var_depth[:]
        ds.geospatial_vertical_min = var_depth[:]

    ds.close()


if __name__ == "__main__":
    add_spatial_attr(sys.argv[1])
