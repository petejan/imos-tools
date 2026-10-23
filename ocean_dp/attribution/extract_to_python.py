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

import sys
from netCDF4 import Dataset

for s in sys.argv[1:]:
    nc = Dataset(s)

    # global attributes
    nc_attrs = nc.ncattrs()

    for a in nc_attrs:
        attr = nc.getncattr(a)

        line = "ds." + a + "="
        if isinstance(attr, str):
            line += "'"+attr+"'"
        else:
            line += str(attr)

        print(line)
    print()

    # dimensions
    dims = nc.dimensions
    for d in dims:
        line = "ds.createDimension('" + d + "')"
        print(line)
    print()

    # variables
    for v in nc.variables:

        line = "ds.createVariable('" + v + "')"
        print(line)

        # variable attributes
        nc_attrs = nc.variables[v].ncattrs()

        for a in nc_attrs:
            attr = nc.variables[v].getncattr(a)

            line = "ds.variables['" + v + "']." + a + "="
            if isinstance(attr, str):
                line += "'"+attr+"'"
            else:
                line += str(attr)

            print(line)

        print()