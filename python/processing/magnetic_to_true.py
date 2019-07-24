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
import numpy as np

#  convert magnetic orientation to true orientation

def magnetic_to_true(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    heading = ds.variables["HEADING_MAG"]

    mag_var = ds.geomagnetic_varition

    ncVarOut = ds.createVariable("HEADING", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    ncVarOut[:] = heading[:] + mag_var
    ncVarOut.units = heading.units
    ncVarOut.reference_datum = 'degrees true'
    ncVarOut.comment = "calculated using magnetic variation " + format(mag_var, "3.1f")

    ds.close()


if __name__ == "__main__":
    magnetic_to_true(sys.argv[1])
