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


def add_doxs(netCDFfiles):

    for netCDFfile in netCDFfiles:
        print('add_doxs:', netCDFfile)

        ds = Dataset(netCDFfile, 'a')

        oxsol = ds.variables["OXSOL"][:]
        dox2 = ds.variables["DOX2"][:]

        added = True
        # calculate and write oxygen solubility, ratio of disolved oxgen / oxygen solubility
        if 'DOXS' not in ds.variables:
            ncVarOut = ds.createVariable("DOXS", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
        else:
            ncVarOut = ds.variables['DOXS']
            added = False

        ncVarOut[:] = dox2/oxsol
        ncVarOut.units = "1"
        ncVarOut.comment = "calculated using DOX2/OXSOL"
        ncVarOut.long_name = "fractional_saturation_of_oxygen_in_sea_water"
        ncVarOut.standard_name = "fractional_saturation_of_oxygen_in_sea_water"
        ncVarOut.coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH"

        if added:
            # update the history attribute
            try:
                hist = ds.history + "\n"
            except AttributeError:
                hist = ""

            ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " added DOXS")

        # finish off, and close file

        ds.close()

    return netCDFfiles


if __name__ == "__main__":
    add_doxs(sys.argv[1:])
