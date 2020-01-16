#!/usr/bin/python3

# global_range.py
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

from netCDF4 import Dataset, num2date
import sys

import numpy as np
from dateutil import parser
import pytz
import os
from datetime import datetime

# flag 4 (bad) when out of global range


def global_range(netCDFfile, variable, max, min):
    ds = Dataset(netCDFfile, 'a')

    var = ds.variables[variable]

    try:
        var_qc = ds.variables[variable + "_quality_control"]
    except KeyError:
        print("no QC variable found")
        return None

    # this is where the actual QC test is done
    mask = ((var[:] > max) | (var[:] < min))
    
    mask = mask & (var_qc[:] < 1) # only mark data that has not been QCd already

    var_qc[mask] = 4
    count = sum(mask)
    print('marked records ', count)

    # update the history attribute
    history = ds.history
    ds.setncattr("history", history + "\n" + datetime.utcnow().strftime("%Y-%m-%d") + " " + variable + " global range min = " + str(min) + " max = " + str(max) + " marked " + str(count))

    ds.close()

    return netCDFfile


if __name__ == "__main__":

    # usage is <file_name> <variable_name> <max> <min>
    global_range(sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4]))