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
        qc_var = var.ancillary_variables
        print("QC var name ", qc_var)
        var_qc = ds.variables[qc_var]
    except KeyError:
        print("no QC variable found")
        return None

    qc = var_qc[:]
    # this is where the actual QC test is done
    mask = ((var[:] > max) | (var[:] < min))
    print('mask data ', mask)

    mask = mask & (qc < 1)  # only mark data that has not been QCd already
    print('mask other qc ', mask)

    qc[mask] = 4
    marked = np.zeros_like(qc)
    marked[mask] = 1
    count = sum(marked)
    print('marked records ', count, mask, qc)

    var_qc[:] = qc

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""
    ds.setncattr("history", hist + "\n" + datetime.utcnow().strftime("%Y-%m-%d") + " " + variable + " global range min = " + str(min) + " max = " + str(max) + " marked " + str(count))

    ds.close()

    return netCDFfile


if __name__ == "__main__":

    # usage is <file_name> <variable_name> <max> <min>
    global_range(sys.argv[1], sys.argv[2], max=float(sys.argv[3]), min=float(sys.argv[4]))
