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
import os

from netCDF4 import Dataset
import sys
from datetime import datetime

val_names = ['C0', 'C1', 'C2', 'A0', 'A1', 'B0', 'B1']

def add(netCDFfile, optode_cal_file):

    ds = Dataset(netCDFfile, 'a')

    bphase = ds.variables['BPHASE']
    bphase.setncattr('calibration_from_file', os.path.basename(optode_cal_file))

    file1 = open(optode_cal_file, 'r')
    lines = file1.readlines()
    i = 0
    for ln in lines:
        if i < len(val_names):
            print(float(ln))
            bphase.setncattr('calibration_'+val_names[i], float(ln))
        i += 1

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : attributes added from file " + os.path.basename(optode_cal_file))

    ds.close()

    return netCDFfile


if __name__ == "__main__":
    add(sys.argv[1], sys.argv[2])
