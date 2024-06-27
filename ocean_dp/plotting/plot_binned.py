#!/usr/bin/python3

# plot_binned
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


from netCDF4 import Dataset, num2date, chartostring
from dateutil import parser
from datetime import datetime, UTC
from datetime import timedelta

import numpy as np
import matplotlib.pyplot as plt

import sys


def plot_binned(netCDFfiles):
    ds = Dataset(netCDFfiles[1], 'r')

    temp = ds.variables["TEMP"]

    t = temp[:]

    plt.imshow(t.transpose(), aspect='auto')
    plt.colorbar()
    plt.show()

    ds.close()

if __name__ == "__main__":
    plot_binned(sys.argv)

