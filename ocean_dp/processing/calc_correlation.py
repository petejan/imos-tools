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

from netCDF4 import Dataset, num2date
from dateutil import parser
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
import scipy.signal as sig

import nufftpy

import sys


def correlate(netCDFfiles):
    ds = Dataset(netCDFfiles[1], 'a')

    var_temp = ds.variables["TEMP"]

    time_var = ds.variables["TIME"]
    time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

    time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
    time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)

    mask = (time <= time_deploy) | (time >= time_recovery)

    temp = var_temp[:]
    temp_mask = temp[~mask]

    print("t0 = ", time[~mask][0], " length = ", len(temp_mask))


    fig, ax = plt.subplots(1, 1)

    ax.plot(time[~mask], temp_mask) # , marker='.'

    ax.grid(True)

    plt.show()

    correlate = sig.correlate(temp_mask, temp_mask)

    fig, ax = plt.subplots(1, 1)

    ax.plot(correlate) # , marker='.'

    ax.grid(True)

    plt.show()


    ds.close()


if __name__ == "__main__":
    correlate(sys.argv)
