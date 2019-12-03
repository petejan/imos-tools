#!/usr/bin/python3

# readDSGfile
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
from datetime import datetime
from datetime import timedelta

import numpy as np
import matplotlib.pyplot as plt

import sys


def readDSGfile(netCDFfiles):
    ds = Dataset(netCDFfiles[1], 'a')

    vs = ds.get_variables_by_attributes(standard_name='sea_water_pressure_due_to_sea_water')
    vs = ds.get_variables_by_attributes(long_name='actual depth')

    pres_var = vs[0]
    var_temp = ds.variables["TEMP"]

    plot_var = var_temp[:]

    plt.plot(plot_var)
    plt.show()

    time_var = ds.variables["TIME"]
    time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

    index_var = ds.variables["instrument_index"]
    idx = index_var[:]
    instrument_id_var = ds.variables["instrument_id"]

    #print(idx)
    i = 0
    for x in chartostring(instrument_id_var[:]):
        print (i, x, time[idx == 1], plot_var[idx == i])
        plt.plot(time[idx == i], plot_var[idx == i])  # , marker='.'
        i += 1

    #plt.gca().invert_yaxis()
    plt.grid(True)

    # close the netCDF file
    ds.close()

    plt.show()


if __name__ == "__main__":
    readDSGfile(sys.argv)
