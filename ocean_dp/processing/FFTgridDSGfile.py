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

import finufftpy

import sys


def FFTgridDSGfile(netCDFfiles):
    ds = Dataset(netCDFfiles[1], 'a')

    vs = ds.get_variables_by_attributes(standard_name='sea_water_pressure_due_to_sea_water')
    vs = ds.get_variables_by_attributes(long_name='actual depth')

    pres_var = vs[0]
    pres = pres_var[:]

    #plt.plot(pres)
    #plt.show()

    temp_var = ds.variables["TEMP"]

    print("Read and convert time")
    time_var = ds.variables["TIME"]
    time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)
    first_hour = time[0].replace(minute=0, second=0, microsecond=0)

    print("first_hour ", first_hour)

    hours = np.array([(t - first_hour).total_seconds()/3600 for t in time])
    print("hours ", hours[0], hours[-1])

    mid_hours = np.round((hours[-1] - hours[0]) / 2)
    print("mid hours, total ", mid_hours, (hours[-1] - hours[0]))

    t2pi = 2*np.pi * (mid_hours - hours)/ hours[-1]

    depths=[0,100,200,500,1000,1500,2000,2500,3500]
    d2pi = 2*np.pi * (pres - 1500)/1500

    nt_points = int(hours[-1] - hours[0])
    nd_points = 9
    print(nt_points, nd_points)

    fft = np.zeros(nt_points, nd_points, dtype=complex)

    print("Calc FFT")
    res = finufftpy.nufft1d2(t2pi, d2pi, temp_var[:], 0, 1e-12, nt_points, nd_points, fft, debug=True)

    print("fft res", res)

    index_var = ds.variables["instrument_index"]
    idx = index_var[:]
    instrument_id_var = ds.variables["instrument_id"]

    #print(idx)
    i = 0
    for x in chartostring(instrument_id_var[:]):
        #print (i, x, time[idx == 1], pres[idx == i])
        plt.plot(time[idx == i], pres[idx == i])  # , marker='.'
        i += 1

    plt.gca().invert_yaxis()
    plt.grid(True)

    # close the netCDF file
    ds.close()

    plt.show()


if __name__ == "__main__":
    FFTgridDSGfile(sys.argv)
