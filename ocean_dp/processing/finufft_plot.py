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
from datetime import timedelta

import numpy as np
import matplotlib.pyplot as plt
import scipy.signal as sig

import nufftpy

import sys
import finufftpy


def fft(netCDFfiles):
    ds = Dataset(netCDFfiles[1], 'a')

    var_temp = ds.variables["TEMP"]

    time_var = ds.variables["TIME"]
    time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

    time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
    time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)

    mask = (time <= time_deploy) | (time >= time_recovery)
    hours = np.array([(t - time_deploy).total_seconds()/3600 for t in time[~mask]])

    print("hours ", hours[0], hours[-1])
    first_hour = time[~mask][0].replace(minute=0, second=0, microsecond=0)
    print("first_hour ", first_hour)

    mid_hours = np.round((hours[-1] - hours[0]) / 2)

    print("mid hours, total ", mid_hours, (hours[-1] - hours[0]))

    t2pi = 2*np.pi * (mid_hours - hours)/ hours[-1]

    temp = var_temp[:]
    temp_mask = temp[~mask]

    print("t0 = ", time[~mask][0], " length = ", len(temp_mask))

    n_points = len(temp_mask)
    n_points = 12853

    td = [first_hour + timedelta(hours=x) for x in range(n_points)]
    #print(td)

    #plt.subplot(2, 1, 1)

    fig, ax = plt.subplots()

    plt.plot(time[~mask], temp_mask) # , marker='.'
    plt.grid(True)

    #plt.show()
    #fig.show()

    res = np.zeros(n_points, dtype=complex)

    fft = finufftpy.nufft1d1(t2pi, temp_mask, 0, 1e-12, n_points, res)

    print("fft res " , fft)

    #fig2, ax1 = plt.subplots(1, 1)

    #ax1.plot(np.log10(np.abs(res))) # , marker='.'

    #ax1.grid(True)

    #plt.show()

    F1 = np.fft.ifft(res)
    f2 = np.fft.fftshift(F1)

    #plt.subplot(2, 1, 2)

    # ax[1].plot(F1)
    plt.plot(td, abs(f2/(len(temp_mask)/n_points)), marker='.')

    binned_data = np.zeros(len(td))
    i= 0
    for t in td:
        binned_data[i] = np.mean(temp_mask[(time[~mask] > (t-timedelta(minutes=30))) & (time[~mask] <= (t+timedelta(minutes=30)))])
        print('binned ',i, t, binned_data[i])
        i += 1

    plt.plot(td, binned_data, marker='.')

    print(binned_data)

    plt.grid(True)
    plt.xticks(fontsize=6)

    # We change the fontsize of minor ticks label
    #ax.tick_params(axis='both', which='major', labelsize=10)
    #ax.tick_params(axis='both', which='minor', labelsize=8)

    plt.show()

    # close the netCDF file
    ds.close()


if __name__ == "__main__":
    fft(sys.argv)
