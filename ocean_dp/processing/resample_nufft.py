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

import nufftpy

import sys


def nufft(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    var_temp = ds.variables["TEMP"]
    var_cndc = ds.variables["CNDC"]

    time_var = ds.variables["TIME"]
    time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

    time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
    time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)

    mask = (time <= time_deploy) | (time >= time_recovery)

    temp = var_temp[:]

    print("t0 = ", time[~mask][0])
    hours = np.array([(t - datetime(2018, 8, 22, 13, 0, 0)).total_seconds()/3600 for t in time[~mask]])

    fig, ax = plt.subplots(1, 1)

    ax.plot(hours, temp[~mask]) # , marker='.'

    ax.grid(True)

    samples = np.arange(0, len(hours))
    print("len samples ", samples.size, hours[-1])
    n_samples = samples.size

    Fnx = nufftpy.nufft1(hours, temp[~mask], M = hours[-1]*np.pi, df = np.pi*2/hours[-1], iflag=-1) * n_samples/(np.pi)  # *6.2832
    freq = nufftpy.nufftfreqs(n_samples)
    F = np.fft.fftshift(Fnx)

    #trange = np.arange(0, 6000)

    Fx = np.fft.fft(temp[~mask])
    freqx = np.fft.fftfreq(samples.size)

    window = 1 - np.hanning(len(F))  # invert the window, making a low pass filter, how to create a cur frequency

    fig2, ax2 = plt.subplots(1, 1)
    #ax2.plot(freq[1:int(len(F)/2)], np.log(abs(F[1:int(len(F)/2)])))
    ax2.plot(np.log(abs(F)))
    ax2.plot(np.log(abs(Fx)))

    print("FFT length " , len(F), hours[-1])

    F1 = np.fft.ifft(F)

    #ax[1].plot(F1)
    ax.plot(abs(F1))

    print(F1)

    plt.show()

    ds.close()


if __name__ == "__main__":
    nufft(sys.argv[1])
