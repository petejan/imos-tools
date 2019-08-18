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
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import sys
from scipy.interpolate import interp1d
import datetime

import matplotlib.dates as mdates

import matplotlib.units as units
import matplotlib.dates as dates
import matplotlib.ticker as ticker
import datetime

matplotlib.use('Agg')
matplotlib.rcParams.update({'font.size': 6})

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


def plot_var(pp, time, pres, v, label, cmap):

    # t = [t.timestamp() for t in time]
    #
    # fig1, ax1 = plt.subplots()
    # tcf = ax1.tricontourf(t, pres, v, levels=20, cmap=cmap)
    # plt.plot(t, pres, 'ko ', markersize=0.1)
    # fig1.colorbar(tcf)
    # plt.ylim(100, 0)

    fig1, ax1 = plt.subplots()

    locator = dates.AutoDateLocator(minticks=3, maxticks=7)
    formatter = dates.ConciseDateFormatter(locator)
    ax1.xaxis.set_major_locator(locator)
    ax1.xaxis.set_major_formatter(formatter)

    sc = plt.scatter(time, pres, c=v, cmap=cmap)
    plt.ylim(100, 0)
    plt.xlim(time[0], time[-1])
    fig1.colorbar(sc)

    plt.xlabel('date-time')
    plt.ylabel('pressure (dbar)')
    plt.title('prawler ' + label + ' profile')

    #plt.show()
    pp.savefig(fig1, papertype='a4')
    plt.close()


def plot(file):
    nc = Dataset(file)

    # get time variable
    time_var = nc.variables['TIME']
    t_unit = time_var.units  # get unit  "days since 1950-01-01T00:00:00Z"

    try:
        t_cal = time_var.calendar
    except AttributeError:  # Attribute doesn't exist
        t_cal = u"gregorian"  # or standard

    time = time_var[:]
    dt_time = [num2date(t, units=t_unit, calendar=t_cal) for t in time]

    temp_var = nc.variables['TEMP']
    temp = temp_var[:]
    psal_var = nc.variables['PSAL']
    doxy_var = nc.variables['DOXY']
    doxs_var = nc.variables['DOXS']
    pres_var = nc.variables['PRES']
    pres = pres_var[:]

    profile_var = nc.variables['PROFILE']
    profile = profile_var[:]

    pdffile = file + '.pdf'

    #pp = None
    pp = PdfPages(pdffile)

    plot_var(pp, dt_time, pres_var[:], temp_var[:], 'temperature', plt.get_cmap('jet'))
    plot_var(pp, dt_time, pres_var[:], psal_var[:], 'salinity', plt.get_cmap('gnuplot'))
    plot_var(pp, dt_time, pres_var[:], doxy_var[:], 'oxygen concentration (mg/l)', plt.get_cmap('ocean'))
    plot_var(pp, dt_time, pres_var[:], doxs_var[:], 'oxygen saturation', plt.get_cmap('ocean'))

    pp.close()

    nc.close()

    return pdffile


if __name__ == "__main__":
    plot(sys.argv[1])
