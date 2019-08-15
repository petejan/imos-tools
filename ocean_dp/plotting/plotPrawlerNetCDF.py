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

    # resample the profile to common depths

    # pres_resample = np.linspace(2, 100, num=100, endpoint=True)
    # profile_range = range(min(profile), max(profile))
    # print(len(profile_range), len(pres_resample))
    # profile_temp_resampled = np.zeros([len(profile_range), len(pres_resample)])
    # profile_time = {}
    #
    # for profile_n in profile_range:
    #     #print (profile_n)
    #     time_n = time[profile == profile_n]
    #     profile_time[profile_n] = num2date(time_n[0], units=t_unit, calendar=t_cal)
    #     pres_n = pres[profile == profile_n]
    #     pres_n_sorted, pres_n_sort_idx = np.unique(pres_n, return_index=True)
    #     temp_n_sorted = temp[profile == profile_n][pres_n_sort_idx]
    #     #print(pres_n_sorted)
    #     temp_resample = interp1d(pres_n_sorted, temp_n_sorted, kind='cubic', fill_value=np.nan, bounds_error=False)
    #     #print(temp_resample(pres_resample))
    #     profile_temp_resampled[profile_n] = temp_resample(pres_resample)
    #
    # print(profile_temp_resampled)
    # print(profile_time)
    #
    # fig, ax = plt.subplots()
    #
    # ax.set_xlim(profile_time[0], profile_time[len(profile_time)-1])
    #
    # plt.imshow(np.transpose(profile_temp_resampled), aspect='auto')
    # ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
    #
    # plt.show()

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
