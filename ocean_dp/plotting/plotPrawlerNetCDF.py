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

matplotlib.use('Agg')

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


def plot(file):
    nc = Dataset(file)

    # get time variable
    time_var = nc.variables['TIME']
    t_unit = time_var.units  # get unit  "days since 1950-01-01T00:00:00Z"

    try:
        t_cal = time_var.calendar
    except AttributeError:  # Attribute doesn't exist
        t_cal = u"gregorian"  # or standard

    dt_time = [num2date(t, units=t_unit, calendar=t_cal) for t in time_var[:]]

    temp_var = nc.variables['TEMP']
    psal_var = nc.variables['PSAL']
    doxs_var = nc.variables['DOXS']
    pres_var = nc.variables['PRES']

    profile_var = nc.variables['PROFILE']

    # colors = matplotlib.cm.rainbow(profile_var[:]/np.max(profile_var[:]))
    # print(colors)
    #
    # plt.plot(temp_var[:], pres_var[:], '.', color=colors)
    #
    # plt.xlabel('temperature (deg C)')
    # plt.ylabel('pressure (dbar)')
    # plt.title('prawler temperature profile')
    #
    # plt.grid(True)
    # plt.ylim(100, 0)
    # plt.show()
    #
    # plt.plot(psal_var[:], pres_var[:], '.')
    #
    # plt.xlabel('practical salinity')
    # plt.ylabel('pressure (dbar)')
    # plt.title('prawler temperature profile')
    #
    # plt.grid(True)
    # plt.ylim(100, 0)
    # plt.show()

    pdffile = file + '.pdf'

    pp = PdfPages(pdffile)

    # Plot Data
    fig1, ax1 = plt.subplots()
    tcf = ax1.tricontourf(time_var[:]*1000, pres_var[:], temp_var[:], 20)
    plt.plot(time_var[:]*1000, pres_var[:], 'ko ', markersize=0.1)
    fig1.colorbar(tcf)
    plt.ylim(100, 0)

    plt.xlabel('date-time')
    plt.ylabel('pressure (dbar)')
    plt.title('prawler temperature profile')

    #plt.show()
    pp.savefig(fig1, papertype='a4')
    plt.close()

    fig1, ax1 = plt.subplots()
    tcf = ax1.tricontourf(time_var[:]*1000, pres_var[:], psal_var[:], 20)
    plt.plot(time_var[:]*1000, pres_var[:], 'ko ', markersize=0.1)
    fig1.colorbar(tcf)
    plt.ylim(100, 0)

    plt.xlabel('date-time')
    plt.ylabel('pressure (dbar)')
    plt.title('prawler practical salinity profile')

    #plt.show()
    pp.savefig(fig1, papertype='a4', orientation='portrait')
    plt.close()

    fig1, ax1 = plt.subplots()
    tcf = ax1.tricontourf(time_var[:]*1000, pres_var[:], doxs_var[:], 20)
    plt.plot(time_var[:]*1000, pres_var[:], 'ko ', markersize=0.1)
    fig1.colorbar(tcf)
    plt.ylim(100, 0)

    plt.xlabel('date-time')
    plt.ylabel('pressure (dbar)')
    plt.title('prawler oxygen saturation profile')

    #plt.show()
    pp.savefig(fig1, papertype='a4', orientation='portrait')
    plt.close()


    pp.close()

    nc.close()

    return pdffile


if __name__ == "__main__":
    plot(sys.argv[1])
