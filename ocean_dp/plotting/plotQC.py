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

import xarray as xr

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns

from pandas.plotting import register_matplotlib_converters

import sys
import os


def plot_all(files):
    register_matplotlib_converters()
    plt.style.use('seaborn-darkgrid')
    sns.set_context("paper")

    pp = PdfPages(os.path.join(os.path.dirname(files[0]), "batch-qc.pdf"))

    for f in files:
        print("file ", f)
        do_plot(f)

        pp.savefig()
        plt.close()

    pp.close()


def plot(fn):
    register_matplotlib_converters()
    plt.style.use('seaborn-darkgrid')
    sns.set_context("paper")

    pp = PdfPages(fn + "-qc.pdf")

    do_plot(fn)

    pp.savefig()
    pp.close()
    plt.close()


def do_plot(fn):

    #fn = sys.argv[1]
    #fn = '/Users/pete/cloudstor/SOTS-Temp-Raw-Data/SOFS-7.5-2018/netCDF/IMOS_ABOS-SOTS_TIP_20180801_SOFS_FV01_SOFS-7.5-2018-Starmon-mini-4047-40m_END-20190331_C-20200429.nc'

    DS = xr.open_dataset(fn)

    ax1 = plt.subplot(2, 1, 1)
    plt.plot(DS.TIME, DS.PAR)
    plt.title(DS.deployment_code + " - " + DS.instrument_model + ":" + DS.instrument_serial_number + " @ " + str(DS.instrument_nominal_depth), {'fontsize': 8})

    ax2 = plt.subplot(2, 1, 2, sharex=ax1)
    aux = DS.PAR.ancillary_variables
    a_vars = aux.split(" ")
    for f in sorted(set(a_vars)):
        print('aux var', f)
        varn = f.split("_")
        plt.plot(DS.TIME, DS.variables[f], label=varn[-1])
    plt.ylim(0, 9)

    plt.legend(prop={'size': 6})

    DS.close()


if __name__ == "__main__":
    plot_all(sys.argv[1:])

