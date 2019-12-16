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
from datetime import datetime
from datetime import timedelta

import numpy as np
import matplotlib.pyplot as plt

import sys
import matplotlib

matplotlib.use('Agg')

from matplotlib.backends.backend_pdf import PdfPages


def plot_binned(netCDFfiles):
    ds = Dataset(netCDFfiles[1], 'r')

    depth = ds.variables["DEPTH"]
    temp = ds.variables["TEMP"]
    time = ds.variables["TIME"]

    d = depth[:]
    t = temp[:]

    h = np.histogram(d, bins=np.arange(0, 3500, 1))
    plt.plot(h[0], h[1][0:-1], '.-')
    plt.gca().invert_yaxis()
    plt.grid(True)

    pdffile = netCDFfiles[1].replace(".nc", "-histogram.pdf")

    pp = PdfPages(pdffile)
    pp.savefig()

    plt.close()

    print("shape", time[:].shape, t.shape, d.shape)
    plt.figure(dpi=1200)
    plt.scatter(time[:], d, c=t, s=0.1, rasterized=True)
    plt.gca().invert_yaxis()
    #plt.show()

    pp.savefig(dpi=1200)

    pp.close()

    #plt.show()

    ds.close()

if __name__ == "__main__":
    plot_binned(sys.argv)

