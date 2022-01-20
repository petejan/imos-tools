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

import sys

from matplotlib.colors import Normalize

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import ocean_dp.processing.plot_atlas_profile

import xarray as xr
import numpy as np
from scipy import stats
from scipy.stats import gaussian_kde

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

from pandas.plotting import register_matplotlib_converters

import os

import ocean_dp.file_name.find_file_with

from matplotlib import cm
from matplotlib.colors import Normalize
from scipy.interpolate import interpn


def density_scatter( x , y, ax = None, fig = None, sort = True, bins = 20, **kwargs ):
    """
    Scatter plot colored by 2d histogram
    """
    if ax is None :
        fig , ax = plt.subplots()

    data , x_e, y_e = np.histogram2d( x, y, bins = bins, density = True )
    z = interpn( ( 0.5*(x_e[1:] + x_e[:-1]) , 0.5*(y_e[1:]+y_e[:-1]) ) , data , np.vstack([x,y]).T , method = "splinef2d", bounds_error = False)

    #To be sure to plot all data
    z[np.where(np.isnan(z))] = 0.0

    # Sort the points by density, so that the densest points are plotted last
    if sort :
        idx = z.argsort()
        x, y, z = x[idx], y[idx], z[idx]

    ax.scatter( x, y, c=z, s=10, edgecolor='', **kwargs )

    #norm = Normalize(vmin = np.min(z), vmax = np.max(z))
    #cbar = fig.colorbar(cm.ScalarMappable(norm = norm), ax=ax)
    #cbar.ax.set_ylabel('Density')

    return ax

def do_plot(fn, ax = None, fig = None):

    #fn = sys.argv[1]
    #fn = '/Users/pete/cloudstor/SOTS-Temp-Raw-Data/SOFS-7.5-2018/netCDF/IMOS_DWM-SOTS_TIP_20180801_SOFS_FV01_SOFS-7.5-2018-Starmon-mini-4047-40m_END-20190331_C-20200429.nc'

    DS = xr.open_dataset(fn)

    #ax1 = plt.subplot(2, 1, 1)
    try:
        msk = DS.TEMP_quality_control_io < 2
    except AttributeError:
        msk = DS.TEMP_quality_control < 2

    #print(DS.TEMP.data)
    print(DS.NOMINAL_DEPTH.data)
    temp_data_msk = DS.TEMP.data[msk]
    nd = DS.NOMINAL_DEPTH.data

    summary = (float(DS.NOMINAL_DEPTH.data), stats.describe(temp_data_msk))

    density_scatter(temp_data_msk, nd*np.ones(len(temp_data_msk)), bins=100, ax=ax, fig=fig)

    #diff_temp = np.diff(temp_data_msk)
    #nd_s = nd*np.ones(len(diff_temp))
    #density_scatter(diff_temp, nd_s, bins=100, ax=ax, fig=fig)

    # Calculate the point density
    # xy = np.vstack([nd_s, diff_temp])
    # z = gaussian_kde(xy)(xy)

    #plt.plot(temp_data_msk, DS.NOMINAL_DEPTH.data*np.ones(len(temp_data_msk)), marker='.', markersize=1, linestyle='none')
    #plt.plot(np.diff(temp_data_msk), DS.NOMINAL_DEPTH.data*np.ones(len(temp_data_msk[:-1])), marker='.', markersize=1, linestyle='none')
    #plt.title(DS.deployment_code + " - " + DS.instrument_model + ":" + DS.instrument_serial_number + " @ " + str(DS.instrument_nominal_depth), {'fontsize': 8})

    # plt.scatter(diff_temp, nd_s, c=z, s=50, edgecolor='')
    #plt.plot(diff_temp, nd_s, c=z, marker='.', markersize=1, linestyle='none')
    # ax2 = plt.subplot(2, 1, 2, sharex=ax1)
    # aux = DS.PAR.ancillary_variables
    # a_vars = aux.split(" ")
    # for f in sorted(set(a_vars)):
    #     print('aux var', f)
    #     varn = f.split("_")
    #     plt.plot(DS.TIME, DS.variables[f], label=varn[-1])
    # plt.ylim(0, 9)
    #
    # plt.legend(prop={'size': 6})

    DS.close()

    return summary


if __name__ == "__main__":

    fig, ax = plt.subplots()
    summary_list = []
    for fp in sys.argv[1:]:
        print('file path : ', fp)
        nc_files = ocean_dp.file_name.find_file_with.find_files_pattern(fp)
        temp_files = ocean_dp.file_name.find_file_with.find_variable(nc_files, 'TEMP')
        #temp_files = ocean_dp.file_name.find_file_with.find_variable(temp_files, 'PRES')

        print('temp_files files:')
        for f in temp_files:
            print(f)
            summary_list.append(do_plot(f, ax=ax, fig=fig))

    t_mean, t_std, depth = ocean_dp.processing.plot_atlas_profile.get_data()

    #plt.plot(np.mean(t_mean, axis=0), depth)
    #plt.plot(np.max(t_mean, axis=0) + np.max(t_std, axis=0), depth)
    #plt.plot(np.min(t_mean, axis=0) - np.max(t_std, axis=0), depth)

    sorted_by_depth = sorted(summary_list, key=lambda tup: tup[0])
    #print(sorted_by_depth)

    depths = [x[0] for x in sorted_by_depth]
    std = [x[1].variance for x in sorted_by_depth]

    #plt.plot(std, depths, marker='*', markersize=5, linestyle='none')

    ax = plt.gca()
    ax.set_yscale('log')
    ax.invert_yaxis()

    plt.xlabel('temperature')
    plt.ylabel('depth, log_scale (dbar)')

    plt.grid()

    plt.show()


