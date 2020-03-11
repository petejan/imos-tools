# Copyright (C) 2020 Ben Weeding
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
import numpy.ma as ma
import sys
from netCDF4 import Dataset
import numpy as np
import argparse
import glob
import pytz
import os
import matplotlib.pyplot as plt
from matplotlib import colors
from matplotlib.ticker import PercentFormatter

netcdf_files = []

temp_diffs = np.array([])

for root, dirs, files in os.walk("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    for file,dirx in files,dirs:
        if file.endswith('.nc'):
            netcdf_files.append(file)
            nc = Dataset(os.path.join(dirs, file),mode='r')
            
            temp_diffs = np.concatenate((temp_diffs,np.diff(np.array(nc.variables['TEMP'][:]))))
            
            nc.close()
            

print (list_of_files)
    
    
    


files = glob.glob('*.nc')

temp_diffs = np.array([])

for current_file in files:
    
    nc = Dataset(current_file,mode='r')
    
    temp_diffs = np.concatenate((temp_diffs,np.diff(np.array(nc.variables['TEMP'][:]))))

fig, ax = plt.subplots()

ax.hist(temp_diffs,100,log=True)


# use os.walk??? to run in each netcdf folder?? os.scandir()?





# sofs75_60m = Dataset('IMOS_ABOS-SOTS_T_20180801_SOFS_FV00_SOFS-7.5-2018-Starmon-mini-4051-60m_END-20190331_C-20200204.nc',mode='r')
# sofs75_70m = Dataset('IMOS_ABOS-SOTS_T_20180801_SOFS_FV00_SOFS-7.5-2018-Starmon-mini-4052-70m_END-20190331_C-20200204.nc',mode='r')
# sofs75_75m = Dataset('IMOS_ABOS-SOTS_T_20180801_SOFS_FV00_SOFS-7.5-2018-Starmon-mini-4053-75m_END-20190331_C-20200204.nc',mode='r')


# temp_60 = np.array(sofs75_60m.variables['TEMP'][:])
# temp_70 = np.array(sofs75_70m.variables['TEMP'][:])
# temp_75 = np.array(sofs75_75m.variables['TEMP'][:])

# label_coords = (0.01, 0.85)
# label_method = 'axes fraction'

# fig, axs = plt.subplots(3, 1, sharey=True)
# axs[0].set_title('Temp sensor comparison SOFS7.5')

# axs[0].hist(np.diff(temp_60),bins=100,log=True, histtype='bar', stacked=True)
# axs[0].set_ylim(bottom=0.1,top=10E5)
# axs[0].set_xlim(left=-40, right=40)
# axs[0].annotate('60m',xy=label_coords, xycoords=label_method)
# axs[0].tick_params(labelbottom=False)

# axs[1].hist(np.diff(temp_70),bins=100,log=True)
# axs[1].set_ylim(bottom=0.1,top=10E5)
# axs[1].set_xlim(left=-40, right=40)
# axs[1].annotate('70m',xy=label_coords, xycoords=label_method)
# axs[1].tick_params(labelbottom=False)

# axs[2].hist(np.diff(temp_75),bins=100,log=True)
# axs[2].set_ylim(bottom=0.1,top=10E5)
# axs[2].set_xlim(left=-40, right=40)
# axs[2].annotate('75m',xy=label_coords, xycoords=label_method)



# fig.savefig('test.pdf')






