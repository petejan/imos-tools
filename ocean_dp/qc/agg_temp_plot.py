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
from netCDF4 import Dataset, num2date
from dateutil import parser
import numpy as np
import argparse
import glob
import pytz
import os
import matplotlib.pyplot as plt
from matplotlib import colors
from matplotlib.ticker import PercentFormatter
from sigfig import round


x=Dataset('IMOS_ABOS-SOTS_COPSTIP_20180822_SOFS_FV02_SOFS-Aggregate-TEMP_END-20190322_C-20200311.nc',mode='r')

temp = np.array(x.variables['TEMP'][:])

time = np.array(x.variables['TIME'][:])

ins_idx = np.array(x.variables['instrument_index'][:])

fig, ax = plt.subplots(6,5)

ax=ax.flatten()

label_coords = (0.1, 0.8)
label_method = 'axes fraction' 

for i in set(np.array(ins_idx)):

    ax[i].plot(time[ins_idx==i],temp[ins_idx==i])
    
    ax[i].annotate('S:'+str(i),xy=label_coords, xycoords=label_method,fontsize=8)
    
i=1
fig, ax = plt.subplots()
ax.plot(time[ins_idx==i],temp[ins_idx==i])


# Remove bad instruments
good_vals = [a!=14 and a!=15 for a in ins_idx]

fig, ax = plt.subplots()
ax.hist(temp[good_vals],21)

sofs75_temp_diffs = np.array([])

good_ins = set(np.array(ins_idx))

good_ins -= {14,15}

for i in good_ins:
    
    cur_temp = temp[ins_idx==i]
    
    cur_time = time[ins_idx==i]
    
    cur_time_hr = cur_time*24
    
    # Calculate time changes
    cur_time_hr_diffs = np.diff(cur_time_hr)
    
    cur_temp_diffs = np.diff(cur_temp)
    
    # Calculate the rate of change of temperature wrt time
    cur_dtemp_dtime = np.divide(cur_temp_diffs,cur_time_hr_diffs)
    
    print('ins '+str(i)+':'+str(np.max(cur_dtemp_dtime)))
    
    sofs75_temp_diffs = np.concatenate((sofs75_temp_diffs,cur_dtemp_dtime))
    
    
    
    
    
    
    
    









