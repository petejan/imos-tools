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
import glob

deployments = []

for x in os.listdir("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
    if ('Pulse' in x) or ('SOFS' in x):
        
        deployments.append(x)
        

deployment_dtemp_dtime = np.array([])

fv01_files = glob.glob("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data/*/*/*FV01*.nc")


fig, ax = plt.subplots(4,4,sharex='all', sharey='all')

ax=ax.flatten()


for current_deployment, plt_idx in zip(deployments, range(0,len(deployments))):
    
    print(current_deployment + 'files')
    
    for fname in fv01_files:
      
        if current_deployment in fname:
        
            #print(fname + ' contains ' + current_deployment)  #Here, the wanted file name is printed

            nc = Dataset(fname, mode = 'r')
            
            if 'TEMP_quality_control' in list(nc.variables) and np.array(nc.variables['TEMP'][:]).ndim == 1:
                
                print(fname)
                
                # Calculate temperature changes
                nc_temp_diffs = np.diff(np.array(nc.variables['TEMP'][np.array(nc.variables['TEMP_quality_control'][:])!=7]))
                
                # Extract the time data
                nc_time = np.array(nc.variables['TIME'][np.array(nc.variables['TEMP_quality_control'][:])!=7])
            
                # Convert from days to hours
                nc_time_hr = nc_time*24
                
                # Calculate time changes
                nc_time_hr_diffs = np.diff(nc_time_hr)
                
                # Calculate the rate of change of temperature wrt time
                nc_dtemp_dtime = np.divide(nc_temp_diffs,nc_time_hr_diffs)
                
                # Add the results for this netcdf to the record for the deployment
                deployment_dtemp_dtime = np.concatenate((deployment_dtemp_dtime,nc_dtemp_dtime))
                
            nc.close()
            
    print('plotting '+ str(len(deployment_dtemp_dtime)) + ' values')
            
    ax[plt_idx].hist(deployment_dtemp_dtime,100)
    
    #ax[plt_idx].set_ylim(bottom=0.1,top=10E5)

    #ax[plt_idx].set_xlim(left=-10, right=10)
    
    deployment_dtemp_dtime = np.array([])



























