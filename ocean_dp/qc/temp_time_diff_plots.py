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



def last_four(entry):
    
    output = entry[-4::]
    
    return output


deployments = []

for x in os.listdir("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
    if ('Pulse' in x) or ('SOFS' in x):
        
        deployments.append(x)

deployments.sort(key=last_four)


fig, ax = plt.subplots(4,4)

ax=ax.flatten()  


all_deployment_dtemp_dtime = [None] * len(deployments)

for current_deployment, plt_idx in zip(deployments, range(0,len(deployments))):
    
    print('current deployment is '+current_deployment)
    
    deployment_dtemp_dtime = np.array([])
    
    for root, dirs, files in os.walk("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
        for fname in files:
          
            if current_deployment in fname and fname.endswith('.nc') and 'FV00' in fname:
            
                #print(fname)  #Here, the wanted file name is printed

                nc = Dataset(os.path.join(root,fname), mode = 'r')
                
                if 'TEMP' in nc.variables and np.array(nc.variables['TEMP'][:]).ndim == 1 and nc.variables['TIME'].getncattr('units') =='days since 1950-01-01 00:00:00 UTC':
                    
                    time_var = nc.variables["TIME"]
                    
                    time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)
                
                    time_deploy = parser.parse(nc.time_deployment_start, ignoretz=True)
                    
                    time_recovery = parser.parse(nc.time_deployment_end, ignoretz=True)
                    
                    #print('using '+fname)
                    
                    temp_extract = np.array(nc.variables['TEMP'][:][(time >= time_deploy) | (time <= time_recovery)])
                    
                    # Calculate temperature changes
                    nc_temp_diffs = np.diff(temp_extract)
                    
                    # Extract the time data
                    nc_time = np.array(nc.variables['TIME'][:][(time >= time_deploy) | (time <= time_recovery)])
                
                    # Convert from days to hours
                    nc_time_hr = nc_time*24
                    
                    ax[plt_idx].plot(nc_time,temp_extract)
                    
                    # Calculate time changes
                    nc_time_hr_diffs = np.diff(nc_time_hr)
                    
                    # Calculate the rate of change of temperature wrt time
                    nc_dtemp_dtime = np.divide(nc_temp_diffs,nc_time_hr_diffs)
                    
                    # Add the results for this netcdf to the record for the deployment
                    deployment_dtemp_dtime = np.concatenate((deployment_dtemp_dtime,nc_dtemp_dtime))
                    
                    all_deployment_dtemp_dtime[plt_idx] = deployment_dtemp_dtime
                
                nc.close()
                
                
                





