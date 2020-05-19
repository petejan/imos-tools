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
import pandas as pd

# creates an empty array to store the names of the SOTS deployments
deployments = []

# loops through all the folders and files contained in the folder
for x in os.listdir("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
    # if the folder/file name contains 'Pulse' or 'SOFS' and doesn't contain '.', append it to deployments
    if (('Pulse' in x) or ('SOFS' in x)) and ('.' not in x):
        
        deployments.append(x)
    
# create a dataframe to store extract information
temp_ensemble = pd.DataFrame(columns = ["Temp rate of change","QC","Nominal depth","Deployment"])

# loops through all files in the directory
for root, dirs, files in os.walk("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
    for fname in files:
        
        # for each netcdf file labelled as FV01
        if fname.endswith('.nc') and 'FV01' in fname:
        
            # print the filename
            print(fname)  
            
            # open the file
            nc = Dataset(os.path.join(root,fname), mode = 'r')
            
            # check that the in_out_water test has been run on the file
            if 'TEMP_quality_control_io' in list(nc.variables):
                
                # check that the file has a single dimension temperature vector, and that the time format is correct
                if np.array(nc.variables['TEMP'][:]).ndim == 1 and nc.variables['TIME'].getncattr('units') =='days since 1950-01-01 00:00:00 UTC':
                    
                    # calculate temperature changes for in water data
                    nc_temp_diffs = np.diff(np.array(nc.variables['TEMP'][np.array(nc.variables['TEMP_quality_control'][:])!=7]))
                
                    # extract the time data
                    nc_time = np.array(nc.variables['TIME'][np.array(nc.variables['TEMP_quality_control'][:])!=7])
            
                    # Convert from days to hours
                    nc_time_hr = nc_time*24
                    
                    # Calculate time changes in hours
                    nc_time_hr_diffs = np.diff(nc_time_hr)
                    
                    # calculate the rate of change of temperature wrt time (degrees °C per hour)
                    nc_dtemp_dtime = np.divide(nc_temp_diffs,nc_time_hr_diffs)
                    
                    
                    
                    # extract temp_qc data
                    nc_temp_qc = np.array(nc.variables['TEMP_quality_control'][np.array(nc.variables['TEMP_quality_control'][:])!=7])
                    
                    # calculate qc values for each nc_dtemp_dtime by taking the maximum of the qc values of the two contributing temps
                    nc_dtemp_dtime_qc = pd.Series(nc_temp_qc).rolling(2).max().dropna().to_numpy()
                    
                    
                    # extract sensor nominal depth
                    nc_nom_depth = np.array(nc.variables['NOMINAL_DEPTH'])
                    
                    
                    # extract deployment name
                    nc_deployment = nc.deployment_code
                    
                    # Next step: append all this information to temp ensemble!
                    
            nc.close()
                    
                    
            
            # pd.Series(lst).rolling(5).max().dropna().to_numpy()
            
            
            
            
            