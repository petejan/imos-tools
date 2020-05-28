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

import warnings
import scipy.stats as st
import statsmodels as sm
import matplotlib

# this function creates a pandas Datatable object, searching through all the 
# netcdf files in the directory given, containing all the variables specified

# "/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"

# ["dTemp/dtime","dTemp/dSample","QC","Nominal depth","Deployment"]

#  qc selection!!!

def panda_maker(dir_spec,var_list,qc_lim=2):

    # creates an empty array to store the names of the SOTS deployments
    deployments = []
    
    checked_files = []
    
    processed_files = []
    
    # loops through all the folders and files contained in the folder
    for x in os.listdir(dir_spec):
        
        # if the folder/file name contains 'Pulse' or 'SOFS' and doesn't contain '.', append it to deployments
        if (('Pulse' in x) or ('SOFS' in x)) and ('.p' not in x):
            
            deployments.append(x)
        
        
        
    # create a dataframe to store extract information
    total_df = pd.DataFrame(columns = var_list)
    
    # add deployment code to the dataframe
    total_df.insert(len(var_list),'Deployment code',[])
    
    # loops through all files in the directory
    for root, dirs, files in os.walk("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
        
        for fname in files:
            
            # append the filename to the list of checked files
            checked_files.append(fname)
            
            # for each netcdf file labelled as FV01 and containing a deployment in its name
            if fname.endswith('.nc') and 'FV01' in fname and any(ele in fname for ele in deployments):
            
                # print the filename
                print(fname)  
                
                # open the file
                nc = Dataset(os.path.join(root,fname), mode = 'r')
                
                # check file contains all the specified variables and the time format is correct
                if (all(ele in list(nc.variables) for ele in var_list)) & (nc.variables['TIME'].getncattr('units') =='days since 1950-01-01 00:00:00 UTC'):
                        
                    # create a current dataframe for the netcdf file, to be appended to the overall dataframe
                    cur_df = pd.DataFrame(columns=var_list)
                    
                    # create a qc vector for the netcdf file
                    cur_qc = np.zeros(nc.variables["TIME"].shape)

                    for cur_var in var_list:
                        
                        if np.array(nc.variables[cur_var]).size == 1:
                            
                            filling = np.ones(nc.variables["TIME"].shape) * np.array(nc.variables[cur_var])
                            
                        else:
                            
                            filling = np.array(nc.variables[cur_var][:])
                            
                        if cur_var + '_quality_control' in list(nc.variables):
                            
                            cur_qc = np.maximum(cur_qc,np.array(nc.variables[cur_var + '_quality_control']))        
                            
                            
                        cur_df[cur_var] = filling
                    
                    cur_df['Deployment code'] = [nc.deployment_code] * len(np.array(nc.variables['TIME']))

                    # append the current netcdf's dataframe to the sots_temp_ensemble
                    total_df = total_df.append(cur_df.iloc[np.where(cur_qc<=qc_lim)])
                    
                    # append the filename to the list of processed files
                    processed_files.append(fname)
                        
                        
                nc.close()
                
                
    return total_df
                        