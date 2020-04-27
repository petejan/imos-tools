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


import re
from datetime import datetime, timedelta
from netCDF4 import num2date, date2num
from netCDF4 import stringtochar
import numpy.ma as ma
import sys
from netCDF4 import Dataset
import numpy as np
import argparse
import glob
import pytz
import os

# Code is design to check that netcdf files processed using the SOTS methods 
# conform to the QC labelling designed by Peter Jansen.

def qc_checker_all_files(target_vars_in=[]):
    
    successful_files=[]
    
    target_files = glob.glob('IMOS*.nc')

    # Returns the files that conform to the qc labelling
    successful_files = qc_checker_files(target_files, target_vars_in=target_vars_in)
    

def qc_checker_files(target_files,target_vars_in=[]):
    
    successful_files=[]
    
    # Loop through each files in target_files
    for current_file in target_files:
        # Print each filename
        print("input file %s" % current_file)

        # Extract netcdf data into nc
        nc = Dataset(current_file, mode="a")

        # If the qc_checker was successfull, add the filename to the list
        if qc_checker(nc,target_vars_in=target_vars_in):
        
            successful_files.append(current_file)
    
    # Returns the files that conform to the qc labelling    
    return successful_files


# Enter args as variable name and rate of change limit, ie. 'TEMP',4
def qc_checker(nc,target_vars_in=[]):
    
    # Collect all the variables from the netcdf
    all_vars = list(nc.variables.keys())
    
    # If target_vars aren't user specified, set it to all the variables of 
    # the current_file, removing unwanted variables (qc, single length, TIME)
    if target_vars_in == []:
                
        target_vars = [s for s in all_vars if 'TIME' not in s and 'quality_control' not in s and nc.variables[s].size!=1]
        
        print('target_vars are '+' '.join(target_vars))
        
    else:
        target_vars = target_vars_in    
           
    # For each of the variables selected    
    for current_var in target_vars:
        
        # Collect the global qc data
        qc_global_data = np.array(nc.variables[current_var+"_quality_control"][:])
        
        # Collect the names of all the other test specific qc vectors
        qc_test_specific = [s for s in all_vars if current_var+"_quality_control" in s and not s.endswith('control')]
        
        # For each of the other test specific qc vectors
        for current_qc_test in qc_test_specific:
            
            # Extract the data
            qc_test_data = np.array(nc.variables[current_qc_test])
            
            #print('checking '+current_qc_test)
            
            # If any of the test specific qc vectors ever have a great value than the global qc vector at a timestamp, the qc process has failed
            if any(np.less(qc_global_data,qc_test_data)):
                
                print(current_qc_test + "failed")
                
                qc_behaving = False
                
            else:
                
                # The qc process has succeeded 
                qc_behaving = True
    
    # Returns true if qc has succeeded, false if not
    return qc_behaving
    

    # Close the current netcdf file
    nc.close()
    
    
# def qc_check_plot(target_file,target_var)

#     nc = Dataset(target_file,'r')

        
    
    
    
    
    
    
    
    