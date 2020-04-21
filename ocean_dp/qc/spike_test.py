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

default_high = 100

default_low = 50


# If files aren't specified, take all the IMOS*.nc files in the current folder
def spike_test_all_files(target_vars_in=[], thresh_low=default_low, thresh_high=default_high, flag_low=3, flag_high=4):
    target_files = glob.glob('IMOS*.nc')

    spike_test_files(target_files, target_vars_in=target_vars_in, thresh_low=thresh_low,thresh_high=thresh_high,flag_low=flag_low, flag_high=flag_high)


def spike_test_files(target_files, target_vars_in=[], thresh_low=default_low, thresh_high=default_high, flag_low=3, flag_high=4):
    
    # Loop through each files in target_files
    for current_file in target_files:
        # Print each filename
        print("input file %s" % current_file)

        # Extract netcdf data into nc
        nc = Dataset(current_file, mode="a")

        # run the spike test
        spike_test(nc=nc, target_vars_in=target_vars_in, thresh_low=thresh_low,thresh_high=thresh_high,flag_low=flag_low, flag_high=flag_high)


def spike_test(nc, target_vars_in=[], thresh_low=default_low, thresh_high=default_high, flag_low=3, flag_high=4):
    
    # If target_vars aren't user specified, set it to all the variables of 
    # the current_file, removing unwanted variables
    if target_vars_in == []:
        
        target_vars = list(nc.variables.keys())
        
        # Remove TIME
        target_vars.remove('TIME')
        
        # Remove any quality_control variables
        qc_vars = [s for s in target_vars if 'quality_control' in s]
        target_vars = [s for s in target_vars if s not in qc_vars]
                        
        # Remove any variables of single length
        single_vars = [s for s in target_vars if nc.variables[s].size==1]
        target_vars = [s for s in target_vars if s not in single_vars]
        
        print('target_vars are '+' '.join(target_vars))
        
    else:
        target_vars = target_vars_in
        
    # For each variable
    for current_var in target_vars:
        
        # Extract the variable
        nc_var = nc.variables[current_var]
        
        # Create a test specific qc variable if it  doesn't already exist
        if nc_var.name + "_quality_control_spk" in nc.variables:
            ncVarOut = nc.variables[nc_var.name + "_quality_control_spk"]
        else:
            ncVarOut = nc.createVariable(nc_var.name + "_quality_control_spk", "i1", nc_var.dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
            ncVarOut[:] = np.zeros(nc_var.shape)
            ncVarOut.long_name = "quality flag for " + nc_var.name
            ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
            ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
    
        # add new variable to list of aux variables
        nc_var.ancillary_variables = nc_var.ancillary_variables + " " + nc_var.name + "_quality_control_spk"
                
        # Extract the variable data
        var_data = np.array(nc.variables[current_var][:])
        
        print('checking '+current_var+' for high spikes')
        
        # Step through the data, one element at a time, starting from the 2nd element
        for i in range(1,(len(var_data)-1)):
            
            # Calculate the mean of the i-1 and i+1 elements
            shoulder_mean = np.mean(np.take(var_data,[i-1,i+1]))
            
            # Calculate the step changes
            shoulder_diff = np.diff(var_data[i-1:i+2])
            
            # Check for spike exceeding high threshold
            if (abs(var_data[i]-shoulder_mean) > thresh_high) & (True in (shoulder_diff>0)) & (True in (shoulder_diff<0)):# & (1.25*abs(shoulder_diff[0]) >= abs(x[1]) >= 0.75*abs(shoulder_diff[0])):
                
                print('High spike found')
                
                #set corresponding QC value to...
                nc.variables[current_var+'_quality_control_spk'][i] = flag_high
                
        
        # Find the indices where qc isn't set to 4 (high spike), removing the final element as it can't be check for a spike
        low_spike_chk_idx = np.where(nc.variables[current_var+'_quality_control'][:]!=4)[0][0:-1]

        
        # Remove from the indices those that are either side of a high spike
        for i in np.where(nc.variables[current_var+'_quality_control'][:]==4)[0]:
            
            low_spike_chk_idx=low_spike_chk_idx[low_spike_chk_idx!=[i-1]]
            
            low_spike_chk_idx=low_spike_chk_idx[low_spike_chk_idx!=[i+1]]
        
        #print(low_spike_chk_idx)
        
        print('checking '+current_var+' for low spikes')
        
        # For each of the remaining indices
        for i in low_spike_chk_idx:
            
            #print('i is '+str(i))
            
            # Calculate the mean of the i-1 and i+1 elements
            shoulder_mean = np.mean(np.take(var_data,[i-1,i+1]))
            
            # Calculate the step changes
            shoulder_diff = np.diff(var_data[i-1:i+2])
            
            # Check for spike exceeding low threshold
            if (abs(var_data[i]-shoulder_mean) > thresh_low) & (True in (shoulder_diff>0)) & (True in (shoulder_diff<0)): #& (1.25*abs(shoulder_diff[0]) >= abs(x[1]) >= 0.75*abs(shoulder_diff[0])):
                
                print('Low spike found')
                
                #set corresponding QC value to...
                nc.variables[current_var+'_quality_control_spk'][i] = flag_low   
                
    nc.variables[current_var  + "_quality_control"][:] = np.maximum(nc.variables[current_var  + "_quality_control_spk"][:],nc.variables[current_var  + "_quality_control"][:])

    # update the history attribute
    try:
        hist = nc.history + "\n"
    except AttributeError:
        hist = ""

    nc.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + ' :spike_test performed on [' + str(target_vars) + '], with spikes greater than '+str(thresh_high)+' flagged as '+str(flag_high)+' and spikes greater than '+str(thresh_low)+' flagged as '+str(flag_low))

    nc.close()
    
if __name__ == "__main__":
    # usage is <file_name> <variable_name> <window> <flag value>
    spike_test_files(target_files=[sys.argv[1]], target_vars_in=[sys.argv[2]], thresh_low=float(sys.argv[3]), thresh_high=float(sys.argv[4]), flag_low= float(sys.argv[5]), flag_high= float(sys.argv[6]))

    





