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

# how to specific rate of change? Will the function take rate of change as an 
# argument?

# Need a way of linking the rate of change to each different variable type

# Could max rate of change be something we upload into the netcdf file atts? Or just in history?

# Tell the function how much change you tolerate, and over what period of time - in sec?

# Then convert to match the file timesteps

# If files aren't specified, take all the IMOS*.nc files in the current folder
def roc_test_all_files(*args,target_vars_in=[]):
    target_files = glob.glob('IMOS*.nc')

    roc_test_files(target_files, target_vars_in=target_vars_in,*args)

def roc_test_files(target_files,*args,target_vars_in=[]):
    
    # Loop through each files in target_files
    for current_file in target_files:
        # Print each filename
        print("input file %s" % current_file)
        
        print(args)

        # Extract netcdf data into nc
        nc = Dataset(current_file, mode="a")

        # run the spike test - specifying *args here makes python unpack args to be passed again successfully as separate items
        roc_test(nc,*args, target_vars_in=target_vars_in)


# Enter args as variable name and rate of change limit, ie. 'TEMP',4
def roc_test(nc,*args,target_vars_in=[]):
    
    # Check the time format
    if nc.variables['TIME'].getncattr('units') =='days since 1950-01-01 00:00:00 UTC':
        
        # Convert the args tuple to a list
        args = list(args)
        
        # If a single rate of change limit is supplied
        if len(args) == 1:
            
            change_per_hr = args[0]
        
            print('One rate of change limit will be applied to all variables')
            
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
            
            # Extract the time data
            nc_time = np.array(nc.variables['TIME'][:])
            
            # Convert from days to hours
            nc_time_hr = nc_time*24
            
            # For each variable
            for current_var in target_vars:
                
                # Extract the variable
                nc_var = nc.variables[current_var]
                
                if nc_var.name + "_quality_control_roc" in nc.variables:
                    ncVarOut = nc.variables[nc_var.name + "_quality_control_roc"]
                else:
                    ncVarOut = nc.createVariable(nc_var.name + "_quality_control_roc", "i1", nc_var.dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
                    ncVarOut[:] = np.zeros(nc_var.shape)
                    ncVarOut.long_name = "quality flag for " + nc_var.name
                    ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
                    ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
            
                # add new variable to list of aux variables
                nc_var.ancillary_variables = nc_var.ancillary_variables + " " + nc_var.name + "_quality_control_roc"
                
                # Extract the variable data
                var_data = np.array(nc.variables[current_var][:])
                
                # Calculate dvar/dtime
                var_roc = np.divide(np.diff(var_data),np.diff(nc_time_hr))
                
                # For any change greater than change_per_hr, assign a qc value of 4
                nc.variables[current_var+'_quality_control_roc'][[x for x in abs(np.insert(var_roc,0,0)) > change_per_hr]] = 4
                
                nc.variables[current_var  + "_quality_control"][:] = np.maximum(nc.variables[current_var  + "_quality_control_roc"][:],nc.variables[current_var  + "_quality_control"][:])
                
                print(current_var + ' tested: '+str(sum([x for x in abs(np.insert(var_roc,0,0)) > change_per_hr])) + ' changes found above '+str(change_per_hr)+' '+nc.variables[current_var].units+' per hour')
                    
            # update the history attribute
            try:
                hist = nc.history + "\n"
            except AttributeError:
                hist = ""
    
            nc.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + 'rate of change test performed, with all changes above '+str(change_per_hr)+' flagged as 4')        
            

        # If multiple rate of change limits are supplied, with variable names
        elif len(args) % 2 == 0 and all(isinstance(x,str) for x in args[0::2]) and all(isinstance(y,(float,int)) for y in args[1::2]):
            
            # Take target variables from args
            target_vars = args[0::2]
            
            print('target_vars are '+' '.join(target_vars))
        
            # Convert arguments to dict
            rate_spec = dict(zip(args[0::2],args[1::2]))
            
            # Extract the time data
            nc_time = np.array(nc.variables['TIME'][:])
            
            # Convert from days to hours
            nc_time_hr = nc_time*24
            
            # For each variable
            for current_var in target_vars:
                
                # Extract the variable
                nc_var = nc.variables[current_var]
                
                if nc_var.name + "_quality_control_roc" in nc.variables:
                    ncVarOut = nc.variables[nc_var.name + "_quality_control_roc"]
                else:
                    ncVarOut = nc.createVariable(nc_var.name + "_quality_control_roc", "i1", nc_var.dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
                    ncVarOut[:] = np.zeros(nc_var.shape)
                    ncVarOut.long_name = "quality flag for " + nc_var.name
                    ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
                    ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
            
                # add new variable to list of aux variables
                nc_var.ancillary_variables = nc_var.ancillary_variables + " " + nc_var.name + "_quality_control_roc"
                                
                
                # Extract the data
                var_data = np.array(nc.variables[current_var])
                
                # Calculate dvar/dtime
                var_roc = np.divide(np.diff(var_data),np.diff(nc_time_hr))
                
                # For any change greater than change_per_hr, assign a qc value of 4
                nc.variables[current_var+'_quality_control_roc'][[x for x in abs(np.insert(var_roc,0,0)) > rate_spec[current_var]]] = 4
                
                nc.variables[current_var  + "_quality_control"][:] = np.maximum(nc.variables[current_var  + "_quality_control_roc"][:],nc.variables[current_var  + "_quality_control"][:])
                
                print(current_var + ' tested: '+str(sum([x for x in abs(np.insert(var_roc,0,0)) > rate_spec[current_var]])) + ' changes found above '+str(rate_spec[current_var])+' '+nc.variables[current_var].units+' per hour')
                    
            # update the history attribute
            try:
                hist = nc.history + "\n"
            except AttributeError:
                hist = ""
    
            nc.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + ': rate of change test performed, with all changes above those specified in the following list flagged as 4: '+str(args))        
            
            
        else:
            print('Arguments passed do not match the required format. No roc test performed.')
        
        
    # If the time format doesn't match IMOS requirements    
    else:
        print('Time format does not match the required IMOS form of: days since 1950-01-01 00:00:00 UTC')
    
    
    nc.close()
    
    
# Not sure how to sys.argv[] with both *args and a keyword argument
if __name__ == "__main__":
    # usage is <file_name> <variable_name> <*args>
    roc_test_files(target_files=[sys.argv[1]], target_vars_in=[sys.argv[2]], *sys.argv[3:])

    


