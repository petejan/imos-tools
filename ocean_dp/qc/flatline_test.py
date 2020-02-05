#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 14:10:41 2020

@author: tru050
"""

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

#!/usr/bin/python3

# add_qc_flags
# Copyright (C) 2020 Peter Jansen
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

# add QC variables to file


def add_qc(netCDFfile):

    new_name = [] # list of new file names

    # loop over all file names given
    for fn in netCDFfile[1:]:
        ds = Dataset(fn, 'a')

        # read the variable names from the netCDF dataset
        vars = ds.variables

        # create a list of variables, don't include the 'TIME' variable
        # TODO: detect 'TIME' variable using the standard name 'time'
        to_add = []
        for v in vars:
            #print (vars[v].dimensions)
            if v != 'TIME':
                to_add.append(v)

        # for each variable, add a new ancillary variable <VAR>_quality_control to each which has 'TIME' as a dimension
        for v in to_add:
            if "TIME" in vars[v].dimensions:
                # print("time dim ", v)

                ncVarOut = ds.createVariable(v+"_quality_control", "i1", vars[v].dimensions, fill_value=99, zlib=True)  # fill_value=99 otherwise defaults to max, imos-toolbox uses 99
                ncVarOut[:] = np.zeros(vars[v].shape)
                ncVarOut.long_name = "quality_code for " + v

                vars[v].ancillary_variables = v + "_quality_control"

        # update the file version attribute
        ds.file_version = "Level 1 - Quality Controlled Data"

        ds.close()

        # rename the file FV00 to FV01 (imos specific)
        fn_new = fn.replace("FV00", "FV01")
        new_name.append(fn_new)

        if fn_new != fn:
            # copy file
            os.copy(fn, fn_new)

        print(fn_new)

    return new_name


if __name__ == "__main__":
    add_qc(sys.argv)

##############################################################################

def flatline_test(*target_files,target_vars=[],window=3):
    
    # If files aren't specified, take all the .nc files in the current folder
    if not target_files:
        
        target_files = glob.glob('*.nc')
    
    # Loop through each files in target_files
    for current_file in target_files:
        
        
        # Print each filename
        print("input file %s" % current_file)
        
        # Extract netcdf data into nc
        nc = Dataset(current_file, mode="r")
        
        # Extract time
        ncTime = nc.get_variables_by_attributes(standard_name='time')
    
        # If target_vars aren't user specified, set it to all the variables of 
        # the current_file, removing TIME
        if target_vars == []:
            
            target_vars = list(nc.variables.keys())
            
            target_vars.remove('TIME')
            
        # Check if file contains quality control variables, and if not create
        
        if not any("_quality_control" in i for i in target_vars:
                   
                   # insert _quality_control variables into file? 
                   # should this be done now, or should we assume it
                   # will have already been done?
                
            
        # For each variable, extract the data 
        for current_var in target_vars:
            
            var_data = np.array(nc.variables[current_var])
            
            for i in 0:(len(var_data)-window+1):
                
                    # This is true if 'window' elements in a row are equal
                if len(set(var_data[i:(i+window)])) == 1
                    
                    # set corresponding QC value to...
                    
    
            
            
            
            
    
    
    
    