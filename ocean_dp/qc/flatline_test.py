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


# If files aren't specified, take all the IMOS*.nc files in the current folder
def flatline_test_all_files(target_vars_in=[], window=5, flag=4):
    target_files = glob.glob('IMOS*.nc')

    flatline_test_files(target_files, target_vars_in=target_vars_in, window=window, flag=flag)


def flatline_test_files(target_files, target_vars_in=[], window=5, flag=4):
    
    # Loop through each files in target_files
    for current_file in target_files:
        # Print each filename
        print("input file %s" % current_file)

        # Extract netcdf data into nc
        nc = Dataset(current_file, mode="a")

        # run the flat line test
        flatline_test(nc=nc, target_vars_in=target_vars_in, window=window, flag=flag)


def flatline_test(nc, target_vars_in=[], window=5, flag=4):
    
    print('Window is '+str(window))
    
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
        
    # For each variable, extract the data 
    for current_var in target_vars:
        
        # Extract the variable
        nc_var = nc.variables[current_var]
        
        if nc_var.name + "_quality_control_flt" in nc.variables:
            print('flt qc variable already present')
            ncVarOut = nc.variables[nc_var.name + "_quality_control_flt"]
            ncVarOut[:] = 0
        else:
            ncVarOut = nc.createVariable(nc_var.name + "_quality_control_flt", "i1", nc_var.dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
            ncVarOut[:] = 0
            # print(all(nc.variables[nc_var.name + "_quality_control_flt"]==0))
            ncVarOut.long_name = "quality flag for " + nc_var.name
            ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
            ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'

            # add new variable to list of aux variables
            nc_var.ancillary_variables = nc_var.ancillary_variables + " " + nc_var.name + "_quality_control_flt"

        var_data = np.array(nc.variables[current_var][:])

        if (all(nc.variables[nc_var.name + "_quality_control_flt"][:] == 0)):
            print('All test specific qc values are zero before filling')

        print('checking ' + current_var)

        print('Window is ' + str(window))

        # Step through the data, one element at a time, using the window
        for i in range(0, (len(var_data) - window + 1)):

            # This is true if 'window' elements in a row are equal
            if len(set(var_data[i:(i + window)])) == 1:
                print(str(i))
                # set corresponding QC value to...
                ncVarOut[i:(i + window)] = flag

        points_marked = len([elem for elem in ncVarOut[:] if elem == 4])
        print('Data points flagged: ', points_marked)

        qc_var = nc.variables[current_var + "_quality_control"]
        qc_var[:] = np.maximum(ncVarOut[:], qc_var[:])
    # update the history attribute
    try:
        hist = nc.history + "\n"
    except AttributeError:
        hist = ""
        
        

    nc.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + 'flatline_test performed on [' + str(target_vars) + '], window '+str(window)+' consecutive values or more were flagged with '+str(flag) )

    nc.close()
    
if __name__ == "__main__":
    # usage is <file_name> <variable_name> <window> <flag value>
    flatline_test_files(target_files=[sys.argv[1]], target_vars_in=[sys.argv[2]], window=float(sys.argv[3]), flag=float(sys.argv[4]))
