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

from dateutil.parser import parse
from netCDF4 import Dataset, num2date, date2num
from datetime import datetime, timedelta
import sys
from datetime import datetime
import numpy as np
from dateutil import parser
import pytz
import os
import shutil

# Provide the function with a filename (don't include .nc), a nominal depth,
# and pairs of names and arrays containing the data to be included as variables.
# A time dimension/variable is created by default, starting at 01/01/2020 using 
# 1 hour timestamps

# For example, netcdf_gen('test',30,'PRES',pres_data,'TEMP',temp_data)

def netcdf_gen(file_name,nominal_depth,*args):
    
    # Convert the args tuple to a list
    args = list(args)
    
    # Check the args are paired
    if len(args) % 2 == 0:
        
        # Assign the names and data to lists
        var_names = args[0::2]
        
        var_data = args[1::2]    
        
        # Check if first of each pair is a string
        if all(isinstance(x, str) for x in var_names):
        
            # Check if second of each pair are all equal in shape
            if all(np.shape(var_data[1]) == np.shape(x) for x in var_data):
                
                # Create the netcdf with IMOS tag
                ds = Dataset("IMOS_" + file_name + ".nc","w", format="NETCDF4")
                
                # Create time dimension with length to match data
                time_dim = ds.createDimension("TIME", len(var_data[0]))
                
                time_var = ds.createVariable("TIME","f8",("TIME"))
                
                ds.variables['TIME'][:] = np.arange(25567,25567+(1/24)*len(var_data[1]),1/24)
                
                time_atts = ['long_name','time','units','days since 1950-01-01 00:00:00 UTC',
                 'calendar','gregorian','axis','T','standard_name','time','valid_max',
                 90000,'valid_min',0]  
                
                for att_name,att_value in zip(time_atts[0::2],time_atts[1::2]):
                    
                    time_var.setncattr(att_name,att_value)
                
                # Create the nominal depth variable
                nom_depth_var = ds.createVariable("NOMINAL_DEPTH","f8")
                
                ds.variables["NOMINAL_DEPTH"] = nominal_depth
                
                nom_dep_atts = ['long_name','nominal depth','units','m',
                 'positive','down','axis','Z','standard_name','depth','valid_max',
                 12000,'valid_min',-5,'reference_datum','sea surface'] 
                
                for att_name,att_value in zip(nom_dep_atts[0::2],nom_dep_atts[1::2]):
                    
                    nom_depth_var.setncattr(att_name,att_value)
                
                # Create variables from input data
                for name_in,data_in in zip(var_names,var_data):
                    
                    ds.createVariable(name_in,"f8",("TIME"))
                    
                    ds.variables[name_in][:] = data_in
                    
                ds.close()
            
            else:
                print('Data arrays not of equal length')
            
        
        else:
            print('Labels not in string format')
        
    else:
        print('Data not passed in pairs')
        
        

        
        
        
        
        
        
        
        
        
        
        
        
        

