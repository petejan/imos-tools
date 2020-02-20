#!/usr/bin/python3

# ocean_dp
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

# Submit argument as a list



def select_in_water(netCDFfiles):
    
    new_name = [] # list of new file names
    now = datetime.utcnow()            

    # loop over all file names given
    for fn in netCDFfiles:
        
        # Check the file is an IMOS formatted file
        if fn.split('_')[0]=='IMOS':        
            fn_new = fn.replace("FV00", "FV01")           
            fn_new_split = fn_new.split('_')            
            # Change the creation date in the filename to today
            fn_new_split[-1] = "C-" + now.strftime("%Y%m%d") + ".nc"            
            fn_new = '_'.join(fn_new_split)
        else:
            fn_new = fn.replace(".nc", "-trim.nc")
            
        # Add the new file name to the list of new file names
        new_name.append(fn_new)

        # Load the original netcdf file
        ods = Dataset(fn,'a')

        # Extract the time dimension, and the deployment start and end    
        # TODO: check this works
        v = ods.get_variables_by_attributes(standard_name='time')
        time = np.array(v[0][:])

        inw = parse(ods.time_deployment_start)
        outw = parse(ods.time_deployment_end)

        # Convert the start and end to the number format used in TIME
        inw_num = date2num(inw.replace(tzinfo=None), units=ods.variables['TIME'].units)
        outw_num = date2num(outw.replace(tzinfo=None), units=ods.variables['TIME'].units)

        # Create logical index of deployed times        
        deployed = np.logical_and(time>=inw_num, time<=outw_num)

        # Determine the length of the new time dimension        
        time_dim_len = len(time[deployed])

        # Create the new netcdf file
        ds = Dataset(fn_new, "w", format="NETCDF4")

        new_time_dim = ds.createDimension("TIME", time_dim_len)

        # Copy global attributes        
        for att in ods.ncattrs():
            ds.setncattr(att, ods.getncattr(att))

        # Copy variables            
        for v_name, varin in ods.variables.items():

            varout = ds.createVariable(v_name, varin.datatype, varin.dimensions)

            # Copy variable attributes
            varout.setncatts({k: varin.getncattr(k) for k in varin.ncattrs()})

            # Fill variables with deployed data            
            # TODO: should check if the dimensions for the variable include TIME, and truncate that dimension
            if np.array(varin[:]).size == 1:
                varout[:] = varin[:]
            else:
                varout[:] = np.array(varin[:])[deployed]

        ds.date_created = now.strftime("%Y-%m-%dT%H:%M:%SZ")     

        # update the time coverage attributes            
        ds.time_coverage_start = ods.time_deployment_start            
        ds.time_coverage_end = ods.time_deployment_end

        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""        
        ds.history += hist + now.strftime("%Y%m%d:") + 'Data subset to only contain deployed (in water) data - the full record can be found in the corresponding FV00 file.'        

        ds.close()       
        ods.close()
        
    return new_name


if __name__ == "__main__":
    select_in_water(sys.argv[1:]) 
