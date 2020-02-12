#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 09:49:26 2020

@author: tru050
"""
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

def select_in_water(netCDFfile):
    
    new_name = [] # list of new file names

    # loop over all file names given
    for fn in netCDFfile:
        
        # Change the creation date in the filename to today
        now=datetime.utcnow()
        
        fn_new = fn.replace("FV00", "FV01")
        
        fn_new = "".join((fn_new[0:-11],now.strftime("%Y%m%d"),fn_new[-3::]))
        
        # Add the new file name to the list of new file names
        new_name.append(fn_new)
            
        # Load the original netcdf file
        ods = Dataset(fn,'a')
        
        # Extract the time dimension, and the deployment start and end        
        time = np.array(ods.variables['TIME'][:])
        
        inw = parse(ods.time_deployment_start)

        outw = parse(ods.time_deployment_end)
        
        # Convert the start and end to the number format used in TIME
        inw_num = date2num(inw.replace(tzinfo=None),units = ods.variables['TIME'].units)
        
        outw_num = date2num(outw.replace(tzinfo=None),units = ods.variables['TIME'].units)
        
        # Create logical index of deployed times
        
        deployed = np.logical_and(time>=inw_num,time<=outw_num)
        
        # Determine the length of the new time dimension
        
        time_dim = len(time[deployed])
        
        # Create the new netcdf file
        ds = Dataset(fn_new, "w", format="NETCDF4")
        
        TIME = ds.createDimension("TIME",time_dim)
        
        # Copy global attributes
        
        for att in ods.ncattrs():
            
            ds.setncattr(att,ods.getncattr(att))
        
        # Copy variables
            
        for v_name, varin in ods.variables.items():
            
            varout = ds.createVariable(v_name, varin.datatype, varin.dimensions)
    
            # Copy variable attributes
            varout.setncatts({k: varin.getncattr(k) for k in varin.ncattrs()})
    
            # Fill variables with deployed data
            
            if np.array(varin[:]).size == 1:
                
                varout[:] = varin[:]
                
            else:
                
                varout[:] = np.array(varin[:])[deployed]
                
        ds.date_created = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        ds.history += ' ' + now.strftime("%Y%m%d:") + 'Data subset to only contain deployed (in water) data - the full record can be found in the corresponding FV00 file.'        
        
        ds.close()
        
        ods.close()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
            
            
            
            