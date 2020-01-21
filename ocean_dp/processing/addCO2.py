#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 14:58:20 2020

@author: tru050
"""




from netCDF4 import Dataset
import sys
import gsw
import numpy as np
from datetime import datetime
import pandas

# addCO2 takes a SOTS FV02 gridded product netCDFfile as an input, and adds 
# CO2 data (delivered from NOAA in a csv file) to the netCDFfile
def addCO2(netCDFfile):
    
    # Import the SOTS netcdf
    ds = Dataset(netCDFfile, 'a')
    
    # Extract the time variable, in serial date numbers (days since 01/01/1950)
    var_time = ds.variables["TIME"]

    # Convert the variable object to an array
    netcdf_serials = np.array(var_time[:])
    
    # Read in the CO2 csv file, ignoring the first five rows
    dcsv = pandas.read_csv('SOFS_prelimdata_Nov2019.csv',header=5)
    
    # Convert the dataframe to an array
    dc = dcsv.to_numpy()
    
    csv_dates = []
    
    # Create a list of datetimes from the csv
    for i in range(len(dc)):
    
        csv_dates.append(datetime.strptime(dc[i,0],'%m/%d/%Y %H:%M'))
        
    # Calculate the difference between the csv dates and 01/01/1950 in order
    # to convert them to the serial date format of the netcdf
    time_offset_1950 = datetime(1950,1,1,0,0,0)
    
    csv_delta= []
    
    for i in range(len(dc)):
    
        csv_delta.append(csv_dates[i] - time_offset_1950)
        
    
    # Convert the datetimes from the csv into an array of serial date numbers
    csv_serials = []
    
    for i in range(len(dc)):
    
        csv_serials.append(csv_delta[i].days + csv_delta[i].seconds/86400)
        
    csv_serials = np.array(csv_serials)
    
    # Find the indices of timestamps of the csv file that are in the deployment
    # period of the netcdf file
    matching_index = (netcdf_serials[0] <= csv_serials) &  (csv_serials <= netcdf_serials[-1])
    
    new_vars = ['XCO2_PRES','XCO2_OCEAN','XCO2_AIR','XCO2_PSAL','XCO2_SSTEMP']
    
    # For each of the variables in the csv file (except time), linearly 
    # interpolate to the timestamps of the netcdf file    
    for i in range(0,len(new_vars)):
    
        np.interp(netcdf_serials,csv_serials[matching_index],np.array(dcsv[dcsv.columns[i+1]])[matching_index].astype('float64'))
        
        ncVarOut = ds.createVariable(new_vars[i], "f4", ("TIME",), fill_value=np.nan, zlib=True)
        
        
        
    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added ")

    ds.close()
        