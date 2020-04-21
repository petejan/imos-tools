#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 11:05:16 2020

@author: tru050
"""

import re
from datetime import datetime, timedelta
from netCDF4 import num2date, date2num
from netCDF4 import stringtochar
import numpy.ma as ma
import numpy as np
import sys
from netCDF4 import Dataset
import numpy
import argparse
import glob
import pandas as pd
import scipy
import os
import shutil

# Supply netCDFfiles as a ['list'] of files, agg as a 'string'

def pressure_interpolator(netCDFfiles = [],agg = []):
    
    files_out = []
    
    if netCDFfiles==[]:
        
        print('netcdffiles = none')
    
        # Load the filenames of the fv01 files in the current folder
        netCDFfiles = glob.glob('*FV01*.nc')
            
    if agg == []:
        
        print('agg = none')
        
    # Extract the aggregate file data
        agg = Dataset(glob.glob('*Aggregate*.nc')[0], mode="r")
        
    else:
        
        agg = Dataset(glob.glob(agg)[0], mode="r")
        
    # Loop through each of the fv01 files
    for fn in netCDFfiles:
        
        print('File selected is '+fn)
        
        # Change the creation date in the filename to today
        now=datetime.utcnow()
            
        fn_new_split = fn.split('_')
            
        fn_new_split[-1] = "C-" + now.strftime("%Y%m%d") + ".nc"
        
        fn_new_split[2] += 'IP'
            
        fn_new = '_'.join(fn_new_split)

        
        # If a new (different) filename has been successfully generated, make 
        # a copy of the old file with the new name
        if fn_new != fn:
            
            files_out.append(fn_new)
            
            print('copying file')
            # copy file
            shutil.copy(fn, fn_new)
        
        # Open and work in the new copy
        fv01_contents = Dataset(fn_new,mode='a')
        
        print('copied file opened')
        
        # Check the current file doesn't contain pressure to run the following
        # interpolator
        if not 'PRES' in fv01_contents.variables:
            
            print("file doesn't contain pressure")
            
            print(fv01_contents.variables.keys())
            print(agg.variables.keys())
            
            # Create a NaN array to fill with pressure values
            interp_agg_pres = np.full((len(agg.variables["NOMINAL_DEPTH"])+1,len(fv01_contents.variables["TIME"])),np.nan)
            
            # Set the first row as zeros to set 0m as 0dbar
            interp_agg_pres[0,:] = 0
            
            # Create a new array representing the nominal depths of the agg file,
            # including the 0m values
            agg_nominal_depths = np.insert(np.array(agg.variables["NOMINAL_DEPTH"][:]),0,0)
            
            # For each nominal depth, interpolate the agg data at the fv01 times
            for j in range(1,len(agg_nominal_depths)):
                
                time_selection = agg.variables["TIME"][agg.variables["instrument_index"][:]==(j-1)]
                
                pres_selection = agg.variables["PRES"][agg.variables["instrument_index"][:]==(j-1)]
                
                interp_agg_pres[j,:] = np.interp(fv01_contents.variables["TIME"][:],time_selection,pres_selection)
                                           
            # Sort the nominal depths and pressures according to nominal depth
            interp_agg_pres = interp_agg_pres[np.argsort(agg_nominal_depths),:]
        
            agg_nominal_depths.sort()
            
            # If there are any NaN values, linearly interpolate profilewise
            if np.isnan(np.sum(interp_agg_pres)):
                
                # Make a dataframe of the interpolated pressure to handle NaNs easily
                interp_agg_pres_df = pd.DataFrame(data=interp_agg_pres,index=agg_nominal_depths)
                
                # Find all the columns where the lowest element is NaN
                nan_cols = np.where(interp_agg_pres_df.iloc[-1].isna())
                
                # Select each column containing an NaN as the deepest value
                for j in nan_cols:
                    
                    # Find the shallowest nominal depth that isn't NaN
                    shallowest_val = pd.Series.last_valid_index(interp_agg_pres_df.iloc[:,j])
                    
                    # Find the index of that nominal depth
                    shallowest_idx = interp_agg_pres_df.index.tolist().index(shallowest_val)
                    
                    # Starting at the shallowest NaN in a continous block of NaNs to the bottom
                    for k in range(shallowest_idx+1,len(interp_agg_pres_df)):
                        
                        # Linearly interpolate from shallow to deep, based on a nominal depth difference of 1m equating to 1dbar
                        interp_agg_pres_df.iloc[k,j] = interp_agg_pres_df.iloc[k-1,j]+np.diff(interp_agg_pres_df.index)[k-1]
                        
                # Linearly interpolate any remaining NaNs
                interp_agg_pres_df = interp_agg_pres_df.interpolate(method="index")
                
                # Convert the DataFrame back to an array
                interp_agg_pres =  interp_agg_pres_df.to_numpy()
            
            # Create a NaN array to receive the fv01 interpolated pressures
            interp_fv01_pres = np.full((np.shape(fv01_contents.variables["TIME"][:])),np.nan)
            
            # At each timestamp, interpolate pressure for the fv01 data
            for j in range(len(fv01_contents.variables["TIME"])):
            
                interp_fv01_pres[j] = np.interp(fv01_contents.variables["NOMINAL_DEPTH"][0],agg_nominal_depths,interp_agg_pres[:,j])
        
            # Create the PRES and PRES_quality_control variables, and their attributes
                
            pres_var = fv01_contents.createVariable('PRES','f8',fv01_contents.variables['TIME'].dimensions,fill_value=99, zlib=True)
            
            pres_atts = ['long_name','sea_water_pressure_due_to_sea_water','units','dbar',
            'standard_name','coordinates','TIME LATITUDE LONGITUDE NOMINAL_DEPTH','sea_water_pressure_due_to_sea_water','valid_max',
            12000,'valid_min',-15] 
                
            for att_name,att_value in zip(pres_atts[0::2],pres_atts[1::2]):
                    
                pres_var.setncattr(att_name,att_value)
                        
            pres_var[:] = interp_fv01_pres
            
            
            pres_qc_var = fv01_contents.createVariable('PRES_quality_control','i1',fv01_contents.variables['TIME'].dimensions,fill_value=99, zlib=True)
            
            pres_qc_var.long_name = "quality_code for PRES"
            
            pres_qc_var.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9])
            
            pres_qc_var.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
                    
            pres_qc_var[:] = 7
            
            pres_var.ancillary_variables = "PRES_quality_control"
            
            # Close the netcdf files
            
            fv01_contents.close()
        
        # Deal with files that already contain pressure, but contain NaNs            
        elif any(np.isnan(np.array(fv01_contents.variables['PRES'][:]))):
            
            print("file contains pressure and agg contains NaNs")
            
            # Create a NaN array to fill with pressure values
            interp_agg_pres = np.full((len(agg.variables["NOMINAL_DEPTH"])+1,len(fv01_contents.variables["TIME"])),np.nan)
            
            # Set the first row as zeros to set 0m as 0dbar
            interp_agg_pres[0,:] = 0
            
            # Create a new array representing the nominal depths of the agg file,
            # including the 0m values
            agg_nominal_depths = np.insert(np.array(agg.variables["NOMINAL_DEPTH"][:]),0,0)
            
            # For each nominal depth, interpolate the agg data at the fv01 times
            for j in range(1,len(agg_nominal_depths)):
                
                time_selection = agg.variables["TIME"][agg.variables["instrument_index"][:]==(j-1)]
                
                pres_selection = agg.variables["PRES"][agg.variables["instrument_index"][:]==(j-1)]
                
                interp_agg_pres[j,:] = np.interp(fv01_contents.variables["TIME"][:],time_selection,pres_selection)
                                           
            # Sort the nominal depths and pressures according to nominal depth
            interp_agg_pres = interp_agg_pres[np.argsort(agg_nominal_depths),:]
        
            agg_nominal_depths.sort()
                
            # Make a dataframe of the interpolated pressure to handle NaNs easily
            interp_agg_pres_df = pd.DataFrame(data=interp_agg_pres,index=agg_nominal_depths)
            
            # Find all the columns where the lowest element is NaN
            nan_cols = np.where(interp_agg_pres_df.iloc[-1].isna())
            
            # Select each column containing an NaN as the deepest value
            for j in nan_cols:
                
                # Find the shallowest nominal depth that isn't NaN
                shallowest_val = pd.Series.last_valid_index(interp_agg_pres_df.iloc[:,j])
                
                # Find the index of that nominal depth
                shallowest_idx = interp_agg_pres_df.index.tolist().index(shallowest_val)
                
                # Starting at the shallowest NaN in a continous block of NaNs to the bottom
                for k in range(shallowest_idx+1,len(interp_agg_pres_df)):
                    
                    # Linearly interpolate from shallow to deep, based on a nominal depth difference of 1m equating to 1dbar
                    interp_agg_pres_df.iloc[k,j] = interp_agg_pres_df.iloc[k-1,j]+np.diff(interp_agg_pres_df.index)[k-1]
                    
            # Linearly interpolate any remaining NaNs
            interp_agg_pres_df = interp_agg_pres_df.interpolate(method="index")
            
            # Convert the DataFrame back to an array
            interp_agg_pres =  interp_agg_pres_df.to_numpy()
        
            # Create a NaN array to receive the fv01 interpolated pressures
            interp_fv01_pres = np.full((np.shape(fv01_contents.variables["TIME"][:])),np.nan)
        
            # Extract the interpolated pressures (NaNs removed) to store in netCDF4
            interp_fv01_pres = interp_agg_pres_df[interp_agg_pres_df.index==fv01_contents.variables["NOMINAL_DEPTH"][:]]
        
            # Find indices where the netcdf data and interpolated data don't match (where the NaNs are in the netcdf)
            nan_rep_idx = np.where(interp_fv01_pres!=fv01_contents.variables['PRES'][:])[1]
            
            #
            fv01_contents.variables['PRES_quality_control'][nan_rep_idx] = 7
        
            print('QC altered in original press')
        
            # Insert pressure value with NaNs interpolated back into netcdf
            fv01_contents.variables['PRES'][:] = interp_fv01_pres
            
            print('press altered in orginal press')
            
            fv01_contents.close()
            
    agg.close()
    
    return files_out        
                
                
                
                
                
