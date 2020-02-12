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

def pressure_interpolator:
    
    # Load the filenames of the FV00 files in the current folder
    fv00_files = glob.glob('*FV00*.nc');

    # Extract the aggregate file data
    agg = Dataset(glob.glob('*Aggregate*.nc')[0], mode="r")
    
    # Loop through each of the FV00 files
    for i in fv00_files:
        
        # Extract the contents of the current file
        fv00_contents = Dataset(i, mode="r")
        
        # Check the current file doesn't contain pressure to run the following
        # interpolator
        if not 'PRES' in fv00_contents.variables:
            
            # Create a NaN array to fill with pressure values
            interp_agg_pres = np.full((len(agg.variables["NOMINAL_DEPTH"])+1,len(fv00_contents.variables["TIME"])),np.nan)
            
            # Set the first row as zeros to set 0m as 0dbar
            interp_agg_pres[0,:] = 0
            
            # Create a new array representing the nominal depths of the agg file,
            # including the 0m values
            agg_nominal_depths = np.insert(np.array(agg.variables["NOMINAL_DEPTH"][:]),0,0)
            
            # For each nominal depth, interpolate the agg data at the FV00 times
            for j in range(1,len(agg_nominal_depths)):
                
                time_selection = agg.variables["TIME"][agg.variables["instrument_index"][:]==(j-1)]
                
                pres_selection = agg.variables["PRES"][agg.variables["instrument_index"][:]==(j-1)]
                
                interp_agg_pres[j,:] = np.interp(fv00_contents.variables["TIME"][:],time_selection,pres_selection)
                                           
            # Sort the nominal depths and pressures according to nominal depth
            interp_agg_pres = interp_agg_pres[np.argsort(agg_nominal_depths),:]
        
            agg_nominal_depths.sort()
            
            # If there are any NaN values, linearly interpolate profilewise
            if np.isnan(np.sum(interp_agg_pres)):
                
                # Make a dataframe of the interpolated pressure to handle NaNs easily
                interp_agg_pres_df = pd.DataFrame(data=interp_agg_pres,index=agg_nominal_depths)
                
                # Find all the columns where the lowest element is NaN
                nan_cols = interp_agg_pres_df[interp_agg_pres_df[-1:].isna()].tolist()
                
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
            
            # Create a NaN array to receive the FV00 interpolated pressures
            interp_fv00_pres = np.full((np.shape(fv00_contents.variables["TIME"][:])),np.nan)
            
            # At each timestamp, interpolate pressure for the FV00 data
            for j in range(len(fv00_contents.variables["TIME"])):
            
                interp_fv00_pres[j] = np.interp(fv00_contents.variables["NOMINAL_DEPTH"][0],agg_nominal_depths,interp_agg_pres[:,j])
        
        # Use methods from add_qc_flags to make a new netcdf?
        
        # Deal with files that already contain pressure, but may contain NaNs            
        else:
             # Create a NaN array to fill with pressure values
            interp_agg_pres = np.full((len(agg.variables["NOMINAL_DEPTH"])+1,len(fv00_contents.variables["TIME"])),np.nan)
            
            # Set the first row as zeros to set 0m as 0dbar
            interp_agg_pres[0,:] = 0
            
            # Set the last row to 5000 to set 5000m as 5000dbar (~seafloor), 
            # only for interpolation in cases where the deepest sensor has failed
            #interp_agg_pres[-1,:] = 5000
            
            # Create a new array representing the nominal depths of the agg file,
            # including the 0m values
            agg_nominal_depths = np.insert(np.array(agg.variables["NOMINAL_DEPTH"][:]),0,0)
            
            # For each nominal depth, interpolate the agg data at the FV00 times
            for j in range(1,len(agg_nominal_depths)):
                
                time_selection = agg.variables["TIME"][agg.variables["instrument_index"][:]==(j-1)]
                
                pres_selection = agg.variables["PRES"][agg.variables["instrument_index"][:]==(j-1)]
                
                interp_agg_pres[j,:] = np.interp(fv00_contents.variables["TIME"][:],time_selection,pres_selection)
                                           
            # Sort the nominal depths and pressures according to nominal depth
            interp_agg_pres = interp_agg_pres[np.argsort(agg_nominal_depths),:]
        
            agg_nominal_depths.sort()
            
            # If there are any NaN values, linearly interpolate profilewise
            if np.isnan(np.sum(interp_agg_pres)):
                
                # Make a dataframe of the interpolated pressure to handle NaNs easily
                interp_agg_pres_df = pd.DataFrame(data=interp_agg_pres,index=agg_nominal_depths)
                
                # Find all the columns where the lowest element is NaN
                nan_cols = interp_agg_pres_df[interp_agg_pres_df[-1:].isna()].tolist()
                
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
            
            # Create a NaN array to receive the FV00 interpolated pressures
            interp_fv00_pres = np.full((np.shape(fv00_contents.variables["TIME"][:])),np.nan)
            
            # Extract the interpolated pressures (NaNs removed) to store in netCDF4
            interp_fv00_pres = interp_agg_pres_df[interp_agg_pres_df.index==fv00_contents.variables["NOMINAL_DEPTH"][:]]
            
        #
                    
                
                
                
                
                
                
