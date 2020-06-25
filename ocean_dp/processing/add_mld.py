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

from netCDF4 import Dataset, num2date
import sys
from datetime import datetime as dt
import numpy as np
import pandas as pd
from scipy import interpolate

def add_mld(nc_in,thresh_in=0.2):
    
    # opens the supplied IMOS netcdf
    nc = Dataset(nc_in,'a')
    
    temp_na = np.array(nc.variables['TEMP'])

    # create two nan filled arrays the length of the FV02 file, one for the mld and one for its uncertainty
    nc_mld = np.full([1,temp_na.shape[1]], np.nan)[0]
    
    nc_mld_uncert = np.full([1,temp_na.shape[1]], np.nan)[0]
    
    # temp sensor depths
    nc_temp_depths = np.array(nc.variables['DEPTH_TEMP'])
    
    temp_na = temp_na[nc_temp_depths>5,:]
    
    nc_temp_depths = nc_temp_depths[nc_temp_depths>5]

    # boolean of sensors at the shallowest depth
    shallowest_sensors = nc_temp_depths == np.min(nc_temp_depths)

    # for each temperature profile where there is at least one non NaN value in the shallowest sensors
    for i in np.where(~np.all(np.isnan(temp_na[shallowest_sensors]),axis=0))[0]:
        
        # check there is at least one non NaN value in the deeper sensors
        if np.any(~np.isnan(temp_na[~shallowest_sensors,i])):
        
            # calculates the mean temperature of the available shallowest sensors to use as a reference to calculate MLD
            shallow_temp = np.nanmean(temp_na[shallowest_sensors,i])
            
            # extract temperature and depth data using a mean for the shallowest depth, and all non NaN data below
            profile_temps = np.append(shallow_temp,temp_na[~shallowest_sensors,i][~np.isnan(temp_na[~shallowest_sensors,i])])
                
            profile_depths = np.append(nc_temp_depths[0],nc_temp_depths[~shallowest_sensors][~np.isnan(temp_na[~shallowest_sensors,i])])
                
            # check if the current profile contains any temperatures outside the specified threshold values
            if np.any(temp_na[~shallowest_sensors,i]>=shallow_temp+thresh_in) or np.any(temp_na[~shallowest_sensors,i]<=shallow_temp-thresh_in):
                
                # generate a linear interpolator for the profile, which returns nan if extrapolation is attempted
                profile_interp_func = interpolate.interp1d(profile_temps,profile_depths,bounds_error=False,fill_value=np.nan)
            
                # finds the shallowest depth at which the linear interpolation of the profile meets a threshold limit
                nc_mld[i] = np.nanmin(profile_interp_func([shallow_temp+thresh_in,shallow_temp-thresh_in]))
                
                # provides an estimate of uncertainty, by giving the distance to the furthest sensor used to interpolate the MLD
                nc_mld_uncert[i] = np.max([np.abs(nc_mld[i]-[x for x in profile_depths if x < nc_mld[i]][-1]),np.abs(nc_mld[i]-next(x for x in profile_depths if x > nc_mld[i]))])
            
            
            # if none of the sensors are outside the threshold
            else:
                
                # set the mld to the depth of the deepest non NaN sensor
                nc_mld[i] = np.max(profile_depths)

                # set the uncertainty to the distance between the sensor and the bottom
                nc_mld_uncert[i] = 4600 - nc_mld[i]

    # create the two variables 
    mld_var_out = nc.createVariable('MLDx', "f4", ("TIME",), fill_value=np.nan, zlib=True)
    mld_var_out[:] = nc_mld
    mld_var_out.units = 'm'
    mld_var_out.comment = 'Calculated using the linear interpolation MLD algorithm found at: INSERT GITHUB ADDRESS'
    
    mld_uncert_var_out = nc.createVariable('MLDx_standard_error', "f4", ("TIME",), fill_value=np.nan, zlib=True)
    mld_uncert_var_out[:] = nc_mld_uncert
    mld_uncert_var_out.units = 'm'
    
    nc.close()
    
    
    
    
    
    
    
    
    
    