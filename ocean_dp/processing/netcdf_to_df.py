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
import pandas as pd
import numpy as np
from datetime import datetime as dt


# =============================================================================
# Returns a list of the time series variables in a IMOS format 
# netcdf file. Takes an open netcdf as its argument.
# =============================================================================
def var_selector_inc_time(nc,qc=False,):
    
    x = [x for x in list(nc.variables) if ('_quality_control_' not in x) & (nc.variables[x].shape!=())]
        
    return x    

# =============================================================================
# 
# =============================================================================
def netcdf_to_df(target_file):
    
    # open the inputted netcdf
    nc = Dataset(target_file,mode='r')
    
    # creates a the list of variables to transfer to the dataframe
    vars_to_transfer = var_selector_inc_time(nc)
    
    # creates the dataframe with column labels
    df = pd.DataFrame(columns = vars_to_transfer)
    
    # sorts the columns alphabetically, with the relevant qc variable following each timeseries variable
    df.sort_index(axis=1, inplace=True)
    
    # fill the dataframe from the netcdf, variable by variable
    for cur_var in vars_to_transfer:
        
        df[cur_var] = np.array(nc.variables[cur_var])    
    
    # store deployment times in attributes
    df.attrs['time_deployment_start'] = nc.time_deployment_start
    df.attrs['time_deployment_end'] = nc.time_deployment_end
    
    # extract the column names
    col_names = list(df.columns)
            
    # append the nominal depth to all column names
    df.columns = [x.replace('quality_control',str(nc.variables['NOMINAL_DEPTH'][0])+'_quality_control') if 'quality_control' in x else x if 'TIME' in x else x + '_' + str(nc.variables['NOMINAL_DEPTH'][0]) for x in col_names]        
    
    nc.close()
       
    return df
    
    
# =============================================================================
# Takes 
# =============================================================================
        
def combine_df(target_dfs):
    
    # for each of the dataframes in the list provided
    for cur_df in target_dfs:
        
        # make a copy of the current dataframe to modify and combine
        df = cur_df.copy()
        
        # convert the IMOS format times to datetime 
        df['TIME']=pd.to_timedelta(df['TIME'],unit='D')+dt(1950,1,1)
        
        # index the dataframe by time - for some reason this makes the df very slow to visually open and navigate!?  
        df = df.set_index('TIME')
        
        # extract and convert deployment times to datetime 
        start_time = dt.strptime(df.attrs['time_deployment_start'],'%Y-%m-%dT%H:%M:%SZ')
        end_time = dt.strptime(df.attrs['time_deployment_end'],'%Y-%m-%dT%H:%M:%SZ')
        
        # trim the df to only include in water data
        df = df.drop(df[(df.index < start_time) | (df.index > end_time)].index)
        
        # resamples using the max method, to create a df of the correct dimensions to fill
        df_to_fill = df.resample('H',base=0.5).max()
        
        
        # gets list of column names
        col_names = list(df.columns)
        
        # makes a list of non qc column names
        col_names_no_qc = [x for x in col_names if 'quality_control' not in x]
        
        # for each of the time series data columns
        for cur_col in col_names_no_qc:
            
            # sets the value of non qc data to nan if the corresponding qc value is not satisfactory (0,1,2,7 at the moment)
            df.loc[(df[cur_col+'_quality_control'] > 2) & (df[cur_col+'_quality_control'] != 7), cur_col] = np.nan
            
            # extracts the time series data
            dS = pd.Series(df[cur_col])
            
            # makes a copy for bin counting
            dS_1s = dS.copy()
            
            dS_1s[:] = 1
            
            # resamples the series, interpoling linearly
            dS_resampled = dS.resample('H',base=0.5).interpolate()
            
            # count how many data points are in each shoulder bin
            dS_bin_counts = dS_1s.resample('H',base=0.5).sum()
            
            # fill the interpolated data back into the dataframe
            df_to_fill[cur_col] = dS_resampled
            
            # give any interpolated point without any data within its hour window a qc code of 7
            df_to_fill.loc[dS_bin_counts==0,[cur_col+'_quality_control']] = 7
            
        # shift the timestamps to the middle of the hour sampling period
        df_to_fill.index = df_to_fill.index + pd.Timedelta('30 min')
        

        
        
        

            
        
        
        
        
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

        
    
    

