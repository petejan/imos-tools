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
        
    # convert time into a datetime object, this is optional, and not needed to continue in the IMOS format
    #df['TIME']=pd.to_timedelta(df['TIME'],unit='D')+dt(1950,1,1)
    
    # index the dataframe by time        
    df = df.set_index('TIME')
    
    # extract the column names
    col_names = list(df.columns)
            
    # append the nominal depth to all column names
    df.columns = [x.replace('quality_control',str(nc.variables['NOMINAL_DEPTH'][0])+'_quality_control') if 'quality_control' in x else x + '_' + str(nc.variables['NOMINAL_DEPTH'][0]) for x in col_names]        
           
        
        
        
        
        
        
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    

        
    
    

