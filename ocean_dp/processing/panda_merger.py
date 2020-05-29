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

import numpy.ma as ma
import sys
from netCDF4 import Dataset, num2date
from dateutil import parser
from datetime import datetime as dt
from datetime import timedelta 
import numpy as np
import argparse
import glob
import pytz
import os
import matplotlib.pyplot as plt
from matplotlib import colors
from matplotlib.ticker import PercentFormatter
from sigfig import round
import pandas as pd

# import two netcdf
nc1 = Dataset('IMOS_ABOS-SOTS_COPST_20180801_SOFS_FV00_SOFS-7.5-2018-SBE37SMP-ODO-RS232-03715971-200m_END-20190324_C-20200401.nc',mode='r')

nc2 = Dataset('IMOS_ABOS-SOTS_T_20180801_SOFS_FV00_SOFS-7.5-2018-Starmon-mini-4048-45m_END-20190331_C-20200401.nc',mode='r')

# convert their time and temp data into dataframes
df1 = pd.DataFrame({'TIME':np.array(nc1.variables['TIME'][:]),'TEMP_200':np.array(nc1.variables['TEMP'][:])})

df2 = pd.DataFrame({'TIME':np.array(nc2.variables['TIME'][:]),'TEMP_45':np.array(nc2.variables['TEMP'][:])})

# convert the times from days since 01-01-1950 to a datetime object
df1['TIME']=pd.to_timedelta(df1['TIME'],unit='D')+dt(1950,1,1)

df2['TIME']=pd.to_timedelta(df2['TIME'],unit='D')+dt(1950,1,1)

# index the dataframes by time
df1=df1.set_index('TIME')

df2=df2.set_index('TIME')


# resample the data, calculating the mean over hourly periods, starting on the half hour
df1=df1.resample('H',base=0.5).mean()

df2=df2.resample('H',base=0.5).mean()

# reset the labels so they read the hour in the centre of the averaging period
df1.index = df1.index + pd.Timedelta('30 min')

df2.index = df2.index + pd.Timedelta('30 min')


# combine the two dataframes based on their time indicies, recording nan if one sensor doesn't have a reading for that timestamp
total_df = pd.concat([df1,df2], join='outer', axis=1)














