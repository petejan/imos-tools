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
from netCDF4 import Dataset
import numpy as np
import argparse
import glob
import pytz
import os
import matplotlib.pyplot as plt
from matplotlib import colors
from matplotlib.ticker import PercentFormatter
import glob
from netCDF4 import num2date
from dateutil import parser
import datetime

for root, dirs, files in os.walk("/Users/tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"):
    
    for fname in files:
      
        if fname.endswith('.nc') and 'FV01' in fname:
        
            print(fname)  #Here, the wanted file name is printed

            ds = Dataset(os.path.join(root,fname), 'a')
        
            vars = ds.variables
        
            to_add = []
            for v in vars:
                #print (vars[v].dimensions)
                if v != 'TIME':
                    to_add.append(v)
        
            time_var = vars["TIME"]
            time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)
        
            time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
            time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)
        
            print(time_deploy)
        
            print(to_add)
            for v in to_add:
                if "TIME" in vars[v].dimensions:
        
                    if v.endswith("_quality_control"):
        
                        print("QC time dim ", v)
        
                        ncVarOut = vars[v]
                        mask = (time <= time_deploy) | (time >= time_recovery)
                        ncVarOut[mask] = np.ones(vars[v].shape)[mask] * 7
        
        
            ds.file_version = "Level 1 - Quality Controlled Data"
            
            # update the history attribute
            try:
                hist = ds.history + "\n"
                
            except AttributeError:
                hist = ""
            
            ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + ': in water test performed, with out of water data flagged at QC=7')        
                    

            ds.close()