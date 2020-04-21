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

# Initial package import
import sys
import glob
import fnmatch
import os

# Addition of folder containing user defined packages/modules
sys.path.append('/Users/tru050/Documents/GitHub/imos-tools/ocean_dp/qc')
sys.path.append('/Users/tru050/Documents/GitHub/imos-tools/ocean_dp/aggregation')
sys.path.append('/Users/tru050/Documents/GitHub/imos-tools/ocean_dp/processing')

# Import user defined packages
import add_qc_flags
import in_out_water
import copyDataset
import pressure_interpolator

import global_range
import rate_of_change_test

# Set the working directory
os.chdir('/Users/tru050/Desktop/sofs7.5 test data')

# Make a list of FV00 filenames
fv00_files = glob.glob('*IMOS_ABOS-SOTS*FV00*.nc')

# Run add_qc_flags.py and collect FV01 filenames
fv01_files = add_qc_flags.add_qc(fv00_files)

# Run in_out_water.py
for ncfile in fv01_files:
    
    in_out_water.in_out_water(ncfile,var_name='TEMP')

# Select pressure files using matching = fnmatch.filter(sofs75filesfv01,'*SOTS*P*_2*.nc')
pres_files = fnmatch.filter(fv01_files,'*IMOS_ABOS-SOTS*P*_2*FV01*.nc')

# Run copyDataset.py
copyDataset.aggregate(pres_files,'PRES')

# Run pressure_interpolator.py
fv01_pres_interp_files = pressure_interpolator.pressure_interpolator(netCDFfiles=fv01_files,agg=glob.glob('*IMOS_ABOS-SOTS*Aggregate*.nc')[0])

# Global range test
for ncfile in fv01_pres_interp_files:
    
    print(ncfile)
    
    global_range.global_range(ncfile,'TEMP',40,-2)

# Rate of change
rate_of_change_test.roc_test_files(fv01_pres_interp_files,'TEMP',20)

# Spike


# Flatline


