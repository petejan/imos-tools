#!/usr/bin/python3

# add_qc_flags
# Copyright (C) 2020 Peter Jansen
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
from datetime import datetime
import numpy as np
from dateutil import parser
import pytz
import os
import shutil

# add QC variables to file


def add_qc(netCDFfile):

    new_name = [] # list of new file names

    # loop over all file names given
    for fn in netCDFfile:

        # TODO: clean this up.
        # rename the file FV00 to FV01 (imos specific)
        fn_new = fn.replace("FV00", "FV01")
        
        # Change the creation date in the filename to today
        now = datetime.utcnow()

        # IMOS_ABOS-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
        fn_new = "".join((fn_new[0:-11], now.strftime("%Y%m%d"), fn_new[-3::]))
        
        # Add the new file name to the list of new file names
        new_name.append(fn_new)

        # If a new (different) filename has been successfully generated, make 
        # a copy of the old file with the new name
        if fn_new != fn:
            # copy file
            shutil.copy(fn, fn_new)
              

        print(fn_new)

        ds = Dataset(fn_new, 'a')

        # read the variable names from the netCDF dataset
        vars = ds.variables

        # create a list of variables, don't include the 'TIME' variable
        # TODO: detect 'TIME' variable using the standard name 'time'
        to_add = []
        for v in vars:
            #print (vars[v].dimensions)
            if v != 'TIME':
                to_add.append(v)

        # for each variable, add a new ancillary variable <VAR>_quality_control to each which has 'TIME' as a dimension
        for v in to_add:
            if "TIME" in vars[v].dimensions:
                # print("time dim ", v)

                if v+"_quality_control" not in ds.variables:
                    ncVarOut = ds.createVariable(v+"_quality_control", "i1", vars[v].dimensions, fill_value=99, zlib=True)  # fill_value=99 otherwise defaults to max, imos-toolbox uses 99
                    ncVarOut[:] = np.zeros(vars[v].shape)
                    ncVarOut.long_name = "quality_code for " + v
                    ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9])
                    ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
                    

                    vars[v].ancillary_variables = v + "_quality_control"

        # update the global attributes
        ds.file_version = "Level 1 - Quality Controlled Data"
        
        ds.date_created = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        ds.history += ' ' + now.strftime("%Y-%m-%d : ") + ' quality_control variables added.'

        # ADD quality control attributes!!

        ds.close()


    return new_name


if __name__ == "__main__":
    add_qc(sys.argv[1:])
    
    
    