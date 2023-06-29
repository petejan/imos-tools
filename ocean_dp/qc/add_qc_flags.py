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


def add_qc(netCDF_files_in, var_name=None):

    new_name = []  # list of new file names

    # Change the creation date in the filename to today
    now = datetime.utcnow()

    # loop over all file names given
    for fn in netCDF_files_in:

        fn_new = fn
        dirname = os.path.dirname(fn_new)
        basename = os.path.basename(fn_new)
        if basename.startswith("IMOS"):
            fn_split = basename.split('_')

            # IMOS_DWM-SOTS_CPT_20090922_SOFS_FV01_Pulse-6-2009-SBE37SM-RS232-6962-100m_END-20100323_C-20200227.nc
            # 0    1         2   3        4    5    6                                    7            8
            # rename the file FV00 to FV01
            fn_split[5] = 'FV01'

            if len(fn_split) > 8:
                fn_split[8] = now.strftime("C-%Y%m%d.nc")
            fn_new = os.path.join(dirname, "_".join(fn_split))

        # Add the new file name to the list of new file names
        new_name.append(fn_new)

        # If a new (different) filename has been successfully generated, make 
        # a copy of the old file with the new name
        if fn_new != fn:
            print('qc from : ', fn)
            print('qc to   : ', fn_new)
            # copy file
            shutil.copyfile(fn, fn_new)

        print("output", fn_new)

        ds = Dataset(fn_new, 'a')

        # read the variable names from the netCDF dataset
        nc_vars = ds.variables

        if var_name:
            to_add = [var_name]
        else:
            # create a list of variables, don't include the 'TIME' variable
            # TODO: detect 'TIME' variable using the standard name 'time'
            to_add = []
            for v in nc_vars:
                #print (vars[v].dimensions)
                if v != 'TIME':
                    to_add.append(v)

            print('addinto to', to_add)
            # remove any anx variables from the list
            for v in nc_vars:
                if 'ancillary_variables' in nc_vars[v].ncattrs():
                    remove = nc_vars[v].getncattr('ancillary_variables').split(' ')
                    print("remove ", remove)
                    for r in remove:
                        to_add.remove(r)

        # for each variable, add a new ancillary variable <VAR>_quality_control to each which has 'TIME' as a dimension
        for v in to_add:
            if v in nc_vars:
                if "TIME" in nc_vars[v].dimensions:
                    print("time dim ", v)

                    qc_var_name = v+"_quality_control"
                    # add if the quality_control variable does not exist
                    if qc_var_name in ds.variables:
                        ncVarOut = ds.variables[qc_var_name]
                    else:
                        print("adding : ", qc_var_name)
                        ncVarOut = ds.createVariable(qc_var_name, "i1", nc_vars[v].dimensions, fill_value=99, zlib=True)  # fill_value=99 otherwise defaults to max, imos-toolbox uses 99
                        ncVarOut[:] = np.zeros(nc_vars[v].shape)
                        nc_vars[v].ancillary_variables = qc_var_name

                    if 'long_name' in nc_vars[v].ncattrs():
                        ncVarOut.long_name = "quality flag for " + nc_vars[v].long_name
                    if 'standard_name' in nc_vars[v].ncattrs():
                        ncVarOut.standard_name = nc_vars[v].standard_name + " status_flag"

                    ncVarOut.quality_control_conventions = "IMOS standard flags"
                    ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
                    ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
                    ncVarOut.comment = 'maximum of all flags'

                    #else:
                    #    ncVarOut = ds.variables[v+"_quality_control"]
                    #    ncVarOut[:] = np.zeros(nc_vars[v].shape)

        # update the global attributes
        ds.file_version = "Level 1 - Quality Controlled Data"
        ds.file_version_quality_control = "Quality controlled data have been through quality assurance procedures such as automated routines and sensor calibration and/or a level of visual inspection and flag of obvious errors. The data are in physical units using standard SI metric units with calibration and other pre- processing routines applied, all time and location values are in absolute coordinates to comply with standards and datum. Data includes flags for each measurement to indicate the estimated quality of the measurement. Metadata exists for the data or for the higher level dataset that the data belongs to. This is the standard IMOS data level and is what should be made available to AODN and to the IMOS community."
        
        ds.date_created = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        ds.history += '\n' + now.strftime("%Y-%m-%d") + ' quality_control variables added.'

        # ADD quality control attributes!!

        ds.close()

    return new_name


if __name__ == "__main__":
    if (len(sys.argv) > 2) & sys.argv[1].startswith('-'):
        add_qc(sys.argv[2:], var_name=sys.argv[1][1:])
    else:
        add_qc(sys.argv[1:])
