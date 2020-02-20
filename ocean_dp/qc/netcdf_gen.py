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

from netCDF4 import Dataset, date2num
import sys
from datetime import datetime
import numpy as np
import shutil

 # Provide the function with a filename (don't include .nc), a nominal depth,
# and pairs of names and arrays containing the data to be included as variables.
# A time dimension/variable is created by default, starting at 01/01/2020 using
# 1 hour timestamps

 # For example, netcdf_gen('test',30,'PRES',pres_data,'TEMP',temp_data)
# from the command line gen_test_data.py test 30 PRES 10,20,30 TEMP 11,12,NaN



def netcdf_gen(file_name, nominal_depth, *args):
    # Convert the args tuple to a list
    args = list(args)
    #print(args, type(args[1]))

    file_name = "IMOS_" + file_name + ".nc" # if we insist on not wanting to pass these

     # deal with passing nominal depth as a string
    if isinstance(nominal_depth, str):
        nominal_depth = float(nominal_depth)
        print('nominal depth :', nominal_depth)

     # Check the args are paired
    if len(args) % 2 == 0:

         # Assign the names and data to lists
        var_names = args[0::2]

         # deal with passing data as a string list of values
        if isinstance(args[1], str):
            var_data = [[float(b) for b in a.split(',')] for a in args[1::2]]
            #print('var_data split', var_data)
        else:
            var_data = args[1::2]

         # Check if first of each pair is a string
        if all(isinstance(x, str) for x in var_names):

             # Check if second of each pair are all equal in shape
            if all(np.shape(var_data[0]) == np.shape(x) for x in var_data):

                 # Create the netcdf with IMOS tag
                ds = Dataset(file_name, "w", format="NETCDF4")

                 # Create time dimension with length to match data
                time_dim = ds.createDimension("TIME", len(var_data[0]))

                time_var = ds.createVariable("TIME", "f8", ("TIME"))

                time_var.setncattr('long_name', 'time')
                time_var.setncattr('standard_name', 'time')
                time_var.setncattr('units', 'days since 1950-01-01 00:00:00 UTC')
                time_var.setncattr('calendar', 'gregorian')
                time_var.setncattr('axis', 'T')
                time_var.setncattr('valid_max', 90000)
                time_var.setncattr('valid_min', 0)

                t0 = date2num(datetime(2020, 1, 1), units=time_var.units)
                ds.variables['TIME'][:] = np.linspace(t0,t0 + (1 / 24) * (len(var_data[0]-1)),num=len(var_data[0]))

                 # Create the nominal depth variable
                nom_depth_var = ds.createVariable("NOMINAL_DEPTH", "f8")
                nom_depth_var.setncattr('long_name', 'nominal depth')
                nom_depth_var.setncattr('units', 'dbar')
                nom_depth_var.setncattr('positive', 'down')
                nom_depth_var.setncattr('axis', 'Z')
                nom_depth_var.setncattr('valid_max', 12000)
                nom_depth_var.setncattr('valid_min', -5)
                nom_depth_var.setncattr('reference_datum', 'sea surface')

                ds.variables["NOMINAL_DEPTH"][:] = nominal_depth

                 # Create variables from input data
                for name_in, data_in in zip(var_names, var_data):
                    ds.createVariable(name_in, "f8", ("TIME"))
                    ds.variables[name_in][:] = data_in

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
                
                ds.history = datetime.utcnow().strftime("%Y%m%d:") + ' converted to FV01 file, quality_control variables added.'
        
                # ADD quality control attributes!!
        
                ds.close()
                
                #add_qc(file_name)
                
                print("generated ", file_name)

                return (file_name)
            
                

            else:
                print('Data arrays not of equal length')


        else:
            print('Labels not in string format')

    else:
        print('Data not passed in pairs')


if __name__ == "__main__":
    netcdf_gen(sys.argv[1], sys.argv[2], *sys.argv[3:])
    
    
    
    
    
    
    