#!/usr/bin/python3

# raw2netCDF
# Copyright (C) 2019 Peter Jansen
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

import sys
from netCDF4 import Dataset

# create a table (csv) that has the columns,
#  global flag (GLOBAL, VAR, or VAR_ATT)
#  variable (blank for global)
#  instrument model,
#  instrument serial number,
#  deployment (time_deployment and time_recovery)
#  variable name
#  variable type
#  variable value
#  attribute name
#  attribute type
#  attribute value

def print_line(typ, var_name, dep_code, model, serial_number, time_deployment, time_recovry, variable_name, variable_dims, variable_shape, variable_type, variable_value, attribute_name, attribute_type, attribute_value):
    if attribute_type == 'str':
        attribute_value = attribute_value.replace("\"", "'")
    print("%s,%s,%s,%s,%s,%s,%s,%s,\"%s\",\"%s\",%s,%s,%s,%s,\"%s\"" % (typ, var_name, dep_code, model, serial_number, time_deployment, time_recovry, variable_name, variable_dims, variable_shape, variable_type, variable_value, attribute_name, attribute_type, attribute_value))

for s in sys.argv[1:]:
    #print(s)

    print("rec_type, var_name, deployment_code, model, serial_number, time_deployment, time_recovery, variable_name, variable_dims, variable_shape, variable_type, variable_value, attribute_name, attribute_type, attribute_value")

    nc = Dataset(s)

    try:
        time_start = nc.getncattr('time_deployment_start')
    except AttributeError:
        time_start = nc.getncattr('time_coverage_start')

    try:
        time_end = nc.getncattr('time_deployment_end')
    except AttributeError:
        time_end = nc.getncattr('time_coverage_end')

    instrument = nc.getncattr('instrument')
    instrument_serial_number = nc.getncattr('instrument_serial_number')
    dep_code = nc.getncattr('deployment_code')

    nc_attrs = nc.ncattrs()

    for a in nc_attrs:
        attr = nc.getncattr(a)
        #print("%s type %s = %s" % (a, type(attr).__name__, attr))
        print_line('GLOBAL', "*", dep_code, instrument, instrument_serial_number, time_start, time_end, "", "", "", "", "", a, type(attr).__name__, attr)

    nc_vars = nc.variables

    for v in nc_vars:
        #print("var %s" % (v))
        ncVar = nc.variables[v]
        v_attrs = ncVar.ncattrs()
        # print(len(ncVar.shape))
        if len(ncVar.shape) == 0:
            print_line('VAR', v, dep_code, instrument, instrument_serial_number, time_start, time_end, v, ncVar.shape, ncVar.dimensions, ncVar.dtype, ncVar[:], "", "", "")
        elif (len(ncVar.shape) == 1) & (ncVar.shape[0] == 1):
            print_line('VAR', v, dep_code, instrument, instrument_serial_number, time_start, time_end, v, ncVar.shape, ncVar.dimensions, ncVar.dtype, ncVar[:], "", "", "")
        else:
            print_line('VAR', v, dep_code, instrument, instrument_serial_number, time_start, time_end, v, ncVar.shape, ncVar.dimensions, ncVar.dtype, "", "", "", "")

        for a in v_attrs:
            attr = ncVar.getncattr(a)
            print_line('VAR_ATT', v, dep_code, instrument, instrument_serial_number, time_start, time_end, "", "", "", "", "", a, type(attr).__name__, attr)
            #print("%s type %s = %s" % (a, type(attr).__name__, attr))

