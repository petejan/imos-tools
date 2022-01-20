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

from netCDF4 import Dataset, num2date
import sys
from datetime import datetime
import numpy as np
from dateutil import parser
import pytz
import os

# flag data from input variables

data = [{'out': 'PSAL', 'in': ('TEMP', 'PRES', 'CNDC')},
        {'out': 'CNDC', 'in': ('TEMP', 'PRES', 'PSAL')},
        {'out': 'DENSITY', 'in': ('TEMP', 'PRES', 'PSAL')},
        {'out': 'SIGMA_T0', 'in': ('TEMP', 'PRES', 'PSAL')},
        {'out': 'SIGMA_T0_SM', 'in': ('TEMP', 'PRES', 'PSAL')},
        {'out': 'OXSOL', 'in': ('TEMP', 'PRES', 'PSAL')},
        {'out': 'DOX2', 'in': ('TEMP', 'PRES', 'PSAL', 'DOX', 'OXSOL')},
        ]


def propogate(netCDFfile, var_name=None):

    out_file = []

    for fn in netCDFfile:
        ds = Dataset(fn, 'a')
        ds.set_auto_mask(False)

        var_list = []
        for d in data:
            v = d['out']
            if v in ds.variables:
                if not var_name or v == var_name:
                    print('processing variable', v)
                    var_list.append(v)

                    # get the existing flags
                    if v + '_quality_control' in ds.variables:
                        ncVarFinal = ds.variables[v + '_quality_control']
                    else:
                        ncVarFinal = ds.createVariable(v + "_quality_control", "i1", ds.variables[v].dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
                        ncVarFinal.units = "1"
                        ds.variables[v].ancillary_variables = v + "_quality_control"
                        if 'long_name' in ds.variables[v].ncattrs():
                            ncVarFinal.long_name = "quality flag for " + ds.variables[v].long_name
                        if 'standard_name' in ds.variables[v].ncattrs():
                            ncVarFinal.standard_name = ds.variables[v].standard_name + " status_flag"

                        ncVarFinal.quality_control_conventions = "IMOS standard flags"
                        ncVarFinal.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
                        ncVarFinal.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'

                        ncVarFinal[:] = 0

                    # create a qc variable just for this test flags
                    if v + "_quality_control_in" in ds.variables:
                        ncVarOut = ds.variables[v + "_quality_control_in"]
                    else:
                        ncVarOut = ds.createVariable(v + "_quality_control_in", "i1", ds.variables[v].dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max
                        try:
                            ds.variables[v].ancillary_variables = ds.variables[v].ancillary_variables + " " + v + "_quality_control_in"
                        except AttributeError:
                            ds.variables[v].ancillary_variables = v + "_quality_control_in"

                    if 'long_name' in ds.variables[v].ncattrs():
                        ncVarOut.long_name = "input data flag for " + ds.variables[v].long_name

                    ncVarOut.units = "1"
                    ncVarOut.comment = 'data flagged from input variables : ' + ','.join(d['in'])

                    ncVarOut[:] = 0
                    for in_d in d['in']:
                        if in_d in ds.variables:
                            print('propogating from ', in_d)
                            ncVarOut[:] = np.max([ncVarOut[:], ds.variables[in_d + '_quality_control']], axis=0)

                    ncVarFinal[:] = np.max([ncVarOut[:], ds.variables[v + '_quality_control']], axis=0)

        if len(var_list) > 0:
            # update the history attribute
            try:
                hist = ds.history + "\n"
            except AttributeError:
                hist = ""

            ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + ' : ' + 'propagate flags to : ' + ','.join(var_list))

        ds.close()

        out_file.append(fn)

    return out_file


if __name__ == "__main__":
    if (len(sys.argv) > 2) & sys.argv[1].startswith('-'):
        propogate(sys.argv[2:], var_name=sys.argv[1][1:])
    else:
        propogate(sys.argv[1:])
