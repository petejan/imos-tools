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

import datetime

from dateutil import parser
from netCDF4 import date2num
from netCDF4 import Dataset, num2date
import struct
import os
from scipy import stats


def flag_count(netCDFfile):

    bins = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 99, 1000]
    bin_names = ['unknown', 'good_data', 'probably_good_data', 'probably_bad_data', 'bad_data', None,
                 'not_deployed', 'interpolated', None, 'missing_value', None, 'not_set']

    line = 'file_name,deployment_date,deployment,instrument,serial_number,depth,flag,' + ','.join(filter(None, bin_names[0:-1]))
    print (line)

    for fn in netCDFfile:
        #print('file', os.path.basename(fn))
        line = os.path.basename(fn)

        ds = Dataset(fn, 'r')

        time_var = ds.variables["TIME"]
        time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

        time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
        time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)

        deployment_time_mask = (time >= time_deploy) & (time < time_recovery)

        deployment = ds.deployment_code

        instrument = ds.instrument_model + " " + ds.instrument_serial_number + " @ " + str(ds.instrument_nominal_depth)

        line += ',' + datetime.datetime.strftime(time_deploy, '%Y-%m-%d') + ',' + deployment + ',' + ds.instrument_model + ',\"' + ds.instrument_serial_number + '\",' + str(ds.instrument_nominal_depth)

        # get a list of auxiliary variables
        auxList = []
        for variable in ds.variables:
            var = ds[variable]

            try:
                aux = var.getncattr('ancillary_variables')
                auxList.extend(aux.split(' '))
            except AttributeError:
                pass

        # loop over aux variables
        for aux_var_name in sorted(set(auxList)):
            try:
                aux_var = ds.variables[aux_var_name]
                if aux_var.long_name.startswith("quality flag for") | aux_var.long_name.startswith('quality_code for'):
                    #print(' qc var', aux_var_name)

                    linevar = line + ',' + aux_var_name

                    aux_var_data = aux_var[:]
                    # mask the non in/out water or main quality flag
                    if not aux_var_name.endswith("quality_control") and not aux_var_name.endswith("quality_control_io"):
                        aux_var_data.mask = ~deployment_time_mask
                    stat = stats.binned_statistic(aux_var_data.compressed(), [], 'count', bins=bins)
                    #print(' ', deployment, instrument) # , aux_var_name, stat)

                    for i in range(0, len(stat.statistic)-1):
                        #print(i, stat.statistic[i], stat.bin_edges[i], bin_names[i])
                        #print('  ', bin_names[i], ': count=', stat.statistic[i])
                        if bin_names[i]:
                            linevar += ',' + str(int(stat.statistic[i]))

                    print (linevar)

            except KeyError:
                pass

        ds.close()


if __name__ == "__main__":
    flag_count(sys.argv[1:])
