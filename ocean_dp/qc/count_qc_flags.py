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

import numpy as np
from dateutil import parser
from netCDF4 import date2num
from netCDF4 import Dataset, num2date
import struct
import os
from scipy import stats
import glob2 as glob


def flag_count(netCDFfile):

    file_list = []
    flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
    flag_list = {}

    for fn in netCDFfile:
        ds = Dataset(fn, 'r')

        file_metadata = {}

        file_metadata['file_name'] = os.path.basename(fn)
        #print(os.path.basename(fn))

        time_var = ds.variables["TIME"]
        time = num2date(time_var[:], units=time_var.units, calendar=time_var.calendar)

        time_deploy = parser.parse(ds.time_deployment_start, ignoretz=True)
        time_recovery = parser.parse(ds.time_deployment_end, ignoretz=True)

        deployment_time_mask = (time >= time_deploy) & (time < time_recovery)

        deployment = ds.deployment_code

        instrument = ds.instrument_model + " " + ds.instrument_serial_number + " @ " + str(ds.instrument_nominal_depth)

        file_metadata['deployment'] = ds.deployment_code
        file_metadata['time_deploy'] = time_deploy
        file_metadata['instrument'] = ds.instrument_model

        file_metadata['sn'] = ds.instrument_serial_number
        file_metadata['nominal_depth'] = float(ds.variables['NOMINAL_DEPTH'][:].data)

        # get a list of auxiliary variables
        auxList = []
        for variable in ['PSAL']: # ds.variables:
            var = ds[variable]

            try:
                aux = var.getncattr('ancillary_variables')
                #print(' aux variables :', aux)
                for a in aux.split(' '):
                    if a in ds.variables:
                        aux_v = {}
                        aux_v['name'] = a
                        aux_v['long_name'] = ds.variables[a].long_name
                        if 'flag_values' in ds.variables[a].ncattrs():
                            aux_v['flag_values'] = ds.variables[a].flag_values
                            flag_meanings = ds.variables[a].flag_meanings
                            aux_v['flag_meanings'] = flag_meanings
                            flag_sep = flag_meanings.split(' ')
                            for i in range(0,len(ds.variables[a].flag_values)):
                                #print(i, flag_sep[i])
                                flag_list[ds.variables[a].flag_values[i]] = flag_sep[i]

                        hist, bin_edges = np.histogram(ds.variables[a], bins=[0, 1, 2, 3, 4, 6, 9, 99])
                        aux_v['hist'] = hist
                        aux_v['bin_edges'] = bin_edges
                        #print('{:35} {}'.format(a, hist))

                        auxList.append(aux_v)

            except AttributeError:
                pass
        file_metadata['aux_variables'] = auxList

        ds.close()
        file_list.append(file_metadata)

    sort_orders = sorted(file_list, key=lambda x: [x['time_deploy'], x['nominal_depth']])

    last_deployment = ''
    flag_k = []
    flag_m = []
    for i in flag_list.keys():
        flag_k.append(str(i))
        flag_m.append(flag_list[i])

    print('deployment,instrument,sn,nominal_depth,flag,'+','.join(flag_m))
    print(',,,,,',','.join(flag_k))

    for f in sort_orders:
        deployment = f['deployment']
        #print(f)
        line = []
        if deployment != last_deployment:
            line.append(deployment)
            last_deployment = deployment
        else:
            line.append('')

        line.append('{}'.format(f['instrument']))
        line.append('{}'.format(f['sn']))
        line.append('{}'.format(f['nominal_depth']))
        for a in f['aux_variables']:
            if 'flag_values' in a:
                line.append(a['long_name'])
                s = np.array2string(a['hist'], separator=',')
                line.append(s[1:-1])

        print(','.join(line))

        long_names = []
        for a in f['aux_variables']:
            if 'flag_values' not in a:
                if a['long_name'] not in long_names:
                    line = []
                    line.append('')
                    line.append('')
                    line.append('')
                    line.append('')
                    line.append(a['long_name'].replace(' for sea_water_practical_salinity', ''))
                    s = np.array2string(a['hist'], separator=',')
                    line.append(s[1:-1])
                    print(','.join(line))
                    long_names.append(a['long_name'])


if __name__ == "__main__":
    files=[]
    for f in sys.argv[1:]:
        files.extend(glob.glob(f))
    flag_count(files)
