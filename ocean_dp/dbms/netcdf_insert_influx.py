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

from netCDF4 import num2date
from netCDF4 import Dataset
import numpy as np

import glob

from influxdb import InfluxDBClient

def parse(files):

    fn = []
    for f in files:
        fn.extend(glob.glob(f))

    client = InfluxDBClient(host='localhost', port=8086)
    client.switch_database('sots')

    for filepath in fn:
        print('file name', filepath)

        nc = Dataset(filepath, 'r')

        # get time variable
        vs = nc.get_variables_by_attributes(standard_name='time')
        nctime = vs[0]
        t_unit = nctime.units  # get unit  "days since 1950-01-01T00:00:00Z"

        try:
            t_cal = nctime.calendar
        except AttributeError:  # Attribute doesn't exist
            t_cal = u"gregorian"  # or standard

        dt_time = [num2date(t, units=t_unit, calendar=t_cal) for t in nctime]

        print('time variable', nctime.name)
        time_dims = nctime.get_dims()
        time_dims_name = time_dims[0].name
        print('time dimension(0)', time_dims_name)

        nom_depth_var = nc.variables['NOMINAL_DEPTH']
        nom_depth = nom_depth_var[:]

        time_vars_name = []
        for v in nc.variables:
            if v != nctime.name:
                dim_names = [d.name for d in nc.variables[v].get_dims()]
                print('variable ', v, dim_names)
                if time_dims_name in dim_names:
                    print(' has time dimension')
                    time_vars_name.append(v)

        #print('time vars', time_vars)

        # remove an auxiliary variables from the list to plot
        aux_vars = list()
        for var in time_vars_name:
            try:
                aux_vars.extend(nc.variables[var].getncattr('ancillary_variables').split(' '))
            except AttributeError:
                pass

        for var in aux_vars:
            print('remove aux', var)
            time_vars_name.remove(var)

        print('time vars not aux', time_vars_name)
        time_vars = []
        for v in time_vars_name:
            data = nc.variables[v]
            time_vars.append(data)

        #points = []
        point = {'measurement': nc.platform_code, 'tags': {'site': nc.deployment_code, 'nominal_depth': nom_depth}}
        for n in range(0, len(time_dims[0])):
            print(n, dt_time[n], nom_depth)
            point['time'] = dt_time[n]
            fields = {}
            for v in time_vars:
                print('field ', v.name)
                fields[v.name] = np.float(v[n].data)

            point['fields'] = fields

            #print('point', point)

            #points.append(point)

        #print('points', len(points))
            client.write_points([point], database='sots', protocol='json', time_precision='s')

        nc.close()

    return None


if __name__ == "__main__":
    parse(sys.argv[1:])
