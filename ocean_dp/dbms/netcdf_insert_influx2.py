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
import os
import sys
sys.path.extend(['.'])

from datetime import datetime

from cftime import num2date
from netCDF4 import Dataset
import numpy as np

import glob

from influxdb import InfluxDBClient


def parse(files):

    fn = files
    print(files)
    #for f in files:
    #    fn.extend(glob.glob(f))

    client = InfluxDBClient(host='144.6.230.0', port=8086)
    client.switch_database('rtdp')

    print(fn)

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

        print('time variable', nctime.name)
        time_dims = nctime.get_dims()
        time_dims_name = time_dims[0].name
        print('time dimension(0)', time_dims_name)

        z_coords = nc.get_variables_by_attributes(axis='Z')
        #print('z coords', z_coords)
        nom_depth = None
        try:
            nom_depth_var = z_coords[0] # TODO not take first one
            nom_depth = nom_depth_var[:].data
        except KeyError:
            pass

        coords = None
        time_vars_name = []
        for v in nc.variables:
            if v != nctime.name:
                dim_names = [d.name for d in nc.variables[v].get_dims()]
                print('variable ', v, dim_names)
                if time_dims_name in dim_names:
                    print(' has time dimension')
                    time_vars_name.append(v)
            if 'coordinates' in nc.variables[v].ncattrs():
                #print('coord', nc.variables[v].ncattrs())
                coords = nc.variables[v].getncattr('coordinates')

            print(' coords:', coords)

        #print('time vars', time_vars)

        # remove an auxiliary variables from the list to plot
        aux_vars = list()
        for var in nc.variables:
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
            qc = None
            if v + "_quality_control" in nc.variables:
                qc = nc.variables[v + "_quality_control"]
            time_vars.append({'var': data, 'time_dim': data.dimensions.index('TIME'), 'qc': qc})

        date_time_start = datetime.strptime(nc.getncattr('time_deployment_start'), '%Y-%m-%dT%H:%M:%SZ')
        try:
            date_time_end = datetime.strptime(nc.getncattr('time_deployment_end'), '%Y-%m-%dT%H:%M:%SZ')
        except AttributeError:
            date_time_end = datetime.strptime(nc.getncattr('time_coverage_end'), '%Y-%m-%dT%H:%M:%SZ')

        # read the TIME variable and convert to python datetime
        dt_time = num2date(nctime[:], units=t_unit, calendar=t_cal, only_use_cftime_datetimes=False)

        point = {'measurement': nc.platform_code, 'tags': {'site': nc.deployment_code, 'nominal_depth': nom_depth}}
        for n in range(0, len(time_dims[0])):
            if (dt_time[n] > date_time_start) & (dt_time[n] < date_time_end):

                #print(n, dt_time[n], nom_depth)

                point['time'] = dt_time[n]
                fields = {}
                for v in time_vars:
                    qc = 0
                    if v['qc']:
                        qc = v['qc'][n]
                    if qc <= 2:
                        #print('field ', v['var'].name, v['time_dim'], qc)
                        if v['time_dim'] == 0:
                            fields[v['var'].name] = np.float(v['var'][n].data)
                        elif v['time_dim'] == 1:
                            fields[v['var'].name] = np.float(v['var'][0, n].data)

                point['fields'] = fields

                if (n % 1000) == 0:
                    print('point', point)

                client.write_points([point], database='rtdp', protocol='json')

        nc.close()

    return None


if __name__ == "__main__":
    parse(sys.argv[1:])
