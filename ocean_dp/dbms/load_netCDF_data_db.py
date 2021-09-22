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

import urllib

import psycopg2
from cftime import date2num, num2date
from netCDF4 import Dataset

import glob
import numpy
import dateutil.parser
from psycopg2._psycopg import IntegrityError

conn = psycopg2.connect("dbname=data_db user=pete")

verbose = 3

def getAttributeOrDefault(nc, name, default):
    try:
        value = nc.getncattr(name)
    except AttributeError:
        value = default

    return value


def getDateOrDefault(nc, name, default):
    value_att = getAttributeOrDefault(nc, name, None)
    try:
        value = dateutil.parser.parse(value_att)
    except (ValueError, TypeError):
        value = default

    return value


# example url
# http://tds0.ifremer.fr/thredds/dodsC/CORIOLIS-OCEANSITES-GDAC-OBS/DATA/WHOTS/OS_WHOTS_2019_R_M-2.nc.html
# https://dods.ndbc.noaa.gov/thredds/dodsC/oceansites/DATA/ALOHA/OS_ACO_20110613-08-16_P_CTD3-4726m.nc.html

def postgres_insert(file_name):

    nc = Dataset(file_name , "r")

    url = file_name

    #if not cur:
    cur = conn.cursor()

    if verbose > 0:
        print('processing', nc.filepath())

    title = getAttributeOrDefault(nc, 'title', None)

    site_code = getAttributeOrDefault(nc, 'site_code', None)
    platform_code = getAttributeOrDefault(nc, 'platform_code', None)
    deployment_code = getAttributeOrDefault(nc, 'deployment_code', None)

    geospatial_lat_min = getAttributeOrDefault(nc, 'geospatial_lat_min', None)
    geospatial_lon_min = getAttributeOrDefault(nc, 'geospatial_lon_min', None)
    geospatial_vertical_min = getAttributeOrDefault(nc, 'geospatial_vertical_min', None)

    date_created = getDateOrDefault(nc, 'date_created', None)
    if date_created is None:
        date_created = getDateOrDefault(nc, 'date_update', None)

    time_coverage_start = getDateOrDefault(nc, 'time_coverage_start', None)
    time_coverage_end = getDateOrDefault(nc, 'time_coverage_end', None)

    principal_investigator = getAttributeOrDefault(nc, 'principal_investigator', None)
    if principal_investigator is None:
        principal_investigator = getAttributeOrDefault(nc, 'pi_name', None)

    file_name = os.path.basename(urllib.parse.unquote(nc.filepath()))

    if verbose > 1:
        print('file_name ', file_name)

    try:
        if verbose > 4:
            print("geospatial_vertical_min", geospatial_vertical_min, type(geospatial_vertical_min))

        # convert float coordinates into strings for storage in database
        if isinstance(geospatial_vertical_min, (numpy.float32, numpy.float64, numpy.int32)):
            geospatial_vertical_min = str(geospatial_vertical_min)
        if isinstance(geospatial_lat_min, (numpy.float32, numpy.float64)):
            geospatial_lat_min = str(geospatial_lat_min)
        if isinstance(geospatial_lon_min, (numpy.float32, numpy.float64)):
            geospatial_lon_min = str(geospatial_lon_min)

        # insert an entry for the file
        cur.execute("INSERT INTO file (file_id, url, file_name, title, site_code, platform_code, deployment_code, "
                    "geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min, "
                    "date_created, time_coverage_start, time_coverage_end, principal_investigator) "
                    "VALUES (nextval('file_id_sequence'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING file_id" ,
                    (url, file_name, title, site_code, platform_code, deployment_code,
                     geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min,
                     date_created, time_coverage_start, time_coverage_end, principal_investigator))

        file_id = cur.fetchone()[0]

        # insert all global attributes
        glob_atts = nc.ncattrs()
        for att_name in glob_atts:
            att_value = nc.getncattr(att_name)
            att_type = type(att_value).__name__
            if isinstance(att_value, (numpy.float32, numpy.float64)) and ~numpy.isnan(att_value):
                if att_value == int(att_value):
                    att_value = int(att_value)
            elif isinstance(att_value, (numpy.int32, numpy.int64)):
                att_value = str(att_value)

            if verbose > 3:
                print('global ', att_name, att_type, att_value)

            cur.execute("INSERT INTO global_attributes (file_id, name, type, value)"
                        "VALUES (%s, %s, %s, %s)",
                        (file_id, att_name, att_type, str(att_value)))

        # get a list of auxiliary variables
        auxList = []
        for variable in nc.variables:
            var = nc[variable]

            try:
                aux = var.getncattr('ancillary_variables')
                auxList.extend(aux.split(' '))
            except AttributeError:
                pass

        if verbose > 3:
            print ('aux list :', auxList)

        ts = None
        # hack for MNF CTD file
        if 'firingTime' in nc.variables:
            ts = num2date(nc.variables['firingTime'], units=nc.variables['time'].units, calendar="gregorian").flatten()
        # standard netCDF TIME
        if 'TIME' in nc.variables:
            if nc.variables['TIME'].long_name == 'time':
                ts = num2date(nc.variables['TIME'], units=nc.variables['TIME'].units, calendar=nc.variables['TIME'].calendar).flatten()

        lat = float(nc.variables['LATITUDE'][0])
        lon = float(nc.variables['LONGITUDE'][0])
        depth = float(nc.variables['NOMINAL_DEPTH'][0])
        sensor = nc.instrument

        print("file coords, sensor", lat, lon, depth, sensor)

        # insert data into file_coords
        cur.execute("INSERT INTO file_sensor_coords (file_id, lat, lon, depth, sensor)"
                    "VALUES (%s, %s, %s, %s, %s)",
                    (file_id, lat, lon, depth, sensor))

        # insert all variable metadata
        for var_name in nc.variables:
            var = nc.variables[var_name]
            #var_type = type(var[:].data[0]).__name__
            var_type = str(var.dtype)
            if var_type == '|S1':
                var_type = 'char'
            is_coord = var_name in nc.dimensions

            # get a standard or long name for this variable
            name = None
            try:
                name = var.getncattr('standard_name')
            except AttributeError:
                if verbose > 2:
                    print ('no standard_name :', var_name)

            if name is None:
                try:
                    name = var.getncattr('long_name')
                except AttributeError:
                    if verbose > 2:
                        print('no long_name :', var_name)

            # get any aux variables
            aux_vars = None
            try:
                aux_vars = var.getncattr('ancillary_variables')
            except AttributeError:
                if verbose > 2:
                    print ('no aux_vars', var_name)

            # get tht variable units
            units = None
            try:
                units = var.getncattr('units')
            except AttributeError:
                if verbose > 2:
                    print ('no units', var_name)

            # is this variable a auxiliary variable
            is_aux = var_name in auxList

            # get the dimensions
            dims = var.dimensions
            shape = var.shape
            # TODO use join here
            buf = ''
            for index in range(len(dims)):
                if index > 0:
                    buf += ','
                buf += '%s[%d]' % (dims[index], shape[index])

            if verbose > 2:
                print('variable', var_name, var_type, units)

            # insert variables into database
            cur.execute("INSERT INTO variable (file_id, variable, name, units, dimensions, is_aux, is_coord, aux_vars, type)"
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (file_id, var_name, name, units, buf, is_aux, is_coord, aux_vars, var_type))

            # add the variable attributes
            for att_name in var.ncattrs():
                att_value = var.getncattr(att_name)
                att_type = type(att_value).__name__
                if att_type.startswith('float') and ~numpy.isnan(att_value) and ~numpy.isinf(att_value):
                    if att_value == int(att_value):
                        att_value = int(att_value)

                if verbose > 2:
                    print('var-att', var_name, att_name, att_type)

                # insert variables_attributes into database
                cur.execute("INSERT INTO variable_attributes (file_id, variable, name, type, value)"
                            "VALUES (%s, %s, %s, %s, %s)",
                            (file_id , var_name, att_name, att_type, str(att_value)))

            # insert any float data
            if not is_aux and var_type.startswith('float'):
                v = var[:].flatten()
                i = 0

                for val in v:
                    if verbose > 3:
                        print('data ', i, val)

                    dval = numpy.float(val)

                    # insert data into database
                    cur.execute("INSERT INTO variable_data (file_id, variable, idx, deployment, lat, lon, depth, sensor, timestamp, value)"
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (file_id, var_name, i, deployment_code, lat, lon, depth, sensor, str(ts[i]), dval))
                    i = i + 1

    except IntegrityError as e:
        print ('IntegrityError', e)

    conn.commit()
    cur.close()

    return id


if __name__ == "__main__":
    for f in sys.argv[1:]:
        postgres_insert(f)
