#!/usr/bin/python3

# load_data
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
from netCDF4 import Dataset

import glob
import numpy
import dateutil.parser
from psycopg2._psycopg import IntegrityError
import traceback

conn = psycopg2.connect("dbname=netcdf_files user=ubuntu")
cur = conn.cursor()

verbose = 1

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


def postgres_insert_file(nc, url, id_of_new_row):

    if verbose > 0:
        print('processing', nc.filepath())

    title = getAttributeOrDefault(nc, 'title', None)

    site_code = getAttributeOrDefault(nc, 'site_code', None)
    platform_code = getAttributeOrDefault(nc, 'platform_code', None)
    deployment_code = getAttributeOrDefault(nc, 'deployment_code', None)

    geospatial_lat_min = getAttributeOrDefault(nc, 'geospatial_lat_min', None)
    geospatial_lon_min = getAttributeOrDefault(nc, 'geospatial_lon_min', None)
    geospatial_vertical_min = getAttributeOrDefault(nc, 'geospatial_vertical_min', None)
    geospatial_lat_max = getAttributeOrDefault(nc, 'geospatial_lat_max', None)
    geospatial_lon_max = getAttributeOrDefault(nc, 'geospatial_lon_max', None)
    geospatial_vertical_max = getAttributeOrDefault(nc, 'geospatial_vertical_max', None)

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

        if isinstance(geospatial_vertical_min, (numpy.float32, numpy.float64, numpy.int32)):
            geospatial_vertical_min = str(geospatial_vertical_min)
        if isinstance(geospatial_lat_min, (numpy.float32, numpy.float64)):
            geospatial_lat_min = str(geospatial_lat_min)
        if isinstance(geospatial_lon_min, (numpy.float32, numpy.float64, numpy.int32)):
            geospatial_lon_min = str(geospatial_lon_min)

        if isinstance(geospatial_vertical_max, (numpy.float32, numpy.float64, numpy.int32)):
            geospatial_vertical_max = str(geospatial_vertical_max)
        if isinstance(geospatial_lat_max, (numpy.float32, numpy.float64)):
            geospatial_lat_max = str(geospatial_lat_max)
        if isinstance(geospatial_lon_max, (numpy.float32, numpy.float64)):
            geospatial_lon_max = str(geospatial_lon_max)

        cur.execute("INSERT INTO file (file_id, url, date_loaded, file_name, title, site_code, platform_code, deployment_code, "
                    "geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min, "
                    "geospatial_lat_max, geospatial_lon_max, geospatial_vertical_max, "
                    "date_created, time_coverage_start, time_coverage_end, principal_investigator) "
                    "VALUES (%s, %s, now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (id_of_new_row, url, file_name, title, site_code, platform_code, deployment_code,
                     geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min,
                     geospatial_lat_max, geospatial_lon_max, geospatial_vertical_max,
                     date_created, time_coverage_start, time_coverage_end, principal_investigator))
        conn.commit()

    except IntegrityError as e:
        print ('IntegrityError', e)

    return id_of_new_row


def postgres_insert_global(nc, id_of_new_row):

    # insert all global attributes
    glob_atts = nc.ncattrs()
    for att_name in glob_atts:
        att_value = nc.getncattr(att_name)
        att_type = type(att_value).__name__
        if att_type.startswith('float') and ~numpy.isnan(att_value):
            if att_value == int(att_value):
                att_value = int(att_value)

        if verbose > 3:
            print('global ', att_name, att_type, att_value)

        cur.execute("INSERT INTO global_attributes (file_id, name, type, value)"
                    "VALUES (%s, %s, %s, %s)",
                    (id_of_new_row, att_name, att_type, str(att_value)))
    conn.commit()

def postgres_insert_variable(nc, id_of_new_row):

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
        is_aux = var_name not in auxList

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

        # insert data into database
        cur.execute("INSERT INTO variables (file_id, variable, name, units, dimensions, is_aux, is_coord, aux_vars, type)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (id_of_new_row, var_name, name, units, buf, is_aux, is_coord, aux_vars, var_type))

        postgres_insert_variable_attributes(nc, var, id_of_new_row)

    conn.commit()


def postgres_insert_variable_attributes(nc, var, id_of_new_row):

    var_name = var.name
    # add the variable attributes
    for att_name in var.ncattrs():
        att_value = var.getncattr(att_name)
        att_type = type(att_value).__name__
        if att_type.startswith('float') and ~numpy.isnan(att_value) and ~numpy.isinf(att_value):
            if att_value == int(att_value):
                att_value = int(att_value)

        if verbose > 2:
            print('var-att', var_name, att_name, att_type)

        # insert data into database
        cur.execute("INSERT INTO variables_attributes (file_id, variable, name, type, value)"
                    "VALUES (%s, %s, %s, %s, %s)",
                    (id_of_new_row, var_name, att_name, att_type, str(att_value)))


if __name__ == "__main__":

    nc = None
    try:
        cur_read = conn.cursor()

        #cur_read.execute("SELECT file_id, file_name FROM index_file order by file_id")
        cur_read.execute("SELECT file_id, file_name FROM index_file WHERE NOT EXISTS (SELECT FROM file WHERE file.file_id = index_file.file_id) ORDER BY file_id")
        row = cur_read.fetchone()

        while row is not None:
            print(row)

            try:
                nc = None;
                url = 'http://dods.ndbc.noaa.gov/thredds/dodsC/data/oceansites/' + row[1]
                nc = Dataset(url)
                postgres_insert_file(nc, url, row[0])
                postgres_insert_global(nc, row[0])
                postgres_insert_variable(nc, row[0])
            except OSError as error:
                print(error)
            finally:
                if nc is not None:
                    nc.close()

            row = cur_read.fetchone()

        cur_read.close()
    except (psycopg2.DatabaseError) as error:
        print(error)
        traceback.print_exc()
    finally:
        if conn is not None:
            conn.close()
