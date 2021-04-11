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
from netCDF4 import Dataset

import glob
import numpy
import dateutil.parser
from psycopg2._psycopg import IntegrityError
from thredds_crawler.crawl import Crawl

conn = psycopg2.connect("dbname=netcdf_files user=ubuntu")

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


def load():
    cur = conn.cursor()

    cur.execute("SELECT file_id, file_name FROM file WHERE url is NULL")
    while True:
        row = cur.fetchone()
        if row is None:
            break
        print(row)

        url = 'http://tds0.ifremer.fr/thredds/dodsC/CORIOLIS-OCEANSITES-GDAC-OBS' + row[1]
        print('url:', url)
        try:
            nc = Dataset(url, mode="r")
            postgres_insert(row[0], nc, url)
        except OSError as e:
            print('OSError', e)


# example url
# http://tds0.ifremer.fr/thredds/dodsC/CORIOLIS-OCEANSITES-GDAC-OBS/DATA/WHOTS/OS_WHOTS_2019_R_M-2.nc.html
# https://dods.ndbc.noaa.gov/thredds/dodsC/oceansites/DATA/ALOHA/OS_ACO_20110613-08-16_P_CTD3-4726m.nc.html

def postgres_insert(id, nc, url=''):

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
        cur.execute("UPDATE file SET (url, title, site_code, platform_code, deployment_code, "
                    "geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min, "
                    "date_created, time_coverage_start, time_coverage_end, principal_investigator) "
                    "= (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) WHERE file_id = " + str(id),
                    (url, title, site_code, platform_code, deployment_code,
                     geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min,
                     date_created, time_coverage_start, time_coverage_end, principal_investigator))

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
                        (id, att_name, att_type, str(att_value)))

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
                        (id, var_name, name, units, buf, is_aux, is_coord, aux_vars, var_type))

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
                            (id, var_name, att_name, att_type, str(att_value)))

    except IntegrityError as e:
        print ('IntegrityError', e)

    conn.commit()
    cur.close()

    return id


if __name__ == "__main__":
    load()
