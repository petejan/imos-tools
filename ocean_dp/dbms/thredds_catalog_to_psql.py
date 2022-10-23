#!/usr/bin/python3

# raw2netCDF
# Copyright (C) 2021 Peter Jansen
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

from datetime import datetime
import os
import sys

import psycopg2
from netCDF4 import Dataset

import sqlite3

from thredds_crawler.crawl import Crawl
import urllib
import dateutil
import pydap.lib

pydap.lib.CACHE = "/tmp/pydap-cache/"


def getAttributeOrDefault(dataset, name, default):
    try:
        value = dataset.getncattr(name)
    except (AttributeError):
        value = default

    print("get attribute", name, value)
    return value


def getDateOrDefault(dataset, name, default):
    value_att = getAttributeOrDefault(dataset, name, default)
    if value_att is None:
        value = None
    else:
        value = dateutil.parser.parse(value_att)

    return value


def sqlite_insert(con, http, opendap):

    print('file-name', opendap)

    nc = Dataset(opendap, "r")

    title = getAttributeOrDefault(nc, 'title', None)

    site_code = getAttributeOrDefault(nc, 'site_code', None)
    platform_code = getAttributeOrDefault(nc, 'platform_code', None)
    deployment_code = getAttributeOrDefault(nc, 'deployment_code', None)
    featureType = getAttributeOrDefault(nc, 'featureType', None)

    geospatial_lat_min = float(getAttributeOrDefault(nc, 'geospatial_lat_min', None))
    geospatial_lon_min = float(getAttributeOrDefault(nc, 'geospatial_lon_min', None))
    geospatial_lat_max = float(getAttributeOrDefault(nc, 'geospatial_lat_max', None))
    geospatial_lon_max = float(getAttributeOrDefault(nc, 'geospatial_lon_max', None))
    geospatial_vertical_min = float(getAttributeOrDefault(nc, 'geospatial_vertical_min', None))
    geospatial_vertical_max = float(getAttributeOrDefault(nc, 'geospatial_vertical_max', None))

    date_created = getDateOrDefault(nc, 'date_created', None)
    if (date_created is None):
        date_created = getDateOrDefault(nc, 'date_update', None)

    time_deployment_start = getDateOrDefault(nc, 'time_deployment_start', None)
    time_deployment_end = getDateOrDefault(nc, 'time_deployment_end', None)

    principal_investigator = getAttributeOrDefault(nc, 'principal_investigator', None)
    print('file_name ', os.path.basename(http))

    cur.execute("INSERT INTO file (name, http, opendap, title, site_code, platform_code, deployment_code, featuretype, "
                "geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min, "
                "geospatial_lat_max, geospatial_lon_max, geospatial_vertical_max, "
                "date_created, time_deployment_start, time_deployment_end, principal_investigator)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING file_id",
                (os.path.basename(http), http, opendap, title, site_code, platform_code, deployment_code, featureType,
                 geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min,
                 geospatial_lat_max, geospatial_lon_max, geospatial_vertical_max,
                 date_created, time_deployment_start, time_deployment_end, principal_investigator))
    con.commit()

    file_id = cur.lastrowid
    print('file_id', file_id)

    # load all global attributes
    for at in nc.ncattrs():
        print('global attribute', at)
        name = at
        value = nc.getncattr(at)
        at_type = type(value).__name__
        cur.execute('INSERT INTO global_attributes (file_id, name, value, type) VALUES (%s,%s,%s,%s)', [file_id, name, str(value), at_type])
        con.commit()

    # get list of auxcilliary variables as we don't load these
    aux_vars = []
    for var_name in nc.variables:
        print('variable', var_name)
        try:
            aux_vars.extend(nc.variables[var_name].ancillary_variables.split(' '))
        except AttributeError:
            pass

    # load variables
    for var_name in nc.variables:
        print('variable', var_name)
        if var_name not in aux_vars:
            var = nc.variables[var_name]
            var.set_auto_mask(False)
            # get the dimensions
            dims = var.dimensions
            shape = var.shape

            # TODO use join here
            var_dims = ''
            for index in range(len(dims)):
                if index > 0:
                    var_dims += ','
                var_dims += '%s[%d]' % (dims[index], shape[index])

            try:
                standard_name = var.getncattr('standard_name')
            except AttributeError:
                print ('no standard_name :', var.name)
                standard_name = None

            try:
                long_name = var.getncattr('long_name')
            except AttributeError:
                print('no long_name :', var.name)
                long_name = None

            try:
                aux_var = var.getncattr('ancillary_variables')
            except AttributeError:
                print ('no aux_vars', var_name)
                aux_var = None

            try:
                units = var.getncattr('units')
            except AttributeError:
                print ('no units', var_name)
                units = None

            at_type = str(var.dtype)
            cur.execute('INSERT INTO variables (file_id, name, standard_name, long_name, units, aux_vars, dimensions, type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                        [file_id, var_name, standard_name, long_name, units, aux_var, var_dims, at_type])
            con.commit()

            # load variable attributes
            for at in var.ncattrs():
                print('variable-attribute', at)
                if not at.startswith('_'):
                    name = at
                    value = var.getncattr(at)
                    at_type = type(value).__name__
                    cur.execute('INSERT INTO variable_attributes (file_id, var_name, name, type, value) VALUES (%s,%s,%s,%s,%s)',
                                [file_id, var_name, name, at_type, str(value)])
                    con.commit()

    nc.close()

if __name__ == "__main__":

    if (len(sys.argv) > 1):
        path = sys.argv[1]
        if not path.endswith('/'):
            path = path + '/'
    else:
        path = ''

    print('path', path)

    skips = Crawl.SKIPS + [".*realtime", ".*Real-time", ".*daily", ".*REAL_TIME", ".*regridded", ".*burst", ".*gridded", ".*long-timeseries"]

    crawl_path = 'http://thredds.aodn.org.au/thredds/catalog/IMOS/DWM/SOTS/'+path+'catalog.xml'

    c = Crawl(crawl_path, select=['.*'], skip=skips)

    # create a list of urls to catalog, save the opendap url and the http links
    urls = []
    for d in c.datasets:
        opendap = None
        http = None

        # service can be httpService or dapService
        for s in d.services:
            if s.get("service").lower() == "opendap":
                opendap = s.get("url")
            if s.get("service").lower() == "httpserver":
                http = s.get("url")

        urls.append({'opendap': opendap, 'http': http})


    now = datetime.utcnow()

    con = psycopg2.connect(host='localhost', dbname='threddscat', user='postgres', password='secret')

    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS file (file_id serial primary key, name TEXT, http TEXT, opendap TEXT"
                ", title TEXT, site_code TEXT, platform_code TEXT, deployment_code TEXT, featuretype TEXT"
                ", geospatial_lat_min REAL, geospatial_lon_min REAL, geospatial_vertical_min REAL"
                ", geospatial_lat_max REAL, geospatial_lon_max REAL, geospatial_vertical_max REAL"
                ", date_created TEXT, time_deployment_start TEXT, time_deployment_end TEXT, principal_investigator TEXT"
                ")")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS global_attributes (file_id integer, name TEXT, type TEXT, value TEXT)")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS variables (file_id integer, name TEXT, standard_name TEXT, long_name TEXT, units TEXT, aux_vars TEXT, dimensions TEXT, type TEXT)")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS variable_attributes (file_id integer, var_name TEXT, name TEXT, type TEXT, value TEXT)")
    con.commit()
    con.commit()

    for url in urls:
        print(os.path.basename(url['http']))
        sqlite_insert(con, url['http'], url['opendap'])

    con.close()
