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

from glob2 import glob
from netCDF4 import Dataset

import sqlite3
import numpy as np
import io

from thredds_crawler.crawl import Crawl


def sqlite_insert(http, opendap):
    now = datetime.utcnow()
    dbname = 'thredds-' + now.strftime("%Y-%m-%d") + '.sqlite'

    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)

    print('file-name', opendap)

    nc = Dataset(opendap, "r")

    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS file (file_id integer primary key autoincrement, name TEXT, http TEXT, opendap TEXT)")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS attributes (file_id, name TEXT, type TEXT, value TEXT)")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS variables (file_id, name TEXT, type TEXT, dimensions TEXT)")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS variable_attributes (file_id, var_name TEXT, name TEXT, type TEXT, value TEXT)")
    con.commit()
    cur.execute("CREATE VIEW IF NOT EXISTS file_deployment AS "
                "SELECT file.file_id, file.name, dep.value AS deployment_code, nd.value AS nominal_depth "
                "FROM file"
                " LEFT join attributes AS dep ON (file.file_id = dep.file_id and dep.name == 'deployment_code')"
                " LEFT join attributes AS nd ON (file.file_id = nd.file_id and nd.name == 'instrument_nominal_depth')")
    cur.execute("CREATE VIEW IF NOT EXISTS variable_depth AS "
                "SELECT file.file_id, file.name AS file_name, variables.name, variables.dimensions, variables.type, variables.data, nd.value AS nominal_depth "
                "FROM variables"
                " JOIN file ON (file.file_id = variables.file_id)"
                " LEFT join attributes AS nd ON (file.file_id = nd.file_id and nd.name == 'instrument_nominal_depth')")
    cur.execute("CREATE VIEW IF NOT EXISTS file_instrument AS select f.file_id , f.name, m.value as model, s.value as sn FROM file f "
                "LEFT JOIN attributes m ON (f.file_id = m.file_id and m.name = 'instrument') "
                "LEFT JOIN attributes s ON (s.file_id = f.file_id and s.name = 'instrument_serial_number')")
    con.commit()

    cur.execute('INSERT INTO file (name, http, opendap) VALUES (?, ?, ?)', [os.path.basename(http), http, opendap])
    con.commit()
    file_id = cur.lastrowid
    print('file_id', file_id)

    # load all global attributes
    for at in nc.ncattrs():
        print('global attribute', at)
        name = at
        value = nc.getncattr(at)
        at_type = type(value).__name__
        cur.execute('INSERT INTO attributes (file_id, name, type, value) VALUES (?,?,?,?)', [file_id, name, at_type, str(value)])
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
            buf = ''
            for index in range(len(dims)):
                if index > 0:
                    buf += ','
                buf += '%s[%d]' % (dims[index], shape[index])

            at_type = str(var.dtype)
            cur.execute('INSERT INTO variables (file_id, name, type, dimensions) VALUES (?,?,?,?)', [file_id, var_name, at_type, buf])
            con.commit()

            # load variable attributes
            for at in var.ncattrs():
                print('variable-atribute', at)
                if not at.startswith('_'):
                    name = at
                    value = var.getncattr(at)
                    at_type = type(value).__name__
                    cur.execute('INSERT INTO variable_attributes (file_id, var_name, name, type, value) VALUES (?,?,?,?,?)', [file_id, var_name, name, at_type, str(value)])
                    con.commit()

    nc.close()

    con.close()


if __name__ == "__main__":

    if (len(sys.argv) > 1):
        path = sys.argv[1]
        if not path.endswith('/'):
            path = path + '/'
    else:
        path = ''

    print('path', path)

    #skips = Crawl.SKIPS + [".*realtime", ".*Real-time", ".*daily", ".*REAL_TIME", ".*regridded", ".*burst", ".*gridded", ".*long-timeseries"]

    crawl_path = 'http://thredds.aodn.org.au/thredds/catalog/IMOS/DWM/SOTS/'+path+'catalog.xml'

    #c = Crawl(crawl_path, select=['.*'], skip=skips)
    c = Crawl(crawl_path, select=['.*'])

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

    for url in urls:
        print(os.path.basename(url['http']))
        sqlite_insert(url['http'], url['opendap'])
