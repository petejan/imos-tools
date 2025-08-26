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
import os
import sys

from glob2 import glob
from netCDF4 import Dataset

import sqlite3
import numpy as np
import io

compressor = 'zlib'  # zlib, bz2

def adapt_array(arr):
    """
    http://stackoverflow.com/a/31312102/190597 (SoulNibbler)
    """
    # zlib uses similar disk size that Matlab v5 .mat files
    # bz2 compress 4 times zlib, but storing process is 20 times slower.
    out = io.BytesIO()
    np.save(out, arr)
    out.seek(0)
    #return sqlite3.Binary(out.read().encode(compressor))  # zlib, bz2
    return sqlite3.Binary(out.read())  # zlib, bz2

def convert_array(text):
    out = io.BytesIO(text)
    out.seek(0)
    #out = io.BytesIO(out.read().decode(compressor))
    out = io.BytesIO(out.read())
    return np.load(out)


sqlite3.register_adapter(np.ndarray, adapt_array)
sqlite3.register_converter("array", convert_array)


def sqlite_insert(files):

    for file_name in files:
        print('file-name', file_name)

        nc = Dataset(file_name, "r")

        deployment_code = nc.deployment_code

        dbname = deployment_code + ".sqlite"
        con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)

        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS file (file_id integer primary key autoincrement, name UNIQUE)")
        con.commit()
        cur.execute("CREATE TABLE IF NOT EXISTS attributes (file_id, name TEXT, type TEXT, value TEXT)")
        con.commit()
        cur.execute("CREATE TABLE IF NOT EXISTS variables (file_id, name TEXT, type TEXT, dimensions TEXT, is_aux TEXT, nominal_depth REAL, data array)")
        con.commit()
        cur.execute("CREATE TABLE IF NOT EXISTS variable_attributes (file_id, var_name TEXT, name TEXT, type TEXT, value TEXT)")
        con.commit()
        cur.execute("CREATE VIEW IF NOT EXISTS file_deployment AS "
                    "SELECT file.file_id, file.name, dep.value AS deployment_code, nd.value AS nominal_depth "
                    "FROM file"
                    " LEFT join attributes AS dep ON (file.file_id = dep.file_id and dep.name == 'deployment_code')"
                    " LEFT join attributes AS nd ON (file.file_id = nd.file_id and nd.name == 'instrument_nominal_depth')")

        cur.execute("CREATE VIEW IF NOT EXISTS variable_depth AS "
                    "SELECT file.file_id AS file_id, file.name AS file_name, variables.name, variables.dimensions, variables.type, variables.is_aux, CAST(nd.value AS REAL) AS file_nominal_depth, nominal_depth AS var_nominal_depth, variables.data "
                    "FROM variables"
                    " JOIN file ON (file.file_id = variables.file_id)"
                    " LEFT join attributes AS nd ON (file.file_id = nd.file_id and nd.name == 'instrument_nominal_depth')")
        con.commit()

        cur.execute('INSERT INTO file (name) VALUES (?)', [os.path.basename(file_name)])
        con.commit()
        file_id = cur.lastrowid
        print('file_id', file_id)

        # load all global attributes
        for at in nc.ncattrs():
            #print('global attribute', at)
            name = at
            value = nc.getncattr(at)
            at_type = type(value).__name__
            cur.execute('INSERT INTO attributes (file_id, name, type, value) VALUES (?,?,?,?)', [file_id, name, at_type, str(value)])
            con.commit()

        # get list of variables, then indicate which are aux variables
        vars = {}
        for var_name in nc.variables:
            vars[var_name] = None

        for var_name in nc.variables:
            try:
                aux_vars = nc.variables[var_name].ancillary_variables.split(' ')
                for a in aux_vars:
                    if var_name in nc.variables:
                        vars[a] = var_name
            except AttributeError:
                pass

        # load variables
        for var_name in nc.variables: #nc.variables:
            is_aux = vars[var_name]

            var = nc.variables[var_name]
            print('variable', var_name, var.dimensions, var.shape)

            var.set_auto_mask(False)
            nd = np.nan
            if hasattr(var, 'coordinates'):
                if hasattr(nc, 'instrument_nominal_depth'):
                    nd = nc.instrument_nominal_depth

                coords = var.coordinates.split(' ')
                for c in coords:
                    if c in nc.variables:
                        #print(var_name, "coords", c)
                        if hasattr(nc.variables[c], 'axis'):
                            if nc.variables[c].axis == 'Z':
                                nd = float(nc.variables[c][0])
                                if nc.variables[c].positive == 'up':
                                    nd = -nd
                                print('found z axis', nd)

            # get the dimensions
            dims = var.dimensions
            shape = var.shape
            # TODO use join here
            buf = ''
            for index in range(len(dims)):
                if index > 0:
                    buf += ','
                buf += '%s[%d]' % (dims[index], shape[index])

            data = np.array(var[:])
            at_type = str(var.dtype)
            cur.execute('INSERT INTO variables (file_id, name, type, dimensions, is_aux, nominal_depth, data) VALUES (?,?,?,?,?,?,?)', [file_id, var_name, at_type, buf, is_aux, nd, data])
            con.commit()

            # load variable attributes
            for at in var.ncattrs():
                #print('variable-atribute', at)
                name = at
                value = var.getncattr(at)
                at_type = type(value).__name__
                cur.execute('INSERT INTO variable_attributes (file_id, var_name, name, type, value) VALUES (?,?,?,?,?)', [file_id, var_name, name, at_type, str(value)])
                con.commit()

        nc.close()
        con.close()


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))
    sqlite_insert(files)
