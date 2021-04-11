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

conn = psycopg2.connect("dbname=netcdf_files user=ubuntu")
cur = conn.cursor()

def postgres_insert_coords(nc, file_id, var, axis):

    v = nc.variables[var]
    value = v[:].tolist()
    if type(value) is not list:
        value = [value]
        
    print (var, value, len(value), type(value))

    # insert data into database
    cur.execute("INSERT INTO coordinates (file_id, name, axis, value) "
                "VALUES (%s, %s, %s, %s)",
                (file_id, var, axis, value))

    conn.commit()


if __name__ == "__main__":

    nc = None
    try:
        cur_read = conn.cursor()
        cur_update = conn.cursor()

        cur_read.execute("SELECT url, file.file_id, variable, variables_attributes.value "
                         "FROM variables_attributes "
                          "JOIN variables using (file_id, variable) "
                          "JOIN file using (file_id) "
                          "WHERE variables_attributes.name = 'axis' AND value != 'T' AND length(value) = 1 AND date_coords IS NULL "
                         "ORDER BY file.file_id")
        
        #cur_read.execute("SELECT file_id, file_name FROM index_file WHERE NOT EXISTS (SELECT FROM file WHERE file.file_id = index_file.file_id) ORDER BY file_id")

        row = cur_read.fetchone()

        nc = None
        last_id = None

        while row is not None:
            print(row)

            if nc is None:
                nc = Dataset(row[0])
                #print(nc)
                last_id = row[1]

            postgres_insert_coords(nc, row[1], row[2], row[3])
            cur_update.execute("UPDATE file SET date_coords = now() WHERE file_id = %s", (row[1],))

            row = cur_read.fetchone()

            if row is not None:
                if row[1] != last_id:
                    nc.close()
                    nc = None
                    conn.commit()

        cur_read.close()
    except (psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
