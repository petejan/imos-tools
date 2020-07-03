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

import psycopg2
import csv


def load(file):

    conn = psycopg2.connect("dbname=netcdf_files user=pete")
    cur = conn.cursor()

    with open(file) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if row[0][0] != '#':

                file_name = row[0]
                #insert an entry for the file
                cur.execute("INSERT INTO file (file_name) VALUES (%s) RETURNING file_id", (file_name, ))

                id_of_new_row = cur.fetchone()[0]

                print (id_of_new_row, row[0])

                line_count += 1

        print(f'Processed {line_count} lines.')

    conn.commit()
    cur.close()


if __name__ == "__main__":
    load(sys.argv[1])
