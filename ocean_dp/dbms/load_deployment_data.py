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
import datetime

import pandas as pd
import psycopg2
import psycopg2.extras
import sys
import numpy as np


# read the csv file using pandas
df = pd.read_excel ('~/cloudstor/Shared/SOTS_Annual Reports/sots-instrument-deployments.xlsx')

# data sample, did we get the data
print(df.columns)

# open a database connection
conn = psycopg2.connect(host="localhost", database="IMOS-DEPLOY", user="pete", password="password")
cur = conn.cursor()

count_link_notes = 0
count_detail_notes = 0

# loop over all points, inserting them into the database
for i, row in df.iterrows():
    # print(j)
    #print(row)

    for col in ['description', 'sample_regime', 'variable', 'storage', 'preparation', 'mounting', 'calibration', 'Parameters_measured', 'Manufacturer_URL', 'Model_Numbers', 'Manufacturer_precisions', 'Data', 'Subsection']:

        if isinstance(row[col], str):
            t = (int(row.il_id[2:]), col.lower(), row[col])

            print('cmditemlinknotes', t)
            try:
                cur.execute("INSERT INTO cmditemlinknotes VALUES (nextval('cmditemlinknotessequence'), %s, %s, %s)", t)
                count_link_notes += 1
            except psycopg2.errors.UniqueViolation:
                print("cmditemlinknotes duplicate entry ", t)

        conn.commit()

    if isinstance(row.l22_code, str):
        code = row.l22_code
        if not code.startswith("SDN"):
            code = 'SDN:L22::' + code
        t = (int(row.id_id[2:]), 'l22_code', code)

        #print(t)

        try:
            cur.execute("INSERT INTO cmditemdetailnotes VALUES (nextval('cmditemdetailsnotessequence'), %s, %s, %s)", t)
            count_detail_notes += 1
        except psycopg2.errors.UniqueViolation:
            print("cmditemdetailnotes duplicate entry ", t)

        conn.commit()

conn.close()

print('count of item detail notes', count_detail_notes)
print('count of link notes', count_link_notes)
