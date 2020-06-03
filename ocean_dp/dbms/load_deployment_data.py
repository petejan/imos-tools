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

# loop over all points, inserting them into the database
for i, row in df.iterrows():
    # print(j)
    #print(row)

    if isinstance(row.description, str):
        t = (int(row.il_id[2:]), 'description', row.description)

        print(t)

        try:
            cur.execute("INSERT INTO cmditemlinknotes VALUES (nextval('cmditemlinknotessequence'), %s, %s, %s)", t)
        except psycopg2.errors.UniqueViolation:
            print("duplicate entry ", t)

    if isinstance(row.preperation, str):
        t = (int(row.il_id[2:]), 'preperation', row.preperation)

        print(t)

        try:
            cur.execute("INSERT INTO cmditemlinknotes VALUES (nextval('cmditemlinknotessequence'), %s, %s, %s)", t)
        except psycopg2.errors.UniqueViolation:
            print("duplicate entry ", t)

    if isinstance(row.mounting, str):
        t = (int(row.il_id[2:]), 'mounting', row.mounting)

        print(t)

        try:
            cur.execute("INSERT INTO cmditemlinknotes VALUES (nextval('cmditemlinknotessequence'), %s, %s, %s)", t)
        except psycopg2.errors.UniqueViolation:
            print("duplicate entry ", t)

    if isinstance(row.calibration, str):
        t = (int(row.il_id[2:]), 'calibration', row.calibration)

        print(t)

        try:
            cur.execute("INSERT INTO cmditemlinknotes VALUES (nextval('cmditemlinknotessequence'), %s, %s, %s)", t)
        except psycopg2.errors.UniqueViolation:
            print("duplicate entry ", t)

    if isinstance(row.storage, str):
        t = (int(row.il_id[2:]), 'storage', row.storage)

        print(t)

        try:
            cur.execute("INSERT INTO cmditemlinknotes VALUES (nextval('cmditemlinknotessequence'), %s, %s, %s)", t)
        except psycopg2.errors.UniqueViolation:
            print("duplicate entry ", t)

    if isinstance(row.sample_regime, str):
        t = (int(row.il_id[2:]), 'sample_regime', row.sample_regime)

        print(t)

        try:
            cur.execute("INSERT INTO cmditemlinknotes VALUES (nextval('cmditemlinknotessequence'), %s, %s, %s)", t)
        except psycopg2.errors.UniqueViolation:
            print("duplicate entry ", t)
        conn.commit()

    if isinstance(row.variable, str):
        t = (int(row.il_id[2:]), 'variable', row.variable)

        print(t)

        try:
            cur.execute("INSERT INTO cmditemlinknotes VALUES (nextval('cmditemlinknotessequence'), %s, %s, %s)", t)
        except psycopg2.errors.UniqueViolation:
            print("duplicate entry ", t)
        conn.commit()

    if isinstance(row.l22_code, str):
        t = (int(row.id_id[2:]), 'l22_code', row.l22_code)

        print(t)

        try:
            cur.execute("INSERT INTO cmditemdetailnotes VALUES (nextval('cmditemdetailsnotessequence'), %s, %s, %s)", t)
        except psycopg2.errors.UniqueViolation:
            print("duplicate entry ", t)

        conn.commit()

conn.close()

