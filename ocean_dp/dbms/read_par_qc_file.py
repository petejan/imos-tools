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

# read the csv file using pandas
data = pd.read_csv(sys.argv[1], dtype={"TIME": float, "INSTRUMENT_ID": int, "VALUE": float, "QC FLAG": int})

# data sample, did we get the data
print(data.head())

# open a database connection
conn = psycopg2.connect(host="localhost", database="DWM", user="pete", password="password")
cur = conn.cursor()

# map quality codes to text
qc_dict = {0: 'RAW', 1: 'GOOD', 2: 'PGOOD', 3: 'PBAD', 4: 'BAD', 5: 'OUT', 9: 'MISSING'}

# a list of metadata from the database, keep a list so we don't have to look it up for every point
metadata = {}

# loop over all points, inserting them into the database
for i, csv_row in data.iterrows():
    # print(j)

    mooring = csv_row["MOORING"].strip()
    instrument = csv_row["INSTRUMENT_ID"]

    meta_data_tup = (mooring, instrument)

    # do we have a the metadat for this combination
    if not meta_data_tup in metadata:
        # lookup metadata in database

        # lookup lat and long
        mooring_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        mooring_cur.execute("SELECT * FROM mooring WHERE mooring_id='%s'" % mooring)
        mooring_info = mooring_cur.fetchone()
        if mooring_info is None:
            print("mooring not found ", mooring)
            break

        # lookup instrument depth
        inst_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        inst_cur.execute("SELECT * FROM mooring_attached_instruments WHERE mooring_id='%s' and instrument_id = %d" % (mooring, instrument))
        inst_info = inst_cur.fetchone()
        if inst_info is None:
            print("instrument not found ", mooring, instrument)
            break

        # lookup sourcefile
        file_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        file_cur.execute("SELECT datafile_pk, instrument_depth FROM instrument_data_files WHERE mooring_id='%s' and instrument_id = %d" % (mooring, instrument))
        file_info = file_cur.fetchone()
        if file_info is None:
            print("source file not found ", mooring, instrument)
            file_info = {'datafile_pk': 2020003, 'instrument_depth': inst_info['depth']}

        #print("mooring", mooring_info, inst_info, file_info)

        # save a new metadata record
        metadata[meta_data_tup] = {'lat': mooring_info['latitude_in'], 'lon': mooring_info['longitude_in'], 'depth_inst': inst_info['depth'],
                                   'source': file_info['datafile_pk']}

        conn.commit()

    # round the time to nearest second as we have been from timestamp to float and backagain
    new_date = datetime.datetime(1950, 1, 1, 0, 0) + datetime.timedelta(days=csv_row["TIME"])

    #print("input date", new_date)

    #print("microseconds", new_date.microsecond)

    a, b = divmod(new_date.microsecond, 500000)
    #print("a", a, "b", b)

    new_date = new_date + datetime.timedelta(microseconds=-b) + datetime.timedelta(microseconds=a*500000)

    #print("new_date", new_date)

    # create a metadata tuple to insert into the database
    file = metadata[meta_data_tup]['source']
    lat_in = metadata[meta_data_tup]['lat']
    lon_in = metadata[meta_data_tup]['lon']
    depth = metadata[meta_data_tup]['depth_inst']
    value = csv_row['VALUE']
    qc = csv_row["QC FLAG"]
    qc_str = qc_dict[qc]

    t = (file, instrument, mooring, new_date, lat_in, lon_in, depth, value, qc_str)

    if (i % 1000) == 0:
        print(i, csv_row["TIME"], t)

    # insert data into database
    try:
        cur.execute("INSERT INTO raw_instrument_data "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, 'PAR', %s, %s)", t
                    )
    except psycopg2.errors.UniqueViolation:
        print("duplicate entry ", csv_row["TIME"], t)

conn.commit()

conn.close()

