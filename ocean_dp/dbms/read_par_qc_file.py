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

data = pd.read_csv("qcPARdata.csv", dtype={"TIME": float, "INSTRUMENT_ID": int, "VALUE": float, "QC FLAG": int})

print(data.head())

conn = psycopg2.connect(host="localhost", database="ABOS", user="pete", password="password")
cur = conn.cursor()

qc_dict = {0: 'NONE', 1: 'GOOD', 2: 'PGOOD', 3: 'PBAD', 4: 'BAD', 5: 'OUT', 9: 'MISSING'}

print("qc_dict : ", qc_dict[0])

for i, j in data.iterrows():
    # print(j)

    mooring = j["MOORING"].strip()
    instrument = j["INSTRUMENT_ID"]

    mooring_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    mooring_cur.execute("SELECT * FROM mooring WHERE mooring_id='%s'" % mooring)
    mooring_info = mooring_cur.fetchone()

    inst_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    inst_cur.execute("SELECT * FROM mooring_attached_instruments WHERE mooring_id='%s' and instrument_id = %d" % (mooring, instrument))
    inst_info = inst_cur.fetchone()

    # print("mooring", mooring_info, inst_info)

    new_date = datetime.datetime(1950, 1, 1, 0, 0) + datetime.timedelta(days=j["TIME"])
    if new_date.microsecond >= 500000:
        new_date = new_date + datetime.timedelta(seconds=1)

    new_date = new_date.replace(microsecond=0)

    #print("INSERT INTO processed_instrument_data (source_file_id, instrument_id, mooring_id, data_timestamp, latitude, longitude, depth, parameter_code, parameter_value, quality_code) VALUES (%(int)s, %(int)s, %(int)s, %(date)s, %s, %s, %s, %s, %s, %s)",
    #            (2020003, instrument, mooring, new_date, mooring_info["latitude_in"], mooring_info["longitude_in"], inst_info['depth'], 'PAR', j["VALUE"], qc_str))

    lat_in = mooring_info['latitude_in']
    lon_in = mooring_info['longitude_in']
    depth = inst_info['depth']
    value = j['VALUE']
    qc = j["QC FLAG"]
    qc_str = qc_dict[qc]
    t = (instrument, mooring, new_date, lat_in, lon_in, depth, value, qc_str)
    print(i, j["TIME"], t)

    #try:
    cur.execute("INSERT INTO processed_instrument_data "
                    "VALUES (2020003, %s, %s, %s, %s, %s, %s, 'PAR', %s, %s)", t
                    )
    #except psycopg2.errors.UniqueViolation:
    #    print("except ", i, j["TIME"], t)


    #if i>10:
    #    break

conn.commit()

conn.close()

