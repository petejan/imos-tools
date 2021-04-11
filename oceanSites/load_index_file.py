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
import sys
import csv
import re
from parser import ParserError

from dateutil.parser import parse

import psycopg2

conn = psycopg2.connect("dbname=netcdf_files user=ubuntu")
cur = conn.cursor()

if __name__ == "__main__":

    file='oceansites_index.txt'
    valid_date = re.compile(r'^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:Z|[+-][01]\d:[0-5]\d)$')

    fp = open(file)
    rdr = csv.DictReader(filter(lambda row: row[0] != '#', fp),  fieldnames=['FILE','DATE_UPDATE','START_DATE','END_DATE','SOUTHERN_MOST_LATITUDE','NORTHERN_MOST_LATITUDE','WESTERN_MOST_LONGITUDE','EASTERN_MOST_LONGITUDE','MINIMUM_DEPTH','MAXIMUM_DEPTH','UPDATE_INTERVAL','SIZE','GDAC_CREATION_DATE','GDAC_UPDATE_DATE','DATA_MODE','PARAMETERS'])
    for row in rdr:
        # print(len(row), row)

        file_name = row['FILE']
        cur.execute("INSERT INTO index_file(file_name, date_loaded)"
                   "VALUES (%s, now()) RETURNING file_id",
                   (file_name,))

        date_update = None if row['DATE_UPDATE'] == 'unknown' else row['DATE_UPDATE']
        start_date = None if row['START_DATE'] == '' else row['START_DATE']
        end_date = None if row['END_DATE'] == '' else row['END_DATE']
        southern_most_latitude = None if row['SOUTHERN_MOST_LATITUDE'] == 'unknown' else row['SOUTHERN_MOST_LATITUDE']
        northern_most_latitude = None if row['NORTHERN_MOST_LATITUDE'] == 'unknown' else row['NORTHERN_MOST_LATITUDE']
        western_most_longitude = None if row['WESTERN_MOST_LONGITUDE'] == 'unknown' else row['WESTERN_MOST_LONGITUDE']
        eastern_most_longitude = None if row['EASTERN_MOST_LONGITUDE'] == 'unknown' else row['EASTERN_MOST_LONGITUDE']
        minimum_depth = None if row['MINIMUM_DEPTH'] == ' ' or row['MINIMUM_DEPTH'] == 'unknown' else row['MINIMUM_DEPTH']
        maximum_depth = None if row['MAXIMUM_DEPTH'] == ' ' or row['MINIMUM_DEPTH'] == 'unknown' else row['MAXIMUM_DEPTH']
        update_interval = None if row['UPDATE_INTERVAL'] == 'void' else row['UPDATE_INTERVAL']
        size = None if row['SIZE'] == 'void' else row['SIZE']
        data_mode = None if row['DATA_MODE'] == 'unknown' else row['DATA_MODE']
        parameters = row['PARAMETERS']

        id_of_new_row = cur.fetchone()[0]

        if len(row) == 16:
            # print(id_of_new_row)

            cur.execute("UPDATE index_file SET date_update = %s, start_date = %s, end_date = %s, "
                       "southern_most_latitude = %s, northern_most_latitude = %s, western_most_longitude = %s, eastern_most_longitude = %s, "
                       "minimum_depth = %s, maximum_depth = %s, "
                       "update_interval = %s, size = %s, "
                       "data_mode = %s, parameters = %s "
                       "WHERE file_id = %s",
                       (date_update, start_date, end_date, southern_most_latitude, northern_most_latitude, western_most_longitude, eastern_most_longitude, minimum_depth, maximum_depth, update_interval, size, data_mode, parameters, id_of_new_row))

            # insert GDAC dates (issues with these)
            gdac_creation_date = row['GDAC_CREATION_DATE']
            gdac_update_date = row['GDAC_UPDATE_DATE']

            m = valid_date.match(gdac_creation_date)
            if not m:
                gdac_creation_date = None
            m = valid_date.match(gdac_update_date)
            if not m:
                gdac_update_date = None

            # print("gdac_date ", gdac_creation_date)
            cur.execute("UPDATE index_file SET "
                       "gdac_creation_date = %s, gdac_update_date = %s "
                       "WHERE file_id = %s",
                       (gdac_creation_date, gdac_update_date, id_of_new_row))

    conn.commit()

    fp.close()

    # FILE (relative to current file directory),DATE_UPDATE,START_DATE,END_DATE,SOUTHERN_MOST_LATITUDE,NORTHERN_MOST_LATITUDE,WESTERN_MOST_LONGITUDE,EASTERN_MOST_LONGITUDE,MINIMUM_DEPTH,MAXIMUM_DEPTH,UPDATE_INTERVAL,SIZE (in bytes),GDAC_CREATION_DATE,GDAC_UPDATE_DATE,DATA_MODE (R: real-time D: delayed mode M: mixed P: provisional),PARAMETERS (space delimited CF standard names)

