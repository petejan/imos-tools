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
from netCDF4 import Dataset, stringtochar

import sqlite3
import numpy as np
import io
import requests

def create(file):

    con = sqlite3.connect(file, detect_types=sqlite3.PARSE_DECLTYPES)

    cur_files = con.cursor()

    search_for = 'standard_name'
    search_value = 'mass_concentration_of_chlorophyll_in_sea_water'
    #search_value = 'sea_water_temperature'

    sql_select_files = 'select file.name, http from variable_attributes va ' \
                       'join file using (file_id) ' \
                       'where va.name = "'+search_for+'" and va.value ="'+search_value+'"' \
                       ' and (file.name like "%FV02%" or file.name like "%FV01%") and file.name like "%FLNTUS%"'

#    'join attributes a on (a.file_id = file.file_id and a.name="file_version" and a.value = "Level 1 - Quality Controlled Data") ' \

    cur_files.execute(sql_select_files)
    row = cur_files.fetchone()

    while row:
        print('file-name', row[0])

        r = requests.get(row[1], allow_redirects=True)
        open(row[0], 'wb').write(r.content)

        row = cur_files.fetchone()

    con.close()


if __name__ == "__main__":
    create(sys.argv[1])
