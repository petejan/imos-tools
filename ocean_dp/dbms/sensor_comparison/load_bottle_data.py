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
from datetime import datetime, UTC

import gsw
import numpy as np
from cftime import num2date
from glob2 import glob
from netCDF4 import Dataset

import sqlite3

map_var = {}
map_var['oxygen'] = 'DOX2'
map_var['salinity'] = 'PSAL'

def sqlite_insert(files):

    for file_name in files:
        print('file-name', file_name)

        nc = Dataset(file_name, "r")
        nc.set_auto_mask(False)

        dbname = 'sensor_qc.sqlite'
        con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)

        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS file (file_id integer primary key autoincrement, name UNIQUE)")
        con.commit()
        cur.execute("CREATE TABLE IF NOT EXISTS attributes (file_id, name TEXT, type TEXT, value TEXT)")
        con.commit()
        cur.execute("CREATE TABLE IF NOT EXISTS data (file_id integer, timestamp timestamp, latitude REAL, longitude REAL, pressure REAL, temperature REAL, salinity REAL, parameter TEXT, value REAL, qc integer, type TEXT)")
        con.commit()
        cur.execute('CREATE INDEX IF NOT EXISTS file_id_type_idx ON data (file_id)')
        con.commit()
        cur.execute('CREATE INDEX IF NOT EXISTS timestamp_id_type_idx ON data (timestamp)')
        con.commit()
        cur.execute('CREATE INDEX IF NOT EXISTS parameter_id_type_idx ON data (parameter)')
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

        in_vars = set([x for x in nc.variables])

        if 'temperature' in in_vars:
            temp = nc.variables['temperature'][:].squeeze()

        t_var = nc.variables['time']
        if 'firingTime' in nc.variables:
            t = nc.variables['firingTime']
            ts = num2date(t, calendar=t_var.calendar, units=t_var.units).squeeze()
        else:
            t = nc.variables['time']
            tx = np.ones_like(temp)*t[:]
            ts = num2date(tx, calendar=t_var.calendar, units=t_var.units).squeeze()

        if 'latitude' in in_vars:
            latitude = nc.variables['latitude'][:]
        else:
            latitude = None
        if 'longitude' in in_vars:
            longitude = nc.variables['longitude'][:]
        else:
            longitude = None
            
        if 'ctd_salinity' in in_vars:
            psal = nc.variables['ctd_salinity'][:].squeeze()
        elif 'salinity' in in_vars:
                psal = nc.variables['salinity'][:].squeeze()
        else:
            psal = np.ones_like(temp) * np.nan
        if 'pressure' in in_vars:
            pres = nc.variables['pressure'][:].squeeze()
        else:
            pres = np.ones_like(temp)*np.nan

        load_vars = in_vars.intersection(['oxygen', 'salinity'])
        for v in load_vars:
            values = nc.variables[v][:].squeeze()
            print('len values', values.shape, len(values))
            if v+'Flag' in in_vars:
                qc = nc.variables[v+'Flag'][:].squeeze()
            else:
                qc = np.zeros_like(values, dtype=int)

            if nc.variables[v].units == 'uM':
                SA = gsw.SA_from_SP(psal, pres, longitude, latitude)
                CT = gsw.CT_from_t(SA, temp, pres)

                sigma_theta0 = gsw.sigma0(SA, CT)

                # calculate disolved oxygen, umol/kg
                values = values / (sigma_theta0 + 1000) * 1000
                print(values)

            for i in range(0, len(values)):
                #print(file_id, ts[i], latitude, longitude, pres[i], temp[i], psal[i], v, values[i], qc[i])
                data = (file_id, ts[i].strftime("%Y-%m-%d %H:%M:%S"), float(latitude), float(longitude), float(pres[i]), float(temp[i]), float(psal[i]), map_var[v], float(values[i]), int(qc[i]), 'BOTTLE')
                print(i, data)
                cur.execute('''INSERT INTO data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', data)

        con.commit()


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))
    sqlite_insert(files)
