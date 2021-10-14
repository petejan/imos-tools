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


def create(file):

    con = sqlite3.connect(file, detect_types=sqlite3.PARSE_DECLTYPES)

    cur_vars = con.cursor()
    cur = con.cursor()
    cur_vatt = con.cursor()
    cur_att = con.cursor()
    cur_files = con.cursor()

    ncOut = Dataset(file+'.nc', "w", format='NETCDF4_CLASSIC')

    # add the global attributes
    att_sql = 'SELECT count(*) FROM file'
    count_rows = cur_att.execute(att_sql)
    file_count = count_rows.fetchone()[0]
    print('file-count', file_count)

    att_sql = 'SELECT name, count(*), type, value FROM attributes  GROUP BY name, value'
    global_rows = cur_att.execute(att_sql)
    for att in global_rows:
        if att[1] == file_count:
            if att[2] == 'str':
                ncOut.setncattr(att[0], att[3])
            elif att[2] == 'float32':
                ncOut.setncattr(att[0], np.float32(att[3]))
            elif att[2] == 'float64':
                ncOut.setncattr(att[0], np.float(att[3]))

    fDim = ncOut.createDimension("FILE_NAME", file_count)
    sDim = ncOut.createDimension("strlen", 256)
    varOutFn = ncOut.createVariable("FILE_NAME", "S1", ('FILE_NAME', 'strlen'))
    varOutInst = ncOut.createVariable("INSTRUMENT", "S1", ('FILE_NAME', 'strlen'))
    varOutIdxFn = ncOut.createVariable("IDX_FILE_NAME", "i4", ("FILE_NAME"))
    varOutNdFn = ncOut.createVariable("DEPTH_FILE_NAME", "i4", ("FILE_NAME"))

    sql_select_files = 'select file.file_id, file.name, a_inst.value, a_sn.value, CAST(a_nd.value AS REAL) AS nom_depth FROM file ' \
                       'left join "attributes" a_inst on (file.file_id = a_inst.file_id and a_inst.name = "instrument_model") ' \
                       'left join "attributes" a_sn on (file.file_id = a_sn.file_id and a_sn.name = "instrument_serial_number") ' \
                       'left join "attributes" a_nd on (file.file_id = a_nd.file_id and a_nd.name = "instrument_nominal_depth") ' \
	                   'ORDER BY nom_depth, file.file_id'

    files = cur_files.execute(sql_select_files)
    row = cur_files.fetchone()

    print('rows', cur.rowcount)

    n = 0
    file_names = np.empty(file_count, dtype='S256')
    instrument = np.empty(file_count, dtype='S256')
    while row:
        print('create-file-name', n, row[1])
        file_names[n] = row[1]
        if row[2] and row[3]:
            instrument[n] = row[2] + ' ; ' + row[3]
        else:
            instrument[n] = 'unknown'
        varOutIdxFn[n] = int(row[0])
        if row[4]:
            varOutNdFn[n] = float(row[4])
        else:
            varOutFn[n] = np.nan

        row = cur_files.fetchone()

        n += 1

    varOutFn[:] = stringtochar(file_names)
    varOutInst[:] = stringtochar(instrument)

    sql_select_vars = 'SELECT name, COUNT(*) FROM variables v WHERE dimensions LIKE "TIME[%]" and name != "TIME" and name not like "%_SAMPLE_TIME_DIFF" GROUP BY name ORDER BY name'

    # generate the FILE instance variables
    vars = cur_vars.execute(sql_select_vars)
    for var in vars:
        var_name = var[0]
        print('var-name', var_name)

        rows = cur.execute('SELECT * FROM variable_depth WHERE name == "'+var_name+'" ORDER BY CAST(nominal_depth AS REAL)')
        row = cur.fetchone()

        print('rows', cur.rowcount, len(row[5]))

        iDim = ncOut.createDimension("INSTANCE_"+var_name, var[1])

        varOutFn = ncOut.createVariable("IDX_"+var_name, "i4", ("INSTANCE_"+var_name))
        n = 0
        while row:
            print('create-file-index', row[1], row[6])
            varOutFn[n] = row[0]
            row = cur.fetchone()

            n += 1

    # generate the DEPTH instance variables
    vars = cur_vars.execute(sql_select_vars)
    for var in vars:
        var_name = var[0]
        print('var-name', var_name)

        rows = cur.execute('SELECT * FROM variable_depth WHERE name == "'+var_name+'" ORDER BY CAST(nominal_depth AS REAL)')
        row = cur.fetchone()

        print('rows', cur.rowcount, len(row[5]))

        varOutNd = ncOut.createVariable("DEPTH_"+var_name, "f4", ("INSTANCE_"+var_name))
        n = 0
        while row:
            print('create-depth', row[1], row[6])
            varOutNd[n] = row[6]
            row = cur.fetchone()

            n += 1

    # generate the time data
    vars = cur_vars.execute(sql_select_vars)

    rows = cur.execute('SELECT * FROM variable_depth WHERE name == "TIME" ORDER BY CAST(nominal_depth AS REAL)')
    row = cur.fetchone()

    print('time rows', cur.rowcount, len(row[5]))

    number_samples_read = len(row[5])

    tDim = ncOut.createDimension("TIME", number_samples_read)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = row[5]

    # generate the data for each variable
    vars = cur_vars.execute(sql_select_vars)
    for var in vars:
        var_name = var[0]
        print('var-name', var_name)

        rows = cur.execute('SELECT * FROM variable_depth WHERE name == "'+var_name+'" ORDER BY CAST(nominal_depth AS REAL)')
        row = cur.fetchone()

        print('rows', cur.rowcount, len(row[5]))

        number_samples_read = len(row[5])

        #iDim = ncOut.createDimension("INSTANCE_"+var_name, var[1])

        varOut = ncOut.createVariable(var_name, "f4", ("INSTANCE_"+var_name, "TIME"), fill_value=np.nan, zlib=True)

        # add the variable attributes
        att_sql = 'SELECT name, count(*), type, value FROM variable_attributes WHERE var_name = "'+var_name+'" GROUP BY name, value'
        att_rows = cur_vatt.execute(att_sql)
        for att in att_rows:
            if att[0] != '_FillValue':
                if att[1] == var[1]:
                    if att[2] == 'str':
                        varOut.setncattr(att[0], att[3])
                    elif att[2] == 'float32':
                        varOut.setncattr(att[0], np.float32(att[3]))
                    elif att[2] == 'float64':
                        varOut.setncattr(att[0], np.float(att[3]))


        #varOutFn = ncOut.createVariable("FILE_"+var_name, "i4", ("INSTANCE_"+var_name))
        #varOutNd = ncOut.createVariable("DEPTH_"+var_name, "f4", ("INSTANCE_"+var_name))

        n = 0
        while row:
            print(row[1], row[6])
            varOut[n, :] = row[5]
            #varOutFn[n] = row[0]
            #varOutNd[n] = row[6]
            row = cur.fetchone()

            n += 1

        print('rows-loaded', n)

    ncOut.close()
    con.close()


if __name__ == "__main__":
    create(sys.argv[1])
