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

    ncOut = Dataset(file+'-sparse.nc', "w", format='NETCDF4_CLASSIC')

    # add the global attributes
    att_sql = 'SELECT count(*) FROM file'
    count_rows = cur_att.execute(att_sql)
    file_count = count_rows.fetchone()[0]
    print('file-count', file_count)

    att_sql = 'SELECT name, count(*), type, value FROM attributes GROUP BY name, value'
    global_rows = cur_att.execute(att_sql)
    for att in global_rows:
        if att[1] == file_count: # all values are the same
            if att[2] == 'str':
                ncOut.setncattr(att[0], att[3])
            elif att[2] == 'float32':
                ncOut.setncattr(att[0], np.float32(att[3]))
            elif att[2] == 'float64':
                ncOut.setncattr(att[0], np.float(att[3]))

    # add the dimension variables and some source file metadata
    fDim = ncOut.createDimension("FILE_N", file_count)
    sDim = ncOut.createDimension("strlen", 256)
    varOutFn = ncOut.createVariable("FILE_NAME", "S1", ('FILE_N', 'strlen'))
    varOutInst = ncOut.createVariable("INSTRUMENT", "S1", ('FILE_N', 'strlen'))
    varOutNdFn = ncOut.createVariable("NOMINAL_DEPTH", "f4", ("FILE_N"))

    # generate the time data, from the first time only
    rows = cur.execute('SELECT file_id, file_name, name, dimensions, type, data, nominal_depth FROM variable_depth WHERE name == "TIME" ORDER BY CAST(nominal_depth AS REAL)')
    time_var_row = cur.fetchone() # simply use the first one

    print('time rows', cur.rowcount, len(time_var_row[5]))

    number_samples_read = len(time_var_row[5])

    tDim = ncOut.createDimension("TIME", number_samples_read)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = time_var_row[5]

    sql_select_files = 'select file.file_id, file.name, a_inst.value, a_sn.value, CAST(a_nd.value AS REAL) AS nom_depth FROM file ' \
                       'left join "attributes" a_inst on (file.file_id = a_inst.file_id and a_inst.name = "instrument_model") ' \
                       'left join "attributes" a_sn on (file.file_id = a_sn.file_id and a_sn.name = "instrument_serial_number") ' \
                       'left join "attributes" a_nd on (file.file_id = a_nd.file_id and a_nd.name = "instrument_nominal_depth") ' \
	                   'ORDER BY nom_depth, file.file_id'

    files = cur_files.execute(sql_select_files)
    file_row = cur_files.fetchone()

    print('file rows', cur.rowcount)

    n = 0
    file_names = np.empty(file_count, dtype='S256')
    instrument = np.empty(file_count, dtype='S256')
    var_in_file = {}

    while file_row:
        print('create-file-name', n, file_row[1])
        file_id = file_row[0]
        file_names[n] = file_row[1]

        # set the instrument, if there is a instrument_model and instrument_serial_number attribute
        if file_row[2] and file_row[3]:
            instrument[n] = file_row[2] + ' ; ' + file_row[3]
        else:
            instrument[n] = 'unknown'

        # set the nominal depth from the instrument_nominal_depth
        print('nominal depth', file_row[4])
        if file_row[4]:
            try:
                varOutNdFn[n] = float(file_row[4])
            except ValueError:
                pass
        else:
            varOutFn[n] = np.nan

        rows = cur_vars.execute('SELECT file_id, file_name, name, dimensions, type, data, nominal_depth FROM variable_depth '
                                'WHERE name not in ("TIME", "LATITUDE","LONGITUDE") AND name not like "NOMINAL_DEPTH%"'
                                'AND name NOT LIKE "%_SAMPLE_TIME_DIFF" AND file_id == ' + str(file_id) +
                                ' ORDER BY name, CAST(nominal_depth AS REAL)')

        var_row = cur_vars.fetchone()

        # generate the data for each variable in this file
        while var_row:
            var_name = var_row[2]
            print('var-name', var_name)

            if var_name in var_in_file:
                var_in_file[var_name] = var_in_file[var_name] + 1
            else:
                var_in_file[var_name] = 1

            if var_name not in ncOut.variables:
                varOut = ncOut.createVariable(var_name, "f4", ("FILE_N", "TIME"), fill_value=np.nan, zlib=True)
            else:
                varOut = ncOut.variables[var_name]

            # write the data into its variable
            print('var_row data', n, 'file_name', var_row[1], 'nom depth', var_row[6])
            print('var out shape', varOut.shape, var_row[5].shape)
            varOut[n, :] = var_row[5]

            var_row = cur_vars.fetchone()

        file_row = cur_files.fetchone()
        n += 1

    print(var_in_file)

    # add the variable attributes
    for var_name in ncOut.variables:
        att_sql = 'SELECT name, count(*), type, value FROM variable_attributes WHERE var_name = "' + var_name + '" GROUP BY name, value'
        att_rows = cur_vatt.execute(att_sql)
        varOut = ncOut.variables[var_name]
        if var_name in var_in_file:
            varOut.comment_number_of_input_files = var_in_file[var_name]
        for att in att_rows:
            #print('Attribute', att[0], att[1], var_name, ncOut.variables[var_name].shape)
            if att[0] != '_FillValue':
                if var_name in var_in_file and att[1] == var_in_file[var_name]:
                    #print('adding', att[0])
                    if att[2] == 'str':
                        varOut.setncattr(att[0], att[3])
                    elif att[2] == 'float32':
                        varOut.setncattr(att[0], np.float32(att[3]))
                    elif att[2] == 'float64':
                        varOut.setncattr(att[0], np.float(att[3]))

    # populate the file and and instrument
    varOutFn[:] = stringtochar(file_names)
    varOutInst[:] = stringtochar(instrument)

    # update the history attribute
    try:
        hist = ncOut.history + "\n"
    except AttributeError:
        hist = ""

    ncOut.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " : created from " + file)


    ncOut.close()
    con.close()


if __name__ == "__main__":
    create(sys.argv[1])
