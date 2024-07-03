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

from cftime import num2date
from glob2 import glob
from netCDF4 import Dataset, stringtochar

import sqlite3
import numpy as np
import io

compressor = 'zlib'  # zlib, bz2
include_attributes = True

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


def create_dimensions(con, ncOut):
    cur = con.cursor()

    # generate the time data

    rows = cur.execute('SELECT * FROM variables WHERE name == "TIME" ORDER BY file_id')
    row = cur.fetchone()

    print('time rows', cur.rowcount, len(row['data']))

    number_samples_read = len(row['data'])

    tDim = ncOut.createDimension("TIME", number_samples_read)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.standard_name = "time"
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut.valid_max = 90000
    ncTimesOut.valid_min = 0
    ncTimesOut[:] = row['data']

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    rows = cur.execute('SELECT * FROM variables WHERE name == "LATITUDE" ORDER BY file_id')
    row = cur.fetchone()

    ncLatOut = ncOut.createVariable("LATITUDE", "d")
    ncLatOut.axis = "Y"
    ncLatOut.long_name = "latitude"
    ncLatOut.reference_datum = "WGS84 geographic coordinate system"
    ncLatOut.standard_name = "latitude"
    ncLatOut.units = "degrees_north"
    ncLatOut.valid_max = 90
    ncLatOut.valid_min = -90

    lat_data = row['data']
    print('lat data', lat_data)
    ncLatOut[:] = lat_data

    rows = cur.execute('SELECT * FROM variables WHERE name == "LONGITUDE" ORDER BY file_id')
    row = cur.fetchone()

    ncLonOut = ncOut.createVariable("LONGITUDE", "d")
    ncLonOut.axis = "X"
    ncLonOut.long_name = "longitude"
    ncLonOut.reference_datum = "WGS84 geographic coordinate system"
    ncLonOut.standard_name = "longitude"
    ncLonOut.units = "degrees_east"
    ncLonOut.valid_max = 180
    ncLonOut.valid_min = -180

    lon_data = row['data']
    ncLonOut[:] = lon_data


def global_attributes(con, ncOut):
    cur_att = con.cursor()

    # add the global attributes
    att_sql = 'SELECT count(*) FROM file'
    count_rows = cur_att.execute(att_sql)
    file_count = count_rows.fetchone()[0]
    print('file-count', file_count)

    # add global attributes
    deployment_code = None
    if include_attributes:
        att_sql = 'SELECT name, count(*) AS count, type, value FROM attributes GROUP BY name, value'
        global_rows = cur_att.execute(att_sql)
        for att in global_rows:
            if att['count'] == file_count:
                if att[0] == 'deployment_code':
                    deployment_code = att[3]
                if att['type'] == 'str':
                    ncOut.setncattr(att[0], att[3])
                elif att['type'] == 'float32':
                    ncOut.setncattr(att[0], np.float32(att[3]))
                elif att['type'] == 'float64':
                    ncOut.setncattr(att[0], float(att[3]))

    return (file_count, deployment_code)


def add_attributes_to_variables(con, ncOut, var_list):

    cur = con.cursor()
    cur_vatt = con.cursor()

    file_vars = cur.execute(
        'SELECT name, is_aux, count(*) AS count FROM variables WHERE dimensions LIKE "TIME[%]" and name != "TIME" and name != "LATITUDE" AND name != "LONGITUDE"' \
        'and name not like "%_SAMPLE_TIME_DIFF" GROUP BY name ORDER BY name')

    for var_rows in file_vars:
        var_name = var_rows['name']
        var_count = var_rows['count']
        varOut = ncOut.variables[var_name]
        print()
        print(var_name, 'dimensions', varOut.dimensions, var_count)

        # add the variable attributes
        att_sql = 'SELECT name, count(*) AS count, type, value FROM variable_attributes WHERE var_name = "' + var_name + '" GROUP BY name, value HAVING count = ' + str(
            var_count)
        att_rows = cur_vatt.execute(att_sql)
        dim_name = varOut.dimensions[0]
        for att in att_rows:
            print(var_name, 'var attributes', att['count'], var_list[dim_name.replace("IDX_", "")], att['value'])
            if att['name'] != '_FillValue':
                if att['type'] == 'str':
                    varOut.setncattr(att['name'], att['value'])
                elif att['type'] == 'float32':
                    varOut.setncattr(att['name'], np.float32(att['value']))
                elif att['type'] == 'float64':
                    varOut.setncattr(att['name'], float(att['value']))
            if var_rows['is_aux'] is None:
                varOut.coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH_" + var_name

        if var_name.endswith('_quality_control'):
            varOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
            varOut.flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value"
        # if var_name.endswith('_number_of_observations'):
        #     varOut.units = '1'

        # if no coordinates were added, add one now
        if var_rows['is_aux'] is None and not hasattr(varOut, 'coordinates'):
            varOut.coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH_" + var_name


def gen_source_file_vars(ncOut, file_count):
    # add source file name, index and there nominal depth variables
    fDim = ncOut.createDimension("IDX", file_count)
    sDim = ncOut.createDimension("strlen", 256)
    varOutFn = ncOut.createVariable("FILE_NAME", "S1", ('IDX', 'strlen'))
    varOutFn.long_name = 'name of source file'
    varOutInst = ncOut.createVariable("INSTRUMENT", "S1", ('IDX', 'strlen'))
    varOutInst.long_name = 'source instrument type:serialnumber'
    varOutNdFn = ncOut.createVariable("NOMINAL_DEPTH", "f4", ("IDX"), fill_value=np.nan)

    varOutNdFn.standard_name = "depth"
    varOutNdFn.long_name = "nominal depth"
    varOutNdFn.coordinates = 'LONGITUDE LATITUDE'
    varOutNdFn.units = "m"
    varOutNdFn.positive = "down"
    varOutNdFn.reference_datum = "sea surface"
    varOutNdFn.valid_max = 12000.
    varOutNdFn.valid_min = -5.
    varOutNdFn.axis = "Z"

    return (varOutFn, varOutNdFn, varOutInst)

def gen_file_instance_vars(con, ncOut):

    cur_vars = con.cursor()

    sql_select_vars = 'SELECT name, COUNT(*) AS count, is_aux FROM variables v WHERE dimensions LIKE "TIME[%]" and name != "TIME" and name != "LATITUDE" AND name != "LONGITUDE"' \
                      'and name not like "%_SAMPLE_TIME_DIFF" AND is_aux IS NULL GROUP BY name'

    # generate the FILE instance variables
    var_list = {}
    varOutFnIdx = None
    varOutNdIdx = None

    file_vars = cur_vars.execute(sql_select_vars)
    for var in file_vars:
        var_name = var['name']
        print('create dimensions, nominal_depth variable for', var_name, var['count'])

        if ("IDX_" + var_name) not in ncOut.dimensions:
            var_list[var_name] = 0
            iDim = ncOut.createDimension("IDX_" + var_name, var['count'])

            varOutFnIdx = ncOut.createVariable("IDX_" + var_name, "i4", ("IDX_" + var_name))
            varOutFnIdx.long_name = 'index of data to FILE_NAME, INSTRUMENT, NOMINAL_DEPTH'
            varOutFnIdx.units = '1'

            varOutNdIdx = ncOut.createVariable("NOMINAL_DEPTH_" + var_name, "f4", ("IDX_" + var_name))
            varOutNdIdx.standard_name = 'depth'
            varOutNdIdx.long_name = 'NOMINAL DEPTH for ' + var_name
            varOutNdIdx.units = "m"
            varOutNdIdx.positive = "down"
            varOutNdIdx.reference_datum = "sea surface"
            varOutNdIdx.valid_max = 12000.
            varOutNdIdx.valid_min = -5.

    return (var_list, varOutFnIdx, varOutNdIdx)


def load_file_data(con, ncOut, file_ids, file_id_map, var_list):
    cur = con.cursor()

    # for each file, now create the variables and load the data
    for file_id in file_ids:
        print()

        # generate the data for each variable

        file_vars = cur.execute('SELECT * FROM variables WHERE dimensions LIKE "TIME[%]" and name != "TIME" and name != "LATITUDE" AND name != "LONGITUDE"' \
                                'and name not like "%_SAMPLE_TIME_DIFF" AND file_id == ' + str(file_id) + ' ORDER BY name')

        for var_rows in file_vars:

            print(file_id, 'variable', var_rows['name'], var_rows['is_aux'], var_rows['type'])
            var_name = var_rows['name']
            dim_name = var_name
            if var_rows['is_aux'] is not None:
                dim_name = var_rows['is_aux']

            fill_value = np.nan
            if var_rows['type'] == 'int8':
                fill_value = 99

            if var_name not in ncOut.variables:
                varOut = ncOut.createVariable(var_name, var_rows['type'], ("IDX_" + dim_name, "TIME"), fill_value=fill_value, zlib=True)
                print('variable created ', var_name)
            else:
                varOut = ncOut[var_name]

            n = var_list[dim_name]
            print(dim_name, 'instance n', n)

            if var_rows['is_aux'] is None:
                varOut[n, :] = var_rows['data']
                idx_var = ncOut.variables['IDX_' + dim_name]
                idx_var[n] = file_id_map[file_id]
                nd_var = ncOut.variables['NOMINAL_DEPTH_' + dim_name]
                nd_var[n] = var_rows['nominal_depth']

                var_list[dim_name] = n + 1
            else:
                varOut[n-1, :] = var_rows['data']


def create_file_ids(con, ncOut, var_list, file_count, varOutNdFn, varOutFn, varOutInst):

    cur = con.cursor()
    cur_files = con.cursor()

    sql_select_files =  'select file.file_id, file.name, a_inst.value AS inst, a_sn.value AS sn, CAST(a_nd.value AS REAL) AS nom_depth FROM file ' \
                        'left join "attributes" a_inst on (file.file_id = a_inst.file_id and a_inst.name = "instrument_model") ' \
                        'left join "attributes" a_sn on (file.file_id = a_sn.file_id and a_sn.name = "instrument_serial_number") ' \
                        'left join "attributes" a_nd on (file.file_id = a_nd.file_id and a_nd.name = "instrument_nominal_depth") ' \
                        'ORDER BY nom_depth'

    print(var_list)

    files = cur_files.execute(sql_select_files)
    row = cur_files.fetchone()

    print('rows', cur.rowcount)

    n = 0
    file_names = np.empty(file_count, dtype='S256')
    instrument = np.empty(file_count, dtype='S256')
    file_id_map = {}
    file_ids = []

    # create a list of file_ids and depths
    depths = []
    while row:
        file_id = row['file_id']
        print('create-file-name', n, file_id, row['nom_depth'], row['name'])
        file_names[n] = row['name']

        if row['inst'] and row['sn']:
            instrument[n] = row['inst'] + ' ; ' + row['sn']
        else:
            instrument[n] = 'unknown'

        if row['nom_depth'] is not None:
            varOutNdFn[n] = float(row['nom_depth'])
            depths.append(float(row['nom_depth']))
        else:
            varOutNdFn[n] = np.nan

        file_id_map[file_id] = n
        file_ids.append(row['file_id'])

        row = cur_files.fetchone()
        n += 1

    print('rows-loaded', n)

    # save the file name and instrument
    varOutFn[:] = stringtochar(file_names)
    varOutInst[:] = stringtochar(instrument)

    return (file_names, instrument, file_ids, file_id_map, depths)


def create(file):

    con = sqlite3.connect(file, detect_types=sqlite3.PARSE_DECLTYPES)
    con.row_factory = sqlite3.Row

    ncOut = Dataset(file+'.nc', "w", format='NETCDF4_CLASSIC')

    create_dimensions(con, ncOut)
    (file_count, deployment_code) = global_attributes(con, ncOut)
    (varOutFn, varOutNdFn, varOutInst) = gen_source_file_vars(ncOut, file_count)
    (var_list, varOutFnIdx, varOutNdIdx) = gen_file_instance_vars(con, ncOut)

    (file_names, instrument, file_ids, file_id_map, depths) = create_file_ids(con, ncOut, var_list, file_count, varOutNdFn, varOutFn, varOutInst)

    load_file_data(con, ncOut, file_ids, file_id_map, var_list)

    if include_attributes:
        add_attributes_to_variables(con, ncOut, var_list)

    # create the final global attributes
    ncOut.geospatial_vertical_max = max(depths)
    ncOut.geospatial_vertical_min = min(depths)

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    now = datetime.now(UTC)

    ncOut.date_created = now.strftime(ncTimeFormat)
    ncOut.history = now.strftime("%Y-%m-%d") + " created from " + file
    ncOut.principal_investigator = 'Shadwick, Elizabeth; Shulz, Eric'
    ncOut.title = 'Gridded oceanographic and meteorological data from the Southern Ocean Time Series observatory in the Southern Ocean southwest of Tasmania'
    ncOut.file_version = 'Level 2 - Derived product'
    ncOut.derived_product_type = 'gridded_data'
    ncOut.data_mode = 'G'  # TODO: corruption of data_mode from OceanSITES manual
    if deployment_code:
        ncOut.deployment_code = deployment_code

    ncOut.close()
    con.close()


if __name__ == "__main__":
    for f in sys.argv[1:]:
        create(f)
