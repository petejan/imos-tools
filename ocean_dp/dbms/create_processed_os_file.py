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

import sys
import psycopg2
import psycopg2.extras

from datetime import datetime, timedelta
from dateutil import parser

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

import numpy as np

qualityFlag = {}
qualityFlag['RAW'] = 1
qualityFlag['DERIVED'] = 1
qualityFlag['EXTRACTED'] = 1
qualityFlag['AVG'] = 1
qualityFlag['INTERPOLATED'] = 1
qualityFlag['GOOD'] = 1
qualityFlag['PGOOD'] = 2
qualityFlag['PBAD'] = 3
qualityFlag['BAD'] = 4
qualityFlag['OOR'] = 4
qualityFlag['OUT'] = 6
qualityFlag['MISSING'] = 9


def add_netcdf_attributes(ncOut, cur):
    row = cur.fetchone()

    while row is not None:
        print(row)
        if (row['attribute_type'].startswith("STRING")):
            ncOut.setncattr(row['attribute_name'], row['attribute_value'])
        elif (row['attribute_type'].startswith("NUMBER")):
            ncOut.setncattr(row['attribute_name'], np.float(row['attribute_value']))

        row = cur.fetchone()


def create(mooring):
    conn = None

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = mooring + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    select_parameters = "SELECT parameter_code, imos_data_code, array_agg(instrument_id) AS instruments, array_agg(source) AS source, array_agg(depth) AS depths, array_agg(instrument_make) AS instrument_makes, array_agg(instrument_model) AS instrument_models, array_agg(instrument_serial) AS instrument_serials " \
                        "FROM (SELECT CAST(parameter_code AS varchar(20)), imos_data_code , d.mooring_id, d.instrument_id, s.instrument_id AS source, CAST(avg(depth) AS numeric(8,3)) AS depth, " \
                        "    trim(from make) AS instrument_make, trim(from model) AS instrument_model, trim(from serial_number) AS instrument_serial" \
                        "  FROM processed_instrument_data AS d JOIN instrument_data_files AS s ON (source_file_id = datafile_pk) JOIN instrument ON (d.instrument_id = instrument.instrument_id) " \
                        "   JOIN parameters ON (d.parameter_code = parameters.code) WHERE d.mooring_id = '" + mooring + "' AND quality_code not in ('BAD', 'INTERPOLATED') " \
                        "  GROUP BY parameter_code, imos_data_code, d.mooring_id, d.instrument_id, s.instrument_id, make, model, serial_number " \
                        "  ORDER BY 1, 2, depth, make, model, serial_number) AS a " \
                        "GROUP BY parameter_code, imos_data_code ORDER BY depths, parameter_code"

    select_time_range = "SELECT min(data_timestamp), max(data_timestamp) FROM processed_instrument_data WHERE mooring_id = '" + mooring + "'"

    select_data = "SELECT data_timestamp, parameter_value, trim(from quality_code), instrument_id, parameter_code FROM processed_instrument_data WHERE mooring_id = '" + mooring + "'"\
                  " AND instrument_id = %d AND parameter_code = '%s'" \
                  " ORDER BY data_timestamp, instrument_id, parameter_code"

    select_parameter = "SELECT trim(from netcdf_std_name) AS netcdf_std_name, trim(from netcdf_long_name) AS netcdf_long_name, trim(from units) AS units FROM parameters WHERE code = '%s'"

    select_mooring = "SELECT * FROM mooring WHERE mooring_id = '%s'"

    #
    # build the netCDF file
    #

    try:
        conn = psycopg2.connect(host="localhost", database="ABOS", user="pete", password="password")

        # get mooring info
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(select_mooring % mooring)
        mooring_info = cur.fetchone()
        print("mooring", mooring_info)

        # get netCDF attributes
        cur.execute("SELECT * FROM netcdf_attributes WHERE naming_authority IN ('OS', '*') AND mooring = '*' AND deployment = '*' AND instrument_id ISNULL AND parameter = '*'" )
        add_netcdf_attributes(ncOut, cur)
        cur.execute("SELECT * FROM netcdf_attributes WHERE naming_authority IN ('OS', '*') AND mooring = 'SOFS' AND deployment = '*' AND instrument_id ISNULL AND parameter = '*'" )
        add_netcdf_attributes(ncOut, cur)
        cur.execute("SELECT * FROM netcdf_attributes WHERE naming_authority IN ('OS', '*') AND mooring = '*' AND deployment = '" + mooring + "' AND instrument_id ISNULL AND parameter = '*'" )
        add_netcdf_attributes(ncOut, cur)

        # get time range for parameters
        cur.execute(select_time_range)
        row = cur.fetchone()
        print ('time range ', row)
        start = row[0]
        end = row[1]
        hrs = int((end - start).total_seconds()/3600)
        print("hours ", hrs)
        times = [start.replace(tzinfo=None) + timedelta(hours=x) for x in range(0, hrs)]

        #     TIME:axis = "T";
        #     TIME:calendar = "gregorian";
        #     TIME:long_name = "time";
        #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

        tDim = ncOut.createDimension("TIME", len(times))
        ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
        ncTimesOut.long_name = "time"
        ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
        ncTimesOut.calendar = "gregorian"
        ncTimesOut.axis = "T"
        # ncTimesOut[:] = date2num(time, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

        ncTimesOut[:] = date2num(times, units=ncTimesOut.units, calendar=ncTimesOut.calendar)

        # should we attributes from netcdf_attributes table, for deployment

        # Get parameters from the database
        cur.execute(select_parameters)
        print("The number of parameters: ", cur.rowcount)

        # storage for variable data and qc flags
        data = np.zeros((len(times)))
        qc = np.zeros(len(times), bytes)

        data_cur = conn.cursor()
        parameter_cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        row = cur.fetchone()

        while row is not None:
            print(row)

            # read metadata from parameters table
            parameter_cursor.execute(select_parameter % row['parameter_code'])
            parameter_row = parameter_cursor.fetchone()
            print("parameter :", parameter_row)

            for i in range(0, len(row['instruments'])):

                # add attributes for the instrument and parameter from the netcdf_attributes table

                # create a depth variable for each variable (needs a dimension as well)

                instruments = row[2]
                print ("index , instrument, depth", i, instruments[i], row[4][i], np.abs(float(row[4][i])))

                # create a variable name
                var_name = row[0]
                if len(row[2]) > 1:
                    var_name += '_' + str(row[2][i])

                # create the variable
                ncVarOut = ncOut.createVariable(var_name, "f4", ("TIME",), fill_value=np.nan, zlib=True)
                if parameter_row['netcdf_std_name'] is not None:
                    ncVarOut.standard_name = parameter_row['netcdf_std_name']
                ncVarOut.long_name = parameter_row['netcdf_long_name']
                ncVarOut.units = parameter_row['units']

                ncVarOut.sensor_manufacturer = row[5][i]
                ncVarOut.sensor_model = row[6][i]
                ncVarOut.sensor_serial_number = row[7][i]
                ncVarOut.sensor_nominal_depth = np.double(row[4][i])

                ncVarOut.ancillary_variables = var_name + "_QC"

                # create the QC aux variable
                ncVarOut_qc = ncOut.createVariable(var_name + "_QC", "i1", ("TIME",), fill_value=0, zlib=True)
                ncVarOut_qc.long_name = "quality flag for " + parameter_row['netcdf_long_name']

                ncVarOut_qc.conventions = "OceanSITES QC Flags"

                ncVarOut_qc.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 8, 9], "i1")
                ncVarOut_qc.flag_meanings = "unknown good_data probably_good_data potentially_correctable bad_data not_deployed nominal_value interpolated_value missing_value"

                # clear the data and qc varables
                ts_idx = 0
                data.fill(np.nan)
                qc.fill(9)

                # select the data from the processed instrument data table
                #print("select data", select_data % (instruments[i], row[0]))
                data_cur.execute(select_data % (instruments[i], row['parameter_code']))
                data_row = data_cur.fetchone()

                while data_row is not None:
                    if data_row is not None:
                        #print (data_row)
                        while ts_idx < len(times) and data_row[0].replace(tzinfo=None) > times[ts_idx]:
                            ts_idx += 1
                        if ts_idx < len(times):
                            data[ts_idx] = data_row[1]
                            qc[ts_idx] = qualityFlag[data_row[2]]

                    data_row = data_cur.fetchone()

                ncVarOut[:] = data
                ncVarOut_qc[:] = qc

            row = cur.fetchone()

        cur.close()
        data_cur.close()
        parameter_cursor.close()

    except (psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        if ncOut is not None:
            # add timespan attributes
            ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
            ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

            # add creating and history entry
            ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
            ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from ABOS database")

            ncOut.close()

    return outputName

if __name__ == "__main__":
    create(sys.argv[1])
