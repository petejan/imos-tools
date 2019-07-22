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


def print_line(typ, var_name, dep_code, model, serial_number, time_deployment, time_recovry, variable_name, variable_dims, variable_shape, variable_type, variable_value, attribute_name, attribute_type, attribute_value):
    if attribute_type == 'str':
        attribute_value = attribute_value.replace("\"", "'")
    print("%s,%s,%s,%s,%s,%s,%s,%s,\"%s\",\"%s\",%s,%s,%s,%s,\"%s\"" % (typ, var_name, dep_code, model, serial_number, time_deployment, time_recovry, variable_name, variable_dims, variable_shape, variable_type, variable_value, attribute_name, attribute_type, attribute_value))


def get_db_metadata(select):
    conn = psycopg2.connect(host="localhost", database="IMOS-DEPLOY", user="pete", password="password")

    # create a cursor
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # output variable : LATITUDE, LONGITUDE, NOMINAL_DEPTH
    #        globals : geospatial_lat_max, geospatial_lon_max, geospatial_vertical_max, site_nominal_depth, deployment_code, instrument_nominal_depth

    print("rec_type, var_name, deployment_code, model, serial_number, time_deployment, time_recovery, variable_name, variable_dims, variable_shape, variable_type, variable_value, attribute_name, attribute_type, attribute_value")

    # select instrument specific attributes
    cur.execute("select * from cmdfieldsite join cmdsitelocation on (cmdfsid = cmdsl_fsid) "
                " join cmddeploymentdetails on (cmdslid = cmddd_slid) "
                " join cmditemlink on (cmdddid = cmdil_ddid) "
                " join cmditemdetail on (cmdidid = cmdil_childidid)	"
                "where cmdddname like '"+select+"' "
                "order by cmdslname, cmdddname, cmdidbrand, cmdidmodel, cmdidserialnumber")

    for row in cur:
        #print(row['cmdslname'], row['cmdddname'], row['cmdddlatitude'], row['cmdddlongitude'], row['cmddddeploymentdate'], row['cmdddrecoverydate'], row['cmdidmodel'], row['cmdidserialnumber'], row['cmdildepth'], row['cmdiddescription'])
        print_line("GLOBAL", "", "", row['cmdidmodel'], row['cmdidserialnumber'],
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "", "", "",
                   "", 'deployment_code', "str", row['cmdddname'])

        print_line("VAR", "NOMINAL_DEPTH", row['cmdddname'], row['cmdidmodel'], row['cmdidserialnumber'],
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'],
                   "NOMINAL_DEPTH", "()", "()", "float64", row['cmdildepth'], "", "", "")
        print_line("GLOBAL", "", row["cmdddname"], row['cmdidmodel'], row['cmdidserialnumber'],
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "", "", "",
                   "", 'instrument_nominal_depth', "float64", row['cmdildepth'])

    # select deployment specific attributes
    cur.execute("select * from cmdfieldsite join cmdsitelocation on (cmdfsid = cmdsl_fsid) "
                " join cmddeploymentdetails on (cmdslid = cmddd_slid) "
                "where cmdddname like '"+select+"' "
                "order by cmdslname, cmdddname")

    for row in cur:
        print_line("VAR", "LATITUDE", row['cmdddname'], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "LATITUDE", "()", "()", "float64",
                   row['cmdddlatitude'], "", "", "")
        print_line("VAR", "LONGITUDE", row['cmdddname'], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "LONGITUDE", "()", "()", "float64",
                   row['cmdddlongitude'], "", "", "")
        print_line("GLOBAL", "", row["cmdddname"], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "", "", "",
                   "", 'geospatial_lat_max', "float64", row['cmdddlatitude'])
        print_line("GLOBAL", "", row["cmdddname"], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "", "", "",
                   "", 'geospatial_lat_min', "float64", row['cmdddlatitude'])
        print_line("GLOBAL", "", row["cmdddname"], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "", "", "",
                   "", 'geospatial_lon_max', "float64", row['cmdddlongitude'])
        print_line("GLOBAL", "", row["cmdddname"], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "", "", "",
                   "", 'geospatial_lon_min', "float64", row['cmdddlongitude'])
        print_line("GLOBAL", "", row["cmdddname"], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "", "", "",
                   "", 'site_nominal_depth', "float64", row['cmdsldepth'])


if __name__ == "__main__":
    get_db_metadata(sys.argv[1])
