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


def print_line(typ, var_name, dep_code, model, serial_number, time_deployment, time_recovry, variable_name, variable_dims, variable_shape, attribute_name, type, value):
    if type == 'str':
        attribute_value = value.replace("\"", "'").replace(",", "\\,")
    model = model.replace(",", "")
    print("%s,%s,%s,%s,%s,%s,%s,%s,\"%s\",\"%s\",%s,%s,\"%s\"" % (typ, var_name, dep_code, model, serial_number, time_deployment, time_recovry, variable_name, variable_dims, variable_shape, attribute_name, type, value))


def get_db_metadata(select):
    conn = psycopg2.connect(host="localhost", database="IMOS-DEPLOY", user="pete", password="password")

    # create a cursor
    cur_site = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur_inst = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # select deployment specific attributes
    cur_site.execute("select * from cmdfieldsite join cmdsitelocation on (cmdfsid = cmdsl_fsid) "
                " join cmddeploymentdetails on (cmdslid = cmddd_slid) "
                "where cmdddname like '"+select+"' "
                "order by cmdslname, cmdddname")

    # select instrument specific attributes
    cur_inst.execute("select * from cmdfieldsite join cmdsitelocation on (cmdfsid = cmdsl_fsid) "
                " join cmddeploymentdetails on (cmdslid = cmddd_slid) "
                " join cmditemlink on (cmdddid = cmdil_ddid) "
                " join cmditemdetail on (cmdidid = cmdil_childidid)	"
                "where cmdddname like '"+select+"' "
                "order by cmdslname, cmdddname, cmdidbrand, cmdidmodel, cmdidserialnumber")

    # output variable : LATITUDE, LONGITUDE, NOMINAL_DEPTH
    #        globals : geospatial_lat_max, geospatial_lon_max, geospatial_vertical_max, site_nominal_depth, deployment_code, instrument_nominal_depth

    print("rec_type, var_name, deployment_code, model, serial_number, time_deployment, time_recovery, variable_name, variable_dims, variable_shape, attribute_name, type, value")

    for row in cur_inst:
        #print(row['cmdslname'], row['cmdddname'], row['cmdddlatitude'], row['cmdddlongitude'], row['cmddddeploymentdate'], row['cmdddrecoverydate'], row['cmdidmodel'], row['cmdidserialnumber'], row['cmdildepth'], row['cmdiddescription'])
        print_line("GLOBAL", "", "", row['cmdidmodel'], row['cmdidserialnumber'],
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
                   "", 'deployment_code', "str", row['cmdddname'])
    print()

    for row in cur_site:
        print_line("VAR", "LATITUDE", row['cmdddname'], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "LATITUDE", "()", "()", "", "float64",
                   row['cmdddlatitude'])
        print_line("VAR", "LONGITUDE", row['cmdddname'], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "LONGITUDE", "()", "()", "", "float64",
                   row['cmdddlongitude'])
        # print_line("GLOBAL", "", row["cmdddname"], "", "",
        #            row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
        #            "", 'geospatial_lat_max', "float64", row['cmdddlatitude'])
        # print_line("GLOBAL", "", row["cmdddname"], "", "",
        #            row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
        #            "", 'geospatial_lat_min', "float64", row['cmdddlatitude'])
        # print_line("GLOBAL", "", row["cmdddname"], "", "",
        #            row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
        #            "", 'geospatial_lon_max', "float64", row['cmdddlongitude'])
        # print_line("GLOBAL", "", row["cmdddname"], "", "",
        #            row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
        #            "", 'geospatial_lon_min', "float64", row['cmdddlongitude'])

        # print_line("GLOBAL", "", row["cmdddname"], "", "",
        #            row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
        #            "", 'site_nominal_depth', "float64", row['cmdsldepth'])
    print()

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
    cur_site.scroll(0,  mode='absolute')
    for row in cur_site:
        print_line("GLOBAL", "", row["cmdddname"], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
                   "", 'site_nominal_depth', "float64", row['cmdsldepth'])
        print_line("GLOBAL", "", row["cmdddname"], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
                   "", 'time_deployment_start', "str", row['cmddddateinposition'].strftime(ncTimeFormat))
        print_line("GLOBAL", "", row["cmdddname"], "", "",
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
                   "", 'time_deployment_end', "str", row['cmddddateoutposition'].strftime(ncTimeFormat))
        # print_line("GLOBAL", "", row["cmdddname"], "", "",
        #            row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
        #            "", 'site_nominal_depth', "float64", row['cmdsldepth'])
    print()

    cur_inst.scroll(0, mode='absolute')  # rewind the cursor
    for row in cur_inst:
        print_line("VAR", "NOMINAL_DEPTH", row['cmdddname'], row['cmdidmodel'], row['cmdidserialnumber'],
                   row['cmddddeploymentdate'], row['cmdddrecoverydate'],
                   "NOMINAL_DEPTH", "()", "()", "", "float64", row['cmdildepth'])

        # print_line("GLOBAL", "", row["cmdddname"], row['cmdidmodel'], row['cmdidserialnumber'],
        #            row['cmddddeploymentdate'], row['cmdddrecoverydate'], "", "",
        #            "", 'instrument_nominal_depth', "float64", row['cmdildepth'])


if __name__ == "__main__":
    get_db_metadata(sys.argv[1])
