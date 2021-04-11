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
from netCDF4 import Dataset

#conn = psycopg2.connect("dbname=netcdf_files user=ubuntu")
#cur = conn.cursor()


if __name__ == "__main__":
    #file_path = 'http://thredds.aodn.org.au/thredds/dodsC/IMOS/DWM/SOTS/2018/IMOS_DWM-SOTS_AETVZ_20180303_SAZ47_FV00_SAZ47-20-2018-Aquadopp-Current-Meter-AQD-5961-1200m_END-20190322_C-20190719.nc'
    file_path = 'http://tds0.ifremer.fr/thredds/dodsC/CORIOLIS-OCEANSITES-GDAC-OBSDATA/ALOHA/OS_ACO_20111005-16-24_D_ADP5-4726m.nc'


    nc = Dataset(file_path, 'r')

    for coord_var in ('LATITUDE', ):
        var = nc.variables[coord_var]
        dims = var.dimensions
        print('dims : ', dims)
        print (var[:])