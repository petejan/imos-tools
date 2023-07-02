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

from datetime import datetime, timedelta
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct
import xml.etree.ElementTree as ET
import untangle
import xmltodict

from collections import OrderedDict

def parse_cal(netCDF, cal):

    ncOut = Dataset(netCDF, 'a', format='NETCDF4_CLASSIC')

    ncVarUVWL = ncOut.variables['UV_DIM']

    if 'NO3' in ncOut.variables:
        ncVarNO3 = ncOut.variables['NO3']
        ncVarSWA = ncOut.variables['SWA']
        ncVarTSWA = ncOut.variables['TAWS']
        ncVarI0 = ncOut.variables['I0']
    else:
        ncVarNO3 = ncOut.createVariable('NO3', "f4", ("UV_DIM",), fill_value=np.nan, zlib=False)  # fill_value=nan otherwise defaults to max
        ncVarSWA = ncOut.createVariable('SWA', "f4", ("UV_DIM",), fill_value=np.nan, zlib=False)  # fill_value=nan otherwise defaults to max
        ncVarTSWA = ncOut.createVariable('TAWS', "f4", ("UV_DIM",), fill_value=np.nan, zlib=False)  # fill_value=nan otherwise defaults to max
        ncVarI0 = ncOut.createVariable('I0', "f4", ("UV_DIM",), fill_value=np.nan, zlib=False)  # fill_value=nan otherwise defaults to max

    with open(cal, 'r', errors='ignore') as fp:
        line = fp.readline()
        cnt = 0
        while line:
            if line.startswith('E'):
                split = line.split(',')
                wl = float(split[1])
                no3 = float(split[2])
                swa = float(split[3])
                tswa = float(split[4])
                i0 = float(split[5])

                print('file wl', ncVarUVWL[cnt], 'cal', wl)
                ncVarUVWL[cnt] = wl

                ncVarNO3[cnt] = no3
                ncVarSWA[cnt] = swa
                ncVarTSWA[cnt] = tswa
                ncVarI0[cnt] = i0

                cnt += 1
            line = fp.readline()

    ncOut.close()

    return


if __name__ == "__main__":

    # arguments are <netCDF file> <.cal file>
    parse_cal(sys.argv[1], sys.argv[2])
