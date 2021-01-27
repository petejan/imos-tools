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
from netCDF4 import Dataset
#print('Python %s on %s' % (sys.version, sys.platform))

import glob
import os

# for each of the new files, process them
if os.path.isfile(sys.argv[1]):
    ncFiles = [sys.argv[1]]
else:
    path = sys.argv[1] + "/"
    ncFiles = glob.glob(os.path.join(path, '*FV01*.nc'))

atts_to_list = ['file', 'platform_code', 'deployment_code', 'instrument', 'instrument_serial_number', 'instrument_nominal_depth', 'time_coverage_end', 'time_coverage_start', 'time_deployment_end', 'time_deployment_start']
print(','.join(atts_to_list))

for fn in ncFiles:
    nc = Dataset(fn, 'r')

    att_list = [fn]
    for att in atts_to_list[1:]:
        att_list.append(str(nc.getncattr(att)))

    print (','.join(att_list))



