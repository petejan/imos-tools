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

import glob2 as glob
import os
import re

ncFiles = []
# for each of the new files, process them
for f in sys.argv[1:]:
    #print(f)
    ncFiles.extend(glob.glob(f))

atts_to_list = ['file', 'platform_code', 'deployment_code', 'instrument', 'instrument_serial_number', 'instrument_nominal_depth', 'time_coverage_end', 'time_coverage_start', 'time_deployment_end', 'time_deployment_start']

pump_map = r"SBE16plus|SBE37SMP"

hdr = []
hdr.extend(atts_to_list)
hdr.append('has conductivity')
hdr.append('conductivity calibration date')
hdr.append('is pumped')

print(','.join(hdr))

for fn in ncFiles:
    #print(fn)
    nc = Dataset(fn, 'r')

    att_list = [os.path.basename(fn)]
    for att in atts_to_list[1:]:
        att_list.append(str(nc.getncattr(att)))

    if 'CNDC' in nc.variables:
        att_list.append('yes')
        if 'calibration_CalibrationDate' in nc.variables['CNDC'].ncattrs() :
            att_list.append(nc.variables['CNDC'].calibration_CalibrationDate)
        else:
            att_list.append('')
    else:
        att_list.append('')
        att_list.append('')
    #print(re.search(pump_map, nc.instrument), nc.instrument)
    if re.search(pump_map, nc.instrument):
        att_list.append('yes')
    else:
        att_list.append('')


    print(','.join(att_list))

    nc.close()



