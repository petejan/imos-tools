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

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import glob
import os
import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water

# for each of the new files, process them
if os.path.isfile(sys.argv[1]):
    ncFiles = [sys.argv[1]]
else:
    path = sys.argv[1] + "/"
    ncFiles = glob.glob(os.path.join(path, '*.nc'))
    print ('file path : ', path)

for fn in ncFiles:
    print ("processing " , fn)

    ocean_dp.qc.add_qc_flags.add_qc([fn], 'TEMP')
    ocean_dp.qc.in_out_water.in_out_water([fn], 'TEMP')
