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
import ocean_dp.qc.select_in_water
import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.global_range
import ocean_dp.aggregation.copyDataset
import ocean_dp.processing.pressure_interpolator

path = sys.argv[1] + "/"

print ('file path : ', path)

print('trim')

file_trim = []

FV00_files = glob.glob(os.path.join(path, "IMOS*FV00*.nc"))
for fn in FV00_files:
    nv = ocean_dp.qc.select_in_water.select_in_water([fn])
    file_trim.extend(nv)
    print(nv[0])

print('add qc')

file_qc = []
for fn in file_trim:
    nv = ocean_dp.qc.add_qc_flags.add_qc([fn])
    file_qc.extend(nv)
    print(nv[0])

print('global range')

file_glob = []
for fn in file_qc:
    nv = ocean_dp.qc.global_range.global_range(fn, 'TEMP', 40, -2)
    file_glob.append(nv)
    print(nv)

pres_file = ocean_dp.aggregation.copyDataset.aggregate(file_glob, ['PRES'])

interp_file = ocean_dp.processing.pressure_interpolator.pressure_interpolator(file_glob, pres_file)

temp_agg_file = ocean_dp.aggregation.copyDataset.aggregate(interp_file, ['TEMP', 'PRES'])




