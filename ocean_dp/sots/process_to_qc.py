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

#print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import glob
import os
import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.qc.manual_by_date

import ocean_dp.file_name.find_file_with

# for each of the new files, process them
if os.path.isfile(sys.argv[1]):
    ncFiles = [sys.argv[1]]
else:
    path = sys.argv[1] + "/"
    ncFiles = glob.glob(os.path.join(path, '*FV00*.nc'))
    print ('file path : ', path)

qc_files = []
for fn in ncFiles:
    print ("processing ", fn)

    f = ocean_dp.qc.add_qc_flags.add_qc([fn])
    f = ocean_dp.qc.in_out_water.in_out_water(f)
    qc_files.extend(f)

# apply global range test

# TEMP
temp_files = ocean_dp.file_name.find_file_with.find_variable(qc_files, 'TEMP')
for fn in temp_files:
    ocean_dp.qc.global_range.global_range([fn], 'TEMP', 20, 0, 4)

# PSAL
psal_files = ocean_dp.file_name.find_file_with.find_variable(qc_files, 'PSAL')
for fn in psal_files:
    ocean_dp.qc.global_range.global_range([fn], 'PSAL', 38, 32, 4)

# DENSITY
density_files = ocean_dp.file_name.find_file_with.find_variable(qc_files, 'DENSITY')
for fn in density_files:
   ocean_dp.qc.global_range.global_range([fn], 'DENSITY', 1000, 1100, 4)  # https://oceanobservatories.org/wp-content/uploads/2015/09/1341-10004_Data_Product_SPEC_GLBLRNG_OOI.pdf

pulse_files = qc_files
temp_files = ocean_dp.file_name.find_file_with.find_variable(pulse_files, 'TEMP')
fv01_files = ocean_dp.file_name.find_file_with.find_global(temp_files, 'file_version', 'Level 1 - Quality Controlled Data')

print('FV01 files:')
for f in fv01_files:
    print(f)

p8_34 = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'deployment_code', 'Pulse-8-2011')
p8_34 = ocean_dp.file_name.find_file_with.find_global(p8_34, 'instrument_serial_number', '01606330')

print('p8_34 files:')
for f in p8_34:
    print(f)

if p8_34:
    ocean_dp.qc.manual_by_date.maunal(p8_34, after_str='2012-01-30 00:00:00', flag=4, reason='low battery')

p9_38 = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'deployment_code', 'Pulse-9-2012')
p9_38 = ocean_dp.file_name.find_file_with.find_global(p9_38, 'instrument_serial_number', '01606331')

print('p9_38 files:')
for f in p9_38:
    print(f)

if p9_38:
    ocean_dp.qc.manual_by_date.maunal(p9_38, after_str='2012-12-29 12:00:00', flag=4, reason='low battery')
