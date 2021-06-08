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

from netCDF4 import Dataset

sys.path.extend(['.'])

import glob

import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.qc.spike_test
import ocean_dp.qc.rate_of_change
import ocean_dp.qc.manual_by_date
import ocean_dp.processing.loess_smoother

import ocean_dp.file_name.find_file_with

# for each of the new files, process them

qc_params = [
              {'depth': 0,    'global_max': 50, 'global_min': -20, 'climate_max': 30, 'climate_min': -10, 'spike_height': 5, 'rate_max': 100},
              {'depth': 10,   'global_max': 30, 'global_min': -2, 'climate_max': 20, 'climate_min': 6, 'spike_height': 2, 'rate_max': 80},
              {'depth': 600,  'global_max': 30, 'global_min': -2, 'climate_max': 16, 'climate_min': 5, 'spike_height': 2, 'rate_max': 80},
              {'depth': 1500, 'global_max': 30, 'global_min': -2, 'climate_max': 12, 'climate_min': 2, 'spike_height': 2, 'rate_max': 20},
              {'depth': 5000, 'global_max': 30, 'global_min': -2, 'climate_max': 5, 'climate_min': 0.8, 'spike_height': 0.1, 'rate_max': 3}
            ]

ncFiles = []
for f in sys.argv[1:]:
    ncFiles.extend(glob.glob(f))

qc_files = []
for fn in ncFiles:
    print ("processing ", fn)
    ds = Dataset(fn, 'r')

    ndepth_var = ds.variables['NOMINAL_DEPTH']
    ndepth = ndepth_var[:]
    has_temp = False
    if 'TEMP' in ds.variables:
        has_temp = True

    # QC report manual flagging
    manual_flag = None
    maunal_date = None
    manual_reason = None
    # Pulse 6,7,8 SOFS 1,2 Vemco Mini sensors with SN < 10000 -> flag 3
    if ds.instrument_model == 'Minilog-T':
        manual_flag = 3
        manual_reason = 'difference to higher precision sensors'
    # Pulse 8 SBE16plusV2 battery fail (after 2012-01-30 15:25) -> flag 3 after Pressure fails
    if ds.instrument_model == 'SBE16plus' and ds.deployment_code == 'Pulse-8-2011':
        manual_flag = 4
        manual_reason = 'battery failed'
        maunal_date = '2012-01-30 15:25:00'
    # Pulse 9 SBE16plusV2 battery fail (after 2012-12-29 12:30) -> flag 3 after Pressure fails
    if ds.instrument_model == 'SBE16plusV2' and ds.deployment_code == 'Pulse-9-2012':
        manual_flag = 4
        manual_reason = 'battery failed'
        maunal_date = '2012-12-29 12:30:00'
    # SOFS-7.5 70 and 75m Starmon mini -> flag 4
    if (ds.instrument_serial_number == '4052' or ds.instrument_serial_number == '4053') and ds.instrument_model == 'Starmon mini' and ds.deployment_code == 'SOFS-7.5-2018':
        manual_flag = 4
        manual_reason = 'sensor data noisy'
    # SOFS-8 55m and 320m show bias -> flag 4
    if (ds.instrument_serial_number == '5304' or ds.instrument_serial_number == '5320') and ds.instrument_model == 'Starmon mini' and ds.deployment_code == 'SOFS-8-2019':
        manual_flag = 4
        manual_reason = 'sensor data noisy'

    ds.close()

    if not has_temp:
        continue

    print(ndepth)
    for q in qc_params:
        if q['depth'] > ndepth:
            break

    print(q)

    f = ocean_dp.qc.add_qc_flags.add_qc([fn])
    f = ocean_dp.qc.in_out_water.in_out_water(f)
    f = ocean_dp.qc.global_range.global_range(f, 'TEMP', q['global_max'], q['global_min'])
    f = ocean_dp.qc.global_range.global_range(f, 'TEMP', q['climate_max'], q['climate_min'], 3)
    f = ocean_dp.qc.spike_test.spike_test(f, 'TEMP', q['spike_height'], 3)
    f = ocean_dp.qc.rate_of_change.rate_of_change(f, 'TEMP', q['rate_max'], 3)

    if manual_flag:
        f = ocean_dp.qc.manual_by_date.maunal(f, 'TEMP', None, manual_flag, manual_reason)

    ds = Dataset(f[0], 'a')
    ds.references += '; Jansen P, Weeding B, Shadwick EH and Trull TW (2020). Southern Ocean Time Series (SOTS) Quality Assessment and Control Report Temperature Records Version 1.0. CSIRO, Australia. DOI: 10.26198/gfgr-fq47 (https://doi.org/10.26198/gfgr-fq47)'
    ds.close()

    #f = ocean_dp.processing.loess_smoother.smooth(f)

    #qc_files.extend(f)

