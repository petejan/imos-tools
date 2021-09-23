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


#print('Python %s on %s' % (sys.version, sys.platform))
import re
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), '..'))

#print(sys.path)

from netCDF4 import Dataset

import glob

import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.qc.spike_test
import ocean_dp.qc.rate_of_change
import ocean_dp.qc.manual_by_date
#import ocean_dp.processing.loess_smoother
import ocean_dp.processing.addPSAL
import ocean_dp.processing.add_density
import ocean_dp.processing.add_sigma_theta0_sm

import ocean_dp.file_name.find_file_with

# for each of the new files, process them

temp_qc_params = [
              {'depth': 0,    'global_max': 50, 'global_min': -20, 'climate_max': 30, 'climate_min': -10, 'spike_height': 5, 'rate_max': 100},
              {'depth': 10,   'global_max': 30, 'global_min': -2, 'climate_max': 20, 'climate_min': 6, 'spike_height': 2, 'rate_max': 80},
              {'depth': 600,  'global_max': 30, 'global_min': -2, 'climate_max': 16, 'climate_min': 5, 'spike_height': 2, 'rate_max': 80},
              {'depth': 1500, 'global_max': 30, 'global_min': -2, 'climate_max': 12, 'climate_min': 2, 'spike_height': 2, 'rate_max': 20},
              {'depth': 5000, 'global_max': 30, 'global_min': -2, 'climate_max': 5, 'climate_min': 0.8, 'spike_height': 0.1, 'rate_max': 3}
            ]

psal_qc_params = [
              {'depth': 0,    'global_max': 41, 'global_min': 2, 'climate_max': 35.5, 'climate_min': 34, 'spike_height': 0.4, 'rate_max': 30},
              {'depth': 400,  'global_max': 41, 'global_min': 2, 'climate_max': 35.5, 'climate_min': 34, 'spike_height': 0.4, 'rate_max': 30},
              {'depth': 1500, 'global_max': 41, 'global_min': 2, 'climate_max': 35.5, 'climate_min': 34, 'spike_height': 0.2, 'rate_max': 10},
              {'depth': 5000, 'global_max': 41, 'global_min': 2, 'climate_max': 34.8, 'climate_min': 34.3, 'spike_height': 0.02, 'rate_max': 1.2}
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

    # what to QC
    has_temp = False
    if 'TEMP' in ds.variables:
        has_temp = True
    has_cndc = False
    if 'CNDC' in ds.variables:
        has_cndc = True
    has_psal = False
    if 'PSAL' in ds.variables:
        has_psal = True

    print('variables temp,cndc,psal', has_temp, has_cndc, has_psal)

    is_pumped = False
    if re.match(r'SBE37SMP.*', ds.instrument_model):
        is_pumped = True
    if re.match(r'SBE16.*', ds.instrument_model):
        is_pumped = True

    # QC report manual flagging
    manual_flag = None
    maunal_date_start = None
    maunal_date_end = None
    manual_reason = None
    manual_var = None

    sn = ds.instrument_serial_number
    model = ds.instrument_model
    deployment = ds.deployment_code

    ds.close()

    # Pulse 6,7,8 SOFS 1,2 Vemco Mini sensors with SN < 10000 -> flag 3
    if model == 'Minilog-T':
        manual_flag = 3
        manual_reason = 'difference to higher precision sensors'
    # Pulse 8 SBE16plusV2 battery fail (after 2012-01-30 15:25) -> flag 3 after Pressure fails
    if model == 'SBE16plus' and deployment == 'Pulse-8-2011':
        manual_flag = 4
        manual_reason = 'battery failed'
        maunal_date_start = '2012-01-30 15:25:00'
    # Pulse 9 SBE16plusV2 battery fail (after 2012-12-29 12:30) -> flag 3 after Pressure fails
    if model == 'SBE16plusV2' and deployment == 'Pulse-9-2012':
        manual_flag = 4
        manual_reason = 'battery failed'
        maunal_date_start = '2012-12-29 12:30:00'
    # SOFS-7.5 70 and 75m Starmon mini -> flag 4
    if (sn == '4052' or sn == '4053') and model == 'Starmon mini' and deployment == 'SOFS-7.5-2018':
        manual_flag = 4
        manual_reason = 'sensor data noisy'
    # SOFS-8 55m and 320m show bias -> flag 4
    if (sn == '5304' or sn == '5320') and model == 'Starmon mini' and deployment == 'SOFS-8-2019':
        manual_flag = 4
        manual_reason = 'sensor data noisy'

    mark_rest = False
    # SOFS-7.5-2018 SBE37 ODO at 200m salinity jump after Feb 09
    if model == 'SBE37SMP-ODO-RS232' and deployment == 'SOFS-7.5-2018' and sn == '03715971':
        manual_flag = 4
        manual_var = 'PSAL'
        manual_reason = 'density inversion'
        maunal_date_start = '2019-02-09 00:00:00'
        mark_rest = True

    # SAZ 15 2013-02-19 2013-03-06
    if model == 'SBE37SM-RS232' and deployment == 'SAZ47-15-2012' and sn == '03708597':
        manual_flag = 4
        manual_var = 'PSAL'
        manual_reason = 'drop in salinity, cell contamination'
        maunal_date_start = '2013-02-19 00:00:00'
        maunal_date_end = '2013-03-06 00:00:00'
    # SAZ 16 2014-01-20 2014-01-24
    if model == 'SBE37-SM' and deployment == 'SAZ47-16-2013' and sn == '1778':
        manual_flag = 4
        manual_var = 'PSAL'
        manual_reason = 'drop in salinity, cell contamination'
        maunal_date_start = '2014-01-20 00:00:00'
        maunal_date_end = '2014-01-24 00:00:00'
    # SAZ 17 2015-05-01 2015-05-03
    if model == 'SBE37SM-RS232' and deployment == 'SAZ47-17-2015' and sn == '03708985':
        manual_flag = 4
        manual_var = 'PSAL'
        manual_reason = 'drop in salinity, cell contamination'
        maunal_date_start = '2015-05-01 00:00:00'
        maunal_date_end = '2015-05-03 00:00:00'
    # SAZ 18 2017-01-05 2015-01-23
    if model == 'SBE37SM-RS232' and deployment == 'SAZ47-18-2016' and sn == '03708597':
        manual_flag = 4
        manual_var = 'PSAL'
        manual_reason = 'drop in salinity, cell contamination'
        maunal_date_start = '2017-01-12 00:00:00'
        maunal_date_end = '2017-01-23 00:00:00'
    # SOFS-5 add data bad
#    if model == 'SBE37SM-RS485' and deployment == 'SOFS-5-2015' and sn == '03707409':
#        manual_flag = 3
#        manual_var = 'PSAL'
#        manual_reason = 'calibration issue, reading high'
    # SOFS-9 add data bad
    if model == 'SBE37SMP-ODO-RS232' and deployment == 'SOFS-9-2020' and sn == '03715971':
        manual_flag = 4
        manual_var = 'PSAL'
        manual_reason = 'high salinity'
        maunal_date_start = '2020-08-01 00:00:00'
        maunal_date_end = '2020-10-25 00:00:00'
        mark_rest = True
    # SOFS-9 70m Starmon mini -> flag 4
    if ( sn == '4052') and model == 'Starmon mini' and deployment == 'SOFS-9-2020':
        manual_flag = 4
        manual_reason = 'sensor data noisy'

    if not has_temp:
        continue

    print(ndepth)

    f = ocean_dp.qc.add_qc_flags.add_qc([fn])
    f = ocean_dp.qc.in_out_water.in_out_water(f)

    # temperature QC
    for q in temp_qc_params:
        if q['depth'] > ndepth:
            break

    print('temp_qc:', q)

    f = ocean_dp.qc.global_range.global_range(f, 'TEMP', q['global_max'], q['global_min'])
    f = ocean_dp.qc.global_range.global_range(f, 'TEMP', q['climate_max'], q['climate_min'], 3)
    f = ocean_dp.qc.spike_test.spike_test(f, 'TEMP', q['spike_height'], 3)
    f = ocean_dp.qc.rate_of_change.rate_of_change(f, 'TEMP', q['rate_max'], 3)

    if has_cndc:
        f = ocean_dp.qc.global_range.global_range(f, 'CNDC', 4.5, 3)

        # salinity QC
        for q in psal_qc_params:
            if q['depth'] > ndepth:
                break

        print('psal_qc:', q)

        if has_psal == False:
            f = ocean_dp.processing.addPSAL.add_psal(f[0])

        f = ocean_dp.qc.global_range.global_range(f, 'PSAL', q['global_max'], q['global_min'])
        f = ocean_dp.qc.global_range.global_range(f, 'PSAL', q['climate_max'], q['climate_min'], 3)
        f = ocean_dp.qc.spike_test.spike_test(f, 'PSAL', q['spike_height'], 3)
        f = ocean_dp.qc.rate_of_change.rate_of_change(f, 'PSAL', q['rate_max'], 3)

        if not is_pumped:
            f = ocean_dp.processing.add_density.add_density(f[0])
            if ndepth > 4000:
                limit = 0.001
            else:
                limit = 0.02
            f = ocean_dp.processing.add_sigma_theta0_sm.add_sigma_theta0_sm(f[0], limit=limit)

    if manual_flag:
        f = ocean_dp.qc.manual_by_date.maunal(f, manual_var, maunal_date_start, manual_flag, manual_reason, end_str=maunal_date_end)
        if model == 'SBE37SMP-ODO-RS232' and deployment == 'SOFS-9-2020' and sn == '03715971':
            manual_flag = 2
            manual_var = 'PSAL'
            manual_reason = 'high salinity at start, rest of data suspect'
            f = ocean_dp.qc.manual_by_date.maunal(f, manual_var, None, manual_flag, manual_reason, end_str=None)
        if model == 'SBE37SMP-ODO-RS232' and deployment == 'SOFS-7.5-2018' and sn == '03715971':
            manual_flag = 2
            manual_var = 'PSAL'
            manual_reason = 'high salinity at end, reset of data suspect'
            f = ocean_dp.qc.manual_by_date.maunal(f, manual_var, None, manual_flag, manual_reason, end_str=None)

    ds = Dataset(f[0], 'a')
    ds.references += '; Jansen P, Weeding B, Shadwick EH and Trull TW (2020). Southern Ocean Time Series (SOTS) Quality Assessment and Control Report Temperature Records Version 1.0. CSIRO, Australia. DOI: 10.26198/gfgr-fq47 (https://doi.org/10.26198/gfgr-fq47)'
    ds.close()

    #f = ocean_dp.processing.loess_smoother.smooth(f)

    #qc_files.extend(f)

