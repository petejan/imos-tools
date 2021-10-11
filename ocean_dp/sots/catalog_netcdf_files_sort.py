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

import numpy as np
from cftime import date2num, num2date
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

atts_to_list = ['file', 'platform_code', 'deployment_code', 'instrument', 'instrument_model', 'instrument_serial_number', 'instrument_nominal_depth', 'time_coverage_end', 'time_coverage_start', 'time_deployment_end', 'time_deployment_start']

pump_map = r"SBE16plus|SBE37SMP|SeaFET"

file_list = []
var_list = []
flags_list = []

var_to_count = 'PSAL'

for fn in ncFiles:
    print('working on', fn)
    nc = Dataset(fn, 'r')
    file_dict = {}

    file_dict['file'] = os.path.basename(fn)
    for att in atts_to_list[1:]:
        file_dict[att] = str(nc.getncattr(att))

    if 'CNDC' in nc.variables:
        file_dict['cndc'] = 'yes'
        if 'calibration_CalibrationDate' in nc.variables['CNDC'].ncattrs() :
            file_dict['calibration_date'] = nc.variables['CNDC'].calibration_CalibrationDate
        else:
            file_dict['calibration_date'] = ''
    else:
        file_dict['cndc'] = 'no'

    if 'PRES' in nc.variables:
        file_dict['pressure'] = 'yes'
    else:
        file_dict['pressure'] = 'no'

    #print(re.search(pump_map, nc.instrument), nc.instrument)
    if re.search(pump_map, nc.instrument):
        file_dict['pumped'] = 'yes'
    else:
        file_dict['pumped'] = 'no'

    # deal with TIME
    var_time = nc.variables["TIME"]

    # create the time window around the time_deployment_start and time_deployment_end
    datetime_deploy_start = datetime.strptime(nc.getncattr('time_deployment_start'), '%Y-%m-%dT%H:%M:%SZ')
    datetime_deploy_end = datetime.strptime(nc.getncattr('time_deployment_end'), '%Y-%m-%dT%H:%M:%SZ')

    num_deploy_start = date2num(datetime_deploy_start, units=var_time.units)
    num_deploy_end = date2num(datetime_deploy_end, units=var_time.units)

    # read existing times, find sample rate
    time = var_time[:]

    # create mask for deployment time
    deployment_msk = (time > num_deploy_start) & (time < num_deploy_end)

    datetime_time = num2date(time, units=var_time.units)
    datetime_time_deployment = datetime_time[deployment_msk]
    time_deployment = time[deployment_msk]

    # use the mid point sample rate, as it may change at start/end
    n_mid = np.int(len(time_deployment)/2)
    t_mid0 = datetime_time_deployment[n_mid]
    t_mid1 = datetime_time_deployment[n_mid+1]

    sample_rate_mid = t_mid1 - t_mid0
    #print('sample rate mid', sample_rate_mid.total_seconds(), '(seconds)')

    file_dict['sample_rate'] = "{:.0f}".format(sample_rate_mid.total_seconds())

    #print(file_dict)

    file_list.append(file_dict)

    aux_var_list = []
    use_vars =[]
    for stats_var in nc.variables:
        if stats_var == var_to_count:
            #print('finding vars that have TIME and are not aux variables', stats_var)

            var_dict = dict(file_dict) # copy file information

            var = nc.variables[stats_var]
            if 'TIME' in var.dimensions and 'ancillary_variables' in var.ncattrs():
                use_vars.append(stats_var)
                aux_vars = var.ancillary_variables
                aux_vars_split = aux_vars.split(" ")
                aux_var_list.extend(aux_vars_split)

                var_dict['variable'] = stats_var

                flag_values = None
                # stats for each QC var
                for v in aux_vars_split:
                    var = nc.variables[v]
                    qc = var[:]

                    # flags are counted hierarchical, so only count flags which did not fail previous test
                    # _quality_control is the final flag value
                    if v.endswith('_quality_control'):
                        prev_tests_qc = np.zeros_like(qc)

                    flags_dict = dict(file_dict)  # copy file information
                    flags_dict['variable'] = v

                    # keep the list of flag values if the qc variable has one for the next flag
                    if 'flag_values' in var.ncattrs():
                        flag_values = var.flag_values

                    # count each flag value in the list of flag values
                    for qc_value in flag_values:
                        msk = prev_tests_qc < qc_value
                        flags_dict['samples'] = len(qc[msk & (prev_tests_qc < 6) & (qc < 6)])
                        count4 = np.zeros_like(qc[msk])
                        count4[qc[msk] == qc_value] = 1
                        s4 = sum(count4)

                        #print(stats_var, 'qc stats for', v, qc_value, 'count', s4)
                        flags_dict['count-'+str(qc_value)] = s4

                    if not v.endswith('_quality_control'):
                        prev_tests_qc = np.max([prev_tests_qc, qc], axis=0)

                    flags_list.append(flags_dict)
                    print('flags_dict', flags_dict)

                var_list.append(var_dict)

    #print('use vars', use_vars)
    #print('aux vars', aux_var_list)

    nc.close()

# sort the list by deployment date (then deployment code) , then depth, then instrument serial number (for instruments deployed at the same depth)

flags_list_sorted = sorted(flags_list, key=lambda k: (k['time_deployment_start'], k['deployment_code'], float(k['instrument_nominal_depth']), k['instrument_serial_number']))

if False:
    print("Deployment, Depth*, Instrument:Serial #, Pump/Pres, Sampling (s)")

    for f in file_list_sorted:
        if True:#f['cndc'] == 'yes':
            print("{:s}, {:.0f}, {:s}:{:s}, {:s}/{:s}, {:s}".format(f['deployment_code'],
                                                                     float(f['instrument_nominal_depth']),
                                                                     f['instrument_model'],
                                                                     f['instrument_serial_number'],
                                                                    f['pumped'], f['pressure'], f['sample_rate']
                                                                     )
        )

# output all flags

f_out = open(var_to_count + "-flags-all.csv", "w")

s="Deployment, Instrument, Serial_#, Depth, flag, flag 1, flag 2, flag 3, flag 4, flag 6, % flag 3 or 4"

f_out.write(s)
f_out.write('\n')

last_inst = 'unknown'
last_dep = 'unkown'

qc_var_name_map = {}
qc_var_name_map[var_to_count + '_quality_control'] = 'final flags'
qc_var_name_map[var_to_count + '_quality_control_loc'] = 'location (test 3)'
qc_var_name_map[var_to_count + '_quality_control_gr'] = 'range (test 4-5)'
qc_var_name_map[var_to_count + '_quality_control_spk'] = 'spike (test 6)'
qc_var_name_map[var_to_count + '_quality_control_roc'] = 'rate-of-change (test 7)'
qc_var_name_map[var_to_count + '_quality_control_dst'] = 'sigma-theta0 (test 9)'
qc_var_name_map[var_to_count + '_quality_control_man'] = 'manual (test 10-13)'

for f in flags_list_sorted:
    if f['variable'].startswith(var_to_count): #and f['variable'].endswith('_quality_control'):
        if f['deployment_code'] != last_dep:
            dep = f['deployment_code']
            last_dep = str(f['deployment_code'])
            last_inst = 'unknown'
        else:
            dep = ''
        if (f['instrument_model']+f['instrument_serial_number']) != last_inst:
            inst = f['instrument_model']
            sn = f['instrument_serial_number']
            nd = '{:.0f}'.format(float(f['instrument_nominal_depth']))
            last_inst = str(f['instrument_model']+f['instrument_serial_number'])
        else:
            inst = ''
            sn = ''
            nd = ''
        flag_name = qc_var_name_map[f['variable']]
        c1 = "{:.0f}".format(f['count-1'])
        c2 = "{:.0f}".format(f['count-2'])
        c3 = "{:.0f}".format(f['count-3'])
        c4 = "{:.0f}".format(f['count-4'])
        c6 = "{:.0f}".format(f['count-6'])
        if f['count-1'] == 0:
            c1 = ''
        if f['count-2'] == 0:
            c2 = ''
        if f['count-3'] == 0:
            c3 = ''
        if f['count-4'] == 0:
            c4 = ''
        if f['count-6'] == 0:
            c6 = ''
        s = "{:s},{:s},{:s},{:s},{:s},{:s},{:s},{:s},{:s},{:s},{:.2f}".format(dep,
                                                                 inst,
                                                                 sn,
                                                                 nd,
                                                                 flag_name,
                                                                 c1, c2, c3,
                                                                 c4, c6,
                                                                 100*(f['count-3'] + f['count-4'])/(f['samples'])
                                                                 )

        f_out.write(s)
        f_out.write('\n')

f_out.close()

# output only final flags

f_out = open(var_to_count + "-flags.csv", "w")

s="Deployment, Instrument, Serial_#, Depth, flag 1, flag 2, flag 3, flag 4, flag 6, % flag 3 or 4"

f_out.write(s)
f_out.write('\n')

last_inst = 'unknown'
last_dep = 'unkown'

for f in flags_list_sorted:
    if f['variable'].startswith(var_to_count) and f['variable'].endswith('_quality_control'):
        if f['deployment_code'] != last_dep:
            dep = f['deployment_code']
            last_dep = str(f['deployment_code'])
            last_inst = 'unknown'
        else:
            dep = ''
        if (f['instrument_model']+f['instrument_serial_number']) != last_inst:
            inst = f['instrument_model']
            sn = f['instrument_serial_number']
            nd = '{:.0f}'.format(float(f['instrument_nominal_depth']))
            last_inst = str(f['instrument_model']+f['instrument_serial_number'])
        else:
            inst = ''
            sn = ''
            nd = ''
        s = "{:s},{:s},{:s},{:s},{:.0f},{:.0f},{:.0f},{:.0f},{:.0f},{:.2f}".format(dep,
                                                                 inst,
                                                                 sn,
                                                                 nd,
                                                                 f['count-1'], f['count-2'], f['count-3'],
                                                                 f['count-4'], f['count-6'],
                                                                 100*(f['count-3'] + f['count-4'])/(f['samples'])
                                                                 )

        f_out.write(s)
        f_out.write('\n')

f_out.close()
