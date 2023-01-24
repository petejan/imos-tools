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
import os
import sys

#print('Python %s on %s' % (sys.version, sys.platform))

from netCDF4 import Dataset

sys.path.extend(['.'])

import glob
import numpy as np

# for each of the new files, process them

ncFiles = []
for f in sys.argv[1:]:
    ncFiles.extend(glob.glob(f))

stats_var = 'DOX2'

meanings_map = {}
meanings_map['unknown'] = 'unknown'
meanings_map['good_data'] = 'good'
meanings_map['probably_good_data'] = 'p.good'
meanings_map['probably_bad_data'] = 'p.bad'
meanings_map['bad_data'] = 'bad'
meanings_map['not_deployed'] = 'ND'
meanings_map['interpolated'] = 'INT'
meanings_map['missing_value'] = 'missing'
hdr_printed = False

qc_files = []
for fn in ncFiles:
    ds = Dataset(fn, 'r')

    if stats_var in ds.variables:
        #print ("processing ", fn)

        ndepth_var = ds.variables['NOMINAL_DEPTH']
        ndepth = ndepth_var[:]
        aux_vars = ds.variables[stats_var].ancillary_variables
        aux_vars_split = aux_vars.split(" ")

        # TODO: add deployment code, total samples

        #print(ndepth, aux_vars_split)

        # variable stats

        qc = ds.variables[stats_var + '_quality_control'][:]
        qc_values = ds.variables[stats_var + '_quality_control'].flag_values
        qc_values = np.append(qc_values, 99)
        #print("qc-values", qc_values)
        qc_meanings = ds.variables[stats_var + '_quality_control'].flag_meanings
        #print("qc-meanings", qc_meanings.split(" "))

        if not hdr_printed:
            out_array = []
            out_array.append('sort-deployment') # use for sorting later
            out_array.append('sort-time-start')
            out_array.append('sort-nominal-depth')
            out_array.append('file name')
            out_array.append('deployment code')
            out_array.append('instrument model')
            out_array.append('serial number')
            out_array.append('time start deployment')
            out_array.append('nominal depth')
            out_array.append('number points')
            out_array.append('')
            out_array.extend([meanings_map[x] for x in qc_meanings.split(" ")])
            print(",".join(out_array))

            hdr_printed = True

        out_array = []
        out_array.append(ds.deployment_code) # use for sorting later
        out_array.append(ds.time_deployment_start)
        out_array.append(str(ndepth))
        out_array.append(os.path.basename(fn))
        out_array.append(ds.deployment_code)
        out_array.append(ds.instrument_model)
        out_array.append(ds.instrument_serial_number)
        out_array.append(ds.time_deployment_start)
        out_array.append(str(ndepth))
        out_array.append(str(len(ds.variables['TIME'])))

        #print(os.path.basename(fn), ds.deployment_code, ds.time_deployment_start, str(ndepth), len(ds.variables['TIME']), qc_meanings.split(" "))
        print(",".join(out_array))

        for v in ds.variables[stats_var].ancillary_variables.split(" "):
            if "_quality_control_" in v:
                out_line = []
                out_line.append(ds.deployment_code)
                out_line.append(ds.time_deployment_start)
                out_line.append(str(ndepth))
                out_line.append('')
                out_line.append('')
                out_line.append('')
                out_line.append('')
                out_line.append('')
                out_line.append('')
                out_line.append('')
                #print('aux var', v)
                idx_q = v.find('_quality_control_')
                out_line.append(v[idx_q+17:])

                qc_sub = ds.variables[v][:]
                (qc_hist, edges) = np.histogram(qc_sub, qc_values)
                #print("histogram", qc_hist)
                #print(v, qc_hist, edges)
                out_line.extend([str(x) for x in qc_hist])
                print(",".join(out_line))
        (qc_hist, edges) = np.histogram(qc, qc_values)
        #print('totals', qc_hist, edges)
        out_line = []
        out_line.append(ds.deployment_code)
        out_line.append(ds.time_deployment_start)
        out_line.append(str(ndepth))
        out_line.append('')
        out_line.append('')
        out_line.append('')
        out_line.append('')
        out_line.append('')
        out_line.append('')
        out_line.append('')
        out_line.append('totals')
        out_line.extend([str(x) for x in qc_hist])
        print(",".join(out_line))

    ds.close()
