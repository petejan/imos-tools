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
import numpy as np

# for each of the new files, process them

ncFiles = []
for f in sys.argv[1:]:
    ncFiles.extend(glob.glob(f))

print('file,deployment,depth,n_points,max,min,max_rate,max_spike,qc')

qc_files = []
for fn in ncFiles:
    #print ("processing ", fn)
    ds = Dataset(fn, 'r')

    ndepth_var = ds.variables['NOMINAL_DEPTH']
    ndepth = ndepth_var[:]
    aux_vars = ds.variables["TEMP"].ancillary_variables
    aux_vars_split = aux_vars.split(" ")

    # TODO: add deployment code, total samples

    #print(ndepth, aux_vars_split)
    line = []
    line.append(fn)
    line.append(ds.deployment_code)
    line.append(str(ndepth))
    line.append(str(len(ds.variables['TIME'])))

    # variable stats

    qc = ds.variables['TEMP_quality_control_loc'][:]
    var_data_inpos = ds.variables['TEMP'][qc <= 1]
    time_inpos = ds.variables['TIME'][qc <= 1]*24
    line.append(str(np.max(var_data_inpos)))
    line.append(str(np.min(var_data_inpos)))
    line.append(str(np.max(np.abs(np.diff(var_data_inpos)/(np.diff(time_inpos)))))) # roc / hr
    #print(len(var_data_inpos[0:-3]), len(var_data_inpos[1:-2]), len(var_data_inpos[2:-1]))
    spk = np.abs(var_data_inpos[1:-2] - (var_data_inpos[0:-3] + var_data_inpos[2:-1])/2)
    line.append(str(np.max(np.diff(spk)))) # spike height

    # stats for each QC var
    for v in aux_vars_split:
        var = ds.variables[v]
        qc = var[:]
        count3 = np.zeros_like(qc)
        count4 = np.zeros_like(qc)
        count3[qc == 3] = 1
        count4[qc > 3] = 1
        s3 = sum(count3)
        s4 = sum(count4)
        line.append(v[v.rfind('_')+1:len(v)])
        line.append(str(s3))
        line.append(str(s4))
        #print(s)

    print(','.join(line))
    ds.close()
