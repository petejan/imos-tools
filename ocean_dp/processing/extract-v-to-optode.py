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

from netCDF4 import Dataset
import sys
import gsw
import numpy as np
from datetime import datetime

# extract V4 and V5 from SBE16 file and output BPHASE and OTEMP for optode processing


def add_optode(netCDFfile):
    ds = Dataset(netCDFfile, 'r')
    ds. set_auto_mask(False)

    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]
    var_v4 = ds.variables["V4"]
    var_v5 = ds.variables["V5"]

    dep_code = ds.deployment_code

    print('deployment', dep_code)

    ds_out = Dataset(dep_code + "-optode.nc", 'w')

    ds_out.createDimension("TIME", len(ds.variables['TIME']))
    ncVarIn = ds.variables['TIME']
    ncVarOut = ds_out.createVariable('TIME', "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    for a in ncVarIn.ncattrs():
        if a != '_FillValue':
            ncVarOut.setncattr(a, ncVarIn.getncattr(a))
    ncVarOut[:] = ds.variables['TIME'][:]

    # create optode bphase (or oxygen) from v4 variable
    if var_v4.long_name == 'optode_oxygen_voltage':
        ncVarOut = ds_out.createVariable("DOX2_RAW", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
        scale_offset = [float(x) for x in var_v4.calibration_scale_offset.split(' ')]
        print('DOX2_RAW scale offset', scale_offset)
        ncVarOut[:] = var_v4[:] * scale_offset[0] + scale_offset[1]
        ncVarOut.units = "umol/l"
    elif var_v4.long_name == 'optode_bphase_voltage':
        ncVarOut = ds_out.createVariable("BPHASE", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
        scale_offset = [float(x) for x in var_v4.calibration_scale_offset.split(' ')]
        print('BPHASE scale offset', scale_offset)
        ncVarOut[:] = var_v4[:] * scale_offset[0] + scale_offset[1]
        ncVarOut.units = "1"

    # create the optode temp from v5 variable
    ncVarOut = ds_out.createVariable("OTEMP", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    scale_offset = [float(x) for x in var_v5.calibration_scale_offset.split(' ')]
    print('OTEMP scale offset', scale_offset)
    ncVarOut[:] = var_v5[:] * scale_offset[0] + scale_offset[1]
    ncVarOut.units = "degrees_Celsius"

    # copy old variables into new file
    for v in ['TEMP', 'PSAL', 'PRES', 'V4', 'V5', 'TEMP_quality_control', 'PSAL_quality_control', 'PRES_quality_control']:
        ncVarIn = ds.variables[v]
        ncVarOut = ds_out.createVariable(v, "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
        for a in ncVarIn.ncattrs():
            if a != '_FillValue':
                ncVarOut.setncattr(a, ncVarIn.getncattr(a))
        ncVarOut[:] = ncVarIn[:]

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    # keep the history so we know where it came from
    ds_out.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " extract BPHASE, OTEMP from " + netCDFfile)

    ds.close()
    ds_out.close()


if __name__ == "__main__":
    add_optode(sys.argv[1])
