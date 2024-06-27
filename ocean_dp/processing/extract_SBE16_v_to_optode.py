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
from datetime import datetime, UTC

# extract V4 and V5 from SBE16 file and output BPHASE and OTEMP for optode processing


def extract_optode(netCDFfile):
    ds = Dataset(netCDFfile, 'r')
    ds. set_auto_mask(False)

    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]
    var_v4 = ds.variables["V4"]
    var_v5 = ds.variables["V5"]

    dep_code = ds.deployment_code

    print('deployment', dep_code)

    out_file = dep_code + "-optode.nc"
    ds_out = Dataset(out_file, 'w')

    ds_out.createDimension("TIME", len(ds.variables['TIME']))
    ncVarIn = ds.variables['TIME']
    ncVarOut = ds_out.createVariable('TIME', "f8", ("TIME",), zlib=True)
    for a in ncVarIn.ncattrs():
        if a != '_FillValue':
            ncVarOut.setncattr(a, ncVarIn.getncattr(a))
    ncVarOut[:] = ds.variables['TIME'][:]

    # copy old variables into new file
    in_vars = set([x for x in ds.variables])

    z = in_vars.intersection(['TEMP', 'PSAL', 'PRES', 'V4', 'V5',
                              'TEMP_quality_control', 'PSAL_quality_control', 'PRES_quality_control',
                              'V4_quality_control', 'V5_quality_control',
                              'LATITUDE', 'LONGITUDE', 'NOMINAL_DEPTH'])

    for v in z:
        print('copying',v,'dimensions', ncVarIn.dimensions)
        ncVarIn = ds.variables[v]
        if '_FillValue' in ncVarIn.ncattrs():
            fill = ncVarIn._FillValue
        else:
            fill = None

        ncVarOut = ds_out.createVariable(v, ncVarIn.dtype, ncVarIn.dimensions, fill_value=fill, zlib=True)  # fill_value=nan otherwise defaults to max
        for a in ncVarIn.ncattrs():
            if a != '_FillValue':
                if a == 'ancillary_variables':
                    ncVarOut.setncattr(a, v + '_quality_control') # only copying main quality control, not all individual flags
                else:
                    ncVarOut.setncattr(a, ncVarIn.getncattr(a))

        # TODO: should we change PSAL, TEMP qc flags to interpolated (8) to stop duplicates?

        ncVarOut[:] = ncVarIn[:]


    is_raw = False
    is_bphase = False
    # create optode bphase (or oxygen) from v4 variable
    if var_v4.long_name == 'optode_oxygen_voltage':
        ncVarOut = ds_out.createVariable("DOX2_RAW", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
        scale_offset = [float(x) for x in var_v4.calibration_scale_offset.split(' ')]
        print('DOX2_RAW scale offset', scale_offset)
        ncVarOut[:] = var_v4[:] * scale_offset[0] + scale_offset[1]
        ncVarOut.units = "umol/l"
        ncVarOut.long_name = "optode oxygen, uncorrected"
        is_raw = True
    elif var_v4.long_name == 'optode_bphase_voltage':
        ncVarOut = ds_out.createVariable("BPHASE", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
        scale_offset = [float(x) for x in var_v4.calibration_scale_offset.split(' ')]
        print('BPHASE scale offset', scale_offset)
        ncVarOut[:] = var_v4[:] * scale_offset[0] + scale_offset[1]
        ncVarOut.units = "1"
        ncVarOut.long_name = "optode bphase"
        is_bphase = True
    ncVarOut.coordinates = 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH'

    # create the optode temp from v5 variable
    ncVarOut = ds_out.createVariable("OTEMP", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    scale_offset = [float(x) for x in var_v5.calibration_scale_offset.split(' ')]
    print('OTEMP scale offset', scale_offset)
    ncVarOut[:] = var_v5[:] * scale_offset[0] + scale_offset[1]
    ncVarOut.units = "degrees_Celsius"
    ncVarOut.long_name = "optode temperature"
    ncVarOut.coordinates = 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH'

    # copy attributes forward
    attrs = ds.ncattrs()
    for at in attrs:
        if at not in ['title', 'instrument', 'instrument_model', 'instrument_serial_number', 'history', 'date_created', 'file_version', 'file_version_quality_control']:
            #print('copy att', at)
            ds_out.setncattr(at, ds.getncattr(at))

    ds_out.deployment_code = ds.deployment_code
    ds_out.instrument = 'Aanderaa ; Optode 3975'
    ds_out.instrument_model = 'Optode 3975'
    ds_out.instrument_serial_number = ds.variables['V4'].sensor_serial_number

    ds_out.file_version = 'Level 0 - Raw data'
    ds_out.file_version_quality_control = 'Data in this file has not been quality controlled'

    ds_out.title = 'Oceanographic mooring data deployment of {platform_code} at latitude {geospatial_lat_max:3.1f} longitude {geospatial_lon_max:3.1f} depth {geospatial_vertical_max:3.0f} (m) instrument {instrument} serial {instrument_serial_number}'

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
    ds_out.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    # keep the history so we know where it came from
    if is_raw:
        ds_out.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " extract DOX2_RAW, OTEMP from " + netCDFfile)
    else:
        ds_out.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " extract BPHASE, OTEMP from " + netCDFfile)

    ds.close()
    ds_out.close()

    return out_file


if __name__ == "__main__":
    extract_optode(sys.argv[1])
