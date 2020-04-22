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

import numpy as np
from datetime import datetime

import os


def extract(netCDFfiles):
    # Extract VOLT2 from a SBE16 file and create PAR

    out_files = []

    for fn in netCDFfiles:
        # Change the creation date in the filename to today
        now = datetime.utcnow()

        fn_new = fn + ".new.nc"
        if os.path.basename(fn).startswith("IMOS_"):
            fn_new_split = os.path.basename(fn).split('_')
            fn_new_split[-1] = "C-" + now.strftime("%Y%m%d") + ".nc"
            try:
                fn_new_split[2].index("R") # for want of a better code
            except ValueError:
                fn_new_split[2] += 'R'
            fn_new_split[6] = 'Pulse-6-2009-PAR-134-37m'

            fn_new = os.path.join(os.path.dirname(fn), '_'.join(fn_new_split))

        ds_in = Dataset(fn, 'r')
        ds = Dataset(fn_new, 'w')

        # copy dimensions to new file
        for dim in ds_in.dimensions:
            print("Create Dimension ", dim)
            ds.createDimension(dim, len(ds_in.dimensions[dim]))

        # copy global attributes
        for ga in ds_in.ncattrs():
            ds.setncattr(ga, ds_in.getncattr(ga))

        ds.setncattr("instrument", "Wet-LABS ; PARS")
        ds.setncattr("instrument_model", "PARS")
        ds.setncattr("instrument_serial_number", "135")

        # copy required variables
        for v in ['TIME', 'NOMINAL_DEPTH', 'LATITUDE', 'LONGITUDE', 'PRES', 'VOLT2', 'VOLT2_quality_control']:
            new_var = ds.createVariable(v, ds_in.variables[v].dtype, dimensions=ds_in.variables[v].dimensions, zlib=True)
            for va in ds_in.variables[v].ncattrs():
                new_var.setncattr(va, ds_in.variables[v].getncattr(va))
            new_var[:] = ds_in.variables[v][:]

        # calculate the PAR from the recorded voltage
        VOLT2_var = ds_in.variables['VOLT2']
        par = VOLT2_var[:]
        print(par)
        new_var = ds.createVariable('PAR', ds_in.variables['VOLT2'].dtype, ds_in.variables['VOLT2'].dimensions, fill_value=np.nan, zlib=True)
        print(new_var)
        new_var[:] = ds_in.variables['VOLT2'].calibration_PAR_Im * 10**((par - ds_in.variables['VOLT2'].calibration_PAR_analog_A0)/ds_in.variables['VOLT2'].calibration_PAR_analog_A1)
        for va in ds_in.variables['VOLT2'].ncattrs():
            if va not in ('_FillValue'):
                new_var.setncattr(va, ds_in.variables['VOLT2'].getncattr(va))
        new_var.units = 'μmol photons/m2/s'
        new_var.comment_sensor_type = 'cosine sensor, with integrated anti-fouling Bio-wiper'
        new_var.ancillary_variables = "PAR_quality_control"

        new_var = ds.createVariable('PAR_quality_control', ds_in.variables['VOLT2_quality_control'].dtype, ds_in.variables['VOLT2_quality_control'].dimensions, fill_value=np.int8(99), zlib=True)
        print(new_var)
        new_var[:] = ds_in.variables['VOLT2_quality_control'][:]
        for va in ds_in.variables['VOLT2_quality_control'].ncattrs():
            if va not in ('_FillValue'):
                new_var.setncattr(va, ds_in.variables['VOLT2_quality_control'].getncattr(va))
        new_var.long_name = 'quality_code for PAR'

        ds_in.close()

        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""

        ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : extracted from " + os.path.basename(fn))

        ds.close()

    return out_files


if __name__ == "__main__":
    extract(sys.argv[1:])
