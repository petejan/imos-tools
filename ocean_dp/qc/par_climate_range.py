#!/usr/bin/python3

# global_range.py
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

from netCDF4 import Dataset, num2date
import sys

#import matplotlib.pyplot as plt

import numpy as np
from dateutil import parser
import pytz
import os
from datetime import datetime, UTC

# flag 4 (bad) when out of climate range


def climate_range(netCDFfiles, variable_name, qc_value=3):

    for netCDFfile in netCDFfiles:
        print("climate_range file", netCDFfile)

        ds = Dataset(netCDFfile, 'a')

        nc_var = ds.variables[variable_name]

        try:
            # find the existing quality_control variable in the auxillary variables list
            aux_vars = nc_var.ancillary_variables
            aux_var = aux_vars.split(" ")
            qc_vars = [i for i in aux_var if i.endswith("_quality_control")]
            qc_var = qc_vars[0]
            print("QC var name ", qc_var)
            var_qc = ds.variables[qc_var]
        except KeyError:
            print("no QC variable found")
            return None

        # read existing quality_control flags
        existing_qc_flags = var_qc[:]

        nc_alt = ds.variables['ALT']
        alt_msk = (nc_alt[:] < -15) | (nc_alt[:] > 10)

        nc_e_var = ds.variables['e' + variable_name]
        e_var = nc_e_var[:]
        e_var.mask = False

        # Southern Ocean Time Series (SOTS) Quality Assessment and Control Report PAR Instruments Version 1.0 page 9 : Test 7:
        # the +10 QC's the night time data as well
        spherical = 'spherical' in nc_var.comment_sensor_type
        if spherical:
            e_var = (e_var * 3) + 15
            note = 'spherical sensor, par < (3 * SOLAR) + 15'
        else:
            e_var = (e_var * 2) + 15
            note = 'cosine sensor, par < (2 * SOLAR) + 15'

        # this is where the actual QC test is done
        mask = (nc_var > e_var) & alt_msk
        print('mask data ', mask)

        # create a qc variable just for this test flags
        if nc_var.name + "_quality_control_cl" in ds.variables:
            ncVarOut = ds.variables[nc_var.name + "_quality_control_cl"]
        else:
            ncVarOut = ds.createVariable(nc_var.name + "_quality_control_cl", "i1", nc_var.dimensions, fill_value=99, zlib=True)  # fill_value=0 otherwise defaults to max

            ncVarOut.long_name = "climate flag for " + nc_var.long_name
            #if 'standard_name' in nc_var.ncattrs():
            #    ncVarOut.standard_name = nc_var.standard_name + " climate_flag"

            #ncVarOut.quality_control_conventions = "IMOS standard flags"
            #ncVarOut.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
            #ncVarOut.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
            ncVarOut.units = "1"

            ncVarOut.comment = 'Test 7. climatology test'
            ncVarOut.comment_note = note

        new_qc_flags = np.zeros(nc_var.shape) + 2

        # add new variable to list of aux variables
        nc_var.ancillary_variables = nc_var.ancillary_variables + " " + nc_var.name + "_quality_control_cl"

        # store the qc flags
        new_qc_flags[alt_msk] = 1
        new_qc_flags[mask] = qc_value
        ncVarOut[:] = new_qc_flags

        # update the existing qc-flags
        existing_qc_flags = np.max([existing_qc_flags, new_qc_flags], axis=0)

        # calculate the number of points marked as bad_data
        marked = np.zeros_like(existing_qc_flags)
        marked[mask] = 1
        count = sum(marked)
        print('marked records ', count, mask, existing_qc_flags)

        # temp plot data and flags
        # time_var = ds.variables['TIME']
        # ax1 = plt.subplot(2, 1, 1)
        # plt.plot(time_var[:], nc_var[:])
        # plt.plot(time_var[:], e_var[:])
        #
        # ax2 = plt.subplot(2, 1, 2, sharex=ax1)
        # plt.plot(time_var[:], new_qc_flags)
        # plt.ylim(0, 9)
        #
        # plt.show()

        # write flags back to main QC variable
        var_qc[:] = existing_qc_flags

        # update the history attribute
        try:
            hist = ds.history + "\n"
        except AttributeError:
            hist = ""
        ds.setncattr("history", hist + datetime.now(UTC).strftime("%Y-%m-%d") + " " + variable_name + " climate range, marked " + str(int(count)))

        ds.close()

    return netCDFfiles


if __name__ == "__main__":

    # usage is <file_name> <variable_name> <qc value>
    if len(sys.argv) > 4:
        climate_range([sys.argv[1]], variable_name=sys.argv[2], qc_value=int(sys.argv[3]))
    else:
        climate_range([sys.argv[1]], variable_name=sys.argv[2])
