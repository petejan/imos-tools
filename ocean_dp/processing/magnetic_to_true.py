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
from datetime import datetime, UTC

#  convert magnetic orientation to true orientation

def magnetic_to_true(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    # read variables from netCDF file
    mag_var = ds.geomagnetic_varition

    var_ucur_mag = ds.variables["UCUR_MAG"]
    var_vcur_mag = ds.variables["VCUR_MAG"]
    var_heading = ds.variables["HEADING_MAG"]

    # recalculate variables
    cur_spd = np.sqrt(np.power(var_ucur_mag[:], 2) + np.power(var_vcur_mag[:], 2))
    cur_dir = np.arctan2(var_vcur_mag[:], var_ucur_mag[:])

    cur_dir = cur_dir + np.deg2rad(mag_var)

    ucur = cur_spd * np.sin(cur_dir)
    vcur = cur_spd * np.cos(cur_dir)

    hdg = var_heading[:] + mag_var

    var_heading[:] = hdg
    # var_heading.units = var_heading.units
    var_heading.reference_datum = 'degrees true'
    var_heading.comment = "calculated from HEADING_MAG using magnetic variation " + format(mag_var, "3.1f")

    var_ucur_mag[:] = ucur
    var_ucur_mag.reference_datum = 'true north'
    var_ucur_mag.comment = "calculated from UCUR_MAG/VCUR_MAG using magnetic variation " + format(mag_var, "3.1f")

    var_vcur_mag[:] = vcur
    var_vcur_mag.reference_datum = 'true east'
    var_vcur_mag.comment = "calculated from UCUR_MAG/VCUR_MAG using magnetic variation " + format(mag_var, "3.1f")

    # rename variables in netCDF file
    ds.renameVariable("HEADING_MAG", "HEADING")
    ds.renameVariable("UCUR_MAG", "UCUR")
    ds.renameVariable("VCUR_MAG", "VCUR")

    # add current total and direction
    var_cur_spd = ds.createVariable("CUR_SPD", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    var_cur_spd[:] = cur_spd
    var_cur_spd.units = var_ucur_mag.units

    var_cur_dir = ds.createVariable("CUR_DIR", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    var_cur_dir[:] = np.degrees(cur_dir)
    var_cur_dir.units = var_heading.units

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " : converted from magnetic orientation to true")

    ds.close()

    return netCDFfile


if __name__ == "__main__":
    magnetic_to_true(sys.argv[1])
