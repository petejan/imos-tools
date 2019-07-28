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

from netCDF4 import Dataset, num2date
import sys

import numpy as np
from dateutil import parser
import pytz
import os

# finalize qc flags, 0 -> 1, add meanings and values

# flag_meanings = "No_QC_performed Good_data Probably_good_data Bad_data_that_are_potentially_correctable Bad_data Value_changed Not_used Not_used Not_used Missing_value"
# flag_values = 0b, 1b, 2b, 3b, 4b, 5b, 6b, 7b, 8b, 9b ;

flag_meanings = ["no_QC_performed", "good_data", "probably_good_data", "bad_data_that_are_potentially_correctable", "bad_data", "value_changed", "not_used", "not_deployed", "interpolated", "missing_value"]

def final_qc(netCDFfile):
    ds = Dataset(netCDFfile, 'a')

    vars = ds.variables

    to_add = []
    for v in vars:
        #print (vars[v].dimensions)
        if v != 'TIME':
            to_add.append(v)

    for v in to_add:
        if "TIME" in vars[v].dimensions:

            if v.endswith("_quality_control"):

                print("QC time dim ", v)

                ncVarOut = vars[v]
                ncVarOut[ncVarOut[:] == 0] = 1

                used_values = sorted(set(ncVarOut[:]))
                used_meanings = [flag_meanings[s] for s in used_values]
                meanings = " ".join(used_meanings)

                print(used_values, meanings)

                ncVarOut.flag_values = used_values
                ncVarOut.flag_meanings = meanings

    ds.close()

    return netCDFfile


if __name__ == "__main__":
    final_qc(sys.argv[1])
