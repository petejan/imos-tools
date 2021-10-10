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
from glob2 import glob
from netCDF4 import Dataset, num2date
import sys
import gsw
import numpy as np
from datetime import datetime

# add OXSOL to a data file with TEMP, PSAL, PRES variables, many assumptions are made about the input file

ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"


def apply_scale_offset(netCDFfiles):

    out_files = []
    for netCDFfile in netCDFfiles:
        ds = Dataset(netCDFfile, 'a')

        scale_vars = ds.get_variables_by_attributes(comment_scale_offset=lambda v: v is not None)

        for v in scale_vars:
            print ("var : ", v)
            t = v[:]
            #t.mask = False

            scale_offset = v.getncattr('comment_scale_offset')
            scale_offset_split = scale_offset.split(' ')
            scale = np.float(scale_offset_split[0])
            offset = np.float(scale_offset_split[1])
            v[:] = t * float(scale) + float(offset)

            v.renameAttribute('comment_scale_offset', 'comment_scale_offset_applied')
            v.comment_scale_offset_applied = "scale = " + str(scale) + " offset = " + str(offset)

            # update the history attribute
            try:
                hist = ds.history + "\n"
            except AttributeError:
                hist = ""

            ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : scale, offset variable " + v.name + "")

            if v.name == 'TIME':
                # update timespan attributes
                ds.setncattr("time_coverage_start", num2date(v[0], units=v.units, calendar=v.calendar).strftime(ncTimeFormat))
                ds.setncattr("time_coverage_end", num2date(v[-1], units=v.units, calendar=v.calendar).strftime(ncTimeFormat))


        ds.close()

        out_files.append(netCDFfile)

    return out_files


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    apply_scale_offset(files)
