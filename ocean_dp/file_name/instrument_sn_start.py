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
import os
from datetime import datetime

def rename(netCDFfiles):

    new_names = []
    for netCDFfile in netCDFfiles:

        print(netCDFfile)

        ds = Dataset(netCDFfile, 'a')

        instrument = ds.instrument_model
        instrument_sn = ds.instrument_serial_number
        start_date = datetime.strptime(ds.getncattr('time_coverage_start'), '%Y-%m-%dT%H:%M:%SZ')

        ds.close()

        new_name = instrument + '_' + instrument_sn + '_' + start_date.strftime('%Y-%m-%d') + ".nc"

        folder = os.path.dirname(netCDFfile)

        new_name = folder + '/' + new_name.replace(" ", "-")

        # rename the file, maybe should be copy

        os.rename(netCDFfile, new_name)

        new_names.append(new_name)

    return new_names


if __name__ == "__main__":
    rename(sys.argv[1:])


