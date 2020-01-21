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

# format any {<attribute>} values with its attribute value


def format_attributes(netCDFfile):

    ds = Dataset(netCDFfile, 'a')

    attrs = ds.ncattrs()
    list_of_values = []
    for item in attrs:
        list_of_values.append(ds.getncattr(item))

    di = dict(zip(attrs, list_of_values))

    # delete all attributes, this allows for them to be added again in sorted order
    for item in attrs:
        ds.delncattr(item)

    #print (di)
    for att in sorted(attrs, key=str.lower):  # or  key=lambda s: s.lower()
        value = di[att]
        if type(value) is str:
            value = value.format(**di)
        ds.setncattr(att, value)
        #print("attr : ", att, " = " , value)

    # update the history attribute
    # try:
    #     hist = ds.history + "\n"
    # except AttributeError:
    #     hist = ""
    #
    # ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : formatted attributes")

    ds.close()

    return netCDFfile


if __name__ == "__main__":
    format_attributes(sys.argv[1])


