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

import glob

import sys
import re

from netCDF4 import Dataset


def find_files_pattern(file_pattern):
    match_files = []
    files = glob.glob(file_pattern)

    match_files.extend(files)
    return match_files

def find_global(files, attribute, regexp):

    match_files = []
    #print("find", file_pattern, files)
    for f in files:
        #print("check file", f)
        ds = Dataset(f, 'r')
        if attribute in ds.ncattrs():
            if re.match(regexp, ds.getncattr(attribute)):
                match_files.append(f)
        ds.close()

    return match_files


def find_variable(files, variable):

    match_files = []
    for f in files:
        #print("check file", f)
        ds = Dataset(f, 'r')
        if variable in ds.variables:
            match_files.append(f)
        ds.close()

    return match_files


def find_variable_attribute(files, attribute, value):

    match_files = []
    for f in files:
        #print("check file", f)
        ds = Dataset(f, 'r')
        nv = {attribute : value}
        find = ds.get_variables_by_attributes(**nv)
        if len(find) > 0:
            match_files.append(f)
        ds.close()

    return match_files


if __name__ == "__main__":
    fns = []
    if sys.argv[1] == '-v':
        files = find_files_pattern(sys.argv[3])
        fns = find_variable(files, variable=sys.argv[2])
    elif sys.argv[1] == '-a':
        files = find_files_pattern(sys.argv[4])
        fns = find_variable_attribute(files, attribute=sys.argv[2], value=sys.argv[3])
    else:
        files = find_files_pattern(sys.argv[4])
        fns = find_global(files, attribute=sys.argv[1], regexp=sys.argv[2])

    for f in fns:
        print(f)