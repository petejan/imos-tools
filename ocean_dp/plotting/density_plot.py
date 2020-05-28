# Copyright (C) 2020 Ben Weeding
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

import sys
import os

sys.path.append('/Users/tru050/Documents/GitHub/imos-tools/ocean_dp/file_name')

import find_file_with

path = "/Users/Tru050/Desktop/cloudstor/Shared/SOTS-Temp-Raw-Data"

sots_files = find_file_with.find_files_pattern(os.path.join(path, "IMOS*FV00*.nc"))