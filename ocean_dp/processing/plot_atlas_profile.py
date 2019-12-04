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
import oceansdb
import datetime
import matplotlib.pyplot as plt

dt = [datetime.datetime(2000,6,21)]

doy = [(x - datetime.datetime(x.year, 1, 1)).total_seconds()/3600/24 for x in dt]
depth = np.arange(0, 5000, 1)

#print (depth)

db = oceansdb.CARS()

t = db['sea_water_salinity'].extract(var='mean', doy=doy, depth=depth, lat=-47, lon=142.5)

#print(t)

plt.plot(t['mean'], depth)
plt.gca().invert_yaxis()
plt.grid(True)
plt.show()
