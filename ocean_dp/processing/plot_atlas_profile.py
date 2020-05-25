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

def get_data():
    base = datetime.datetime(2000, 1, 1)
    date_list = [base + datetime.timedelta(days=x*30) for x in range(12)]

    doy = [(x - datetime.datetime(x.year, 1, 1)).total_seconds()/3600/24 for x in date_list]
    depth = np.arange(0.5, 5000, 10) # WOA only goes to 2000m (?), WOA-18 goes to full ocean, need to check oceansdb

    #print (depth)

    db = oceansdb.WOA(dbname='WOA18')

    t_std = np.zeros([len(doy), len(depth)])
    t_mean = np.zeros([len(doy), len(depth)])

    i = 0
    for doy in date_list:
        t = db['sea_water_temperature'].extract(doy=doy, depth=depth, lat=-47, lon=142.5)
        #print(t)
        #t_mean[i][:] = t['mean']
        #t_std[i][:] = t['std_dev']
        t_mean[i][:] = t['t_mn']
        t_std[i][:] = t['t_sd']

        i += 1

    return t_mean, t_std, depth

def plot():
    t_mean, t_std, depth = get_data()

    #print(t_mean)

    plt.plot(np.mean(t_mean, axis=0), depth)
    plt.plot(np.max(t_mean, axis=0) + 3*np.max(t_std, axis=0), depth)
    plt.plot(np.min(t_mean, axis=0) - 3*np.max(t_std, axis=0), depth)


if __name__ == "__main__":
    plot()

    plt.gca().invert_yaxis()
    plt.grid(True)
    plt.show()
