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

def main(netCDFfile):

    print(netCDFfile)

    ds = Dataset(netCDFfile, 'a')

    create_temp = 'TEMP' in ds.variables
    create_psal = 'PSAL' in ds.variables

    lat = ds.variables['LATITUDE'][:]
    lon = ds.variables['LONGITUDE'][:]
    nom_depth = ds.variables['NOMINAL_DEPTH'][:]

    time = ds.variables['TIME']
    dt = num2date(time[:], units=time.units, calendar=time.calendar)
    doy = [(x - datetime.datetime(x.year, 1, 1)).total_seconds()/3600/24 for x in dt]

    depth = ds.variables['PRES_REL'][:]+1

    print("netCDF data ", doy[0:2], lat, lon, nom_depth, time.shape)

    db = oceansdb.CARS()

    temp_cars = np.zeros((time.shape[0], ))
    temp_cars.fill(np.nan)
    temp_std_cars = np.zeros((time.shape[0], ))
    temp_std_cars.fill(np.nan)

    psal_cars = np.zeros((time.shape[0], ))
    psal_cars.fill(np.nan)
    psal_std_cars = np.zeros((time.shape[0], ))
    psal_std_cars.fill(np.nan)

    for i in range(0, time.shape[0]):
        #print (i)
        temp_cars[i] = db['sea_water_temperature'].extract(var='mean', doy=doy[i], lat=lat, lon=lon, depth=depth[i])['mean']
        temp_std_cars[i] = db['sea_water_temperature'].extract(var='std_dev', doy=doy[i], lat=lat, lon=lon, depth=depth[i])['std_dev']

        if create_psal:
            psal_cars[i] = db['sea_water_salinity'].extract(var='mean', doy=doy[i], lat=lat, lon=lon, depth=depth[i])['mean']
            psal_std_cars[i] = db['sea_water_salinity'].extract(var='std_dev', doy=doy[i], lat=lat, lon=lon, depth=depth[i])['std_dev']

        print(temp_cars[i], depth[i])

    # create variables in netCDF file

    # temp
    if not 'TEMP_CARS' in ds.variables:
        ncVarOut = ds.createVariable("TEMP_CARS", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables["TEMP_CARS"]

    ncVarOut[:] = temp_cars
    ncVarOut.standard_name = "sea_water_temperature"
    ncVarOut.units = "degrees_Celsius"
    ncVarOut.comment = "calculated using CARS database, https://github.com/castelao/oceansdb"

    if not 'TEMP_STD_CARS' in ds.variables:
        ncVarOut = ds.createVariable("TEMP_STD_CARS", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables["TEMP_STD_CARS"]

    ncVarOut[:] = temp_std_cars

    # psal
    if not 'PSAL_CARS' in ds.variables:
        ncVarOut = ds.createVariable("PSAL_CARS", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables["PSAL_CARS"]

    ncVarOut[:] = psal_cars
    ncVarOut.standard_name = "sea_water_salinity"
    ncVarOut.units = "1"
    ncVarOut.comment = "calculated using CARS database, https://github.com/castelao/oceansdb"

    if not 'PSAL_STD_CARS' in ds.variables:
        ncVarOut = ds.createVariable("PSAL_STD_CARS", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables["PSAL_STD_CARS"]

    ncVarOut[:] = psal_std_cars


    ds.close()


if __name__ == "__main__":
    main(sys.argv[1])
