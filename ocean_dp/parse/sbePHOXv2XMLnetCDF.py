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

import sys
import re

from datetime import datetime, timedelta
from dateutil import parser
from glob2 import glob

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

import os

import numpy as np
import zipfile
from bs4 import BeautifulSoup
import bs4

# add attributes from sbedat file to a seaphox netcdf file


hdr_map = {}
hdr_map['FrameSync'] = {'var': None, 'long_name': None, 'units': None}
hdr_map['DateTime (UTC+00:00)'] = {'var': None, 'long_name': None, 'units': None} # handelled separatly
hdr_map['Sample Number (#)'] = {'var': None, 'long_name': None, 'units': None}
hdr_map['Error Flags (#)'] = {'var': None, 'long_name': None, 'units': None}
hdr_map['Temperature (Celsius)'] = {'var': 'TEMP', 'long_name': 'sea_water_temperature', 'standard_name': 'sea_water_temperature', 'units': 'degrees_Celsius'}
hdr_map['External pH (pH)'] = {'var': 'pHt', 'long_name': 'sea_water_ph_reported_on_total_scale', 'units': '1', 'comment': 'pH_TS_measured (durafet)'}
hdr_map['External pH (Volt)'] = {'var': 'VEXT_PH', 'long_name': 'voltage_external_ph', 'units': 'Volts'}
hdr_map['Pressure (Decibar)'] = {'var': 'PRES', 'long_name': 'sea_water_pressure_due_to_sea_water', 'units': 'dbar'}
hdr_map['Salinity (psu)'] = {'var': 'PSAL', 'long_name': 'sea_water_practical_salinity', 'units': '1'}
hdr_map['Conductivity (S/m)'] = {'var': 'CNDC', 'long_name': 'sea_water_electrical_conductivity', 'units': 'S/m'}
hdr_map['Oxygen (ml/L)'] = {'var': 'DOX', 'long_name': 'volume_concentration_of_dissolved_molecular_oxygen_in_sea_water', 'units': 'ml/l'}
hdr_map['Relative Humidity (%)'] = {'var': None, 'long_name': None, 'units': None}
hdr_map['Int Temperature (Celsius)'] = {'var': 'ITEMP', 'long_name': 'internal_temperature', 'units': 'degrees_Celsius'}


def add_xml(xml_file, outputName):

    with open(xml_file, errors='ignore') as fp:
        soup = BeautifulSoup(fp, features="xml")

    ncOut = Dataset(outputName, 'a')

    cals = soup.find_all('Calibration')

    for c in cals:
        # print(c)
        # print(c.attrs)

        for vals in c.contents:
            if isinstance(vals, bs4.element.Tag):
                print(c.attrs['id'], ':', vals.name, '=', vals.getText())
                if c.attrs['id'] == 'Oxygen':
                    ncOut.variables['DOX'].setncattr('calibration_' + vals.name, vals.getText())
                if c.attrs['id'] == 'Pressure':
                    ncOut.variables['PRES'].setncattr('calibration_' + vals.name, vals.getText())
                if c.attrs['id'] == 'Conductivity':
                    ncOut.variables['CNDC'].setncattr('calibration_' + vals.name, vals.getText())
                if c.attrs['id'] == 'Temperature':
                    ncOut.variables['TEMP'].setncattr('calibration_' + vals.name, vals.getText())
                if c.attrs['id'] == 'pH':
                    ncOut.variables['pHt'].setncattr('calibration_' + vals.name, vals.getText())

        print()

    hardware_data = soup.find_all('HardwareData')
    for h in hardware_data:
        print(h.attrs)
        print(h.attrs['DeviceType'])

        ncOut.instrument_model = h.attrs['DeviceType']
        ncOut.instrument_serial_number = h.attrs['SerialNumber']

    firmware_version = soup.find('FirmwareVersion')
    for f in firmware_version:
        print(f.getText())

        ncOut.instrument_firmware_version = f.getText()

    try:
        hist = ncOut.history + "\n"
    except AttributeError:
        hist = ""

    ncOut.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " attributes from " + xml_file)


    ncOut.close()

    return


if __name__ == "__main__":
    outfile = sys.argv[2]
    xmlfile = sys.argv[1]

    add_xml(xmlfile, outfile)
