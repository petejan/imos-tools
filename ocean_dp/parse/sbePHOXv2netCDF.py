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

from datetime import datetime, UTC

from glob2 import glob

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

import os

import numpy as np
import zipfile

# source file must have 'timek' column for time
#  flag column is excluded
#
# parsers need to output
#  instrument
#  instrument_serial_number
#  time_coverage_start
#  time_coverage_end
# optional
#  date_created
#  history
#
# convert time to netCDF cf-timeformat (double days since 1950-01-01 00:00:00 UTC)

# example:

# SATFHR,APP-VERSION,SeaFET v3.8.0
# SATFHR,SERIAL-NUMBER,1001
# SATFHR,CAL_PHINT_OFFSET_COEFF,-1.132275e+00
# SATFHR,CAL_PHINT_SLOPE_COEFF,-1.101000e-03
# SATFHR,CAL_PHEXT_OFFSET_COEFF,-1.138389e+00
# SATFHR,CAL_PHEXT_SLOPE_COEFF,-1.048000e-03
# SATFHR,ONBOARD_SALINITY_PSU,35.000
# SATFHR,SAMPLES_AVERAGED,10
# SATPHA1001,2020246,0.0188318,-18.81183,-19.56019,9.8193,9.8076,34.7276,8.769,30.059,-2.50000000,-2.50000000,1.16209387,12.497,47,41.1,4.922,12.442,6.119,5.964,205,205,0.00000000,0x0000,83
# SATPHA1001,2020246,0.0216094,-18.81185,-19.56020,9.8215,9.8073,34.7277,8.770,31.416,-2.50000000,-2.50000000,1.16202563,12.513,36,40.0,4.938,12.450,6.118,5.967,205,205,0.00000000,0x0000,131
# SATPHA1001,2020246,0.0243873,-18.81186,-19.56022,9.8249,9.8072,34.7279,8.767,29.811,-2.50000000,-2.50000000,1.16192042,12.497,40,39.8,4.938,12.434,6.118,5.967,205,205,0.00000000,0x0000,79
# SATPHA1001,2020246,0.0271657,-18.81185,-19.56020,9.8286,9.8073,34.7279,8.767,32.127,-2.50000000,-2.50000000,1.16180807,12.505,36,39.7,4.922,12.458,6.118,5.967,205,205,0.00000000,0x0000,86
#
# SATPHC1001,9.8076,3.76566,30.059,8.769,34.7277,N/A,N/A,02 Sep 2020, 00:01:08
# SATPHC1001,9.8073,3.76564,30.005,8.766,34.7279,N/A,N/A,02 Sep 2020, 00:01:12
# SATPHC1001,9.8074,3.76570,31.417,8.771,34.7278,N/A,N/A,02 Sep 2020, 00:01:16
# SATPHC1001,9.8073,3.76565,30.533,8.772,34.7277,N/A,N/A,02 Sep 2020, 00:01:19
# SATPHC1001,9.8078,3.76573,31.794,8.769,34.7275,N/A,N/A,02 Sep 2020, 00:01:23
# SATPHC1001,9.8072,3.76563,29.811,8.767,34.7279,N/A,N/A,02 Sep 2020, 00:01:26
# SATPHC1001,9.8076,3.76570,30.867,8.772,34.7278,N/A,N/A,02 Sep 2020, 00:01:30
# SATPHC1001,9.8074,3.76566,30.759,8.773,34.7276,N/A,N/A,02 Sep 2020, 00:01:33
# SATPHC1001,9.8074,3.76575,32.128,8.768,34.7279,N/A,N/A,02 Sep 2020, 00:01:37

# FrameSync,DateTime (UTC+11:00),Sample Number (#),Error Flags (#),Temperature (Celsius),External pH (pH),External pH (Volt),Pressure (Decibar),Salinity (psu),Conductivity (S/m),Oxygen (ml/L),Relative Humidity (%),Int Temperature (Celsius)
# DSPHOX02016,03/09/2021 02:38:04,1,0000,  21.7839,2.3933,-1.093007,    0.074,   0.0107,  0.00002,  6.013,  0.0, 21.5
# DSPHOX02016,03/17/2021 01:04:40,2,0008,  19.2803,25.4355,0.246031,    0.319,   0.0097,  0.00002,  6.607,  0.0, 21.7
# DSPHOX02016,03/17/2021 01:05:58,3,0008,  19.3264,24.5690,0.195918,    0.324,   0.0097,  0.00002,  6.603,  0.0, 21.8
# DSPHOX02016,03/17/2021 01:06:58,4,0008,  19.3598,23.3119,0.123069,    0.317,   0.0097,  0.00002,  6.592,  0.0, 21.8
# DSPHOX02016,03/17/2021 01:07:58,5,0008,  19.3924,22.8063,0.093832,    0.298,   0.0097,  0.00002,  6.592,  0.0, 21.8
# DSPHOX02016,03/17/2021 01:19:37,6,0008,  19.7024,25.1331,0.230021,    0.305,   0.0099,  0.00002,  6.538,  0.0, 21.8

# search expressions within file

deep_sea_phox = r"DSPHOX(\d*),(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}),(.*)$"

#
# parse the file
#

instrument_model = 'Deep SeapHox2'
instrument_serialnumber = "0002026"

instrument_ctd_model = 'SBE37SMP-ODO'
instrument_ctd_serialnumber = '20127'

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
hdr_map['Relative Humidity (%)'] = {'var': 'IHUMID', 'long_name': 'internal humidity', 'units': '1'}
hdr_map['Int Temperature (Celsius)'] = {'var': 'ITEMP', 'long_name': 'internal_temperature', 'units': 'degrees_Celsius'}


def read(fp, times, data, number_samples):
    global instrument_model
    global instrument_serialnumber
    global instrument_ctd_model
    global instrument_ctd_serialnumber
    global hdrline_split

    hdrline = fp.readline().strip()
    hdrline_split = hdrline.split(',')
    print('header line', hdrline_split)

    last_dt = datetime(2000,1,1)
    try:
        line = fp.readline()
        while line:
            #print(line)
            matchObj = re.match(deep_sea_phox, line)
            if matchObj:
                instrument_serialnumber = matchObj.group(1)
                ts = matchObj.group(2)
                data_csv = matchObj.group(3)
                dt = datetime.strptime(ts, '%m/%d/%Y %H:%M:%S')
                times.append(dt)
                number_samples += 1
                split = data_csv.split(',')
                d = []
                for i in split:
                    d.append(float(i))
                data.append(d)

                print(dt, d)

            line = fp.readline()
    except UnicodeDecodeError:
        pass

    return times, data, number_samples

def sbe_phox_parse(files):
    global instrument_model
    global instrument_serialnumber
    global instrument_ctd_model
    global instrument_ctd_serialnumber

    outputNames = []

    number_samples = 0
    times = []
    data = []
    #Sample Number (#),Error Flags (#),Temperature (Celsius),External pH (pH),External pH (Volt),Pressure (Decibar),Salinity (psu),Conductivity (S/m),Oxygen (ml/L),Relative Humidity (%),Int Temperature (Celsius)

    for filepath in files:
        print('processing file', filepath)

        if zipfile.is_zipfile(filepath):
            file = zipfile.ZipFile(filepath, "r")
            for zip_name in file.namelist():
                print('processing zip name', zip_name, number_samples)
                fp = file.open(zip_name, 'r')
                (times, data, number_samples) = read(fp, times, data, number_samples)
        else:
            fp = open(filepath, 'r', errors='ignore')
            (times, data, number_samples) = read(fp, times, data, number_samples)


    print("nSamples %d" % number_samples)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = files[0] + ".nc"
    print("output file : %s" % outputName)
    outputNames.append(outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    if instrument_ctd_model:
        ncOut.instrument_ctd_model = instrument_ctd_model
    if instrument_ctd_serialnumber:
        ncOut.instrument_ctd_serialnumber = instrument_ctd_serialnumber

    ncOut.instrument = 'Sea-Bird Electronics ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME", number_samples)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

    # for each variable in the data file, create a netCDF variable
    i = -2
    for v in hdrline_split:

        var_name = hdr_map[v]['var']
        if var_name is not None:

            print('variable:', v, hdr_map[v], i)
            ncVarOut = ncOut.createVariable(var_name, "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max

            if 'standard_name' in hdr_map[v]:
                ncVarOut.standard_name = hdr_map[v]['standard_name']
            ncVarOut.long_name = hdr_map[v]['long_name']
            if 'comment' in hdr_map[v]:
                ncVarOut.comment = hdr_map[v]['comment']

            ncVarOut.units = hdr_map[v]['units']

            ncVarOut[:] = [d[i] for d in data]

        i = i + 1

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

    ncOut.close()

    return outputNames


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    sbe_phox_parse(files)
