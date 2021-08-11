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
import os
import re

from datetime import datetime

from glob2 import glob
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np

hdr_line_expr = r"Date Time \(UTC\)"
hdr_qc_line_expr = r"Mooring Name,Latitude,Longitude,Date,Time"

# QCd data file
# expocode: 316420180823
# vessel name: SOFS_142E_46S
# "PIs: Sutton, A.; Trull, T"
# vessel type: mooring
# Mooring Name,Latitude,Longitude,Date,Time,xCO2 SW (wet) (umol/mol),CO2 SW QF,H2O SW (mmol/mol),xCO2 Air (wet) (umol/mol),CO2 Air QF,H2O Air (mmol/mol),Licor Atm Pressure (hPa),Licor Temp (C),MAPCO2 %O2,SST (C),Salinity,xCO2 SW (dry) (umol/mol),xCO2 Air (dry) (umol/mol),fCO2 SW (sat) uatm,fCO2 Air (sat) uatm,dfCO2,pCO2 SW (sat) uatm,pCO2 Air (sat) uatm,dpCO2,pH (total scale),pH QF
# SOFS_142E_46S,-47.028,142.274,08/23/2018,03:17,405,3,1.48,405,2,1.46,1011.9,10.8,100.01,10.013,34.752,405.6,405.6,398.7,398.7,0,400.2,400.2,0,-999,5
# SOFS_142E_46S,-47.028,142.274,08/23/2018,06:17,404.8,3,1.38,404.5,2,1.37,1011.3,10.9,100.21,9.849,34.716,405.4,405,398.3,398,0.3,399.8,399.5,0.3,-999,5
# SOFS_142E_46S,-47.028,142.274,08/23/2018,09:17,401.2,3,1.36,404.7,2,1.35,1011.5,10.8,100.13,9.699,34.676,401.7,405.2,394.9,398.3,-3.4,396.4,399.8,-3.4,-999,5

# PLEASE READ:
# These data are made available to our partners in the belief that their wide dissemination will lead to greater public understanding and new scientific insights. The availability of these data does not constitute publication of the data. These data have not been post-calibrated or quality controlled and are therefore not of the climate quality (<2uatm pCO2) that PMEL is required to achieve.  These unverified data are made available to our partners for qualitative use (e.g., generating plots for public outreach and pre-publication analyses). We rely on the ethics and integrity of the user to assure that PMEL receives fair credit for our work. Only post-calibrated and quality controlled data available at www.nodc.noaa.gov/ocads/oceans/time_series_moorings.html should be used for scientific publications.
#
# NaN = missing data or data that have been preliminarily flagged as bad.  Since there is a >1 day delay in flagging bad data, we ask that if the user is generating public plots, that they update the entire time series daily, not just the last day of measurements.  The community-established calibration bias of 2 for the WET Labs ECO-series fluorometer was applied to these in situ fluorometric chlorophyll values (Roesler et al. 2017, https://doi.org/10.1002/lom3.10185).
#
# From time to time, we may make changes to our database that could affect the data file name, and columns and column names of the data in these files. We will strive to inform you in advance of such changes.
#
# If you have any questions, please contact adrienne.sutton@noaa.gov. If you have technical issues related to file contents, please contact john.osborne@noaa.gov.
#
# Date Time (UTC), Pressure (kPa), xCO2 of Seawater (umol/mol), xCO2 of Air (umol/mol), pH of Seawater (total scale), Sea Surface Salinity, Sea Surface Temperature (deg C), Chlorophyll (ug/l), Turbidity (NTU), Salinity-Compensated O2 (uM), Salinity-Compensated O2 (mg/l), Salinity-Compensated O2 (umol/kg), sigma-theta (kg/m3)
# 2011-11-24 09:47:00,    102.47  ,    355.0  ,    388.2  , NaN  , NaN  , NaN  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 10:17:00,    102.46  ,    356.4  ,    388.9  , NaN  , NaN  , NaN  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 10:47:00,    102.47  ,    356.4  ,    389.4  , NaN  , NaN  , NaN  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 11:17:00,    102.43  ,    356.6  ,    389.2  , NaN  , NaN  , NaN  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 11:47:00,    102.44  ,    357.4  ,    389.5  , NaN  , NaN  , NaN  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 12:17:00,    102.41  ,    357.5  ,    389.5  , NaN  , NaN  , NaN  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 12:47:00,    102.39  ,    357.9  ,    389.7  , NaN  , NaN  , NaN  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 13:17:00,    102.33  ,    357.8  ,    389.5  , NaN  ,    34.70  ,    10.46  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 13:47:00,    102.33  ,    357.5  ,    389.5  , NaN  ,    34.70  ,    10.46  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 14:17:00,    102.31  ,    357.8  ,    389.5  , NaN  ,    34.70  ,    10.45  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 15:17:00,    102.26  ,    357.9  ,    388.8  , NaN  ,    34.70  ,    10.44  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00
# 2011-11-24 18:17:00,    102.15  ,    357.9  ,    388.1  , NaN  ,    34.70  ,    10.43  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00  ,     0.00

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

#
# parse the file
#

# Mooring Name,
# Latitude,
# Longitude,
# Date,
# Time,
# xCO2 SW (wet) (umol/mol),
# CO2 SW QF,H2O SW (mmol/mol),
# xCO2 Air (wet) (umol/mol),
# CO2 Air QF,
# H2O Air (mmol/mol),
# Licor Atm Pressure (hPa),
# Licor Temp (C),
# MAPCO2 %O2,
# SST (C),
# Salinity,
# xCO2 SW (dry) (umol/mol),
# xCO2 Air (dry) (umol/mol),
# fCO2 SW (sat) uatm,
# fCO2 Air (sat) uatm,
# dfCO2,
# pCO2 SW (sat) uatm,
# pCO2 Air (sat) uatm,
# dpCO2,
# pH (total scale),
# pH QF
#
# Date Time (UTC),
# Pressure (kPa),
# xCO2 of Seawater (umol/mol),
# xCO2 of Air (umol/mol),
# pH of Seawater (total scale),
# Sea Surface Salinity,
# Sea Surface Temperature (deg C),
# Chlorophyll (ug/l),
# Turbidity (NTU),
# Salinity-Compensated O2 (uM),
# Salinity-Compensated O2 (mg/l),
# Salinity-Compensated O2 (umol/kg),
# sigma-theta (kg/m3)

column_map = {}
column_map['SST(C)'] = ('TEMP', 'degrees_Celsius', 'sea_water_temperature')
column_map['SeaSurfaceTemperature(degC)'] = ('TEMP', 'degrees_Celsius', 'sea_water_temperature')
column_map['Salinity'] = ('PSAL', '1', 'sea_water_practical_salinity')
column_map['SeaSurfaceSalinity'] = ('PSAL', '1', 'sea_water_practical_salinity')
column_map['Pressure(kPa)'] = ('ATMP', 'hPa', 'licor_atm_pressure')
column_map['LicorAtmPressure(hPa)'] = ('ATMP', 'hPa', 'licor_atm_pressure')
column_map['LicorTemp(C)'] = ('ITEMP', 'degrees_Celsius', 'licor_temperature')

column_map['xCO2ofSeawater(umol/mol)'] = ('xCO2_SW', 'umol/mol', 'xCO2 of Seawater')
column_map['xCO2SW(dry)(umol/mol)'] = ('xCO2_SW', 'umol/mol', 'xCO2 of Seawater')
column_map['xCO2SW(wet)(umol/mol)'] = ('xCO2_SW_WET', 'umol/mol', 'xCO2 of Seawater')
column_map['fCO2SW(sat)uatm'] = ('fCO2_SW_SAT', 'uatm', 'fCO2 of Seawater')
column_map['fCO2Air(sat)uatm'] = ('fCO2_AIR_SAT', 'uatm', 'fCO2 of Air')
column_map['pCO2SW(sat)uatm'] = ('pCO2_SW_SAT', 'uatm', 'pCO2 of Seawater')
column_map['pCO2Air(sat)uatm'] = ('pCO2_AIR_SAT', 'uatm', 'pCO2 of Air')

column_map['xCO2ofAir(umol/mol)'] = ('xCO2_AIR', 'umol/mol', 'xCO2 of Air')
column_map['xCO2Air(dry)(umol/mol)'] = ('xCO2_AIR', 'umol/mol', 'xCO2 of Air')
column_map['xCO2Air(wet)(umol/mol)'] = ('xCO2_AIR_WET', 'umol/mol', 'xCO2 of Air')


def parse(files):
    qc_file = False

    output_names = []
    column_names = []

    for filepath in files:

        print('file name', filepath)

        hdr = None
        cnt = 0
        note = ''
        cite = None
        data = []
        times = []
        with open(filepath, 'r', errors='ignore') as fp:
            line = fp.readline()
            while line:
                line = line.rstrip('\n')
                line = line.replace('"', '')
                #print(line)
                if not hdr:
                    matchObj = re.match(hdr_line_expr, line)
                    if matchObj:
                        hdr = [s.strip() for s in line.split(',')]
                        column_names = [s.replace(' ', '') for s in line.split(',')]
                    else:
                        note += line + '\n'

                    matchObj = re.match(hdr_qc_line_expr, line)
                    if matchObj:
                        hdr = [s.strip() for s in line.split(',')]
                        qc_file = True
                        column_names = [s.replace(' ', '') for s in line.split(',')]
                        note = 'https://www.ncei.noaa.gov/data/oceans/ncei/ocads/metadata/0118546.html'
                        cite = 'Sutton, Adrienne J.; Sabine, Christopher L.; Trull, Tom; Dietrich, Colin; Maenner Jones, Stacy; Musielewicz, Sylvia; Bott, Randy; Osborne, John (2014). High-resolution ocean and atmosphere pCO2 time-series measurements from mooring SOFS_142E_46S in the Indian Ocean (NCEI Accession 0118546). [indicate subset used]. NOAA National Centers for Environmental Information. Dataset. https://doi.org/10.3334/cdiac/otg.tsm_sofs_142w_46s. Accessed [date]'

                    if hdr:
                        print("column names", column_names)
                else:
                    row = line.split(',')
                    if len(row) == len(column_names):
                        d = {}
                        if row[0] != '':
                            for j in range(0, len(column_names)):
                                v = row[j].strip()
                                if v == '-999' or v == '-999.0':
                                    v = 'NaN'
                                d[column_names[j].strip()] = v
                            #print(d)
                            data.append(d)
                            cnt += 1
                    #else:
                    #    print(cnt, 'missing values', row)

                line = fp.readline()

        #
        # build the netCDF file
        #

        print('samples', cnt, len(data))
        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        for d in data:
            if 'DateTime(UTC)' in column_names:
                times.append(datetime.strptime(d['DateTime(UTC)'], "%Y-%m-%d %H:%M:%S"))
            if 'Date' in column_names:
                try:
                    times.append(datetime.strptime(d['Date'] + ' ' + d['Time'], "%m/%d/%Y %H:%M"))
                except ValueError:
                    times.append(datetime.strptime(d['Date'] + ' ' + d['Time'], "%m/%d/%y %H:%M"))

        outputName = filepath + ".nc"

        print("output file : %s" % outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4')

        ncOut.instrument = 'Battelle'
        ncOut.instrument_model = 'Seaology pCO2 monitor'
        if len(note) > 0:
            ncOut.note = note
        if cite:
            ncOut.cite = cite

        ncOut.instrument_serial_number = 'unknown'

        #     TIME:axis = "T";
        #     TIME:calendar = "gregorian";
        #     TIME:long_name = "time";
        #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

        tDim = ncOut.createDimension("TIME", cnt)
        ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
        ncTimesOut.long_name = "time"
        ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
        ncTimesOut.calendar = "gregorian"
        ncTimesOut.axis = "T"
        ncTimesOut[:] = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

        for v in column_names:
            print (v)
            if v in column_map:
                v_out = column_map[v][0]
                values = []
                for d in data:
                    values.append(d[v])
                # for each variable in the data file, create a netCDF variable
                ncVarOut = ncOut.createVariable(v_out, "f4", ("TIME",), zlib=True, fill_value = np.nan)
                ncVarOut.long_name = column_map[v][2]
                ncVarOut.units = column_map[v][1]
                ncVarOut.comment = "from source file column `" + v + "`"
                ncVarOut[:] = values

        # add timespan attributes
        ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

        # add creating and history entry
        ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
        ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

        ncOut.close()

        output_names.append(outputName)

    return output_names


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))
    parse(files)
