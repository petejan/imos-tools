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

from datetime import datetime
from netCDF4 import date2num, num2date
from netCDF4 import Dataset
import numpy as np

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

# search expressions within file

first_line_expr = r"/  Alec MDS5 Data Processing"
inst_no         = r"Inst_No=(.*),$"
inst_type       = r"Inst_Type=(.*),$"
coeff_date_expr = r"CoefDate=(.*),$"
item            = r"\[Item\],$"
hdr_setting     = r"(.*)=(.*),$"

# /  Alec MDS5 Data Processing,
# /  Version V1.22 Jul. 1.2002,
# /  Copyright(C)1999-2002 Alec Electronics Co.,Ltd.,
# /  to: CSIRO,
#
# [Head],
# File_Name=E:\ABOS\pulse-6\surface.Csv,
# File_Type=.CSV,
# Read_Flag=TRUE,
# Data_Flag=TRUE,
# Raw_File=E:\ABOS\pulse-6\surface.Raw,
# Info_File=C:\WinMds5\User\WinMds5.Inf,
# Inst_Type=L,
# Inst_No=200341,
# Start_Time=2009/09/22 12:00:00,
# End_Time=2010/03/23 02:24:00,
# Interval= 6.00000000000000E+0001,
# CoefDate=1999/03/20,
# Comments=Pulse-6-2009 Surface,
# Channel_Count=1,
# Cycle_Unit=Hz,
# Time_Unit=Sec,
# Sample_Count=261445,
#
# [Item],
# /   Sample,YYYY/MM/DD,hh:mm:ss,Day,Light[Micromol],Light,
# 1,2009/09/22,12:00:00,22,0.00,1,
# 2,2009/09/22,12:01:00,22,0.00,1,
# 3,2009/09/22,12:02:00,22,0.00,1,


def parse(file):

    hdr = True
    dataLine = 0
    number_samples_read = 0
    nVars = 0
    data = []
    raw = []
    ts = []
    settings = []
    instrument_model = 'MDS-MKV'
    instrument_serial_number = 'unknown'
    sep = ','

    filepath = file[0]

    with open(filepath, 'r', errors='ignore') as fp:
        line = fp.readline()
        matchObj = re.match(first_line_expr, line)
        if not matchObj:
            print("Alec PAR datafile !")
            return None

        cnt = 1
        while line:
            # print("Line ", line)
            if hdr:
                line_s = line.strip()
                matchObj = re.match(item, line_s)
                if matchObj:
                    line = fp.readline()
                    line_s = line.strip()
                    #print("first item line : ", line_s)
                    try:
                        line.index(',')
                        sep = ','
                    except:
                        sep = ' '
                    hdr = False
                else:
                    matchObj = re.match(inst_no, line_s)
                    if matchObj:
                        #print("inst_no:matchObj.group() : ", matchObj.group())
                        #print("inst_no:matchObj.group(1) : ", matchObj.group(1))
                        instrument_serial_number = matchObj.group(1)

                    matchObj = re.match(coeff_date_expr, line_s)
                    if matchObj:
                        #print("coeff_date_expr:matchObj.group() : ", matchObj.group())
                        #print("coeff_date_expr:matchObj.group(1) : ", matchObj.group(1))
                        coeff_date = matchObj.group(1)

                    matchObj = re.match(inst_type, line_s)
                    if matchObj:
                        #print("inst_type:matchObj.group() : ", matchObj.group())
                        #print("inst_type:matchObj.group(1) : ", matchObj.group(1))
                        instrument_model += matchObj.group(1)

                    matchObj = re.match(hdr_setting, line_s)
                    if matchObj:
                        #print("hdr_setting:matchObj.group() : ", matchObj.group())
                        #print("hdr_setting:matchObj.group(1) : ", matchObj.group(1))
                        settings.append([matchObj.group(1), matchObj.group(2)])

            else:
                lineSplit = line.strip().split(sep)
                #print('Split ', lineSplit)

                t = datetime.strptime(lineSplit[1] + " " + lineSplit[2], '%Y/%m/%d %H:%M:%S')

                ts.append(t)
                data.append(float(lineSplit[4]))
                raw.append(float(lineSplit[5]))

                number_samples_read = number_samples_read + 1

                dataLine = dataLine + 1

            line = fp.readline()
            cnt += 1

    # trim data
    print("samplesRead %d" % (number_samples_read))

    if number_samples_read == 0:
        return

    print('instrument ', instrument_model, 'serial', instrument_serial_number)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    ncOut.instrument = 'Alec Electronics ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serial_number

    for s in settings:
        ncOut.setncattr("comment_file_settings_" + s[0], s[1])

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME", number_samples_read)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = date2num(ts, units=ncTimesOut.units, calendar=ncTimesOut.calendar)

    ncVarOut = ncOut.createVariable('PAR', "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
    ncVarOut.units = 'umol/m^2/s'
    ncVarOut.calibration_date = coeff_date
    ncVarOut.comment_sensor_type = 'spherical sensor'
    ncVarOut[:] = data

    #ncVarOut = ncOut.createVariable('PAR_RAW', "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
    #ncVarOut.units = '1'
    #ncVarOut[:] = raw

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])

