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
import os

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

first_line_expr = r"// DEFI Series"
inst_no         = r"SondeNo=(.*)$"
inst_type       = r"SondeName=(.*)$"
coeff_date_expr = r"CoefDate=(.*)$"
item            = r"\[Item\]$"
hdr_setting     = r"(.*)=(.*)$"

# // DEFI Series
# // CSV File
# // Firmware Version 1.00
# // Software Version 1.01
# [Head]
# SondeName=DEFI-L
# SondeNo=082V023
# SensorType=Q2B0
# SensorType2=0101
# SensorType3=60
# Channel=2
# Interval=60
# SampleCnt=256579
# StartTime=2013/04/28 00:00:00
# EndTime=2013/10/23 04:18:00
# StopTime=2013/10/23 04:18:34
# Status=01000
# DepAdjRho=1.0250
# ImmersionEN=1
# Old_ImmersionEN=0
# CoefDate=2013/02/06
# Immersion_Effect=1.39
# Ch1=1.191277e04,-1.836407e-01,0.000000e00,0.000000e00,0.000000e00,0.000000e00,0.000000e00,0.000000e00,
# Ch2=1.169766e-02,8.109043e-04,0.000000e00,0.000000e00,0.000000e00,0.000000e00,0.000000e00,0.000000e00,
# [Item]
# TimeStamp,Quantum [umol/(m^2s)],Batt. [V],
# 2013/04/28 00:00:00,0.4,1.6,
# 2013/04/28 00:01:00,0.6,1.6,
# 2013/04/28 00:02:00,0.4,1.6,
# 2013/04/28 00:03:00,0.4,1.6,
# 2013/04/28 00:04:00,0.3,1.6,


def parse(file):

    hdr = True
    dataLine = 0
    number_samples_read = 0
    nVars = 0
    data = []
    raw = []
    ts = []
    settings = []
    instrument_model = 'DEFI-L'
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
                        instrument_model = matchObj.group(1)

                    matchObj = re.match(hdr_setting, line_s)
                    if matchObj:
                        #print("hdr_setting:matchObj.group() : ", matchObj.group())
                        #print("hdr_setting:matchObj.group(1) : ", matchObj.group(1))
                        settings.append([matchObj.group(1), matchObj.group(2)])

            else:
                lineSplit = line.strip().split(sep)
                #print('Split ', lineSplit)

                t = datetime.strptime(lineSplit[0], '%Y/%m/%d %H:%M:%S')

                ts.append(t)
                data.append(float(lineSplit[1]))
                raw.append(float(lineSplit[2]))

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

    ncOut.instrument = 'JFE Advantech ; ' + instrument_model
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
    ncVarOut.standard_name = 'downwelling_photosynthetic_photon_flux_in_sea_water'
    ncVarOut.long_name = 'downwelling_photosynthetic_photon_flux_in_sea_water'
    ncVarOut.units = 'umol/m^2/s'
    ncVarOut.calibration_date = coeff_date
    ncVarOut.sensor_SeaVoX_L22_code = 'SDN:L22::TOOL1126'
    ncVarOut.comment_sensor_type = 'cosine sensor'
    ncVarOut[:] = data

    #ncVarOut = ncOut.createVariable('PAR_RAW', "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
    #ncVarOut.units = '1'
    #ncVarOut[:] = raw

    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])

