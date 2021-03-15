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

from datetime import datetime, timedelta
from cftime import num2date, date2num
from netCDF4 import Dataset
import numpy as np
from dateutil import parser

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

# map sea bird name to netCDF variable name
nameMap = {}
nameMap["TIMEK"] = "TIME"
nameMap["TIMEJ"] = "TIME"
nameMap["TIMEJV2"] = "TIME"
nameMap["TV290C"] = "TEMP"
nameMap["T090C"] = "TEMP"
nameMap["T090"] = "TEMP"
nameMap["COND0SM"] = "CNDC"
nameMap["C0SM"] = "CNDC"
nameMap["COND0USCM"] = "CNDC"
nameMap["PRDM"] = "PRES"
nameMap["PRM"] = "PRES"
nameMap["PR"] = "PRES"
nameMap["SAL00"] = "PSAL"
nameMap["SBEOPOXMMKG"] = "DOX2"
nameMap["SBEOPOXMLL"] = "DOXS"
nameMap["SBEOPOXMML"] = "DOX1"
nameMap["SBEOXTC"] = "DOX_TEMP"
nameMap["OXSOLMMKG"] = "OXSOL"
nameMap["DENSITY00"] = "DENSITY"
nameMap["DEPSM"] = "DEPTH"
nameMap["FLAG"] = None  # don't keep this variable

# also map units .....

unitMap = {}
unitMap["PSU"] = "1"
unitMap["deg C"] = "degrees_Celsius"
unitMap["ITS-90, deg C"] = "degrees_Celsius"
unitMap["db"] = "dbar"
unitMap["salt water, m"] = "m"
unitMap["density, kg/m^3"] = "kg/m^3"

# search expressions within file

first_line_expr = r"\* Sea-Bird (.*) Data File:"

hardware_expr = r"\* <HardwareData DeviceType='(\S+)' SerialNumber='(\S+)'>"
name_expr = r"# name (\d+) = (.*):\s*(.*)"
end_expr = r"\*END\*"
sampleExpr = r"\* sample interval = (\d+) seconds"
startTimeExpr = r"# start_time = ([^\[]*)"
intervalExpr = r"# interval = (\S+): (\d+)"

nvalues_expr = r"# nvalues =\s*(\d+)"
nquant_expr = r"# nquan = (\d+)"

use_expr = r".*<(Use.*)>(.?)<\/\1>"
equa_group = r".*<(.*) equation=\"(\d+)\" >"

sensor_start = r".*<sensor Channel=\"(.*)\" >"
sensor_end = r".*</sensor(.*)>"
sensor_type = r".*<(.*) SensorID=\"(.*)\" >"

comment = r"<!--(.+?)-->"
tag = r".*<(.+?)>(.+)<\/\1>|.*<(.+=.*)>"

instr_exp = r"\* Sea-Bird.?(\S+)"
sn_expr = r"\* SBE\s+\S+\s+V\s+\S+\s+SERIAL NO. (\S*)"
sn2_6_expr = r"\* Temperature SN = (\S*)"
sn39_expr = r"\* SerialNumber: (\S*)"

cast_exp = r"\* cast\s+(.*$)"


#
# parse the file
#

def parse(files):

    output_names = []

    for filepath in files:

        hdr = True
        sensor = False
        dataLine = 0
        name = []
        text = []
        number_samples_read = 0
        nVars = 0

        use_eqn = None
        eqn = None
        cal_param = None
        cal_sensor = None
        cal_tags = []
        cast = None
        instrument_serialnumber = None
        instrument_model = None

        with open(filepath, 'r', errors='ignore') as fp:
            line = fp.readline()
            matchObj = re.match(first_line_expr, line)
            #if not matchObj:
            #    print("Not a Sea Bird CNV file !")
            #    return None

            cnt = 1
            while line:
                #print("Line {}: {} : {}".format(cnt, dataLine, line.strip()))

                if hdr:
                    if sensor:
                        tmatchObj = re.match(tag, line)
                        if tmatchObj:
                            #print("sensor_tag:matchObj.group() : ", tmatchObj.group())
                            #print("sensor_tag:matchObj.group(1) : ", tmatchObj.group(1))
                            #print("sensor_tag:matchObj.group(2) : ", tmatchObj.group(2))
                            cal_param = tmatchObj.group(1)
                            cal_value = tmatchObj.group(2)

                        smatchObj = re.match(sensor_type, line)
                        if smatchObj:
                            #print("sensor_type:matchObj.group() : ", smatchObj.group())
                            #print("sensor_type:matchObj.group(1) : ", smatchObj.group(1))
                            #print("sensor_type:matchObj.group(2) : ", smatchObj.group(2))
                            cal_sensor = smatchObj.group(1)

                        if cal_param and cal_sensor and tmatchObj:
                            add_cal_tag = False
                            if not use_eqn:
                                add_cal_tag = True
                            elif use_eqn == eqn:
                                add_cal_tag = True

                            if add_cal_tag:
                                if cal_param != 'SerialNumber' and cal_param != 'CalibrationDate' and cal_param != 'SensorName':
                                    cal_value = float(cal_value)

                                cal_tags.append((cal_sensor, cal_param, cal_value))
                                #print("calibration type %s param %s value %s" % (cal_sensor, cal_param, cal_value))

                    matchObj = re.match(sensor_start, line)
                    if matchObj:
                        #print("sensor_start:matchObj.group() : ", matchObj.group())
                        #print("sensor_start:matchObj.group(1) : ", matchObj.group(1))
                        sensor = True
                        use_eqn = None
                        cal_param = None
                        cal_sensor = None

                    matchObj = re.match(sensor_end, line)
                    if matchObj:
                        #print("sensor_end:matchObj.group() : ", matchObj.group())
                        #print("sensor_end:matchObj.group(1) : ", matchObj.group(1))
                        sensor = False

                    matchObj = re.match(cast_exp, line)
                    if matchObj:
                        #print("cast_exp:matchObj.group() : ", matchObj.group())
                        #print("cast_exp:matchObj.group(1) : ", matchObj.group(1))
                        cast = matchObj.group(1)

                    matchObj = re.match(use_expr, line)
                    if matchObj:
                        #print("use_expr:matchObj.group() : ", matchObj.group())
                        #print("use_expr:matchObj.group(1) : ", matchObj.group(1))
                        #print("use_expr:matchObj.group(2) : ", matchObj.group(2))
                        use_eqn = matchObj.group(2)

                    matchObj = re.match(startTimeExpr, line)
                    if matchObj:
                        print("start_time_expr:matchObj.group() : ", matchObj.group())
                        start_time = parser.parse(matchObj.group(1))
                        print("start time ", start_time)

                    matchObj = re.match(equa_group, line)
                    if matchObj:
                        #print("equa_group:matchObj.group() : ", matchObj.group())
                        #print("equa_group:matchObj.group(1) : ", matchObj.group(1))
                        #print("equa_group:matchObj.group(2) : ", matchObj.group(2))
                        eqn = matchObj.group(2)

                    matchObj = re.match(instr_exp, line)
                    if matchObj:
                        #print("instr_exp:matchObj.group() : ", matchObj.group())
                        #print("instr_exp:matchObj.group(1) : ", matchObj.group(1))
                        instrument_model = matchObj.group(1)

                    matchObj = re.match(hardware_expr, line)
                    if matchObj:
                        #print("hardware_expr:matchObj.group() : ", matchObj.group())
                        #print("hardware_expr:matchObj.group(1) : ", matchObj.group(1))
                        #print("hardware_expr:matchObj.group(2) : ", matchObj.group(2))
                        instrument_model = matchObj.group(1)
                        instrument_serialnumber = matchObj.group(2)

                    matchObj = re.match(sn_expr, line)
                    if matchObj:
                        #print("sn_expr:matchObj.group() : ", matchObj.group())
                        print("sn_expr:matchObj.group(1) : ", matchObj.group(1))
                        instrument_serialnumber = matchObj.group(1)

                    matchObj = re.match(sn2_6_expr, line)
                    if matchObj:
                        #print("sn2_6_expr:matchObj.group() : ", matchObj.group())
                        print("sn2_6_expr:matchObj.group(1) : ", matchObj.group(1))
                        instrument_serialnumber = matchObj.group(1)

                    matchObj = re.match(sn39_expr, line)
                    if matchObj:
                        #print("sn39_expr:matchObj.group() : ", matchObj.group())
                        print("sn39_expr:matchObj.group(1) : ", matchObj.group(1))
                        instrument_serialnumber = matchObj.group(1)

                    matchObj = re.match(sampleExpr, line)
                    if matchObj:
                        #print("sampleExpr:matchObj.group() : ", matchObj.group())
                        #print("sampleExpr:matchObj.group(1) : ", matchObj.group(1))
                        sample_interval = int(matchObj.group(1))

                    matchObj = re.match(intervalExpr, line)
                    if matchObj:
                        #print("intervalExpr:matchObj.group() : ", matchObj.group())
                        #print("intervalExpr:matchObj.group(1) : ", matchObj.group(1))
                        #print("intervalExpr:matchObj.group(1) : ", matchObj.group(2))
                        sample_interval = int(matchObj.group(2))

                    matchObj = re.match(nvalues_expr, line)
                    if matchObj:
                        #print("nvalues_expr:matchObj.group() : ", matchObj.group())
                        print("nvalues_expr:matchObj.group(1) : ", matchObj.group(1))
                        number_samples = int(matchObj.group(1))

                    matchObj = re.match(name_expr, line)
                    if matchObj:
                        #print("name_expr:matchObj.group() : ", matchObj.group())
                        #print("name_expr:matchObj.group(1) : ", matchObj.group(1))
                        #print("name_expr:matchObj.group(2) : ", matchObj.group(2))
                        #print("name_expr:matchObj.group(3) : ", matchObj.group(3))
                        nameN = int(matchObj.group(1))
                        comment = matchObj.group(3)

                        unitObj = re.match(r'# name \d+ = .*:\s*[^\[]*\[([^]]*)\]', line)
                        unit = None
                        if unitObj:
                            unit = unitObj.group(1)

                        #print("unit match ", unitObj, comment)
                        try:
                            unit = unitMap[unit]
                        except KeyError:
                            pass
                            #print("unit:unitObj.group(3) : ", unit)

                        # construct a var name from the sea bird short name
                        varName = matchObj.group(2)
                        varName = re.sub(r'[-/]', '', varName).upper()

                        ncVarName = varName
                        if varName in nameMap:
                            #print('name map ', nameMap[varName])
                            ncVarName = nameMap[varName]
                        if ncVarName:
                            name.insert(nVars, {'sbe-name' : varName, 'col': nameN, 'var_name': ncVarName, 'comment': comment, 'unit': unit})
                            nVars = nVars + 1
                        print("name {} : {} ncName {}".format(nameN, varName, ncVarName))

                    matchObj = re.match(end_expr, line)
                    if matchObj:
                        hdr = False
                        nVariables = len(name)
                        if nVariables < 1:
                            print('No Variables, exiting')
                            exit(-1)

                        data = np.zeros((number_samples, nVariables))
                        data.fill(np.nan)
                else:
                    lineSplit = line.split()
                    #print(data)
                    splitVarNo = 0
                    try:
                        for v in name:
                            #print("{} : {}".format(i, v))
                            data[number_samples_read][splitVarNo] = float(lineSplit[v['col']])
                            splitVarNo = splitVarNo + 1
                        number_samples_read = number_samples_read + 1
                    except ValueError:
                        #print("bad line")
                        pass

                    dataLine = dataLine + 1

                line = fp.readline()
                cnt += 1

        if nVariables < 1:
            print('No Variables, exiting')
            exit(-1)

        # trim data to what was read
        odata = data[:number_samples_read]
        print("nSamples %d samplesRead %d nVariables %d data shape %s read %s" % (number_samples, number_samples_read, nVariables, data.shape, odata.shape))

        #
        # build the netCDF file
        #

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        outputName = filepath + ".nc"

        print("output file : %s" % outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4')

        ncOut.instrument = 'Sea-Bird Electronics ; ' + instrument_model
        ncOut.instrument_model = instrument_model
        if instrument_serialnumber:
            ncOut.instrument_serial_number = instrument_serialnumber
        if cast:
            ncOut.instrument_cast = cast

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

        t_epoc = date2num(datetime(2000, 1, 1), calendar=ncTimesOut.calendar, units=ncTimesOut.units)

        if 'TIME' not in name:
            print('No time variable, start time', start_time, 'interval', sample_interval)

            ncTimesOut[:] = [date2num(start_time + timedelta(seconds=x*sample_interval), calendar=ncTimesOut.calendar, units=ncTimesOut.units) for x in range(number_samples_read)]

        # for each variable in the data file, create a netCDF variable
        i = 0
        for v in name:
            print("Variable %s : unit %s" % (v['var_name'], v['unit']))
            varName = v['var_name']
            if varName == 'TIME':
                #print(data[:, v['col']])
                if (v['unit'] == 'julian days') | (v['sbe-name'] == 'TIMEJ'):
                    t_epoc_start = datetime(start_time.year, 1, 1) - timedelta(days=1)
                    t_epoc_jd = date2num(t_epoc_start, calendar=ncTimesOut.calendar, units=ncTimesOut.units)
                    ncTimesOut[:] = (odata[:, v['col']] + t_epoc_jd)
                    print("julian days time ", odata[0, v['col']], start_time, ncTimesOut[0])
                else:
                    print("time is seconds")
                    ncTimesOut[:] = (odata[:, v['col']]/ 3600 / 24) + t_epoc
            else:
                ncVarOut = ncOut.createVariable(varName, "f4", ("TIME",), fill_value=np.nan, zlib=True) # fill_value=nan otherwise defaults to max
                ncVarOut.comment = v['comment']
                if v['unit']:
                    ncVarOut.units = v['unit']

                # add any relevant calibration information to variables, bit of a hard coded hack
                for c in cal_tags:
                    add = False
                    if varName == 'TEMP' and c[0] == 'TemperatureSensor':
                        add = True
                    if varName == 'DOX2' and c[0] == 'OxygenSensor':
                        add = True
                    if varName == 'PRES' and c[0] == 'PressureSensor':
                        add = True
                    if varName == 'CNDC' and c[0] == 'ConductivitySensor':
                        add = True

                    if add:
                        ncVarOut.setncattr('calibration_' + c[1], c[2])

                #print("Create Variable %s : %s" % (ncVarOut, data[v[0]]))
                ncVarOut[:] = odata[:, v['col']]

            i = i + 1

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
    parse(sys.argv[1:])
