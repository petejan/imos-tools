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

import numpy
import pytz
from netCDF4 import date2num, num2date
from netCDF4 import Dataset
import numpy as np
from dateutil import parser

import sqlite3
from datetime import datetime, timezone

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

# map RBR engineerng file name to netCDF variable name
nameMap = {}
nameMap["Temp"] = "TEMP"
nameMap["Pres"] = "PRES"
nameMap["Depth"] = "DEPTH"

nameMap["temp20"] = "TEMP"
nameMap["pres26"] = "PRES"
nameMap["temp05"] = None

nameMap["temp14"] = "TEMP"
nameMap["cond10"] = "CNDC"
nameMap["pres24"] = "PRES"
nameMap["fluo10"] = "CPHL"
nameMap["par_00"] = "PAR"
nameMap["turb00"] = "TURB"

# also map units .....

unitMap = {}
unitMap["C"] = "degrees_Celsius"
unitMap["Degrees_C"] = "degrees_Celsius"
unitMap["uMol/m2/s"] = "umol/m^2/s"

def parse(file):

    filepath = file[0]

    outputName = filepath + ".nc"

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = filepath + ".nc"
    print("output file : %s" % outputName)
    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    # get a SQL lite connection
    conn = sqlite3.connect(filepath)
    cur = conn.cursor()

    # read instrument info
    cur.execute('SELECT model, serialID, firmwareVersion, partNumber FROM instruments')

    row = cur.fetchone()

    print("instrument row", row)

    # dance around to force ascii as retured result is UTF-8 and this causes netCDF to use string types instead of char arrays
    instrument_model = row[0].encode('ascii', 'ignore').decode('ascii')

    instrument_serial_number = str(row[1])
    ncOut.instrument = 'RBR ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serial_number
    ncOut.instrument_firmware_version = row[2]
    ncOut.instrument_part_number = row[3]

    print('instrument model :', instrument_model)
    print('instrument serial number :', instrument_serial_number)

    #return

    # create the netCDF TIME, PROFILE, PROFILE_SAMPLE variables

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME")

    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"

    # time offset between instrument time (ms since 1970-1-1) and netCDF time (days since 1950)
    t0 = date2num(datetime(1950, 1, 1), units=ncTimesOut.units, calendar=ncTimesOut.calendar) - date2num(datetime(1970, 1, 1), units=ncTimesOut.units, calendar=ncTimesOut.calendar)

    # fetch the channel info
    cur = conn.cursor()
    cur.execute('select channelID, shortname, longName, units, longNamePlainText, unitsPlainText, serialID from channels c join instrumentChannels ic USING (channelID) left join instrumentSensors is2 USING (channelOrder)')

    row = cur.fetchone()
    #print(cur.description)

    channel_list = {}
    while (row):
        channel_list[row[0]] = {
                                          'shortName': row[1],
                                          'longName': row[2],
                                          'units': row[3],
                                          'longNamePlainText': row[4],
                                          'unitsPlainText': row[5],
                                          'serialID': str(row[6])
                                          }
        row = cur.fetchone()

    for c in channel_list:
        print('channel list', channel_list[c])

    # fetch the start and end time from the epochs table
    cur = conn.cursor()
    cur.execute('SELECT * FROM epochs')
    row = cur.fetchone()
    print('epochs count (deploymentID, start_time, end_time)', row)
    start_time = datetime.fromtimestamp(row[1]/1000, tz=pytz.UTC)
    end_time = datetime.fromtimestamp(row[2]/1000, tz=pytz.UTC)
    print('start_time =', start_time, 'end_time =', end_time)

    # fetch the sample rates from the directional table
    cur = conn.cursor()
    cur.execute('SELECT * FROM directional')
    row = cur.fetchone()
    print('directional ', row)

    has_direction = False
    if row is not None:
        has_direction = True

        fast_period = row[3]
        slow_period = row[4]
        print('fast_period =', fast_period, 'slow_period =', slow_period)

        ncOut.comment_fast_sampling_period_ms = np.int32(fast_period)
        ncOut.comment_slow_sampling_period_ms = np.int32(slow_period)

    cur = conn.cursor()

    sql = "SELECT"+ \
            " profile.tstamp1 AS profile_start,"+ \
            " profile_down.tstamp1 AS down_start,"+ \
            " profile_down.tstamp2 AS down_end,"+ \
            " profile_up.tstamp1 AS up_start,"+ \
            " profile_up.tstamp2 AS up_end, "+ \
            " profile.tstamp2 AS profile_end "+ \
        " FROM region AS profile "+ \
        " JOIN regionCast AS up ON (profile.regionID = up.regionProfileID AND up.type = 'UP')"+ \
            " JOIN region AS profile_up ON (profile_up.regionID = up.regionID)"+ \
        " JOIN regionCast AS down ON (profile.regionID = down.regionProfileID AND down.type = 'DOWN')"+ \
            " JOIN region AS profile_down ON (profile_down.regionID = down.regionID)"+ \
        " WHERE profile.type = 'PROFILE'"

    cur.execute(sql)
    regionCasts = cur.fetchall()
    profiles = []
    mid_idx = -1
    for regionCast in regionCasts:
        profile_start_time = datetime.fromtimestamp(regionCast[0] / 1000, tz=pytz.UTC)
        if mid_idx == -1:
            # do profiles start with upcast first or downcast first
            if regionCast[0] == regionCast[1]:
                mid_idx = 2
            else:
                mid_idx = 1
        up_start_time = datetime.fromtimestamp(regionCast[mid_idx] / 1000, tz=pytz.UTC)
        profile_end_time = datetime.fromtimestamp(regionCast[5] / 1000, tz=pytz.UTC)
        p = (profile_start_time, up_start_time, profile_end_time)
        profiles.append(p)
        print('profile region', regionCast)

    print('is up first, mid_idx', mid_idx)
    # fetch the data
    cur = conn.cursor()
    cur.execute('SELECT * FROM data') # downsample100 has smaller data table

    # build the variables needed
    #print(cur.description)
    data_channels = []
    i = 0
    for channel_desc in cur.description:
        #print('data header', channel_desc)
        matchObj = re.match("channel(\d*)", channel_desc[0])
        if matchObj:
            id = matchObj.group(1)
            ch = channel_list[int(id)]
            print('channel id', id, ch, i)
            var_name = ch['shortName']
            if var_name in nameMap:
                var_name = nameMap[var_name]

            if var_name is not None:
                nc_var_out = ncOut.createVariable(var_name, "f4", ("TIME",), fill_value=np.nan, zlib=True)
                nc_var_out.long_name = ch['longNamePlainText']
                units = ch['unitsPlainText']
                if units in unitMap:
                    units = unitMap[units]

                nc_var_out.units = units

                if ch['serialID']:
                    if ch['serialID'] != 'None':
                        nc_var_out.sensor_serial_number = ch['serialID']

                data_channels.append([i, ch, nc_var_out])

        i = i + 1

    # read data table into netCDF variables
    sample = 0
    cur.arraysize = 1024*1024
    records = cur.fetchmany()
    print('fetch records=', len(records), 'sample number=', sample)
    print('array size', cur.arraysize, len(records))
    data = numpy.array(records, dtype=float)

    while len(records) > 0:
        #timestamp = datetime.fromtimestamp(data[:, 0] / 1000, tz=timezone.utc)
        #print(timestamp, records)
        #print(data[:, 0].shape)
        #ncTimesOut[sample:sample+len(records)] = data[:, 0]/1000/24/3600 - t0 # date2num(timestamp, units=ncTimesOut.units, calendar=ncTimesOut.calendar)
        
        ncTimesOut[sample:sample + len(records)] = data[:, 0]/1000/24/3600-t0

        print('data', data[0, :], ncTimesOut[sample])

        for channel_desc in data_channels:
            channel_desc[2][sample:sample+len(records)] = data[:, channel_desc[0]]

        sample = sample + len(records)

        records = cur.fetchmany()
        print('fetch records=', len(records), 'sample number=', sample)
        data = numpy.array(records, dtype=float)

        # some feedback
        #sample = sample + len(records)
        #if (sample % 1000) == 0:
        #    print(sample, timestamp)
        #if sample > 1024*1024:
        #    break

    conn.close()

    # this is slow, must be a better implementation
    print('marking profile samples')
    times = np.array(ncTimesOut[:])

    if has_direction:
        ncVarProfile = ncOut.createVariable("PROFILE", "i", ("TIME",), zlib=True, fill_value=-1)
        ncVarProfile.comment = 'profile number'
        ncVarProfile_sample = ncOut.createVariable("PROFILE_SAMPLE", "i", ("TIME",), zlib=True)
        ncVarProfile_sample.comment = 'profile sample +ve for up part and -ve down down part of profile'
        ncVarProfile_sample[:] = 0

        profile_n = 0
        for profile in profiles:
            profile_t_start = date2num(profile[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar)
            up_t_start = date2num(profile[1], units=ncTimesOut.units, calendar=ncTimesOut.calendar)
            profile_t_end = date2num(profile[2], units=ncTimesOut.units, calendar=ncTimesOut.calendar)

            idx_s = np.where(times >= profile_t_start)
            print(profile_n, 'index of first profile start', idx_s[0][0])
            idx_mid = np.where(times >= up_t_start)
            print(profile_n, 'index of first profile mid', idx_mid[0][0])
            idx_e = np.where(times >= profile_t_end)
            print(profile_n, 'index of first profile end', idx_e[0][0])

            ncVarProfile[idx_s[0][0]:idx_e[0][0]] = profile_n
            if mid_idx == 2:
                # up first
                ncVarProfile_sample[idx_s[0][0]:idx_mid[0][0]] = np.arange(1, (idx_mid[0][0] - idx_s[0][0]) + 1)
                ncVarProfile_sample[idx_mid[0][0]:idx_e[0][0]] = np.arange(1, (idx_e[0][0] - idx_mid[0][0]) + 1) * -1
            else:
                # down first
                ncVarProfile_sample[idx_s[0][0]:idx_mid[0][0]] = np.arange(1, (idx_mid[0][0] - idx_s[0][0]) + 1) * -1
                ncVarProfile_sample[idx_mid[0][0]:idx_e[0][0]] = np.arange(1, (idx_e[0][0] - idx_mid[0][0]) + 1)

            profile_n += 1


    # up_t_start = date2num(cast_up[cast_up_n][1], units=ncTimesOut.units, calendar=ncTimesOut.calendar)
    # up_t_end = date2num(cast_up[cast_up_n][2], units=ncTimesOut.units, calendar=ncTimesOut.calendar)
    # down_t_start = date2num(cast_down[cast_down_n][1], units=ncTimesOut.units, calendar=ncTimesOut.calendar)
    # down_t_end = date2num(cast_down[cast_down_n][2], units=ncTimesOut.units, calendar=ncTimesOut.calendar)

    #for i in range(1, len(ncTimesOut)):
    #    print(i, ncVarProfile[i], ncVarProfile_sample[i], ncTimesOut[i], data_channels[2][2][i])

    print('time range', ncTimesOut[0], 'to', ncTimesOut[-1])

    # mark any invalid times with max/min values
    ncTimesOut[ncTimesOut[:] < date2num(datetime(2000, 1, 1), units=ncTimesOut.units, calendar=ncTimesOut.calendar)] = date2num(datetime(2000, 1, 1), units=ncTimesOut.units, calendar=ncTimesOut.calendar)
    ncTimesOut[ncTimesOut[:] >= date2num(datetime(2100, 1, 1), units=ncTimesOut.units, calendar=ncTimesOut.calendar)] = date2num(datetime(2100, 1, 1), units=ncTimesOut.units, calendar=ncTimesOut.calendar)

    # save metadata to netCDF file
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

    ncOut.close()

    return outputName


if __name__ == "__main__":
    parse(sys.argv[1:])

