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
import re
import sys

import datetime
from datetime import UTC
from netCDF4 import date2num, num2date
from netCDF4 import Dataset
import struct
import os
import numpy as np

# /* LOGR 64 byte packed data record structure for storage in FLASH */
# struct LOGR_record
#    {
#    unsigned char hour;   /* time is not packed for ease of verifying */
#    unsigned char min;
#    unsigned char day;
#    unsigned char mon;
#    unsigned char year;   /* year is offset from 2000 (no good after year 2255 :-) */
#    unsigned short record;   /* sequential record number from startup */
#    unsigned char mux_parm;  /* which option parameter in this record */
#    short we,wn;   /* wind speed m/sec */
#                   /* (short)(we * 100) ==> +/- 327.67 m/s */
#                   /* (short)(wn * 100) ==> +/- 327.67 m/s */
#    unsigned short wsavg,wmax,wmin;      /* (ushort)(wsavg * 100) ==> 0 - 655.35 m/s */
#                                         /* (ushort)(wmax * 100) ==> 0 - 655.35 m/s */
#                                         /* (ushort)(wmin * 100) ==> 0 - 655.35 m/s */
#
#    short vdavg,compass;  /* last vane degrees, last compass degrees */
#                          /* (short)(vdavg * 10) ==> +/- 3276.7 degrees */
#                          /* (short)(compass * 10) ==> +/- 3276.7 degrees */
#
#    unsigned short bp;   /* barometer millibars */
#                /* (ushort)((bp - 900.0) * 100) ==> 900.00 - 1555.35 mbar */
#
#    short rh;  /* humidity %, deg C */
#               /* (short)(rh * 100) ==> +/- 327.67 %RH */
#    unsigned short th;  /* (ushort)((th + 20.0) * 1000) ==> -20.000 to +45.535 degC */
#
#    short sr;  /* short wave w/m^2 */
#               /* (ushort)(sr * 10) ==> +/- 3276.7 w/m^2 */
#
#    unsigned short dome,body;  /* long wave dome and body thermistors
#                                  deg Kelvin, thermopile microvolts */
#                               /* (ushort)(dome * 100) ==> 0 - 655.35 degK */
#                               /* (ushort)(body * 100) ==> 0 - 655.35 degK */
#    short tpile;               /* (short)(tpile * 10) ==> +/- 3276.7 microvolts */
#
#    short lwflux;  /* lwr flux */
#                   /* (short)(lwflux * 10) ==> +/-3276.7 w/m^2 */
#
#    short prlev;  /* precipitation values */
#                  /* (short)(prlev * 100) ==> +/-327.67 mm */
#
#    unsigned short sct; /* SeaCat sea temp deg C */
#                        /* (ushort)((sct + 5.0) * 1000) ==> -5.000 to +60.535 degC */
#    unsigned short scc; /* SeaCat conductivity Siemens */
#                        /* (ushort)(scc * 10000) ==> 0.000 to +6.5535 Siemens/meter */
#
#    short bat1,bat2,bat3,bat4;  /* misc. battery */
#                                /* (short)(bat1 * 1000) ==> +/- 32.767 VDC */
#
#    unsigned long opt_parm;     /* optional value indicated in mux_parm */
#    unsigned char ird_stat;	   /* iridium & wmo status values */
#    unsigned char wmo_stat;
#    unsigned short spare1,spare2;      /* optional spare value */
#    unsigned short used;         /* set to 0xA5A5 upon record write */
#    };

decode = []
# this is in order of unpacking
decode.append({'key': 'hour', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})
decode.append({'key': 'min', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})
decode.append({'key': 'day', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})
decode.append({'key': 'mon', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})
decode.append({'key': 'year', 'var_name': None, 'units': None, 'scale': 1, 'offset': 2000, 'unpack': 'B'})

decode.append({'key': 'record', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'mux_param', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})

decode.append({'key': 'we', 'var_name': 'UWIND', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'wn', 'var_name': 'VWIND', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'wsavg', 'var_name': 'WSPD', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'wsmax', 'var_name': 'WSPD_MAX', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'wsmin', 'var_name': 'WSPD_MIN', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'vdavg', 'var_name': 'WDIR', 'units': 'degree', 'scale': 10, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'compass', 'var_name': 'COMPASS', 'units': 'degree', 'scale': 10, 'offset': 0, 'unpack': 'h'})

decode.append({'key': 'bp', 'var_name': 'ATMP', 'units': 'mbar', 'scale': 100, 'offset': 900, 'unpack': 'H'})

decode.append({'key': 'rh', 'var_name': 'RELH', 'units': 'percent', 'scale': 100, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'th', 'var_name': 'AIRT', 'units': 'degrees_Celsius', 'scale': 1000, 'offset': -20, 'unpack': 'H'})

decode.append({'key': 'sr', 'var_name': 'SW', 'units': 'W/m^2', 'scale': 10, 'offset': 0, 'unpack': 'h'})

decode.append({'key': 'dome', 'var_name': 'TDOME', 'units': 'degrees_kelvin', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'body', 'var_name': 'TBODY', 'units': 'degrees_kelvin', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'vpile', 'var_name': 'VPILE', 'units': 'uV', 'scale': 10, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'lwflux', 'var_name': 'LW', 'units': 'W/m^2', 'scale': 10, 'offset': 0, 'unpack': 'h'})

decode.append({'key': 'prlev', 'var_name': 'RAIT', 'units': 'mm', 'scale': 100, 'offset': 0, 'unpack': 'h'})

decode.append({'key': 'sct', 'var_name': 'TEMP', 'units': 'degrees_Celsius', 'scale': 1000, 'offset': -5, 'unpack': 'H'})
decode.append({'key': 'scc', 'var_name': 'CNDC', 'units': 'S/m', 'scale': 10000, 'offset': 0, 'unpack': 'H'})

decode.append({'key': 'bat1', 'var_name': None, 'units': 'V', 'scale': 1000, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'bat2', 'var_name': None, 'units': 'V', 'scale': 1000, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'bat3', 'var_name': None, 'units': 'V', 'scale': 1000, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'bat4', 'var_name': None, 'units': 'V', 'scale': 1000, 'offset': 0, 'unpack': 'h'})

decode.append({'key': 'opt_param', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'I'})
decode.append({'key': 'ird_stat', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})
decode.append({'key': 'who_stat', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})

decode.append({'key': 'spare1', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'spare2', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'H'})

decode.append({'key': 'used', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'H'})


def parse(filepath, start_date):
    output_files = []

    ts_start = None

    number_samples_read = 0

    # create the netCDF file
    outputName = os.path.basename(filepath) + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    # add time var_nameiable

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    t_cal = "gregorian"
    t_unit = "days since 1950-01-01 00:00:00 UTC"
    tDim = ncOut.createDimension("TIME")
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=False)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = t_unit
    ncTimesOut.calendar = t_cal
    ncTimesOut.axis = "T"

    # add global attributes
    instrument_model = 'ASIMET LOG53'
    # extract logger SN from file name, from .DAT or .RAW files
    matchObj = re.match(r'.*L.*(\d\d).*[DR]A[TW]', os.path.basename(filepath))
    if matchObj:
        instrument_serialnumber = 'L' + matchObj.group(1)
    else:
        instrument_serialnumber = 'unknown'
    matchObj = re.match(r'.*L.*(\d\d).BIN', os.path.basename(filepath))
    if matchObj:
        instrument_serialnumber = 'L' + matchObj.group(1)

    ncOut.instrument = 'WHOI ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    # create keys for data index
    decode_idx = {}
    unpack = '>'
    vars = []
    data_scale = np.zeros([len(decode)])
    data_offset = np.zeros([len(decode)])
    for i in range(len(decode)):
        decode_idx[decode[i]['key']] = i
        unpack += decode[i]['unpack']
        data_scale[i] = decode[i]['scale']
        data_offset[i] = decode[i]['offset']
        if decode[i]['var_name']:
            v = ncOut.createVariable(decode[i]['var_name'], "f4", ("TIME",), zlib=True)
            v.units = decode[i]['units']
        else:
            v = None

        vars.append(v)

    # array to cache data, could be size(file)/64
    file_size = os.path.getsize(filepath)
    cache_size = int(np.floor(file_size/64))
    data_array = np.zeros([cache_size, len(decode)])
    print('file size, cache size', file_size, cache_size)

    # loop over file, adding data to netCDF file for each timestamp
    ts = None
    last_ts = None
    sample_cache_start = 0
    sample_cache_n = 0
    step_back = False

    with open(filepath, "rb") as binary_file:
        data_raw = binary_file.read(64)
        while data_raw:

            data = struct.unpack(unpack, data_raw)

            # check that this record is a used record
            if data[decode_idx['used']] == 42405 and data[decode_idx['year']] < 40 and data[decode_idx['year']] > 5:

                # decode the time
                try:
                    ts = datetime.datetime(int(data[decode_idx['year']]+2000), int(data[decode_idx['mon']]), int(data[decode_idx['day']]),
                                           int(data[decode_idx['hour']]), int(data[decode_idx['min']]), 0)

                    if start_date is None or ts > start_date:
                        # hack as sometimes the time jumps back, seems to be a fault in the logger, mostly when minutes roll over
                        if (last_ts is not None) and (ts < last_ts) and not step_back:
                            print('time step back', ts)
                            ts = last_ts + datetime.timedelta(seconds=30)
                            step_back = True

                        # keep the first timestamp
                        if ts_start is None:
                            ts_start = ts
                            print('start time', ts_start)

                        # keep sample number, only keep data where time is increasing
                        if last_ts is None or (ts > last_ts):
                            step_back = False
                            # save data to cache
                            data_array[sample_cache_n] = np.asarray(data)
                            data_array[sample_cache_n] = data_array[sample_cache_n] / data_scale + data_offset
                            # re-use data[0] as time (was hour)
                            data_array[sample_cache_n, 0] = date2num(ts, calendar=t_cal, units=t_unit)

                            sample_cache_n += 1
                            number_samples_read += 1

                            # cache full, write to netCDF file
                            if sample_cache_n >= data_array.shape[0]:
                                # some user feedback
                                feedback = []
                                for x in range(len(data)):
                                    feedback.append(decode[x]['key'] + '=' + str(data[x]))
                                print(number_samples_read, ts, ','.join(feedback))

                                ncTimesOut[sample_cache_start:number_samples_read] = data_array[:, 0]
                                for x in range(len(data)):
                                    # print(x, data_decoded[x], metadata[x])
                                    if vars[x]:
                                        vars[x][sample_cache_start:number_samples_read] = data_array[:, x]

                                sample_cache_start = number_samples_read
                                sample_cache_n = 0

                            last_ts = ts
                        else:
                            print('non-monotonic time,', number_samples_read, ts, last_ts)
                except ValueError:
                    pass

            data_raw = binary_file.read(64)

    # flush last of cache
    feedback = []
    for x in range(len(data)):
        feedback.append(decode[x]['key'] + '=' + str(data[x]))
    print(number_samples_read, ts, ','.join(feedback))
    ncTimesOut[sample_cache_start:number_samples_read] = data_array[0:sample_cache_n, 0]
    for x in range(len(data)):
        # print(x, data_decoded[x], metadata[x])
        if vars[x]:
            vars[x][sample_cache_start:number_samples_read] = data_array[0:sample_cache_n, x]

    print("number of samples", number_samples_read)
    print("file first timestamp", ts_start)
    print("file last timestamp", ts)

    ts_start = num2date(np.min(ncTimesOut[:]), calendar=t_cal, units=t_unit)
    ts_end = num2date(np.max(ncTimesOut[:]), calendar=t_cal, units=t_unit)

    ncOut.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", ts_end.strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.now(UTC).strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

    ncOut.close()

    output_files.append(outputName)

    return output_files


if __name__ == "__main__":
    start_date = None
    if len(sys.argv) > 2:
        start_date = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d")

    parse(sys.argv[1], start_date)
