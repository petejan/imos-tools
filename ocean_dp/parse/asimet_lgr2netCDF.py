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
decode.append({'key': 'hour', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})
decode.append({'key': 'min', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})
decode.append({'key': 'day', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})
decode.append({'key': 'mon', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})
decode.append({'key': 'year', 'var_name': None, 'units': None, 'scale': 1, 'offset': 2000, 'unpack': 'B'})

decode.append({'key': 'record', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'mux_param', 'var_name': None, 'units': None, 'scale': 1, 'offset': 0, 'unpack': 'B'})

decode.append({'key': 'we', 'var_name': 'WIND_E', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'wn', 'var_name': 'WIND_N', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'wsavg', 'var_name': 'WSPD', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'wsmax', 'var_name': 'WSPD_MAX', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'wsmin', 'var_name': 'WSPD_MIN', 'units': 'm/s', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'vdavg', 'var_name': 'WDIR', 'units': 'degree', 'scale': 10, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'compass', 'var_name': None, 'units': 'degree', 'scale': 10, 'offset': 0, 'unpack': 'h'})

decode.append({'key': 'bp', 'var_name': 'AIR_PRES', 'units': 'mbar', 'scale': 100, 'offset': 900, 'unpack': 'H'})

decode.append({'key': 'rh', 'var_name': 'RELH', 'units': 'percent', 'scale': 100, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'th', 'var_name': 'ATMP', 'units': 'degrees_Celsius', 'scale': 1000, 'offset': -20, 'unpack': 'H'})

decode.append({'key': 'sr', 'var_name': 'SWR', 'units': 'W/m^2', 'scale': 10, 'offset': 0, 'unpack': 'h'})

decode.append({'key': 'dome', 'var_name': None, 'units': 'degrees_kelvin', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'body', 'var_name': None, 'units': 'degrees_kelvin', 'scale': 100, 'offset': 0, 'unpack': 'H'})
decode.append({'key': 'tpile', 'var_name': None, 'units': 'uV', 'scale': 10, 'offset': 0, 'unpack': 'h'})
decode.append({'key': 'lwflux', 'var_name': 'LWR', 'units': 'W/m^2', 'scale': 10, 'offset': 0, 'unpack': 'h'})

decode.append({'key': 'prlev', 'var_name': None, 'units': 'mm', 'scale': 100, 'offset': 0, 'unpack': 'h'})

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


def parse(files):
    output_files = []

    for filepath in files:
        ts_start = None

        number_samples_read = 0

        # create the netCDF file
        outputName = filepath + ".nc"

        print("output file : %s" % outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4')

        # add time var_nameiable

        #     TIME:axis = "T";
        #     TIME:calendar = "gregorian";
        #     TIME:long_name = "time";
        #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

        tDim = ncOut.createDimension("TIME")
        ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=False)
        ncTimesOut.long_name = "time"
        ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
        ncTimesOut.calendar = "gregorian"
        ncTimesOut.axis = "T"

        # add global attributes
        instrument_model = 'ASIMET LOG53'
        # extract logger SN from file name, from .DAT or .RAW files
        matchObj = re.match(r'.*L.*(\d\d).*[DR]A[TW]', os.path.basename(filepath))
        if matchObj:
            instrument_serialnumber = 'L' + matchObj.group(1)
        else:
            instrument_serialnumber = 'unknown'

        ncOut.instrument = 'WHOI ; ' + instrument_model
        ncOut.instrument_model = instrument_model
        ncOut.instrument_serial_number = instrument_serialnumber

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        # create decoder dictonary
        decoder = {'unpack': '>', 'keys': []}
        for x in decode:
            decoder['unpack'] += x['unpack']
            decoder['keys'].append(x['key'])
        metadata = dict(zip(decoder['keys'], decode))

        # create a variable for the matadata with variables not None
        for x in metadata:
            metadata[x]['var'] = None
            if metadata[x]['var_name']:
                new_var = ncOut.createVariable(metadata[x]['var_name'], "f4", ("TIME",), zlib=False)
                new_var.units = metadata[x]['units']
                metadata[x]['var'] = new_var

        # loop over file, adding data to netCDF file for each timestamp
        ts = None
        with open(filepath, "rb") as binary_file:
            data_raw = binary_file.read(64)
            while data_raw:

                data = struct.unpack(decoder['unpack'], data_raw)
                #print(data)
                data_scaled = []
                for i in range(0, len(data)):
                    data_scaled.append((data[i] / decode[i]['scale']) + decode[i]['offset'])
                    #print('data', i, decode[i]['key'], data[i], 'scale', decode[i]['scale'], 'offset', decode[i]['offset'], data_scaled[i])

                data_decoded = dict(zip(decoder['keys'], data_scaled))

                # check that this record is a used record
                if data_decoded['used'] == 42405 and int(data_decoded['year']) < 2100 and int(data_decoded['year']) > 2005:

                    # decode the time
                    ts = datetime.datetime(int(data_decoded['year']), int(data_decoded['mon']), int(data_decoded['day']),
                                           int(data_decoded['hour']), int(data_decoded['min']), 0)

                    # print(ts, "data", data_decoded)

                    # keep the first timestamp
                    if ts_start is None:
                        ts_start = ts
                        print('start time', ts_start)

                    # save data to netCDF file
                    ncTimesOut[number_samples_read] = date2num(ts, calendar=ncTimesOut.calendar, units=ncTimesOut.units)
                    for x in data_decoded:
                        #print(x, data_decoded[x], metadata[x])
                        if metadata[x]['var_name']:
                            metadata[x]['var'][number_samples_read] = data_decoded[x]

                    # keep sample number
                    number_samples_read = number_samples_read + 1

                    # some user feedback
                    if number_samples_read % 1000 == 0:
                        print(number_samples_read, ts, "data", data_decoded)

                data_raw = binary_file.read(64)

        print("file first timestamp ", ts_start)
        print("file last timestamp ", ts)

        ts_start = num2date(np.min(ncTimesOut[:]), calendar=ncTimesOut.calendar, units=ncTimesOut.units)
        ts_end = num2date(np.max(ncTimesOut[:]), calendar=ncTimesOut.calendar, units=ncTimesOut.units)

        ncOut.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", ts_end.strftime(ncTimeFormat))

        # add creating and history entry
        ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
        ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

        ncOut.close()

        output_files.append(outputName)

    return output_files


if __name__ == "__main__":
    parse(sys.argv[1:])
