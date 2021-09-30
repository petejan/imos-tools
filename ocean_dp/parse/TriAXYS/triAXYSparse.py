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

import datetime
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct
import csv
from os.path import isfile, isdir, join
from os import listdir, walk

import re
import os

first_line_expr   = r"^TRIAXYS BUOY DATA REPORT"
raw_first_line_expr = r"^Sample"
date_expr         = r"^DATE.*= ([^\(\n]*)"
num_freq_expr     = r"^NUMBER OF FREQUENCIES.*=\s*(\S*)"
num_dir_expr      = r"^NUMBER OF DIRECTIONS.*=\s*(\S*)"
num_points_expr   = r"^NUMBER OF POINTS.*=\s*(\S*)"
ini_freq_expr     = r"^INITIAL FREQUENCY \(Hz\).*=\s*(\S*)"
freq_space_expr   = r"^FREQUENCY SPACING \(Hz\).*=\s*(\S*)"
dir_space_expr    = r"^DIRECTION SPACING \(DEG\).*=\s*(\S*)"
res_freq_range    = r"^RESOLVABLE FREQUENCY RANGE \(Hz\).*=\s*(\S*).*TO\s*(\S*)"
data_line_expr    = r"^([0-9.]+)\s+([0-9.E+-]+)"
data4_line_expr    = r"^\s*([0-9.]+)\s+([0-9.E+-]+)\s+([0-9.E+-]+)\s+([0-9.E+-]+)"
non_dir_line_expr = r"^ ([0-9.E+-]+)"
version_expr      = r"^VERSION.*=\s*(\S*)"


def parse_dir_spec(output_name, file):

    # TRIAXYS BUOY DATA REPORT - TAS04811
    # VERSION = 5b.02.08
    # TYPE    = DIRECTIONAL SPECTRUM
    # DATE    = 2018 Feb 09 04:00(UTC)
    # NUMBER OF FREQUENCIES              =     129
    # INITIAL FREQUENCY (Hz)             =   0.000
    # FREQUENCY SPACING (Hz)             =   0.005
    # RESOLVABLE FREQUENCY RANGE (Hz)    =   0.030  TO  0.455
    # NUMBER OF DIRECTIONS               =     121
    # DIRECTION SPACING (DEG)            =     3.0
    # COLUMNS = 0.00 TO 360.00 DEG
    # ROWS    = 0.00 TO   0.64 Hz

    data_line = 0
    first = True

    ds = Dataset(output_name, 'a')

    with open(file, 'r', errors='ignore') as fp:
        line = fp.readline()
        matchObj = re.match(first_line_expr, line)
        if not matchObj:
            print("Not a TriAXYS file !")
            exit(-1)

        cnt = 1
        while line:

            matchObj = re.match(date_expr, line)
            if matchObj:
                # print("date_expr:matchObj.group() : ", matchObj.group())
                # print("date_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("date_expr:matchObj.group(2) : ", matchObj.group(2))
                ts = datetime.datetime.strptime(matchObj.group(1), "%Y %b %d %H:%M")
                print("timestamp ", ts)
            matchObj = re.match(num_freq_expr, line)
            if matchObj:
                # print("num_freq_expr:matchObj.group() : ", matchObj.group())
                # print("num_freq_expr:matchObj.group(1) : ", matchObj.group(1))
                num_frequencies = int(matchObj.group(1))
            matchObj = re.match(num_dir_expr, line)
            if matchObj:
                # print("num_dir_expr:matchObj.group() : ", matchObj.group())
                # print("num_dir_expr:matchObj.group(1) : ", matchObj.group(1))
                num_dir = int(matchObj.group(1))
            matchObj = re.match(ini_freq_expr, line)
            if matchObj:
                # print("ini_freq_expr:matchObj.group() : ", matchObj.group())
                # print("ini_freq_expr:matchObj.group(1) : ", matchObj.group(1))
                pass
            matchObj = re.match(freq_space_expr, line)
            if matchObj:
                # print("freq_space_expr:matchObj.group() : ", matchObj.group())
                # print("freq_space_expr:matchObj.group(1) : ", matchObj.group(1))
                freq_space = float(matchObj.group(1))
            matchObj = re.match(dir_space_expr, line)
            if matchObj:
                # print("dir_space_expr:matchObj.group() : ", matchObj.group())
                # print("dir_space_expr:matchObj.group(1) : ", matchObj.group(1))
                dir_space = float(matchObj.group(1))
            matchObj = re.match(res_freq_range, line)
            if matchObj:
                # print("res_freq_range:matchObj.group() : ", matchObj.group())
                # print("res_freq_range:matchObj.group(1) : ", matchObj.group(1))
                resolution_low = matchObj.group(1)
                resolution_high = matchObj.group(2)
            matchObj = re.match(version_expr, line)
            if matchObj:
                # print("freq_space_expr:matchObj.group() : ", matchObj.group())
                # print("freq_space_expr:matchObj.group(1) : ", matchObj.group(1))
                version = matchObj.group(1)
                pass
            matchObj = re.match(non_dir_line_expr, line)
            if matchObj:
                if first:
                    times_num = ds.variables["TIME"]
                    times = num2date(times_num[:], units=times_num.units, calendar=times_num.calendar)

                    # create dimensions
                    # print("dimensions ", ds.dimensions)
                    if "FREQ" not in ds.dimensions:
                        freq_dim = ds.createDimension("FREQ", num_frequencies)
                        # print(freq_dim)
                    if "DIR" not in ds.dimensions:
                        freq_dim = ds.createDimension("DIR", num_dir)

                    # create variables if needed
                    if "DIR_SPEC" not in ds.variables:
                        ncVarOut = ds.createVariable("DIR_SPEC", "f4", ("TIME", "FREQ", "DIR"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = "m^2/Hz"
                        ncVarOut.comment = "from directional spectrum processed file"
                        ncVarOut.comment_processing_version = version
                    else:
                        ncVarOut = ds.variables["DIR_SPEC"]

                    if "DIR_SPEC_RES_LOW" not in ds.variables:
                        dir_spec_low = ds.createVariable("DIR_SPEC_RES_LOW", "f4", ("TIME"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        dir_spec_low.units = "Hz"
                        dir_spec_low.comment = "directional spectra range low"
                    else:
                        dir_spec_low = ds.variables["DIR_SPEC_RES_LOW"]
                    if "DIR_SPEC_RES_HI" not in ds.variables:
                        dir_spec_hi = ds.createVariable("DIR_SPEC_RES_HI", "f4", ("TIME"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        dir_spec_hi.units = "Hz"
                        dir_spec_hi.comment = "directional spectra range high"
                    else:
                        dir_spec_hi = ds.variables["DIR_SPEC_RES_HI"]

                    # create coordinate variable values
                    if "FREQ" not in ds.variables:
                        freq_var = ds.createVariable("FREQ", "f4", ("FREQ"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        freq_var.units = "Hz"
                        freq_var.comment = "specra frequency"
                        f = np.arange(0, freq_space * num_frequencies, freq_space)
                        print("f shape ", f.shape)
                        freq_var[:] = f
                    else:
                        freq_var = ds.variables["FREQ"]

                    if "DIR" not in ds.variables:
                        dir_var = ds.createVariable("DIR", "f4", ("DIR"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        dir_var.units = "Hz"
                        dir_var.comment = "specra direction"
                        d = np.arange(0, dir_space * num_dir, dir_space)
                        print("d shape ", d.shape)
                        dir_var[:] = d
                    else:
                        dir_var = ds.variables["DIR"]

                    # print (ncVarOut)
                    first = False

                    data_out = np.zeros((num_frequencies, num_dir))
                    data_out.fill(np.nan)

                    time_idx = np.where(times == ts)
                    print("time index", time_idx, time_idx[0].shape)

                # print("data_line_expr:matchObj.group() : ", matchObj.group())
                # print("data_line_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("data_line_expr:matchObj.group(2) : ", matchObj.group(2))
                j = 0
                for x in line[1:].split(" "):
                    # print(data_line, j, x)
                    data_out[data_line, j] = float(x)
                    j += 1
                data_line += 1

            line = fp.readline()
            cnt += 1

        # if time index found, write data to netCDF file
        if time_idx[0].shape[0] > 0:
            ncVarOut[time_idx[0], :, :] = data_out
            dir_spec_low[time_idx[0]] = float(resolution_low)
            dir_spec_hi[time_idx[0]] = float(resolution_high)

    ds.close()


def parse_raw(output_name, file):
    # Sample,Comp(deg),Ax(m/s^2),Ay(m/s^2),Az(m/s^2),Rx(deg/s),Ry(deg/s),Rz(deg/s)
    # 1,55,0.135378,0.060822,-9.804115,4.218758,3.515633,5.781258
    # 2,55,0.135378,0.059841,-9.807058,4.218758,3.531258,5.765633
    # 3,56,0.134397,0.059841,-9.808039,4.250008,3.531258,5.781258
    # 4,56,0.134397,0.059841,-9.808039,4.250008,3.531258,5.765633
    # ...
    # 4799,56,0.135378,0.060822,-9.807058,4.218758,3.609383,5.781258
    # 4800,56,0.135378,0.060822,-9.807058,4.218758,3.609383,5.796883
    # 1802090400,SOFS-7,+000.0,,13.15,4.0,4800,120,120,1,04811,1.01.0687,,47,0,0.000,+00.0

    data_out = np.zeros((4800, 8))
    data_out.fill(np.nan)

    data_line = 0
    first = True

    ds = Dataset(output_name, 'a')

    with open(file, 'r', errors='ignore') as fp:
        line = fp.readline()
        matchObj = re.match(raw_first_line_expr, line)
        if not matchObj:
            print("Not a TriAXYS file !")
            exit(-1)
        line = fp.readline()

        cnt = 0
        while line:
            line_split = line.split(",")
            # print("RAW split len", len(line_split))
            if len(line_split) > 8:
                break

            j = 0
            for x in line_split:
                # print(cnt, j, x)
                data_out[cnt, j] = float(x)
                j += 1

            line = fp.readline()
            cnt += 1

        num_samples = cnt
        print("total samples", num_samples)

        ts = datetime.datetime.strptime(line_split[0], "%y%m%d%H%M")

        print("timestamp", ts)

        times_num = ds.variables["TIME"]
        times = num2date(times_num[:], units=times_num.units, calendar=times_num.calendar)
        time_idx = np.where(times == ts)
        print("time index", time_idx, time_idx[0].shape)

        # if time index found, write data to netCDF file
        if time_idx[0].shape[0] > 0:

            # create dimensions
            # print("dimensions ", ds.dimensions)
            if "SAMPLE" not in ds.dimensions:
                freq_dim = ds.createDimension("SAMPLE", num_samples)
                # print(freq_dim)
            if "VECTOR" not in ds.dimensions:
                freq_dim = ds.createDimension("VECTOR", 3)

            # create variables if needed
            if "ACCEL" not in ds.variables:
                accel_var = ds.createVariable("ACCEL", "f4", ("TIME", "SAMPLE", "VECTOR"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                accel_var.units = "m/s^2"
                accel_var.comment = "raw acceleration"
            else:
                accel_var = ds.variables["ACCEL"]

            if "GYRO" not in ds.variables:
                gyro_var = ds.createVariable("GYRO", "f4", ("TIME", "SAMPLE", "VECTOR"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                gyro_var.units = "rad/s"
                gyro_var.comment = "raw gyroscope measurement"
            else:
                gyro_var = ds.variables["GYRO"]

            print("size ", data_out.shape)
            accel_var[time_idx[0], :, :] = data_out[:, 2:5]
            gyro_var[time_idx[0], :, :] = data_out[:, 5:8]

    ds.close()


def parse_non_dir_spec(output_name, file):
    # TRIAXYS BUOY DATA REPORT - TAS04811
    # VERSION = 5b.02.08
    # TYPE    = NON-DIRECTIONAL SPECTRUM
    # DATE    = 2019 Jan 01 00:00(UTC)
    # NUMBER OF FREQUENCIES              =     129
    # INITIAL FREQUENCY (Hz)             =   0.000
    # FREQUENCY SPACING (Hz)             =   0.005
    # COLUMN 1 = FREQUENCY (Hz)
    # COLUMN 2 = SPECTRAL DENSITY (M^2/Hz)
    # 0.000  0.0000000E+00
    # 0.005  0.0000000E+00
    # 0.010  0.0000000E+00

    data_line = 0
    first = True

    ds = Dataset(output_name, 'a')

    with open(file, 'r', errors='ignore') as fp:
        line = fp.readline()
        matchObj = re.match(first_line_expr, line)
        if not matchObj:
            print("Not a TriAXYS file !")
            exit(-1)

        cnt = 1
        while line:

            matchObj = re.match(date_expr, line)
            if matchObj:
                # print("date_expr:matchObj.group() : ", matchObj.group())
                # print("date_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("date_expr:matchObj.group(2) : ", matchObj.group(2))
                ts = datetime.datetime.strptime(matchObj.group(1), "%Y %b %d %H:%M")
                print("timestamp ", ts)
            matchObj = re.match(num_freq_expr, line)
            if matchObj:
                # print("num_freq_expr:matchObj.group() : ", matchObj.group())
                # print("num_freq_expr:matchObj.group(1) : ", matchObj.group(1))
                num_frequencies = int(matchObj.group(1))
            matchObj = re.match(ini_freq_expr, line)
            if matchObj:
                # print("ini_freq_expr:matchObj.group() : ", matchObj.group())
                # print("ini_freq_expr:matchObj.group(1) : ", matchObj.group(1))
                pass
            matchObj = re.match(freq_space_expr, line)
            if matchObj:
                # print("freq_space_expr:matchObj.group() : ", matchObj.group())
                # print("freq_space_expr:matchObj.group(1) : ", matchObj.group(1))
                pass
            matchObj = re.match(version_expr, line)
            if matchObj:
                # print("freq_space_expr:matchObj.group() : ", matchObj.group())
                # print("freq_space_expr:matchObj.group(1) : ", matchObj.group(1))
                version = matchObj.group(1)
                pass
            matchObj = re.match(data_line_expr, line)
            if matchObj:
                if first:
                    times_num = ds.variables["TIME"]
                    times = num2date(times_num[:], units=times_num.units, calendar=times_num.calendar)
                    # print("dimensions ", ds.dimensions)
                    if "FREQ" not in ds.dimensions:
                        freq_dim = ds.createDimension("FREQ", num_frequencies)
                        # print(freq_dim)

                    if "NON_DIR_SPEC" not in ds.variables:
                        ncVarOut = ds.createVariable("NON_DIR_SPEC", "f4", ("TIME", "FREQ"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = "m^2/Hz"
                        ncVarOut.comment = "from non-directional spectrum processed file"
                        ncVarOut.comment_processing_version = version
                    else:
                        ncVarOut = ds.variables["NON_DIR_SPEC"]

                    if "FREQ" not in ds.variables:
                        freq_var = ds.createVariable("FREQ", "f4", ("FREQ"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        freq_var.units = "Hz"
                        freq_var.comment = "spectra frequency"
                    else:
                        freq_var = ds.variables["FREQ"]

                    # print (ncVarOut)
                    first = False

                    data_out = np.zeros((num_frequencies, 2))
                    data_out.fill(np.nan)

                    time_idx = np.where(times == ts)
                    print("time index", time_idx, time_idx[0].shape)

                # print("data_line_expr:matchObj.group() : ", matchObj.group())
                # print("data_line_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("data_line_expr:matchObj.group(2) : ", matchObj.group(2))
                data_out[data_line, 1] = float(matchObj.group(2))
                data_out[data_line, 0] = float(matchObj.group(1))
                data_line += 1

            line = fp.readline()
            cnt += 1

        if time_idx[0].shape[0] > 0:
            ncVarOut[time_idx[0], :] = data_out[:, 1]

        freq_var[:] = data_out[:, 0]

    ds.close()


def parse_heave(output_name, file):
    # TRIAXYS·BUOY·DATA·REPORT·-·TAS04811¶
    # VERSION→=·6a.02.08¶
    # TYPE→   =·HNE¶
    # DATE→   =·2018·Feb·09·04:00¶
    # NUMBER·OF·POINTS→       =····1382¶
    # TIME·OF·FIRST·POINT·(s)→=···60.16¶
    # SAMPLE·INTERVAL·(s)→    =····0.78¶
    # COLUMN·1·=·TIME·(s)¶
    # COLUMN·2·=·HEAVE·(m)¶
    # COLUMN·3·=·DSP·NORTH·(m)¶
    # COLUMN·4·=·DSP·EAST·(m)¶
    # ··60.16···0.00···0.00···0.00¶
    # ··60.95···0.00···0.00···0.00¶
    # ··61.73···0.00···0.00···0.00¶
    # ··62.51···0.00···0.00···0.00¶
    # ··63.29···0.00···0.00···0.00¶
    # ··64.07···0.00···0.00···0.00¶

    data_line = 0
    first = True

    ds = Dataset(output_name, 'a')

    with open(file, 'r', errors='ignore') as fp:
        line = fp.readline()
        matchObj = re.match(first_line_expr, line)
        if not matchObj:
            print("Not a TriAXYS file !")
            exit(-1)

        cnt = 1
        while line:

            matchObj = re.match(date_expr, line)
            if matchObj:
                # print("date_expr:matchObj.group() : ", matchObj.group())
                # print("date_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("date_expr:matchObj.group(2) : ", matchObj.group(2))
                ts = datetime.datetime.strptime(matchObj.group(1), "%Y %b %d %H:%M")
                print("timestamp ", ts)
            matchObj = re.match(num_points_expr, line)
            if matchObj:
                # print("num_freq_expr:matchObj.group() : ", matchObj.group())
                # print("num_freq_expr:matchObj.group(1) : ", matchObj.group(1))
                num_points = int(matchObj.group(1))
            matchObj = re.match(version_expr, line)
            if matchObj:
                # print("freq_space_expr:matchObj.group() : ", matchObj.group())
                # print("freq_space_expr:matchObj.group(1) : ", matchObj.group(1))
                version = matchObj.group(1)

            matchObj = re.match(data4_line_expr, line)
            if matchObj:
                if first:
                    times_num = ds.variables["TIME"]
                    times = num2date(times_num[:], units=times_num.units, calendar=times_num.calendar)
                    # print("dimensions ", ds.dimensions)
                    if "SAMPLE_TIME" not in ds.dimensions:
                        freq_dim = ds.createDimension("SAMPLE_TIME", num_points)
                        # print(freq_dim)
                    if "VECTOR" not in ds.dimensions:
                        freq_dim = ds.createDimension("VECTOR", 3)

                    if "HEAVE" not in ds.variables:
                        ncVarOut = ds.createVariable("HEAVE", "f4", ("TIME", "SAMPLE_TIME", "VECTOR"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = "m"
                        ncVarOut.comment = "from heave processed file, heave, north, east"
                        ncVarOut.comment_processing_version = version
                    else:
                        ncVarOut = ds.variables["HEAVE"]

                    if "SAMPLE_TIME" not in ds.variables:
                        sample_t_var = ds.createVariable("SAMPLE_TIME", "f4", ("SAMPLE_TIME"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        sample_t_var.units = "s"
                        sample_t_var.comment = "sample time"
                    else:
                        sample_t_var = ds.variables["SAMPLE_TIME"]
                        num_points = sample_t_var.shape[0]

                    # print (ncVarOut)
                    first = False
                    cnt = 0

                    data_out = np.zeros((num_points, 3))
                    data_out.fill(np.nan)
                    freq_out = np.zeros((num_points))
                    freq_out.fill(np.nan)

                    time_idx = np.where(times == ts)
                    print("time index", time_idx, time_idx[0].shape)

                # print("data_line_expr:matchObj.group() : ", matchObj.group())
                # print("data_line_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("data_line_expr:matchObj.group(2) : ", matchObj.group(2))
                freq_out[data_line] = float(matchObj.group(1))

                data_out[data_line, 0] = float(matchObj.group(2))
                data_out[data_line, 1] = float(matchObj.group(3))
                data_out[data_line, 2] = float(matchObj.group(4))
                data_line += 1

            line = fp.readline()

        if time_idx[0].shape[0] > 0:
            ncVarOut[time_idx[0], :] = data_out

        sample_t_var[:] = freq_out

    ds.close()


def parse_summary(output_name, file):

    instrument_model = 'TriAXYS'
    instrument_serialnumber = '04811'
    sample_interval = 0
    number_samples_read = 0
    times = []
    odata = []
    name = {}
    name[0] = {'var_name': 'ZCROSS', 'header': 'Zero Crossings', 'units': 'count'}
    name[1] = {'var_name': 'WAVE_HEIGHT_MEAN', 'header': 'Ave. Ht.', 'units': 'm'}
    name[2] = {'var_name': 'WAVE_PERIOD_MEAN', 'header': 'Ave. Per.', 'units': 's'}
    name[3] = {'var_name': 'WAVE_HEIGHT_MAX', 'header': 'Max Ht.', 'units': 'm'}
    name[4] = {'var_name': 'WAVE_HEIGHT_SIG', 'header': 'Sig. Wave', 'units': 'm'}
    name[5] = {'var_name': 'WAVE_PERIOD_SIG', 'header': ' Sig. Per.', 'units': 's'}
    name[6] = {'var_name': 'WAVE_PERIOD_TP', 'header': 'Peak Per.(Tp)', 'units': 's'}
    name[7] = {'var_name': 'WAVE_PERIOD_READ', 'header': 'Peak Per.(READ)', 'units': 's'}
    name[8] = {'var_name': 'WM0', 'header': 'HM0', 'units': 'm'}
    name[9] = {'var_name': 'WAVE_DIR_MEAN', 'header': 'Mean Theta', 'units': 'degrees'}
    name[10] = {'var_name': 'WAVE_DIR_SIGMA', 'header': 'Sigma Theta', 'units': 'degrees'}

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        for row in reader:
            # print(row)
            times.append(datetime.datetime.strptime(row['Date'], "%Y/%m/%d %H:%M"))
            odata.append(row)

    number_samples_read = len(times)
    print("sampled read ", number_samples_read)

    idx_sort = np.argsort(times)
    print(idx_sort)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut = Dataset(output_name, 'w', format='NETCDF4')

    ncOut.instrument = 'AXYS Technologies ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

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
    # sort the times
    t_unsorted = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)
    ncTimesOut[:] = t_unsorted[idx_sort]

    for i in name:
        v = name[i]
        print("Variable %s (%s)" % (v['var_name'], v['units']))
        varName = v['var_name']
        ncVarOut = ncOut.createVariable(varName, "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

        # print("Create Variable %s : %s" % (ncVarOut, data[v[0]]))
        data = np.zeros((number_samples_read))
        data.fill(np.nan)
        x = 0
        for j in idx_sort:
            # print (j)
            val = odata[j]
            data[x] = val[v['header']]
            x += 1

        ncVarOut[:] = data
        ncVarOut.units = v['units']

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + file)

    ncOut.close()


def parse_wave(output_name, file):

    instrument_model = 'TriAXYS'
    instrument_serialnumber = '04811'
    sample_interval = 0
    number_samples_read = 0
    times = []
    odata = []
    name = {}
    name[0] = {'var_name': 'ZCROSS', 'header': 'Number of Zero Crossings', 'units': 'count'}
    name[1] = {'var_name': 'WAVE_HEIGHT_MEAN', 'header': 'Average Wave Height (Havg)', 'units': 'm'}
    name[2] = {'var_name': 'WAVE_PERIOD_MEAN', 'header': 'Tz', 'units': 's'}
    name[3] = {'var_name': 'WAVE_HEIGHT_MAX', 'header': 'Max Wave Height (Hmax)', 'units': 'm'}
    name[4] = {'var_name': 'WAVE_HEIGHT_SIG', 'header': 'Significant Wave Height (Hsig)', 'units': 'm'}
    name[5] = {'var_name': 'WAVE_PERIOD_SIG', 'header': 'Significant Wave Period (Tsig)', 'units': 's'}
    name[6] = {'var_name': 'WAVE_PERIOD_TP', 'header': 'Tp5', 'units': 's'}
    name[7] = {'var_name': 'WAVE_PERIOD_READ', 'header': 'Peak Period', 'units': 's'}
    name[8] = {'var_name': 'WM0', 'header': 'Hm0', 'units': 'm'}
    name[9] = {'var_name': 'WAVE_DIR_MEAN', 'header': 'Mean Magnetic Direction', 'units': 'degrees'}
    name[10] = {'var_name': 'WAVE_DIR_SIGMA', 'header': 'Mean Spread', 'units': 'degrees'}

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        if 'Year' not in reader.fieldnames:
            return

        for row in reader:
            #print(row)
            times.append(datetime.datetime.strptime(row['Year']+' '+row['MonthDay']+' '+row['Time'], "%Y %m%d %H%M%S"))
            odata.append(row)

    number_samples_read = len(times)
    print("sampled read ", number_samples_read)

    idx_sort = np.argsort(times)
    print(idx_sort)

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut = Dataset(output_name, 'w', format='NETCDF4')

    ncOut.instrument = 'AXYS Technologies ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

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
    # sort the times
    t_unsorted = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)
    ncTimesOut[:] = t_unsorted[idx_sort]

    for i in name:
        v = name[i]
        print("Variable %s (%s)" % (v['var_name'], v['units']))
        varName = v['var_name']
        ncVarOut = ncOut.createVariable(varName, "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

        # print("Create Variable %s : %s" % (ncVarOut, data[v[0]]))
        data = np.zeros((number_samples_read))
        data.fill(np.nan)
        x = 0
        for j in idx_sort:
            # print (j)
            val = odata[j]
            data[x] = val[v['header']]
            x += 1

        ncVarOut[:] = data
        ncVarOut.units = v['units']

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(file))

    ncOut.close()

def parse_triaxys(files):
    output_name = "TriAXYS.nc"

    print("output file : %s" % output_name)

    # create a list of files, scanning any directories
    filelist = []
    for file in files:
        if isfile(file):
            filelist.append(file)
        elif isdir(file):
            for root, dirs, files in walk(file):
                for name in files:
                    f = join(root, name)

                    if isfile(f):
                        filelist.append(f)

    # parse each file
    for filepath in filelist:
        print(filepath)

        if filepath.endswith('Summary.txt') or filepath.endswith('Summary-Sort.txt'):
            parse_summary(output_name, filepath)

        elif filepath.endswith('.WAVE'):
            print("WAVE")
            parse_wave(output_name, filepath)

        elif filepath.endswith('.RAW'):
            print("RAW")
            parse_raw(output_name, filepath)

        elif filepath.endswith('.DIRSPEC'):
            print("DIRSPEC")
            parse_dir_spec(output_name, filepath)

        elif filepath.endswith('.NONDIRSPEC'):
            print("NONDIR SPEC")
            parse_non_dir_spec(output_name, filepath)

        elif filepath.endswith('.HNE'):
            print("Heave")
            #parse_heave(output_name, filepath)

    return output_name


if __name__ == "__main__":
    parse_triaxys(sys.argv)
