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
import zipfile

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

times = []


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
                    if "DIR_SPEC" not in ds.dimensions:
                        freq_dim = ds.createDimension("DIR_SPEC", num_dir)

                    # create variables if needed
                    if "DIR_SPEC" not in ds.variables:
                        ncVarOut = ds.createVariable("DIR_SPEC", "f4", ("TIME", "FREQ", "DIR_SPEC"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = "m^2/Hz"
                        ncVarOut.comment = "from directional spectrum processed file"
                        ncVarOut.comment_processing_version = version
                    else:
                        ncVarOut = ds.variables["DIR_SPEC"]

                    # TODO: use these are a mask on DIR_SPEC rather than creating new variables
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
                        freq_var = ds.createVariable("FREQ", "f4", ("FREQ"), fill_value=None)  # no fill value for dimensions
                        freq_var.units = "Hz"
                        freq_var.comment = "specra frequency"
                        f = np.arange(0, freq_space * num_frequencies, freq_space)
                        print("f shape ", f.shape)
                        freq_var[:] = f
                    else:
                        freq_var = ds.variables["FREQ"]

                    if "DIR_SPEC" not in ds.variables:
                        dir_var = ds.createVariable("DIR_SPEC", "f4", ("DIR_SPEC"), fill_value=None)  # no fill value for dimensions
                        dir_var.units = "Hz"
                        dir_var.comment = "specra direction"
                        d = np.arange(0, dir_space * num_dir, dir_space)
                        print("d shape ", d.shape)
                        dir_var[:] = d
                    else:
                        dir_var = ds.variables["DIR_SPEC"]

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
        print("raw total samples", num_samples)

        ts = datetime.datetime.strptime(line_split[0], "%y%m%d%H%M")

        print("raw timestamp", ts)

        times_num = ds.variables["TIME"]
        times = num2date(times_num[:], units=times_num.units, calendar=times_num.calendar)
        time_idx = np.where(times == ts)
        print("time index", time_idx, time_idx[0].shape)

        # if time index found, write data to netCDF file
        if time_idx[0].shape[0] > 0:

            # create dimensions
            # print("dimensions ", ds.dimensions)
            if "RAW_SAMPLE" not in ds.dimensions:
                freq_dim = ds.createDimension("RAW_SAMPLE", num_samples)
                # print(freq_dim)
            if "RAW_VECTOR" not in ds.dimensions:
                freq_dim = ds.createDimension("RAW_VECTOR", 3)

            # create variables if needed
            if "ACCEL" not in ds.variables:
                accel_var = ds.createVariable("ACCEL", "f4", ("TIME", "RAW_SAMPLE", "RAW_VECTOR"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                accel_var.units = "m/s^2"
                accel_var.comment = "raw acceleration, x y z"
            else:
                accel_var = ds.variables["ACCEL"]

            if "GYRO" not in ds.variables:
                gyro_var = ds.createVariable("GYRO", "f4", ("TIME", "RAW_SAMPLE", "RAW_VECTOR"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                gyro_var.units = "rad/s"
                gyro_var.comment = "raw gyroscope measurement, x y z"
            else:
                gyro_var = ds.variables["GYRO"]

            if "COMPASS" not in ds.variables:
                comp_var = ds.createVariable("COMPASS", "f4", ("TIME", "RAW_SAMPLE"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                comp_var.units = "degrees"
                comp_var.comment = "compass direction"
            else:
                comp_var = ds.variables["COMPASS"]

            comp_var[time_idx[0], :] = data_out[:, 1]
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
                        freq_dim = ds.createDimension("FREQ", 129)
                        # print(freq_dim)

                    if "NON_DIR_SPEC" not in ds.variables:
                        ncVarOut = ds.createVariable("NON_DIR_SPEC", "f4", ("TIME", "FREQ"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = "m^2/Hz"
                        ncVarOut.comment = "from non-directional spectrum processed file"
                        ncVarOut.comment_processing_version = version
                    else:
                        ncVarOut = ds.variables["NON_DIR_SPEC"]

                    freq = np.arange(0, 0.005 * 129, 0.005)
                    if "FREQ" not in ds.variables:
                        freq_var = ds.createVariable("FREQ", "f4", ("FREQ"), fill_value=None)  # fill_value=nan otherwise defaults to max
                        freq_var.units = "Hz"
                        freq_var.comment = "frequency"
                        freq_var[:] = freq
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

        xy, x_ind, y_ind = np.intersect1d(np.rint(data_out[:, 0]*1000), np.rint(freq*1000), return_indices=True)
        print('freq overlap', xy)

        d0 = np.zeros(129)
        if time_idx[0].shape[0] > 0:
            d0.fill(np.nan)
            d0[y_ind] = data_out[x_ind, 1]
            ncVarOut[time_idx[0], :] = d0

    ds.close()


def parse_mean_dir(output_name, file):
    # TRIAXYS BUOY DATA REPORT
    # VERSION = 5b.02.08
    # TYPE    = MEAN DIRECTION
    # DATE    = 2011 Aug 06 00:00(UTC)
    # NUMBER OF FREQUENCIES              =      98
    # INITIAL FREQUENCY (Hz)             =   0.030
    # FREQUENCY SPACING (Hz)             =   0.005
    # RESOLVABLE FREQUENCY RANGE (Hz)    =   0.030  TO  0.515
    # S(f) WEIGHTED MEAN WAVE DIRECTION  =  307.41
    # S(f) WEIGHTED MEAN SPREADING WIDTH =   35.98
    # COLUMN 1 = FREQUENCY (Hz)
    # COLUMN 2 = SPECTRAL DENSITY (M^2/Hz)
    # COLUMN 3 = MEAN WAVE DIRECTION (DEG)
    # COLUMN 4 = SPREADING WIDTH (DEG)
    # 0.030  0.5485713E-05    221.77     33.13
    # 0.035  0.8881006E-05    264.26     41.79
    # 0.040  0.5643119E-05    339.83     34.62
    # 0.045  0.3431466E-05    326.11     34.49

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
                init_freq = float(matchObj.group(1))
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
            matchObj = re.match(data4_line_expr, line)
            if matchObj:
                if first:
                    times_num = ds.variables["TIME"]
                    times = num2date(times_num[:], units=times_num.units, calendar=times_num.calendar)
                    # print("dimensions ", ds.dimensions)
                    if "FREQ" not in ds.dimensions:
                        freq_dim = ds.createDimension("FREQ", 129)
                    else:
                        freq_dim = ds.dimensions['FREQ']
                    # print(freq_dim)

                    if "MEAN_DENSITY" not in ds.variables:
                        ncVarOut_d = ds.createVariable("MEAN_DENSITY", "f4", ("TIME", "FREQ"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        ncVarOut_d.units = "m^2/Hz"
                        ncVarOut_d.comment = "spectral density"
                        ncVarOut_d.comment_processing_version = version
                    else:
                        ncVarOut_d = ds.variables["MEAN_DENSITY"]
                    if "MEAN_DIR" not in ds.variables:
                        ncVarOut = ds.createVariable("MEAN_DIR", "f4", ("TIME", "FREQ"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = "degrees"
                        ncVarOut.comment = "mean direction"
                        ncVarOut.comment_processing_version = version
                    else:
                        ncVarOut = ds.variables["MEAN_DIR"]

                    if "MEAN_DIR_SPREAD" not in ds.variables:
                        ncVarOut_s = ds.createVariable("MEAN_DIR_SPREAD", "f4", ("TIME", "FREQ"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        ncVarOut_s.units = "degrees"
                        ncVarOut_s.comment = "mean direction spread"
                        ncVarOut_s.comment_processing_version = version
                    else:
                        ncVarOut_s = ds.variables["MEAN_DIR_SPREAD"]

                    freq = np.arange(0, 0.005 * 129, 0.005)
                    if "FREQ" not in ds.variables:
                        freq_var = ds.createVariable("FREQ", "f4", ("FREQ"), fill_value=None)  # fill_value=nan otherwise defaults to max
                        freq_var.units = "Hz"
                        freq_var.comment = "frequency"
                        freq_var[:] = freq

                    else:
                        freq_var = ds.variables["FREQ"]

                    # print (ncVarOut)
                    first = False

                    data_out = np.zeros((num_frequencies, 4))
                    data_out.fill(np.nan)

                    time_idx = np.where(times == ts)
                    print("time index", time_idx, time_idx[0].shape)

                # print("data_line_expr:matchObj.group() : ", matchObj.group())
                # print("data_line_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("data_line_expr:matchObj.group(2) : ", matchObj.group(2))
                data_out[data_line, 1] = float(matchObj.group(2))
                data_out[data_line, 2] = float(matchObj.group(3))
                data_out[data_line, 3] = float(matchObj.group(4))
                data_out[data_line, 0] = float(matchObj.group(1))
                data_line += 1

            line = fp.readline()
            cnt += 1

        # match frequencies from file with ones in netCDF FREQ variable, need to be careful that
        # frequences (which are floats) match (need to be the same type, not single and float)

        #print('freq dim', freq)
        #print('freq read', data_out[:, 0])

        xy, x_ind, y_ind = np.intersect1d(np.rint(data_out[:, 0]*1000), np.rint(freq*1000), return_indices=True)

        #print('freq overlap', xy)
        #print('freq overlap x_ind', x_ind)
        #print('freq overlap y_ind', y_ind)

        #print('shape ncVarOut', ncVarOut[:].shape)
        #print('shape data_out', data_out.shape)

        #print('data_out', data_out[x_ind, 1])

        d0 = np.zeros(129)
        if time_idx[0].shape[0] > 0:
            d0.fill(np.nan)
            d0[y_ind] = data_out[x_ind, 1]
            ncVarOut_d[time_idx[0], :] = d0
            d0.fill(np.nan)
            d0[y_ind] = data_out[x_ind, 2]
            ncVarOut[time_idx[0], :] = d0
            d0.fill(np.nan)
            d0[y_ind] = data_out[x_ind, 3]
            ncVarOut_s[time_idx[0], :] = d0

    ds.close()


def parse_heave(output_name, file):
    # TRIAXYS·BUOY·DATA·REPORT·-·TAS04811¶
    # VERSION→=·6a.02.08¶
    # TYPE→   =·HNE¶
    # DATE→   =·2018·Feb·09·04:00¶
    # NUMBER·OF·POINTS→       =····1382¶
    # TIME·OF·FIRST·POINT·(s)→=···60.16¶
    # RAW_SAMPLE·INTERVAL·(s)→    =····0.78¶
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
                    if "UVH_SAMPLE" not in ds.dimensions:
                        sample_dim = ds.createDimension("UVH_SAMPLE", num_points)
                        # print(freq_dim)
                    if "UVH_VECTOR" not in ds.dimensions:
                        heave_vector_dim = ds.createDimension("UVH_VECTOR", 3)

                    if "UVH" not in ds.variables:
                        ncVarOut = ds.createVariable("UVH", "f4", ("TIME", "UVH_SAMPLE", "UVH_VECTOR"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = "m"
                        ncVarOut.comment = "from heave processed file, up (heave), north, east"
                        ncVarOut.comment_processing_version = version
                    else:
                        ncVarOut = ds.variables["UVH"]

                    if "UVH_SAMPLE" not in ds.variables:
                        sample_t_var = ds.createVariable("UVH_SAMPLE", "f4", ("UVH_SAMPLE"), fill_value=None)  # fill_value=nan otherwise defaults to max
                        sample_t_var.units = "s"
                        sample_t_var.comment = "heave sample time"
                    else:
                        sample_t_var = ds.variables["UVH_SAMPLE"]
                        num_points = sample_t_var.shape[0]

                    # print (ncVarOut)
                    first = False
                    cnt = 0

                    data_out = np.zeros((num_points, 3))
                    data_out.fill(np.nan)
                    sample_time_out = np.zeros((num_points))
                    sample_time_out.fill(np.nan)

                    time_idx = np.where(times == ts)
                    print("time index", time_idx, time_idx[0].shape)

                # print("data_line_expr:matchObj.group() : ", matchObj.group())
                # print("data_line_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("data_line_expr:matchObj.group(2) : ", matchObj.group(2))
                sample_time_out[data_line] = float(matchObj.group(1))

                data_out[data_line, 0] = float(matchObj.group(2))
                data_out[data_line, 1] = float(matchObj.group(3))
                data_out[data_line, 2] = float(matchObj.group(4))
                data_line += 1

            line = fp.readline()

        if time_idx[0].shape[0] > 0:
            ncVarOut[time_idx[0], :] = data_out

        sample_t_var[:] = sample_time_out

    ds.close()


def parse_velocity(output_name, file):
    # TRIAXYS BUOY DATA REPORT - TAS04811
    # VERSION = 6a.02.08
    # TYPE    = UVH
    # DATE    = 2018 Feb 19 04:00
    # NUMBER OF POINTS        =    7761
    # TIME OF FIRST POINT (s) =   59.98
    # RAW_SAMPLE INTERVAL (s)     =    0.14
    # COLUMN 1 = TIME (s)
    # COLUMN 2 = HEAVE (m)
    # COLUMN 3 = VEL NORTH (m/s)
    # COLUMN 4 = VEL WEST (m/s)
    # 59.98   0.00   0.00   0.00
    # 60.12   0.00   0.00   0.00

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
                    if "HNE_SAMPLE" not in ds.dimensions:
                        velocity_sample_dim = ds.createDimension("HNE_SAMPLE", num_points)
                        # print(freq_dim)
                    if "HNE_VECTOR" not in ds.dimensions:
                        velocity_sample_dim = ds.createDimension("HNE_VECTOR", 3)

                    if "HNE" not in ds.variables:
                        ncVarOut = ds.createVariable("HNE", "f4", ("TIME", "HNE_SAMPLE", "HNE_VECTOR"), fill_value=np.nan)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = "m"
                        ncVarOut.comment = "from velocity file processed file, heave, velocity north, velocity east"
                        ncVarOut.comment_processing_version = version
                    else:
                        ncVarOut = ds.variables["HNE"]

                    if "HNE_SAMPLE" not in ds.variables:
                        sample_t_var = ds.createVariable("HNE_SAMPLE", "f4", ("HNE_SAMPLE"), fill_value=None)  # fill_value=nan otherwise defaults to max
                        sample_t_var.units = "s"
                        sample_t_var.comment = "HNE sample time"
                    else:
                        sample_t_var = ds.variables["HNE_SAMPLE"]
                        num_points = sample_t_var.shape[0]

                    # print (ncVarOut)
                    first = False
                    cnt = 0

                    data_out = np.zeros((num_points, 3))
                    data_out.fill(np.nan)
                    sample_time_out = np.zeros((num_points))
                    sample_time_out.fill(np.nan)

                    time_idx = np.where(times == ts)
                    print("time index", time_idx, time_idx[0].shape)

                # print("data_line_expr:matchObj.group() : ", matchObj.group())
                # print("data_line_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("data_line_expr:matchObj.group(2) : ", matchObj.group(2))
                sample_time_out[data_line] = float(matchObj.group(1))

                data_out[data_line, 0] = float(matchObj.group(2))
                data_out[data_line, 1] = float(matchObj.group(3))
                data_out[data_line, 2] = float(matchObj.group(4))
                data_line += 1

            line = fp.readline()

        if time_idx[0].shape[0] > 0:
            ncVarOut[time_idx[0], :] = data_out

        sample_t_var[:] = sample_time_out

    ds.close()


def parse_fourier(output_name, file):
    # TRIAXYS BUOY DATA REPORT
    # VERSION = 5b.02.08
    # TYPE    = FOURIER COEFFICIENTS
    # DATE    = 2013 Apr 19 06:00(UTC)
    # NUMBER OF FREQUENCIES              =      93
    # INITIAL FREQUENCY (Hz)             =   0.030
    # FREQUENCY SPACING (Hz)             =   0.005
    # RESOLVABLE FREQUENCY RANGE (Hz)    =   0.030  TO  0.490
    # COLUMN 1 = FREQUENCY (Hz)
    # COLUMN 2 = FOURIER COEFFICIENT a1
    # COLUMN 3 = FOURIER COEFFICIENT b1
    # COLUMN 4 = FOURIER COEFFICIENT a2
    # COLUMN 5 = FOURIER COEFFICIENT b2
    # 0.030 -0.2765341E+00 -0.2847367E+00 -0.1726459E-01  0.6567497E+00
    # 0.035 -0.1771941E+00 -0.2597628E+00  0.3675945E-01  0.6103938E+00
    # 0.040 -0.4989497E-01 -0.1844904E+00  0.1371254E+00  0.5349311E+00
    # 0.045  0.5443965E-01 -0.4889831E-01  0.2872230E+00  0.4530604E+00
    # 0.050  0.5766423E-01  0.4027935E-01  0.4134434E+00  0.4250077E+00
    # 0.055  0.9941293E-01  0.6338648E-01  0.4903382E+00  0.3010178E+00

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
            matchObj = re.match(data4_line_expr, line)
            if matchObj:
                if first:
                    times_num = ds.variables["TIME"]
                    times = num2date(times_num[:], units=times_num.units, calendar=times_num.calendar)
                    # print("dimensions ", ds.dimensions)
                    if "FREQ" not in ds.dimensions:
                        freq_dim = ds.createDimension("FREQ", 129)
                    else:
                        freq_dim = ds.dimensions['FREQ']
                    # print(freq_dim)
                    if "FOURIER_COEFF" not in ds.dimensions:
                        coeff_dim = ds.createDimension("FOURIER_COEFF", 4)

                    if "FOURIER_SPEC" not in ds.variables:
                        ncVarOut = ds.createVariable("FOURIER_SPEC", "f4", ("TIME", "FREQ", "FOURIER_COEFF"), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
                        ncVarOut.units = "m^2/Hz"
                        ncVarOut.comment = "fourier coefficents, a1, b1, a2, b2"
                        ncVarOut.comment_processing_version = version
                    else:
                        ncVarOut = ds.variables["FOURIER_SPEC"]

                    freq = np.arange(0, 0.005 * 129, 0.005)
                    if "FREQ" not in ds.variables:
                        freq_var = ds.createVariable("FREQ", "f4", ("FREQ"), fill_value=None)
                        freq_var.units = "Hz"
                        freq_var.comment = "frequency"
                        freq_var[:] = freq
                    else:
                        freq_var = ds.variables["FREQ"]

                    # print (ncVarOut)
                    first = False

                    data_out = np.zeros((num_frequencies, 5))
                    data_out.fill(np.nan)

                    time_idx = np.where(times == ts)
                    print("time index", time_idx, time_idx[0].shape)

                # print("data_line_expr:matchObj.group() : ", matchObj.group())
                # print("data_line_expr:matchObj.group(1) : ", matchObj.group(1))
                # print("data_line_expr:matchObj.group(2) : ", matchObj.group(2))
                data_out[data_line, 0] = float(matchObj.group(1))
                data_out[data_line, 1] = float(matchObj.group(2))
                data_out[data_line, 2] = float(matchObj.group(3))
                data_out[data_line, 3] = float(matchObj.group(4))
                data_out[data_line, 4] = float(matchObj.group(4))

                data_line += 1

            line = fp.readline()
            cnt += 1

        xy, x_ind, y_ind = np.intersect1d(np.rint(data_out[:, 0]*1000), np.rint(freq*1000), return_indices=True)
        #print('freq overlap', xy)
        #print('freq overlap x_ind', x_ind)
        #print('freq overlap y_ind', y_ind)

        #print('shape ncVarOut', ncVarOut[:].shape)
        #print('shape data_out', data_out.shape)

        #print('data_out', data_out[x_ind, 1])

        d0 = np.zeros((129, 4))
        if time_idx[0].shape[0] > 0:
            d0.fill(np.nan)
            d0[y_ind, :] = data_out[x_ind, 1:5]
            ncVarOut[time_idx[0], :] = d0

    ds.close()

wave_params = {}

# WAVE file:
# Received
# Year
# MonthDay
# Time
# Buoy ID
# Location
# Number of Zero Crossings
# Average Wave Height (Havg)
# Tz
# Max Wave Height (Hmax)
# Significant Wave Height (Hsig)
# Significant Wave Period (Tsig)
# H 10
# T 10
# Mean Period
# Peak Period
# Tp5
# Hm0
# Mean Magnetic Direction -- maybe a wave direction to (not wave from direction)
# Mean Spread
# Mean True Direction
# Te
# Wave Steepness
wave_params['Number of Zero Crossings'] = {'var_name': 'ZC', 'units': '1', 'comment': 'Number of waves detected by zero-crossing analysis of the wave elevation record'}
wave_params['Average Wave Height (Havg)'] = {'var_name': 'Hav', 'units': 'm', 'comment': 'Average zero down-crossing wave height (m)'}
wave_params['Mean Period'] = {'var_name': 'Tav', 'units': 's', 'comment': 'Average zero down-crossing wave period (s)'}
wave_params['Max Wave Height (Hmax)'] = {'var_name': 'Hmax', 'units': 'm', 'comment': 'Maximum zero down-crossing wave height (trough to peak) (m)'}
wave_params['Significant Wave Height (Hsig)'] = {'var_name': 'Hs', 'units': 'm', 'comment': 'Zero down-crossing significant wave height, Hs, where Hs is the average height of the highest third of the waves (m)'}
wave_params['Significant Wave Period (Tsig)'] = {'var_name': 'Ts', 'units': '1', 'comment': 'Average period of the significant zero down-crossing waves (s)'}
wave_params['Peak Period'] = {'var_name': 'Tp', 'units': 's', 'comment': 'Peak wave period Tp in seconds. Tp = 1.0/fp where fp is the frequency at which the wave spectrum S(f) has its maximum value'}
wave_params['Tp5'] = {'var_name': 'Tp5', 'units': 's', 'comment': 'Peak wave period in seconds as computed by the Read method. Tp5 has less statistical variability than Tp because it is based on spectral moments'}
wave_params['Hm0'] = {'var_name': 'Hm0', 'units': 'm', 'comment': 'Significant wave height in metres as estimated from spectral moment mo. Hmo = 4.0 * SQRT(m0) where m0 is the integral of S(f)*df from f = F1 to F2'}
wave_params['Mean Magnetic Direction'] = {'var_name': 'WAVE_DIR_MEAN', 'units': 'degree', 'comment': 'Overall mean wave direction in degrees obtained by averaging the mean wave angle θ over all frequencies with weighting function S(f). θ is calculated by the KVH method'}
wave_params['Mean Spread'] = {'var_name': 'WAVE_DIR_SPREAD', 'units': 'degree', 'comment': 'Overall directional spreading width in degrees obtained by averaging the spreading width sigma theta, σθ, over all frequencies with weighting function S(f). σθ is calculated by the KVH method'}
wave_params['Tz'] = {'var_name': 'Tz', 'units': 's', 'comment': 'Estimated period from spectral moments m0 and m2, where Tz = SQRT(m0/m2)'}
wave_params['H 10'] = {'var_name': 'H10', 'units': 'm', 'comment': 'average height of highest tenth of waves'}
wave_params['T 10'] = {'var_name': 'T10', 'units': 's', 'comment': 'average period of H10 waves'}

# Summary file: (SOFS-2 data has this as a summary file, the older processing software outputs this)
# Date
# Year
# Julian Date
# Zero Crossings
# Ave. Ht.
# Ave. Per.
# Max Ht.
# Sig. Wave
# Sig. Per.
# Peak Per.(Tp)
# Peak Per.(READ)
# HM0
# Mean Theta
# Sigma Theta
# H1/10
# T.H1/10
# Mean Per.(Tz)

wave_params['Zero Crossings'] = {'var_name': 'ZC', 'units': '1', 'comment': 'Number of waves detected by zero-crossing analysis of the wave elevation record'}
wave_params['Ave. Ht.'] = {'var_name': 'Hav', 'units': 'm', 'comment': 'Average zero down-crossing wave height (m)'}
wave_params['Ave. Per.'] = {'var_name': 'Tav', 'units': 's', 'comment': 'Average zero down-crossing wave period (s)'}
wave_params['Max Ht.'] = {'var_name': 'Hmax', 'units': 'm', 'comment': 'Maximum zero down-crossing wave height (trough to peak) (m)'}
wave_params['Sig. Wave'] = {'var_name': 'Hs', 'units': 'm', 'comment': 'Zero down-crossing significant wave height, Hs, where Hs is the average height of the highest third of the waves (m)'}
wave_params[' Sig. Per.'] = {'var_name': 'Ts', 'units': '1', 'comment': 'Average period of the significant zero down-crossing waves (s)'}
wave_params['Peak Per.(Tp)'] = {'var_name': 'Tp', 'units': 's', 'comment': 'Peak wave period Tp in seconds. Tp = 1.0/fp where fp is the frequency at which the wave spectrum S(f) has its maximum value'}
wave_params['Peak Per.(READ)'] = {'var_name': 'Tp5', 'units': 's', 'comment': 'Peak wave period in seconds as computed by the Read method. Tp5 has less statistical variability than Tp because it is based on spectral moments'}
wave_params['HM0'] = {'var_name': 'Hm0', 'units': 'm', 'comment': 'Significant wave height in metres as estimated from spectral moment mo. Hmo = 4.0 * SQRT(m0) where m0 is the integral of S(f)*df from f = F1 to F2'}
wave_params['Mean Theta'] = {'var_name': 'WAVE_DIR_MEAN', 'units': 'degree', 'comment': 'Overall mean wave direction in degrees obtained by averaging the mean wave angle θ over all frequencies with weighting function S(f). θ is calculated by the KVH method'}
wave_params['Sigma Theta'] = {'var_name': 'WAVE_DIR_SPREAD', 'units': 'degree', 'comment': 'Overall directional spreading width in degrees obtained by averaging the spreading width sigma theta, σθ, over all frequencies with weighting function S(f). σθ is calculated by the KVH method'}
wave_params['Mean Per.(Tz)'] = {'var_name': 'Tz', 'units': 's', 'comment': 'Estimated period from spectral moments m0 and m2, where Tz = SQRT(m0/m2)'}
wave_params['H1/10'] = {'var_name': 'H10', 'units': 'm', 'comment': 'average height of highest tenth of waves'}
wave_params['T.H1/10'] = {'var_name': 'T10', 'units': 's', 'comment': 'average period of H10 waves'}

def parse_summary(output_name, file):

    ncOut = Dataset(output_name, 'a')

    odata = []

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        for row in reader:
            #print(row)
            times.append(datetime.datetime.strptime(row['Date'], "%Y/%m/%d %H:%M"))
            odata.append(row)

    number_samples_read = len(times)
    print("sampled read ", number_samples_read)

    idx_sort = np.argsort(times)
    print('sorted time idx', idx_sort)

    for var in odata[0]:
        print('var', var, 'value', odata[0][var])
        if var in wave_params:
            v = wave_params[var]
            print("Variable %s (%s)" % (v['var_name'], v['units']))
            varName = v['var_name']
            if varName not in ncOut.variables:
                ncVarOut = ncOut.createVariable(varName, "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
            else:
                ncVarOut = ncOut.variables[varName]

            data = np.zeros((number_samples_read))
            data.fill(np.nan)
            x = 0
            for time_idx in idx_sort:

                data[x] = float(odata[time_idx][var])
                x += 1
                ncVarOut[:] = data
                ncVarOut.units = v['units']
                if 'comment' in v:
                    ncVarOut.comment = v['comment']

    ncOut.close()

    return number_samples_read


def parse_wave_time(output_name, file):

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        if 'Year' not in reader.fieldnames:
            return

        for row in reader:
            #print(row)
            times.append(datetime.datetime.strptime(row['Year']+' '+row['MonthDay']+' '+row['Time'], "%Y %m%d %H%M%S"))

    number_samples_read = len(times)
    print("time samples read ", number_samples_read)

    return number_samples_read


def parse_wave(output_name, file):

    ncOut = Dataset(output_name, 'a')

    wave_time = []
    odata = []

    with open(file, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        if 'Year' not in reader.fieldnames:
            return

        for row in reader:
            #print(row)
            wave_time.append(datetime.datetime.strptime(row['Year']+' '+row['MonthDay']+' '+row['Time'], "%Y %m%d %H%M%S"))
            odata.append(row)

    number_samples_read = len(wave_time)
    print("sampled read ", number_samples_read)

    for var in odata[0]:
        print('var', var, 'value', odata[0][var])
        if var in wave_params:
            v = wave_params[var]
            print("Variable %s (%s)" % (v['var_name'], v['units']))
            varName = v['var_name']
            if varName not in ncOut.variables:
                ncVarOut = ncOut.createVariable(varName, "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
            else:
                ncVarOut = ncOut.variables[varName]

            for sample in range(0, number_samples_read):
                print(wave_time[sample])
                ts = wave_time[sample]
                time_idx = np.where(np.array(times) == ts)
                print("time index", time_idx, time_idx[0].shape)
                ncVarOut[time_idx[0]] = float(odata[sample][var])

            ncVarOut.units = v['units']
            if 'comment' in v:
                ncVarOut.comment = v['comment']


    ncOut.close()


def parse_triaxys(files):
    output_name = "TriAXYS.nc"

    inc_raw = False

    # create a list of files, scanning any directories
    filelist = []
    for file in files:
        if file == '--raw':
            inc_raw = True
            output_name = "TriAXYS-incRAW.nc"
        if isfile(file):
            filelist.append(file)
        elif isdir(file):
            for root, dirs, files in walk(file):
                for name in files:
                    f = join(root, name)

                    if isfile(f):
                        filelist.append(f)

    print("output file : %s" % output_name)

    ncOut = Dataset(output_name, 'w')  #, format='NETCDF4_CLASSIC') # CLASSIC does not allow modifting after creation
    tDim = ncOut.createDimension("TIME")
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut.comment = "timestamp is at start of 20 min sampling period"

    ncOut.close()

    # needs re-write to deal with files inside zip files
    # if filelist[0].endswith(".zip"):
    #     print('file list zero', filelist[0])
    #     file = zipfile.ZipFile(filelist[0], "r")
    #     filelist = file.namelist()
    #     for f in filelist:
    #         print('zip file, filename', f)

    # parse the Summary or WAVE files to get the timestamps
    for filepath in filelist:
        print(filepath)

        # we either get a Summary file (SOFS-2 processor, 3.00.0005) or a .WAVE file (SOFS-4 onwards processor, 4.01.000)
        if filepath.endswith('Summary.txt') or filepath.endswith('Summary-Sort.txt'):
            parse_summary(output_name, filepath)

        elif filepath.endswith('.WAVE'):
            print("WAVE read times")
            parse_wave_time(output_name, filepath)

    times.sort()

    number_samples_read = len(times)

    #
    # build the netCDF file
    #
    ncOut = Dataset(output_name, 'a')

    instrument_model = 'TriAXYS'
    instrument_serialnumber = '04811'

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut.instrument = 'AXYS Technologies ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    # sort the times
    t_datenum = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)
    ncTimesOut[:] = t_datenum

    # add timespan attributes
    ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + os.path.basename(filelist[0]) + '...')

    # TODO make this able to take zip files

    # parse each file
    for filepath in filelist:
        print(filepath)

        if filepath.endswith('.WAVE'):
            print("WAVE")
            parse_wave(output_name, filepath)

        elif filepath.endswith('.DIRSPEC'):
            print("DIRSPEC")
            parse_dir_spec(output_name, filepath)

        elif filepath.endswith('.NONDIRSPEC'):
            print("NONDIR SPEC")
            parse_non_dir_spec(output_name, filepath)

        elif filepath.endswith('.MEANDIR'):
            print("MEAN DIR")
            parse_mean_dir(output_name, filepath)

        if inc_raw:
            if filepath.endswith('.RAW'):
                print("RAW")
                parse_raw(output_name, filepath)

            elif filepath.endswith('.HNE'):
                print("Heave")
                parse_heave(output_name, filepath)

            elif filepath.endswith('.UVH'):
                print("Heave, Velocity")
                parse_velocity(output_name, filepath)

            elif filepath.endswith('.FOURIER'):
                print("FOURIER")
                parse_fourier(output_name, filepath)

    return output_name


if __name__ == "__main__":
    parse_triaxys(sys.argv[1:])
