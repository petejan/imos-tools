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
from cftime import num2date
from netCDF4 import Dataset
import sys
import numpy as np
from datetime import datetime, UTC

from scipy.spatial.transform import Rotation
from scipy import signal

import time

zl = False
append_to_file = False

# is this quicker than using the Rotation library, not really


def quaternion_mult(q, r):
    return [r[0]*q[0]-r[1]*q[1]-r[2]*q[2]-r[3]*q[3],
            r[0]*q[1]+r[1]*q[0]-r[2]*q[3]+r[3]*q[2],
            r[0]*q[2]+r[1]*q[3]+r[2]*q[0]-r[3]*q[1],
            r[0]*q[3]-r[1]*q[2]+r[2]*q[1]+r[3]*q[0]]


def point_rotation_by_quaternion(point, q):
    r = np.array((0, point[0], point[1], point[2]))
    q_conj = [q[0], -1*q[1], -1*q[2], -1*q[3]]

    return quaternion_mult(quaternion_mult(q,r),q_conj)[1:]


def quaternion_mult_vect(q, r):

    w = r[:, 0]*q[:, 0]-r[:, 1]*q[:, 1]-r[:, 2]*q[:, 2]-r[:, 3]*q[:, 3]
    x = r[:, 0]*q[:, 1]+r[:, 1]*q[:, 0]-r[:, 2]*q[:, 3]+r[:, 3]*q[:, 2]
    y = r[:, 0]*q[:, 2]+r[:, 1]*q[:, 3]+r[:, 2]*q[:, 0]-r[:, 3]*q[:, 1]
    z = r[:, 0]*q[:, 3]-r[:, 1]*q[:, 2]+r[:, 2]*q[:, 1]+r[:, 3]*q[:, 0]

    return np.array((w, x, y, z,)).transpose()


def point_rotation_by_quaternion_vect(point, q):

    r = np.array([np.zeros_like(point[:, 0]), point[:, 0], point[:, 1], point[:, 2]]).transpose()
    q_conj = np.array((q[:, 0], -1 * q[:, 1], -1 * q[:, 2], -1 * q[:, 3])).transpose()

    #print('vect shape', point.shape, q.shape, r.shape, q_conj.shape)

    qr = quaternion_mult_vect(q, r)
    #print('res1', res1.shape)
    qrq = quaternion_mult_vect(qr, q_conj)

    #print('return', res.shape)

    return qrq[:, 1:]


def add_wave_spectra(netCDFfile):
    if append_to_file:
        dsIn = Dataset(netCDFfile, 'a')
        dsOut = dsIn
    else:
        dsIn = Dataset(netCDFfile, 'r')
        fn = dsIn.instrument_imu_model + '-' + dsIn.instrument_imu_serial_number
        dsOut = Dataset(fn + '.nc', 'w', format='NETCDF4_CLASSIC')

    dsIn.set_auto_mask(False)

    var_time = dsIn.variables['TIME']

    if not append_to_file:
        dsOut.createDimension('TIME', len(dsIn.variables['TIME']))
        dsOut.createVariable('TIME', 'f8', ("TIME", ))
        dsOut.variables['TIME'][:] = var_time[:]

        # copy global attributes
        for ga in dsIn.ncattrs():
            if ga not in ['instrument_imu', 'instrument_imu_model', 'instrument_imu_serial_number']:
                dsOut.setncattr(ga, dsIn.getncattr(ga))

        # copy TIME attributes
        for a in var_time.ncattrs():
            dsOut.variables['TIME'].setncattr(a, var_time.getncattr(a))

        dsOut.instrument = dsIn.instrument_imu
        dsOut.instrument_model = dsIn.instrument_imu_model
        dsOut.instrument_serial_number = dsIn.instrument_imu_serial_number

    # create frequency dimension
    if "FREQ" not in dsOut.dimensions:
        dsOut.createDimension('FREQ', 256)
    if "FREQ" in dsOut.variables:
        freq_var_var = dsOut.variables["FREQ"]
    else:
        freq_var_var = dsOut.createVariable("FREQ", "f4", ('FREQ',), fill_value=None, zlib=zl)  # fill_value=nan otherwise defaults to max

    # create (or reuse) new variables WAVE_SPECTRA, FREQUENCY,  WAVE_HEIGHT
    if "WAVE_SPECTRA" in dsOut.variables:
        wave_spec_out_var = dsOut.variables["WAVE_SPECTRA"]
    else:
        wave_spec_out_var = dsOut.createVariable("WAVE_SPECTRA", "f4", ("FREQ", "TIME"), fill_value=np.nan, zlib=zl)  # fill_value=nan otherwise defaults to max
    wave_spec_out_var.units = "m^2/Hz"
    wave_spec_out_var.long_name = "wave_spectral_density"
    wave_spec_out_var.comment = "calculated from world coordinate acceleration, with 512 point welch FFT, using hamming window, mean removed, zero filled where data is NaN"

    if "Hm0" in dsOut.variables:
        swh_out_var = dsOut.variables["Hm0"]
    else:
        swh_out_var = dsOut.createVariable("Hm0", "f4", ("TIME",), fill_value=np.nan, zlib=zl)  # fill_value=nan otherwise defaults to max

    if "Tz" in dsOut.variables:
        apd_out_var = dsOut.variables["Tz"]
    else:
        apd_out_var = dsOut.createVariable("Tz", "f4", ("TIME",), fill_value=np.nan, zlib=zl)  # fill_value=nan otherwise defaults to max

    if append_to_file:
        if "acceleration_world" in dsOut.variables:
            accel_w_var = dsOut.variables["acceleration_world"]
        else:
            accel_w_var = dsOut.createVariable("acceleration_world", "f4", ("sample_time", "vector", "TIME"), fill_value=np.nan, zlib=zl)  # fill_value=nan otherwise defaults to max

    # handle for variables
    var_q = dsIn.variables["orientation"]
    var_accel = dsIn.variables["acceleration"]
    accel = var_accel[:]
    q = var_q[:]

    # create an array for wave_displacement spectra
    wave_displacement_spectra = np.zeros(256) * np.nan

    accel_world = np.zeros([3072, 3])
    for i in range(len(dsIn.variables['TIME'])):
        nans_missing = np.sum(np.isnan(var_accel[:, :, i]), axis=0)
        print('accel inst nans', nans_missing)
        start = time.time()

        if nans_missing[2] < 1000:

            # read quaternion for this time

            accel_world[:, :] = point_rotation_by_quaternion_vect(accel[:, :, i], q[:, :, i])

            if append_to_file:
                accel_w_var[:, :, i] = accel_world[:, :]

            # for j in range(0, 3072):
            #     try:
            #         accel_inst = var_accel[j, :, i]  # dimensions are sample_time, vector, TIME
            #
            #         print(j, accel_inst.shape, q.shape)
            #
            #         # read the quaternion, the IMU data is in w, x, y, z whereas Rotation.from_quant is in x, y, z, w
            #
            #         #r = Rotation.from_quat(np.transpose([q[j, 1], q[j, 2], q[j, 3], q[j, 0]]))
            #         #accel_world[j, :] = r.apply(accel_inst)
            #
            #         accel_world[j, :] = point_rotation_by_quaternion(accel_inst, q[j, :])
            #
            #         accel_w_var[j, :, i] = accel_world[j, :]
            #
            #         print(j, accel_inst, accel_world[j, :])
            #
            #     except ValueError as v:
            #         print(j, v)

            # convert accelerations to world coordinates

            print("rotation % s seconds" % (time.time() - start))
            start = time.time()

            # compute power spectral density from vertical acceleration
            # removing mean seems to work as well as detrend='linear' or detrend='constant'

            a = accel_world[:, 2]
            #accel_w_var[:] = a
            a_mean = np.nanmean(a)
            #a = a - a_mean
            nan_a = np.isnan(a)
            #a[nan_a] = 0 # zero fill
            a[nan_a] = a_mean # mean fill
            freq_spec, wave_acceleration_spectra = signal.welch(a, fs=5, nfft=512, scaling='density', window='hamming', detrend='linear', nperseg=512)

            # compute displacement spectra from wave acceleration spectra
            # by divinding by (2*pi*f) ^ 4, first point is nan as f[0] = 0
            f_wave_disp = freq_spec[0:-1]
            wave_displacement_spectra[0] = np.nan
            wave_displacement_spectra[1:] = wave_acceleration_spectra[1:-1] / (2*np.pi*freq_spec[1:-1])**4

            # save wave displacement spectra
            wave_spec_out_var[:, i] = wave_displacement_spectra

            # calculate wave height, NOAA use frequency band 0.0325 to 0.485 https://www.ndbc.noaa.gov/wavecalc.shtml
            # 0.05 = 20 sec wave period, MRU overestimates the acceleration at this low frequency,
            # almost 1m at 7m SWH, ~ 10% because of noise
            # use f[1] as the delta frequency (the width of a frequency bin width)
            msk = (f_wave_disp > 0.05) & (f_wave_disp < 0.485)
            m0 = sum(wave_displacement_spectra[msk] * freq_spec[1])
            swh = 4 * np.sqrt(m0)

            m2 = sum(wave_displacement_spectra[msk] * freq_spec[1] * (f_wave_disp[msk] ** 2))

            apd = np.sqrt(m0/m2)
            print("calc % s seconds" % (time.time() - start))
            start = time.time()

            print(num2date(var_time[i], calendar=var_time.calendar, units=var_time.units), 'wave height', swh, 'period', apd)
            swh_out_var[i] = swh
            apd_out_var[i] = apd
        else:
            print('missing data', num2date(var_time[i], calendar=var_time.calendar, units=var_time.units))
            swh_out_var[i] = np.nan
            apd_out_var[i] = np.nan
            wave_spec_out_var[:, i] = np.nan

        print("save % s seconds" % (time.time() - start))

    # save the frequency
    freq_var_var[:] = f_wave_disp

    # update the history attribute
    try:
        hist = dsIn.history + "\n"
    except AttributeError:
        hist = ""

    if append_to_file:
        dsOut.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " added wave spectra, and significant wave height")
    else:
        dsOut.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " created wave spectra, wave height and period from " + netCDFfile)
        dsOut.setncattr("date_created", datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))

    dsOut.close()


if __name__ == "__main__":
    for f in sys.argv[1:]:
        add_wave_spectra(f)
