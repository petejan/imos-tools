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
import gsw
import numpy as np
from datetime import datetime

from scipy.spatial.transform import Rotation
from scipy import signal

import time

zl = False

# is this quicker than using the Rotation library, not really


def quaternion_mult(q, r):
    return [r[0]*q[0]-r[1]*q[1]-r[2]*q[2]-r[3]*q[3],
            r[0]*q[1]+r[1]*q[0]-r[2]*q[3]+r[3]*q[2],
            r[0]*q[2]+r[1]*q[3]+r[2]*q[0]-r[3]*q[1],
            r[0]*q[3]-r[1]*q[2]+r[2]*q[1]+r[3]*q[0]]


def point_rotation_by_quaternion(point,q):
    r = np.array((0, point[0], point[1], point[2]))
    q_conj = [q[0],-1*q[1],-1*q[2],-1*q[3]]
    return quaternion_mult(quaternion_mult(q,r),q_conj)[1:]


def add_wave_spectra(netCDFfile):
    ds = Dataset(netCDFfile, 'a')
    ds.set_auto_mask(False)

    # create frequency dimension
    if "FREQ" not in ds.dimensions:
        ds.createDimension('FREQ', 256)

    # create (or reuse) new variables WAVE_SPECTRA, FREQUENCY,  WAVE_HEIGHT
    if "WAVE_SPECTRA" in ds.variables:
        wave_spec_out_var = ds.variables["WAVE_SPECTRA"]
    else:
        wave_spec_out_var = ds.createVariable("WAVE_SPECTRA", "f4", ("TIME", 'FREQ'), fill_value=np.nan, zlib=zl)  # fill_value=nan otherwise defaults to max
    wave_spec_out_var.units = "m^2/Hz"
    wave_spec_out_var.long_name = "wave_spectral_density"

    if "Hm0" in ds.variables:
        swh_out_var = ds.variables["Hm0"]
    else:
        swh_out_var = ds.createVariable("Hm0", "f4", ("TIME",), fill_value=np.nan, zlib=zl)  # fill_value=nan otherwise defaults to max

    if "Tz" in ds.variables:
        apd_out_var = ds.variables["Tz"]
    else:
        apd_out_var = ds.createVariable("Tz", "f4", ("TIME",), fill_value=np.nan, zlib=zl)  # fill_value=nan otherwise defaults to max

    if "FREQ" in ds.variables:
        freq_var_var = ds.variables["FREQ"]
    else:
        freq_var_var = ds.createVariable("FREQ", "f4", ('FREQ',), fill_value=np.nan, zlib=zl)  # fill_value=nan otherwise defaults to max

    # handle for variables
    var_q = ds.variables["orientation"]
    var_accel = ds.variables["acceleration"]
    var_time = ds.variables['TIME']

    # create an array for wave_displacement spectra
    wave_displacement_spectra = np.zeros(256) * np.nan

    accel_world = np.zeros([3072, 3])
    for i in range(len(ds.variables['TIME'])):
        start = time.time()
        # read quaternion for this time
        q = var_q[:, :, i]
        accel_world.fill(np.nan)
        for j in range(0, 3072):
            try:
                # read the quaternion, the IMU data is in w, x, y, z where as Rotation.from_quant is in x, y, z, w
                r = Rotation.from_quat(np.transpose([q[j, 1], q[j, 2], q[j, 3], q[j, 0]]))

                accel_inst = var_accel[j, :, i]  # dimensions are sample_time, vector, TIME

                accel_world[j, :] = r.apply(accel_inst)

                #accel_inst = var_accel[j, :, i]  # dimensions are sample_time, vector, TIME
                #accel_world[j, :] = point_rotation_by_quaternion(accel_inst, q[j, :])

            except ValueError as v:
                print(j, v)
        # convert accelerations to world coordinates

        print("rotation % s seconds" % (time.time() - start))
        print('accel world nans', np.sum(np.isnan(accel_world), axis=0))
        start = time.time()

        # compute power spectral density from vertical acceleration
        # removing mean seems to work better than detrend='linear' or detrend='constant'

        #a = accel_world[:, 2]-np.nanmean(accel_world[:, 2])
        #nana = np.isnan(a)
        #a[nana] = 0 # zero fill
        #f, wave_acceleration_spectra = signal.welch(a, fs=5, nfft=512, scaling='density', window='hamming', detrend=None)
        #a = accel_world[:, 2]-np.nanmean(accel_world[:, 2])
        #nana = np.isnan(a)
        #a[nana] = 0 # zero fill
        #f, wave_acceleration_spectra = signal.welch(a, fs=5, nfft=512, scaling='density', window='hamming', detrend=None)

        a = accel_world[:, 2]
        nan_a = np.isnan(a)
        a[nan_a] = np.nanmean(a)
        f, wave_acceleration_spectra = signal.welch(a, fs=5, nfft=512, scaling='density', window='hamming', detrend='constant')

        # compute displacement spectra from wave acceleration spectra
        # by divinding by (2*pi*f) ^ 4, first point is nan as f[0] = 0
        f_wave_disp = f[0:-1]
        wave_displacement_spectra[0] = np.nan
        wave_displacement_spectra[1:] = wave_acceleration_spectra[1:-1] / (2*np.pi*f[1:-1])**4

        # save wave displacement spectra
        wave_spec_out_var[i, :] = wave_displacement_spectra

        # calculate wave height, NOAA use frequency band 0.0325 to 0.485 https://www.ndbc.noaa.gov/wavecalc.shtml
        # 0.05 = 20 sec wave period, MRU overestimates the acceleration at this low frequency,
        # almost 1m at 7m SWH, ~ 10% because of noise
        # use f[1] as the delta frequency (the width of a frequency bin width)
        msk = (f_wave_disp > 0.05) & (f_wave_disp < 0.485)
        m0 = sum(wave_displacement_spectra[msk] * f[1])
        swh = 4 * np.sqrt(m0)

        m2 = sum(wave_displacement_spectra[msk] * f[1] * (f_wave_disp[msk] ** 2))

        apd = np.sqrt(m0/m2)
        print("calc % s seconds" % (time.time() - start))
        start = time.time()

        print(num2date(var_time[i], calendar=var_time.calendar, units=var_time.units), 'wave height', swh, 'period', apd)
        swh_out_var[i] = swh
        apd_out_var[i] = apd
        print("save % s seconds" % (time.time() - start))

    # save the frequency
    freq_var_var[:] = f_wave_disp

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " added wave spectra, and significant wave height")

    ds.close()


if __name__ == "__main__":
    for f in sys.argv[1:]:
        add_wave_spectra(f)
