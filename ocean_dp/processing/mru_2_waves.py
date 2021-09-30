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


def add_wave_spectra(netCDFfile):
    ds = Dataset(netCDFfile, 'a')
    ds.set_auto_mask(False)

    # create (or reuse) new variables WAVE_SPECTRA, FREQUENCY,  WAVE_HEIGHT
    if "WAVE_SPECTRA" in ds.variables:
        wave_spec_out_var = ds.variables["WAVE_SPECTRA"]
    else:
        wave_spec_out_var = ds.createVariable("WAVE_SPECTRA", "f4", ("TIME", 'FREQ'), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    wave_spec_out_var.units = "m^2/Hz"
    wave_spec_out_var.long_name = "wave_spectral_density"

    if "SWH" in ds.variables:
        swh_out_var = ds.variables["SWH"]
    else:
        swh_out_var = ds.createVariable("SWH", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    if "APD" in ds.variables:
        apd_out_var = ds.variables["APD"]
    else:
        apd_out_var = ds.createVariable("APD", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    if "FREQ" in ds.variables:
        freq_var_var = ds.variables["FREQ"]
    else:
        freq_var_var = ds.createVariable("FREQ", "f4", ('FREQ',), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    # handle for variables
    var_q = ds.variables["quaternion"]
    var_accel = ds.variables["accel"]
    var_time = ds.variables['TIME']

    # create an array for wave_displacement spectra
    wave_displacement_spectra = np.zeros(256) * np.nan

    for i in range(len(ds.variables['TIME'])):
        # read quaternion for this time
        q = var_q[i, :, :]
        nans = np.any(np.isnan(q), axis=1)
        # don't process if any quaternions are nan
        if np.sum(nans) != len(nans):
            try:
                # read the quaternian, the IMU data is in w, x, y, x where as Rotation.from_quant is in x, y, z, w
                r = Rotation.from_quat(np.transpose([var_q[i, :, 1], var_q[i, :, 2], var_q[i, :, 3], var_q[i, :, 0]]))

                # convert accelerations to world coordinates
                accel_inst = var_accel[i, :, :] # dimensions are TIME, SAMPLE, VECTOR
                accel_world = r.apply(accel_inst)

                # compute power spectral density from vertical acceleration
                # removing mean seems to work better than detrend='linear' or detrend='constant'
                f, wave_acceleration_spectra = signal.welch(accel_world[:, 2]-np.mean(accel_world[:, 2]), fs=5, nfft=512, scaling='density', window='hamming', detrend=None)

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

                print(num2date(var_time[i], calendar=var_time.calendar, units=var_time.units), 'wave height', swh, 'period', apd)
                swh_out_var[i] = swh
                apd_out_var[i] = apd
            except ValueError as v:
                print(v)

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
