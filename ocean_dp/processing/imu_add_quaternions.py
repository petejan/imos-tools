from netCDF4 import Dataset
import numpy as np
from ahrs.filters import EKF
from ahrs.filters import Madgwick
from ahrs.common.orientation import acc2q
from ahrs.common.orientation import ecompass

import sys


def add_q(file):

    ds_in = Dataset(file, 'a')

    acc_var = ds_in.variables['acceleration']
    mag_var = ds_in.variables['magnetic']
    gyro_var = ds_in.variables['rotational_velocity']

    acc_data = np.transpose(acc_var[:])
    mag_data = np.transpose(mag_var[:])
    gyro_data = np.transpose(gyro_var[:])

    Q = ecompass(acc_data[0, :], mag_data[1, :], frame='ENU', representation='quaternion')  # First sample of tri-axial accelerometer
    print('initial orientation', Q)

    # https://ahrs.readthedocs.io/en/latest/filters/ekf.html
    ekf = Madgwick(gyr=gyro_data, acc=acc_data, mag=mag_data, frequency=10, frame='ENU', q0=Q)

    print('return shape', ekf.Q.shape)

    print('output', ekf.Q[0:2, :])

    if "quaternion" not in ds_in.dimensions:
        ds_in.createDimension("quaternion")

    if "orientation" not in ds_in.variables:
        q_var = ds_in.createVariable('orientation', np.float32, ('quaternion', 'TIME'), fill_value=np.nan, zlib=True)
    else:
        q_var = ds_in.variables['orientation']

    q_var[:] = np.transpose(ekf.Q)

    ds_in.close()


if __name__ == "__main__":
    for f in sys.argv[1:]:
        add_q(f)

