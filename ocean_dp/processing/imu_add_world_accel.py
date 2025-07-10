from netCDF4 import Dataset
import numpy as np

import sys

def quaternion_mult(q, r):
    return [r[0]*q[0]-r[1]*q[1]-r[2]*q[2]-r[3]*q[3],
            r[0]*q[1]+r[1]*q[0]-r[2]*q[3]+r[3]*q[2],
            r[0]*q[2]+r[1]*q[3]+r[2]*q[0]-r[3]*q[1],
            r[0]*q[3]-r[1]*q[2]+r[2]*q[1]+r[3]*q[0]]


def point_rotation_by_quaternion(point,q):
    r = np.array((0, point[0], point[1], point[2]))
    q_conj = [q[0],-1*q[1],-1*q[2],-1*q[3]]
    return quaternion_mult(quaternion_mult(q,r),q_conj)[1:]


def add_a(file):

    ds_in = Dataset(file, 'a')

    acc_var = ds_in.variables['acceleration']
    mag_var = ds_in.variables['magnetic']
    gyro_var = ds_in.variables['rotational_velocity']
    q_var = ds_in.variables['orientation']

    acc_data = np.transpose(acc_var[:])
    mag_data = np.transpose(mag_var[:])
    gyro_data = np.transpose(gyro_var[:])
    q_data = np.transpose(q_var[:])

    if "quaternion" not in ds_in.dimensions:
        ds_in.createDimension("quaternion")

    if "world_acceleration" not in ds_in.variables:
        wacc_var = ds_in.createVariable('world_acceleration', np.float32, ('vector', 'TIME'), fill_value=np.nan, zlib=True)
    else:
        wacc_var = ds_in.variables['world_acceleration']

    for i in range(q_data.shape[0]):
        wacc_var[:, i] = point_rotation_by_quaternion(acc_data[i, :], q_data[i, :])
        if i % 10000 == 0:
            print('.', i, q_data.shape[0])

    ds_in.close()


if __name__ == "__main__":
    for f in sys.argv[1:]:
        add_a(f)

