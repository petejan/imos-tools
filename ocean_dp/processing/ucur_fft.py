from netCDF4 import Dataset
import numpy as np
import matplotlib.pyplot as plt

# import NUFFT_cpu class
from pynufft import NUFFT_cpu

# Initiate the NufftObj object
NufftObj = NUFFT_cpu()

ds = Dataset('file-name-EAC4200-2018.nc', 'r')
var_depth = ds.variables["DEPTH"]
var_time = ds.variables["TIME"]
var_ucur = ds.variables["UCUR"]
var_ucur_qc = ds.variables["UCUR_quality_control"]

time = var_time[:]

depth = var_depth[:]

ucur = var_ucur[:]
ucur_qc = var_ucur_qc[:]

depth.mask = False
ucur.mask = False

msk = (depth > 200) & (depth < 300)

t1 = time[msk]
d1 = depth[msk]
u1 = ucur[msk]
u_qc1 = ucur_qc[msk]

qc_msk = u_qc1 > 2

t1.mask = qc_msk
d1.mask = qc_msk
u1.msk = qc_msk

out_t_samples = 2048
out_d_samples = 8
Nd = (out_t_samples, out_d_samples)
Kd = (out_t_samples*2, out_d_samples*2)
Jd = (3, 3)

t1_max = t1.max()
t1_min = t1.min()

d1_max = d1.max()
d1_min = d1.min()

t_range = t1_max - t1_min
t_scale = 2 * np.pi / t_range
t_offset = (-2 * np.pi * t1_min / t_range) - np.pi
t1s = t_scale * t1 + t_offset

d_range = d1_max - d1_min
d_scale = 2 * np.pi / d_range
d_offset = (-2 * np.pi * d1_min / d_range) - np.pi
d1s = d_scale * d1 + d_offset

om = np.array([t1s, d1s]).transpose()

NufftObj.plan(om, Nd, Kd, Jd, ft_axes=(0,1), batch=None)

image0 = NufftObj.solve(u1, solver='cg',maxiter=50)

# the sample points (1024) are now time scalled between -pi and pi, depth scalled -pi to pi

t_sub_sample = (np.arange(0,(out_t_samples)) - out_t_samples/2) * 2 * np.pi / out_t_samples
t_sub_sample_t = (t_sub_sample - t_offset) / t_scale

d_sub_sample = (np.arange(0,(out_d_samples)) - out_d_samples/2) * 2 * np.pi / out_d_samples
d_sub_sample_t = (d_sub_sample - d_offset) / d_scale

#plt.scatter(t1,d1,c=u1,marker='.')

plt.imshow(abs(image0.transpose()), aspect='auto', extent=[t_sub_sample_t[0],t_sub_sample_t[-1],d_sub_sample_t[0],d_sub_sample_t[-1]])
plt.colorbar()
plt.show()
