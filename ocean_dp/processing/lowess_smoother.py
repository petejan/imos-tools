from netCDF4 import Dataset, date2num, num2date
import sys
import gsw
import numpy as np
from datetime import datetime
import statsmodels.api as sm
import matplotlib.pyplot as plt

netCDFfile = '../../data/TEMP/netcdf-surface/IMOS_ABOS-SOTS_CST_20130328_SOFS_FV00_SOFS-4-2013-SBE37SM-RS485-03707409-1m_END-20131028_C-20200317.nc'

#netCDFfile = sys.argv[1]

ds = Dataset(netCDFfile, 'r')

var_time = ds.variables["TIME"]
var_psal = ds.variables["PSAL"]
date_time_start = datetime.strptime(ds.getncattr('time_deployment_start'), '%Y-%m-%dT%H:%M:%SZ')
date_time_end = datetime.strptime(ds.getncattr('time_deployment_end'), '%Y-%m-%dT%H:%M:%SZ')

time = var_time[:]
msk = (time > date2num(date_time_start, units=var_time.units)) & (time < date2num(date_time_end, units=var_time.units))

dt = num2date(time, units=var_time.units)
t_msk = var_time[msk]
psal = var_psal[msk]

print(var_psal[msk])

delta = 0.001
frac = 10/(60*(t_msk[-1] - t_msk[0]))

print(t_msk[0], t_msk[1], t_msk[1] - t_msk[0], t_msk[-1])
print((t_msk[-1] - t_msk[0]) * frac * 60)

lowess = sm.nonparametric.lowess

z = lowess(psal, t_msk, frac=frac, delta=delta, is_sorted=True)

plt.plot(dt[msk], psal)
plt.plot(dt[msk], z[:,1])
plt.grid()

plt.show()

ds.close()