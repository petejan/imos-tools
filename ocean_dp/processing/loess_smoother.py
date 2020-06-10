from netCDF4 import Dataset, date2num, num2date

from datetime import datetime
import matplotlib.pyplot as plt

from pyloess import Loess
import numpy as np

#netCDFfile = '../../data/TEMP/netcdf-surface/IMOS_ABOS-SOTS_CST_20130328_SOFS_FV00_SOFS-4-2013-SBE37SM-RS485-03707409-1m_END-20131028_C-20200317.nc'
#netCDFfile = '../../data/TEMP/netCDF-upper/IMOS_ABOS-SOTS_CFPST_20100817_SOFS_FV00_Pulse-7-2010-SBE16plus-01606331-31m_END-20110430_C-20200428.nc'
netCDFfile = '../../data/TEMP/netCDF-upper/IMOS_ABOS-SOTS_CPT_20110729_SOFS_FV00_Pulse-8-2011-SBE16plusV2-01606330-34m_END-20120711_C-20200427.nc'

#netCDFfile = sys.argv[1]

ds = Dataset(netCDFfile, 'r')

var_time = ds.variables["TIME"]
var_psal = ds.variables["TEMP"]

date_time_start = datetime.strptime(ds.getncattr('time_deployment_start'), '%Y-%m-%dT%H:%M:%SZ')
date_time_end = datetime.strptime(ds.getncattr('time_deployment_end'), '%Y-%m-%dT%H:%M:%SZ')

time = var_time[:]
msk = (time > date2num(date_time_start, units=var_time.units)) & (time < date2num(date_time_end, units=var_time.units))

t_dt = num2date(time, units=var_time.units)
t_msk = var_time[msk]

psal = var_psal[msk]

print(var_psal[msk])

loess = Loess.Loess(np.array(t_msk), np.array(psal))

#y = []
#i = 0
#for x in dt[msk]:
#    y[i] = loess.estimate(x, window=7, use_matrix=False, degree=2)
#    #print(x, y)
#    i += 1
t0 = t_msk[0]
t1 = t_msk[1]
tend = t_msk[-1]

sample_rate = np.round(24*3600*(tend-t0)/len(t_msk))
print('dt ', (t1 - t0)*24*3600, 'sec, sample_rate', sample_rate)

window = 5 * 3600 / sample_rate
print('5 hr in samples', window)

d0 = np.ceil(t0*24) / 24
dend = np.floor(tend*24) / 24
d = np.arange(d0, dend, 1/24)
d_dt = num2date(d, units=var_time.units)

y = [loess.estimate(x, window=int(window), use_matrix=False, degree=3) for x in d]

print(t0, t1)

#y = loess.estimate(t_msk, window=7, use_matrix=False, degree=1)

print(y)

plt.plot(t_dt[msk], psal)
plt.plot(d_dt, y)
plt.grid()

plt.show()

ds.close()