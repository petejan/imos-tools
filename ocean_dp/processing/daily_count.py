import xarray as xr
import pandas as pd
import numpy as np
from scipy import stats

f='data/PAR/raw_files/netCDF/IMOS_DWM-SOTS_F_20111108_SOFS_FV01_SOFS-2-2011-LI-190SA-Q40966-3m_END-20120708_C-20200525.nc'
DS=xr.open_dataset(f)
df = DS.to_dataframe()
x = df.PAR.resample('D')

y = x.agg([np.sum, np.mean, np.std])

y = x.agg(stats.describe)

obs = [v.nobs for v in y.values]

print(np.median(obs))
