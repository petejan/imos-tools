# Copyright (C) 2020 Ben Weeding and Peter Jansen
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


from netCDF4 import Dataset
import sys
import gsw
import numpy as np
from datetime import datetime


from statsmodels.nonparametric.smoothers_lowess import lowess

# TODO: separate of the QC parts of this into a QC test

def add_sigma_theta0_sm(netCDFfile, limit=0.02):
    # loads the netcdf file
    ds = Dataset(netCDFfile, 'a')

    var_time = ds.variables['TIME']
    time_deployment = var_time[:]

    frac = 30 / len(time_deployment)

    print("samples", len(time_deployment), "fraction", frac)

    var_to_resample_in = ds.variables['SIGMA_T0']
    data_in = var_to_resample_in[:]

    y = lowess(np.array(data_in), np.array(time_deployment), frac=frac, it=2, is_sorted=False, xvals=time_deployment)
    qc_abs = abs(y - np.array(data_in))

    # generates a new variable 'SIGMAT0' in the netcdf
    if 'SIGMA_T0_SM' not in ds.variables:
        ncVarOut = ds.createVariable("SIGMA_T0_SM", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOut = ds.variables['SIGMA_T0_SM']

    # assigns the calculated densities to the DENSITY variable, sets the units as kg/m^3, and comments on the variable's origin
    ncVarOut[:] = y
    ncVarOut.units = "kg/m^3"
    ncVarOut.long_name = "smoothed sea_water_sigma_theta"
    ncVarOut.reference_pressure = "0 dbar"
    ncVarOut.valid_max = np.float32(100)
    ncVarOut.valid_min = np.float32(0)

    if 'SIGMA_T0_quality_control_dst' not in ds.variables:
        ncVarOutQc = ds.createVariable("SIGMA_T0_quality_control_dst", "i1", ("TIME",), fill_value=99, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOutQc = ds.variables['SIGMA_T0_quality_control_dst']

    ncVarOutQc[:] = 3
    ncVarOutQc[qc_abs < limit] = 1
    ncVarOutQc.long_name = "sigma-theta0-not-smooth flag for sea_water_sigma_theta"
    ncVarOutQc.units = "1"
    ncVarOutQc.coordinates = var_to_resample_in.coordinates
    ncVarOutQc.comment = "data flagged when sigma-theta0 jumps "+str(limit)+" more than the 30 point smoothed data"

    if 'PSAL_quality_control_dst' not in ds.variables:
        ncVarOutQc = ds.createVariable("PSAL_quality_control_dst", "i1", ("TIME",), fill_value=99, zlib=True)  # fill_value=nan otherwise defaults to max
    else:
        ncVarOutQc = ds.variables['PSAL_quality_control_dst']

    ncVarOutQc[:] = 3
    ncVarOutQc[qc_abs < limit] = 1
    ncVarOutQc.long_name = "sigma-theta0-not-smooth flag for sea_water_practical_salinity"
    ncVarOutQc.units = "1"
    ncVarOutQc.coordinates = var_to_resample_in.coordinates
    ncVarOutQc.comment = "data flagged 3 when sigma-theta0 jumps "+str(limit)+" more than the 30 point smoothed data"

    var_psal = ds.variables['PSAL']
    var_psal.ancillary_variables = var_psal.ancillary_variables + " PSAL_quality_control_dst"
    var_psal_qc = ds.variables['PSAL_quality_control']

    var_psal_qc[:] = np.max([var_psal_qc[:], ncVarOutQc[:]], axis=0)

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added smoothed SIGMA_T0 smoothed data, and QC flags to SIGMA_T0 and PSAL")

    ds.close()

    print('added sigma-theta0-smoothed')

    return [netCDFfile]


if __name__ == "__main__":
    for f in sys.argv[1:]:
        add_sigma_theta0_sm(f)
