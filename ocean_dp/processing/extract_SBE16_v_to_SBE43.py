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
import os

from cftime import num2date
from glob2 import glob
from netCDF4 import Dataset
import sys
import gsw
import numpy as np
from datetime import datetime

# extract V0 from SBE16 file and output DOX2_VOLT, and DOX2


def extract_sbe43(netCDFfile):
    ds = Dataset(netCDFfile, 'r')
    ds.set_auto_mask(False)

    var_temp = ds.variables["TEMP"]
    var_psal = ds.variables["PSAL"]
    var_pres = ds.variables["PRES"]

    # the SBE43 voltage
    var_v0 = ds.variables["V0"]

    dep_code = ds.deployment_code

    print('deployment', dep_code)

    out_file = dep_code + "-SBE43.nc"
    ds_out = Dataset(out_file, 'w', data_model='NETCDF4_CLASSIC')

    ds_out.createDimension("TIME", len(ds.variables['TIME']))
    ncVarIn = ds.variables['TIME']
    ncTimesOut = ds_out.createVariable('TIME', "f8", ("TIME",), zlib=True)
    for a in ncVarIn.ncattrs():
        if a != '_FillValue':
            ncTimesOut.setncattr(a, ncVarIn.getncattr(a))
    ncTimesOut[:] = ds.variables['TIME'][:]

    # copy old variables into new file
    in_vars = set([x for x in ds.variables])

    z = in_vars.intersection(['TEMP', 'PSAL', 'PRES', 'V0',
                              'TEMP_quality_control', 'PSAL_quality_control', 'PRES_quality_control',
                              'V0_quality_control',
                              'LATITUDE', 'LONGITUDE', 'NOMINAL_DEPTH'])

    for v in z:
        print('copying',v,'dimensions', ncVarIn.dimensions)
        ncVarIn = ds.variables[v]
        if '_FillValue' in ncVarIn.ncattrs():
            fill = ncVarIn._FillValue
        else:
            fill = None

        ncVarOut = ds_out.createVariable(v, ncVarIn.dtype, ncVarIn.dimensions, fill_value=fill, zlib=True)  # fill_value=nan otherwise defaults to max
        for a in ncVarIn.ncattrs():
            if a != '_FillValue':
                if a == 'ancillary_variables':
                    ncVarOut.setncattr(a, v + '_quality_control') # only copying main quality control, not all individual flags
                else:
                    ncVarOut.setncattr(a, ncVarIn.getncattr(a))
        ncVarOut[:] = ncVarIn[:]

    if 'DOX2' in ds.variables:
        ncVarIn = ds.variables['DOX2']
        ncVarOut = ds_out.createVariable('DOX2_SBE', "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
        for a in ncVarIn.ncattrs():
            if a not in ['_FillValue', 'ancillary_variables'] :
                ncVarOut.setncattr(a, ncVarIn.getncattr(a))
        ncVarOut[:] = ncVarIn[:]

    T = var_temp[:]
    calibration_Soc = float(var_v0.calibration_Soc)
    calibration_offset = float(var_v0.calibration_offset)
    calibration_A = float(var_v0.calibration_A)
    calibration_B = float(var_v0.calibration_B)
    calibration_C = float(var_v0.calibration_C)
    calibration_E = float(var_v0.calibration_E)
    slope_correction = 1.0
    if 'oxygen_correction_slope' in var_v0.ncattrs():
        slope_correction = float(var_v0.oxygen_correction_slope)

    lat = -47
    lon = 142
    try:
        lat = ds.variables["LATITUDE"][0]
        lon = ds.variables["LONGITUDE"][0]
    except:
        pass

    SP = var_psal[:]
    p = var_pres[:]
    SA = gsw.SA_from_SP(SP, p, lon , lat)
    pt = gsw.pt0_from_t(SA, T, p)
    CT = gsw.CT_from_t(SA, T, p)
    sigma_t0 = gsw.sigma0(SA, CT)

    # calc oxygen solubility
    # 0.01 % difference to sea bird calculation
    #oxsol = gsw.O2sol_SP_pt(SP, pt) # umol/kg returned

    # calc OXSOL in ml/l as per seabird application note 64
    # this method gives a 0.2 % difference to what is calculated by sea bird (and what is calculated by TEOS-10)
    A0 = 2.00907
    A1 = 3.22014
    A2 = 4.0501
    A3 = 4.94457
    A4 = -0.256847
    A5 = 3.88767
    B0 = -0.00624523
    B1 = -0.00737614
    B2 = -0.010341
    B3 = -0.00817083
    C0 = -0.000000488682
    ts = np.log((298.15 - T) / (273.15 + T))

    oxsol = np.exp(A0 + A1*ts + A2*(ts**2) + A3*(ts**3) + A4*(ts**4) + A5*(ts**5) + SP*[B0+B1*(ts)+B2*(ts**2) +B3*(ts**3)]+C0*(SP**2))
    #
    # # calculate oxygen from V0
    dox = slope_correction * calibration_Soc * (var_v0[:] + calibration_offset) * oxsol * \
          (1 + calibration_A * T + calibration_B * T**2 + calibration_C * T**3) * \
          np.exp(calibration_E * p / (T + 273.15))
    #
    # # create SBE43 oxygen ml/l
    # # ncVarOut = ds_out.createVariable("DOX", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max
    # #
    # # ncVarOut[:] = dox
    # # ncVarOut.long_name = "volume_concentration_of_dissolved_molecular_oxygen_in_sea_water"
    # # ncVarOut.valid_min = 0
    # # ncVarOut.valid_max = 40
    # # ncVarOut.units = "ml/l"
    # # ncVarOut.equation_1 = "Ox(ml/l)=Soc.(V+Voffset).(1+A.T+B.T^2+V.T^3).OxSOL(T,S).exp(E.P/K) ... SeaBird (AN64-2)"

    # create SBE43 oxygen in umol/kg
    ncVarOut = ds_out.createVariable("DOX2", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    ncVarOut[:] = dox * 44600 / (1000+sigma_t0)

    #ncVarOut[:] = dox * 44.6

    ncVarOut.standard_name = "moles_of_oxygen_per_unit_mass_in_sea_water"
    ncVarOut.long_name = "moles_of_oxygen_per_unit_mass_in_sea_water"
    ncVarOut.coordinates = 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH'
    ncVarOut.valid_min = 0
    ncVarOut.valid_max = 400
    ncVarOut.units = "umol/kg"
    ncVarOut.equation_1 = "Ox[ml/l]=Soc.(V+Voffset).(1+A.T+B.T^2+C.T^3).OxSOL(T,S)[ml/l].exp(E.P/K) ... SeaBird (AN64)"
    ncVarOut.equation_2 = "Ox[umol/kg]=Ox[ml/l].44660/(sigma-theta(P=0,Theta,S)+1000)"
    #ncVarOut.equation_1 = "Ox[umol/kg]=Soc.(V+Voffset).(1+A.T+B.T^2+C.T^3).OxSOL(T,S)[umol/kg].exp(E.P/K) ... SeaBird (AN64)"
    #ncVarOut.comment = 'OxSOL in umol/kg'
    #ncVarOut.ancillary_variables = "DOX2_quality_control DOX2_quality_control_in"

    # quality flags
    # ncVarOut_qc = ds_out.createVariable("DOX2_quality_control", "i1", ("TIME",), fill_value=99, zlib=True)  # fill_value=99 otherwise defaults to max, imos-toolbox uses 99
    # ncVarOut_qc[:] = np.zeros(ncVarOut_qc.shape)
    # if 'V0_quality_control' in ds.variables:
    #     mx = np.max([ncVarOut_qc[:], ds.variables['V0_quality_control'][:]], axis=0)
    #     ncVarOut_qc[:] = mx
    # if 'TEMP_quality_control' in ds.variables:
    #     mx = np.max([ncVarOut_qc[:], ds.variables['TEMP_quality_control'][:]], axis=0)
    #     print('TEMP max', mx)
    #     ncVarOut_qc[:] = mx
    # if 'PSAL_quality_control' in ds.variables:
    #     mx = np.max([ncVarOut_qc[:], ds.variables['PSAL_quality_control'][:]], axis=0)
    #     print('PSAL max', mx)
    #     ncVarOut_qc[:] = mx
    # if 'PRES_quality_control' in ds.variables:
    #     mx = np.max([ncVarOut_qc[:], ds.variables['PRES_quality_control'][:]], axis=0)
    #     print('PRES max', mx)
    #     ncVarOut_qc[:] = mx
    #
    # ncVarOut_qc.standard_name = ncVarOut.standard_name + " status_flag"
    # ncVarOut_qc.quality_control_conventions = "IMOS standard flags"
    # ncVarOut_qc.flag_values = np.array([0, 1, 2, 3, 4, 6, 7, 9], dtype=np.int8)
    # ncVarOut_qc.flag_meanings = 'unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value'
    # ncVarOut_qc.comment = 'maximum of all flags'
    #
    # # create a QC flag variable for the input data
    # ncVarOut_qc = ds_out.createVariable("DOX2_quality_control_in", "i1", ("TIME",), fill_value=99, zlib=True)  # fill_value=99 otherwise defaults to max, imos-toolbox uses 99
    # ncVarOut_qc[:] = mx
    # ncVarOut_qc.long_name = "input data flag for moles_of_oxygen_per_unit_mass_in_sea_water"
    # ncVarOut_qc.units = "1"
    # ncVarOut_qc.comment = "data flagged from input variables TEMP, PSAL, PRES"

    # save the OxSOL
    if 'OXSOL' in ds_out.variables:
        ncVarOut = ds_out.variables['OXSOL']
    else:
        ncVarOut = ds_out.createVariable("OXSOL", "f4", ("TIME",), fill_value=np.nan, zlib=True)  # fill_value=nan otherwise defaults to max

    #ncVarOut[:] = oxsol * 44600 / (1000+sigma_t0)
    ncVarOut[:] = oxsol
    ncVarOut.units = "umol/kg"
    ncVarOut.comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html function gsw.O2sol_SP_pt"
    ncVarOut.long_name = "moles_of_oxygen_per_unit_mass_in_sea_water_at_saturation"
    ncVarOut.coordinates = 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH'

    for v in ds.ncattrs():
        if not v.startswith('sea_bird'):
            if v not in ['title', 'instrument', 'instrument_model', 'instrument_serial_number', 'history', 'date_created', 'file_version', 'file_version_quality_control']:
                #print('copy att', v)
                ds_out.setncattr(v, ds.getncattr(v))

    ds_out.instrument = 'Sea-Bird Electronics ; SBE43'
    ds_out.instrument_model = 'SBE43'
    ds_out.instrument_serial_number = '43' + var_v0.calibration_SerialNumber

    ds_out.file_version = 'Level 0 - Raw data'
    ds_out.file_version_quality_control = 'Data in this file has not been quality controlled'

    ds_out.title = 'Oceanographic mooring data deployment of {platform_code} at latitude {geospatial_lat_max:3.1f} longitude {geospatial_lon_max:3.1f} depth {geospatial_vertical_max:3.0f} (m) instrument {instrument} serial {instrument_serial_number}'

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    # add creating and history entry
    ds_out.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    # keep the history so we know where it came from
    ds_out.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " calculate DOX2 from file " + os.path.basename(netCDFfile))

    ds.close()
    ds_out.close()

    return out_file


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    for f in files:
        extract_sbe43(f)

