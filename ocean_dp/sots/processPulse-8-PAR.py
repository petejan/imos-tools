import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import shutil

import ocean_dp.parse.mds5_to_netCDF
import ocean_dp.parse.eco_par2netCDF

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.find_file_with
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName
import ocean_dp.processing.pandas_pres_interp
import ocean_dp.processing.add_incoming_radiation
import ocean_dp.processing.apply_scale_offset_attributes
import ocean_dp.processing.extract_SBE16_PAR
import ocean_dp.processing.eco_parcount_2_par

import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.qc.climate_range
import ocean_dp.qc.par_nearest_qc

import glob

import psutil
import os
import sys

process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

path = sys.argv[1] + "/"

print ('file path : ', path)

print('step 1 (parse)')

files = []
cnv_files = glob.glob(os.path.join(path, "*.Csv"))
for fn in cnv_files:
    print(fn)
    files.append(ocean_dp.parse.mds5_to_netCDF.parse([fn]))

log_files = glob.glob(os.path.join(path, "*.log"))
for fn in log_files:
    print(fn)
    files.append(ocean_dp.parse.eco_par2netCDF.eco_parse([fn]))

print(files)

# make a netCDF directory to put them in
try:
    os.mkdir(path + '../netCDF')
except FileExistsError:
    pass

new_names = []
for fn in files:
    new_name = os.path.join(path, "../netCDF", os.path.basename(fn))
    os.rename(fn, new_name)
    new_names.append(new_name)

# for each of the new files, process them
for fn in new_names:
    print ("processing " , fn)
    filename = ocean_dp.attribution.addAttributes.add(fn,
                                                      ['metadata/pulse-8.metadata.csv',
                                                       'metadata/imos.metadata.csv',
                                                       'metadata/sots.metadata.csv',
                                                       'metadata/sofs.metadata.csv',
                                                       'metadata/variable.metadata.csv'])

    filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)
    filename = ocean_dp.attribution.format_attributes.format_attributes(filename)
    filename = ocean_dp.processing.apply_scale_offset_attributes.apply_scale_offset(filename)

    print('step 2 (attributes) filename : ', filename)

    filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)
    print('step 3 imos name : ', filename)

#     filenames = ocean_dp.processing.pandas_pres_interp.interpolator([filename], os.path.join(path, 'IMOS_ABOS-SOTS_CPT_20110729_SOFS_FV02_Pulse-Aggregate-PRES_END-20120806_C-20200427.nc'))
#     print('step 4 pressure interpolator : ', filename)
#
#     filename = ocean_dp.processing.add_incoming_radiation.add_solar(filenames)
#     print('step 5 add incoming radiation : ', filename)
#
pulse_8_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "../netCDF/IMOS*.nc"))
pulse_8_files = ocean_dp.file_name.find_file_with.find_global(pulse_8_files, 'deployment_code', 'Pulse-8-2011')
# print('pulse-8 files')
# for f in pulse_8_files:
#     print(f)

print('step ECO-PAR cal')
eco_par = ocean_dp.file_name.find_file_with.find_global(pulse_8_files, 'instrument_model', 'ECO-PARS')
print("eco par", eco_par)
ocean_dp.processing.eco_parcount_2_par.cal(eco_par)

# par_files = ocean_dp.file_name.find_file_with.find_variable(pulse_8_files, 'PAR')
# par_files = ocean_dp.file_name.find_file_with.find_variable(par_files, 'ALT')
# par_files = ocean_dp.file_name.find_file_with.find_variable(par_files, 'PRES')
# print('par:')
# for f in par_files:
#     print(f)
# fv00_files = ocean_dp.file_name.find_file_with.find_global(par_files, 'file_version', 'Level 0 - Raw data')
#
# print('step 6 add qc flags')
# qc_files = ocean_dp.qc.add_qc_flags.add_qc(fv00_files, "PAR")
#
# fv01_files = ocean_dp.file_name.find_file_with.find_global(qc_files, 'file_version', 'Level 1 - Quality Controlled Data')
# print('fv01:')
# for f in fv01_files:
#     print(f)
# print('step 7 in/out water')
# ocean_dp.qc.in_out_water.in_out_water(fv01_files, "PAR")
#
# print('step 8 global range')
# ocean_dp.qc.global_range.global_range(fv01_files, 'PAR', max=10000, min=-1.7)
#
# print('step 9 global range, pbad 4500')
# ocean_dp.qc.global_range.global_range(fv01_files, 'PAR', max=4500, min=-1.7)
#
# print('step 10 climate qc')
# ocean_dp.qc.climate_range.climate_range(fv01_files, "PAR")
#
# print('step 11 nearest')
# ocean_dp.qc.par_nearest_qc.add_qc(fv01_files)

print(process.memory_info().rss)  # in bytes

