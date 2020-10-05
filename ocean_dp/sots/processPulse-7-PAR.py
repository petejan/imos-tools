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
import ocean_dp.processing.apply_scale_offset_attributes
import ocean_dp.processing.extract_SBE16_PAR
import ocean_dp.processing.eco_parcount_2_par

import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.qc.par_climate_range
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
                                                      ['metadata/pulse-7.metadata.csv',
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

print('step ECO-PAR cal')
pulse_files = glob.glob(os.path.join(path, "../netCDF/IMOS*.nc"))
pulse_7_files = ocean_dp.file_name.find_file_with.find_global(pulse_files, 'deployment_code', 'Pulse-7-2010')
par_files = ocean_dp.file_name.find_file_with.find_variable(pulse_7_files, 'PAR_COUNT')
ocean_dp.processing.eco_parcount_2_par.cal(par_files)

print(process.memory_info().rss)  # in bytes

