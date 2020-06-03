import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import shutil

import ocean_dp.parse.sbeASC2netCDF
import ocean_dp.parse.rbr2netCDF
import ocean_dp.parse.sbeCNV2netCDF
import ocean_dp.parse.vemco2netCDF
import ocean_dp.parse.sbe37DD2netCDF
import ocean_dp.parse.sbe16DD2netCDF

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.site_instrument
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName

import ocean_dp.aggregation.copyDataset
import ocean_dp.file_name.find_file_with
import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water

import glob

import psutil
import os
import sys

process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

path = sys.argv[1] + "/"

print ('file path : ', path)

# print('step 1 (parse)')
#
# filename = ocean_dp.parse.sbe16DD2netCDF.parse("01606330", os.path.join(path, 'Pulse-10-SBE16-6330-download.txt'))
#
# cnv_files = glob.glob(os.path.join(path, "*.cnv"))
# for fn in cnv_files:
#     filename = ocean_dp.parse.sbeCNV2netCDF.parse([fn])
#
# rbr_files = glob.glob(os.path.join(path, "*_eng.txt"))
# for fn in rbr_files:
#     filename = ocean_dp.parse.rbr2netCDF.parse([fn])
#
# vemco_files = glob.glob(os.path.join(path, "Minilog-*.csv"))
# for fn in vemco_files:
#     filename = ocean_dp.parse.vemco2netCDF.parse([fn])
#
# # make a netCDF directory to put them in
# try:
#     os.mkdir(path + 'netCDF')
# except FileExistsError:
#     pass
#
# ncFiles = glob.glob(os.path.join(path, "*.nc"))
# for fn in ncFiles:
#     os.rename(fn, os.path.join(path, "netCDF", os.path.basename(fn)))
#
# # for each of the new files, process them
# ncFiles = glob.glob(os.path.join(path, 'netCDF', '*.nc'))
# for fn in ncFiles:
#     print ("processing " , fn)
#
#     filename = ocean_dp.attribution.addAttributes.add(fn,
#                                                       ['metadata/pulse-10.metadata.csv',
#                                                        'metadata/imos.metadata.csv',
#                                                        'metadata/sots.metadata.csv',
#                                                        'metadata/sofs.metadata.csv',
#                                                        'metadata/variable.metadata.csv'])
#
#     filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)
#     filename = ocean_dp.attribution.format_attributes.format_attributes(filename)
#
#     print('step 2 (attributes) filename : ', filename)
#
#     filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)
#     print('step 3 imos name : ', filename)

pulse_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "netCDF/IMOS*.nc"))
pulse_files = ocean_dp.file_name.find_file_with.find_global(pulse_files, 'deployment_code', 'Pulse-10-2013')
print('pulse files')
for f in pulse_files:
    print(f)

fv00_files = ocean_dp.file_name.find_file_with.find_global(pulse_files, 'file_version', 'Level 0 - Raw data')

print('pulse FV00 files')
for f in fv00_files:
    print(f)

fv01_files = ocean_dp.qc.add_qc_flags.add_qc(fv00_files)
fv01_files = ocean_dp.qc.in_out_water.in_out_water(fv01_files)

pres_files = ocean_dp.file_name.find_file_with.find_variable(fv01_files, 'PRES')
print('pulse files FV01 PRESSURE files')
for f in pres_files:
    print(f)

# create an aggregate of all files with pressure
ocean_dp.aggregation.copyDataset.aggregate(pres_files, ['PRES'])


print(process.memory_info().rss)  # in bytes

