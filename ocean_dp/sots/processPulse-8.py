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
import ocean_dp.file_name.find_file_with
import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.aggregation.copyDataset

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
# raw_files = []
# filename = ocean_dp.parse.sbe16DD2netCDF.parse("01606330", os.path.join(path, 'Pulse-8-SBE16-2012-07-21.cap'))
# raw_files.append(filename)
# filename = ocean_dp.parse.sbe37DD2netCDF.parse([os.path.join(path, 'Pulse-8-SBE37-6962-2012-07-21A.cap')])
# raw_files.append(filename)
#
# cnv_files = glob.glob(os.path.join(path, "*.cnv"))
# for fn in cnv_files:
#     filename = ocean_dp.parse.sbeCNV2netCDF.parse([fn])
#     raw_files.append(filename)
#
# rbr_files = glob.glob(os.path.join(path, "*_eng.txt"))
# for fn in rbr_files:
#     filename = ocean_dp.parse.rbr2netCDF.parse([fn])
#     raw_files.append(filename)
#
# vemco_files = glob.glob(os.path.join(path, "Minilog-T*.csv"))
# for fn in vemco_files:
#     filename = ocean_dp.parse.vemco2netCDF.parse([fn])
#     raw_files.append(filename)
#
# # make a netCDF directory to put them in
# try:
#     os.mkdir(path + 'netCDF')
# except FileExistsError:
#     pass
#
# nc_files = []
# for fn in raw_files:
#     new_name = os.path.join(path, "netCDF", os.path.basename(fn))
#     os.rename(fn, new_name)
#     nc_files.append(new_name)
#
# # for each of the new files, process them
# for fn in nc_files:
#     print ("processing " , fn)
#
#     filename = ocean_dp.attribution.addAttributes.add(fn,
#                                                       ['metadata/pulse-8.metadata.csv',
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
#

pulse_8_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "netCDF/IMOS*.nc"))

fv00_files = ocean_dp.file_name.find_file_with.find_global(pulse_8_files, 'file_version', 'Level 0 - Raw data')

print('pulse-8 FV00 files')
for f in fv00_files:
    print(f)

fv01_files = ocean_dp.qc.add_qc_flags.add_qc(fv00_files)
fv01_files = ocean_dp.qc.in_out_water.in_out_water(fv01_files)

sbe16 = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'instrument_model', 'SBE16plusV2')
print('sbe16 file', sbe16)
ocean_dp.qc.global_range.global_range(sbe16, "PRES", 80, 20)
#
fv01_files = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'file_version', 'Level 1 - Quality Controlled Data')
print('pulse-8 files FV01')
for f in fv01_files:
    print(f)

pres_files = ocean_dp.file_name.find_file_with.find_variable(fv01_files, 'PRES')
print('pulse-8 files FV01 PRESSURE files')
for f in pres_files:
    print(f)

# create an aggregate of all files with pressure
ocean_dp.aggregation.copyDataset.aggregate(pres_files, ['PRES'])

print(process.memory_info().rss)  # in bytes

