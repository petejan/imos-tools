import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import shutil

import ocean_dp.parse.sbeASC2netCDF
import ocean_dp.parse.rbr2netCDF
import ocean_dp.parse.sbeCNV2netCDF
import ocean_dp.parse.vemco2netCDF
import ocean_dp.parse.sbe37DD2netCDF
import ocean_dp.parse.rdi2netCDF
import ocean_dp.processing.scale_offset_var
import ocean_dp.parse.starmon2netCDF

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.site_instrument
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName

import ocean_dp.qc.select_in_water
import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.global_range
import ocean_dp.aggregation.copyDataset
import ocean_dp.processing.pressure_interpolator_new
import ocean_dp.processing.agg_to_bin

import glob

import psutil
import os
import sys

from netCDF4 import date2num, num2date
from netCDF4 import Dataset

process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

path = sys.argv[1] + "/"

print('file path : ', path)

print('step 1 (parse)')

filename = ocean_dp.parse.sbeASC2netCDF.sbe_asc_parse([os.path.join(path, 'SOFS-7.5-SBE39-6273-4500m.asc')])

cnv_files = glob.glob(os.path.join(path, "*.cnv"))
for fn in cnv_files:
    filename = ocean_dp.parse.sbeCNV2netCDF.parse([fn])

# need to offset Starmon-MINI TIMES by 10 hours
starmon_files = glob.glob(os.path.join(path, "*.DAT"))
for fn in starmon_files:
    filename = ocean_dp.parse.starmon2netCDF.parse([fn])
    filename = ocean_dp.processing.scale_offset_var.scale_offset(filename, 'TIME', '1', '-0.41666667')

# make a netCDF directory to put them in
try:
    os.mkdir(path + 'netCDF')
except FileExistsError:
    pass

ncFiles = glob.glob(os.path.join(path, "*.nc"))
for fn in ncFiles:
    os.rename(fn, os.path.join(path, "netCDF", os.path.basename(fn)))

# for each of the new files, process them
ncFiles = glob.glob(os.path.join(path, 'netCDF', '*.nc'))
for fn in ncFiles:
    print("processing file : " + fn)

    filename = ocean_dp.attribution.addAttributes.add(fn,
                                                      ['metadata/sofs-7.5.metadata.csv',
                                                       'metadata/imos.metadata.csv',
                                                       'metadata/sots.metadata.csv',
                                                       'metadata/sofs.metadata.csv',
                                                       'metadata/variable.metadata.csv'])

    filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)
    filename = ocean_dp.attribution.format_attributes.format_attributes(filename)

    print('step 2 (attributes) filename : ', filename)

    filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)
    print('step 3 imos name : ', filename)

print('trim')

file_trim = []

FV00_files = glob.glob(os.path.join(path, 'netCDF', "IMOS*FV00*.nc"))
for fn in FV00_files:
    nv = ocean_dp.qc.select_in_water.select_in_water([fn])
    file_trim.extend(nv)
    print(nv[0])

print('add qc')

file_qc = []
for fn in file_trim:
    nv = ocean_dp.qc.add_qc_flags.add_qc([fn])
    file_qc.extend(nv)
    print(nv[0])

print('global range')

file_glob = []
for fn in file_qc:
    nv = ocean_dp.qc.global_range.global_range(fn, 'TEMP', 40, -2)
    file_glob.append(nv)
    print(nv)

pres_file = ocean_dp.aggregation.copyDataset.aggregate(file_glob, ['PRES'])

# TODO: only run interpolator on files that don't contain pressure, or at least don't add IP to every filename
interp_file = ocean_dp.processing.pressure_interpolator_new.pressure_interpolator(file_glob, pres_file)

temp_agg_file = ocean_dp.aggregation.copyDataset.aggregate(interp_file, ['TEMP', 'PRES'])

# now bin the aggregate file
temp_agg_files = glob.glob(os.path.join(path, "IMOS*Aggregate-TEMP*.nc"))
for fn in temp_agg_files:
    bin_file = ocean_dp.processing.agg_to_bin.agg_to_bin(fn)

print(process.memory_info().rss)  # in bytes

