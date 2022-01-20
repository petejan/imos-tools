import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['/Users/jan079/DWM/git/imos-tools'])
sys.path.extend(['.'])

import shutil

import ocean_dp.parse.sbeASC2netCDF
import ocean_dp.parse.sbe37DD2netCDF
import ocean_dp.parse.sbeCNV2netCDF
import ocean_dp.parse.vemco2netCDF

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.site_instrument
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName

import glob

import psutil
import os
import sys

process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

path = sys.argv[1] + "/"

print ('file path : ', path)

print('step 1 (parse)')

filename = ocean_dp.parse.sbeASC2netCDF.sbe_asc_parse([os.path.join(path, 'SOFS-1-SBE37SM-1840-SS-7.asc')])
filename = ocean_dp.parse.sbe37DD2netCDF.parse([os.path.join(path, 'SOFS-1-SBE37SM-2971-100m-3.asc')])

cnv_files = glob.glob(os.path.join(path, "*.cnv"))
for fn in cnv_files:
    filename = ocean_dp.parse.sbeCNV2netCDF.parse([fn])

vemco_files = glob.glob(os.path.join(path, "Asc-*.000"))
for fn in vemco_files:
    filename = ocean_dp.parse.vemco2netCDF.parse([fn])

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
    filename = ocean_dp.attribution.addAttributes.add(fn,
                                                      ['metadata/sofs-1.metadata.csv',
                                                       'metadata/imos.metadata.csv',
                                                       'metadata/sots.metadata.csv',
                                                       'metadata/sofs.metadata.csv',
                                                       'metadata/variable.metadata.csv'])

    filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)
    filename = ocean_dp.attribution.format_attributes.format_attributes(filename)

    print('step 2 (attributes) filename : ', filename)

    filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)
    print('step 3 imos name : ', filename)

print(process.memory_info().rss)  # in bytes

