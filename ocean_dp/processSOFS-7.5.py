import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['/Users/jan079/ABOS/git/imos-tools'])

import shutil

import ocean_dp.parse.sbeASC2netCDF
import ocean_dp.parse.sbe37DD2netCDF
import ocean_dp.parse.sbeCNV2netCDF
import ocean_dp.parse.starmon2netCDF

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.site_instrument
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName

import glob

import psutil
import os

process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

print('step 1 (parse)')

cnv_files = glob.glob("*.cnv")
for fn in cnv_files:
    filename = ocean_dp.parse.sbeCNV2netCDF.parse([fn])

star_files = glob.glob("*.DAT")
for fn in star_files:
    print(fn)
    filename = ocean_dp.parse.starmon2netCDF.parse([fn])

# make a netCDF directory to put them in
try:
    os.mkdir('netCDF')
except FileExistsError:
    pass

ncFiles = glob.glob("*.nc")
for fn in ncFiles:
    os.rename(fn, "netCDF/"+fn)

# for each of the new files, process them
ncFiles = glob.glob("netCDF/*.nc")
for fn in ncFiles:
    print("process : ", fn)

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

print(process.memory_info().rss)  # in bytes

