import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

#print("path", sys.path)

import shutil

import ocean_dp.parse.sbeASC2netCDF
import ocean_dp.parse.rbr2netCDF
import ocean_dp.parse.rbrDAT2netCDF
import ocean_dp.parse.sbeCNV2netCDF

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

print('file path : ', path)

print('step 1 (parse)')

files = []
cnv_files = glob.glob(os.path.join(path, "*.cnv"))
for fn in cnv_files:
    files.append(ocean_dp.parse.sbeCNV2netCDF.parse([fn]))

asc_files = glob.glob(os.path.join(path, "*.asc"))
for fn in asc_files:
    print(fn)
    files.append(ocean_dp.parse.sbeASC2netCDF.sbe_asc_parse([fn]))

rbr_files = glob.glob(os.path.join(path, "*_eng.txt"))
for fn in rbr_files:
    files.append(ocean_dp.parse.rbr2netCDF.parse([fn]))

rbr_files = glob.glob(os.path.join(path, "R*.dat"))
for fn in rbr_files:
    files.append(ocean_dp.parse.rbrDAT2netCDF.parse([fn]))

rbr_files = glob.glob(os.path.join(path, "r*.dat"))
for fn in rbr_files:
    files.append(ocean_dp.parse.rbrDAT2netCDF.parse([fn]))


print('files:')
for f in files:
    print(f)

#make a netCDF directory to put them in
try:
    os.mkdir(os.path.join(os.path.dirname(fn), "../netCDF"))
except FileExistsError:
    pass

new_fns = []
for fn in files:
    new_fn = os.path.join(os.path.dirname(fn), "../netCDF", os.path.basename(fn))
    print('rename', new_fn)
    new_fns.append(new_fn)
    os.rename(fn, os.path.join(os.path.dirname(fn), "../netCDF", os.path.basename(fn)))

# for each of the new files, process them
#ncFiles = glob.glob(os.path.join(path, '../netCDF', '*.nc'))
for fn in new_fns:
    print("processing file : " + fn)

    filename = ocean_dp.attribution.addAttributes.add(fn,
                                                      ['metadata/SAZ-all.metadata.csv',
                                                       'metadata/imos.metadata.csv',
                                                       'metadata/sots.metadata.csv',
                                                       'metadata/saz.metadata.csv',
                                                       'metadata/variable.metadata.csv'])

    filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)
    filename = ocean_dp.attribution.format_attributes.format_attributes(filename)

    print('step 2 (attributes) filename : ', filename)

    filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)
    print('step 3 imos name : ', filename)

print(process.memory_info().rss)  # in bytes

