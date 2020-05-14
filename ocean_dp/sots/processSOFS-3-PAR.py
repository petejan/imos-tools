import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import ocean_dp.parse.mds5_to_netCDF
import ocean_dp.parse.name_value_2_netCDF

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName
import ocean_dp.processing.apply_scale_offset_attributes

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
cnv_files = glob.glob(os.path.join(path, "*-done.txt"))
for fn in cnv_files:
    print(fn)
    files.append(ocean_dp.parse.name_value_2_netCDF.parse([fn], 'PAR'))

cnv_files = glob.glob(os.path.join(path, "*.Csv"))
for fn in cnv_files:
    print(fn)
    files.append(ocean_dp.parse.mds5_to_netCDF.parse([fn]))

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
                                                      ['metadata/sofs-3.metadata.csv',
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

print(process.memory_info().rss)  # in bytes

