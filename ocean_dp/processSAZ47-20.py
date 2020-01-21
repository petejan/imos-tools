import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['/Users/jan079/ABOS/git/imos-tools'])

import shutil

import ocean_dp.parse.nortek2netCDF
import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.site_instrument
import ocean_dp.processing.add_mag_variation
import ocean_dp.processing.magnetic_to_true
import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.attribution.format_attributes
import ocean_dp.qc.finalize_qc
import ocean_dp.file_name.imosNetCDFfileName

import psutil
import os
process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

filename = ocean_dp.parse.nortek2netCDF.parse_file('data/SAZ20-2018.aqd')

print('step 1 (parse) filename : ', filename)

filename = ocean_dp.attribution.addAttributes.add(filename,
                                                  ['metadata/SAZ47-20.metadata.csv',
                                                   'metadata/imos.metadata.csv',
                                                   'metadata/sots.metadata.csv',
                                                   'metadata/SAZ47.metadata.csv',
                                                   'metadata/variable.metadata.csv'])

filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)

print('step 2 (attributes) filename : ', filename)

filename = ocean_dp.file_name.site_instrument.rename(filename)

print('step 3 filename (rename deploy-inst) : ', filename)

filename = ocean_dp.processing.add_mag_variation.mag_var(filename)
filename = ocean_dp.processing.magnetic_to_true.magnetic_to_true(filename)

print('step 4 filename (magnetic variation) : ', filename)

newname = filename.replace(".nc", "-FV00.nc")
shutil.copy(filename, newname)

filename = ocean_dp.qc.add_qc_flags.add_qc(filename)
filename = ocean_dp.qc.in_out_water.in_out_water(filename)
filename = ocean_dp.qc.finalize_qc.final_qc(filename)

print('step 5 filename (QC) : ', filename)

filename = ocean_dp.attribution.format_attributes.format_attributes(filename)
print(filename)
filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)

print('step 6 imos name : ', filename)

print(process.memory_info().rss)  # in bytes

