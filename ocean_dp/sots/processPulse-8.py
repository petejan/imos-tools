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

import glob

import psutil
import os
import sys

process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

path = sys.argv[1] + "/"

print ('file path : ', path)

print('step 1 (parse)')

filename = ocean_dp.parse.sbe16DD2netCDF.parse("01606330", os.path.join(path, 'Pulse-8-SBE16-2012-07-21.cap'))
filename = ocean_dp.parse.sbe37DD2netCDF.parse([os.path.join(path, 'Pulse-8-SBE37-6962-2012-07-21A.cap')])

cnv_files = glob.glob(os.path.join(path, "*.cnv"))
for fn in cnv_files:
    filename = ocean_dp.parse.sbeCNV2netCDF.parse([fn])

rbr_files = glob.glob(os.path.join(path, "*_eng.txt"))
for fn in rbr_files:
    filename = ocean_dp.parse.rbr2netCDF.parse([fn])

vemco_files = glob.glob(os.path.join(path, "Minilog-T*.csv"))
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
    print ("processing " , fn)

    filename = ocean_dp.attribution.addAttributes.add(fn,
                                                      ['metadata/pulse-8.metadata.csv',
                                                       'metadata/imos.metadata.csv',
                                                       'metadata/sots.metadata.csv',
                                                       'metadata/sofs.metadata.csv',
                                                       'metadata/variable.metadata.csv'])

    filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)
    filename = ocean_dp.attribution.format_attributes.format_attributes(filename)

    print('step 2 (attributes) filename : ', filename)

    filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)
    print('step 3 imos name : ', filename)

fn = glob.glob(os.path.join(path, "netCDF", "IMOS_ABOS-SOTS_CPT_20110729_SOFS_FV00_Pulse-8-2011-SBE16plusV2-01606330-34m_END-20120711_C-*.nc"))
fn = ocean_dp.qc.add_qc_flags(fn[0])
ocean_dp.qc.global_range(fn[0], "PRES", "80", "20")

print(process.memory_info().rss)  # in bytes
