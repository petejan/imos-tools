import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import shutil

import ocean_dp.parse.sbeASC2netCDF
import ocean_dp.parse.rbr2netCDF
import ocean_dp.parse.sbeCNV2netCDF
import ocean_dp.parse.vemco2netCDF
import ocean_dp.parse.starmon2netCDF
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

path = sys.argv[1] + "/*"

print ('file path : ', path)

print('step 1 (parse)')
file_names = []

cap_files = glob.glob(os.path.join(path, "*37*.cap"))
for fn in cap_files:
    print ('cap files', fn)
    filename = ocean_dp.parse.sbe37DD2netCDF.parse([fn])
    file_names.append((fn, filename))

cap_files = glob.glob(os.path.join(path, "*SBE16*.cap"))
for fn in cap_files:
    print ('SBE16 files', fn)
    filename = ocean_dp.parse.sbe16DD2netCDF.parse("01606331", fn)
    file_names.append((fn, filename))

cnv_files = glob.glob(os.path.join(path, "*.cnv"))
for fn in cnv_files:
    print ('cnv files', fn)
    filename = ocean_dp.parse.sbeCNV2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

asc_files = glob.glob(os.path.join(path, "*.asc"))
for fn in asc_files:
    print ('asc files', fn)
    filename = ocean_dp.parse.sbeASC2netCDF.sbe_asc_parse([fn])
    file_names.append((fn, filename))

rbr_files = glob.glob(os.path.join(path, "*_eng.txt"))
for fn in rbr_files:
    print ('rbr files', fn)
    filename = ocean_dp.parse.rbr2netCDF.parse([fn])
    file_names.append((fn, filename))

vemco_files = glob.glob(os.path.join(path, "[A]*.000"))
for fn in vemco_files:
    print ('vemco1 files', fn)
    filename = ocean_dp.parse.vemco2netCDF.parse([fn])
    file_names.append((fn, filename))

vemco_files = glob.glob(os.path.join(path, "Asc*.txt"))
for fn in vemco_files:
    print ('vemco2 files', fn)
    filename = ocean_dp.parse.vemco2netCDF.parse([fn])
    file_names.append((fn, filename))

vemco_files = glob.glob(os.path.join(path, "Minilog-*.csv"))
for fn in vemco_files:
    print ('vemco3 files', fn)
    filename = ocean_dp.parse.vemco2netCDF.parse([fn])
    file_names.append((fn, filename))

oddi_files = glob.glob(os.path.join(path, "*T*.DAT"))
for fn in oddi_files:
    print ('oddi files', fn)
    filename = ocean_dp.parse.starmon2netCDF.parse([fn])
    file_names.append((fn, filename))

print('files processed')
for f in file_names:
    print(f)
    if f[1]:
        shutil.move(f[1], os.path.join(sys.argv[1], 'netCDF'))

print(process.memory_info().rss)  # in bytes
