import re
import sys
import os

print('Python %s on %s' % (sys.version, sys.platform))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), '..'))

import shutil

import ocean_dp.parse.sbeASC2netCDF
import ocean_dp.parse.rbr2netCDF
import ocean_dp.parse.rbrDAT2netCDF
import ocean_dp.parse.sbeCNV2netCDF
import ocean_dp.parse.vemco2netCDF
import ocean_dp.parse.starmon2netCDF
import ocean_dp.parse.sbe37DD2netCDF
import ocean_dp.parse.sbe16DD2netCDF
import ocean_dp.parse.asimet_lgr2netCDF

import ocean_dp.file_name.imosNetCDFfileName

import glob

import sys


def run_fast_scandir(dir):    # dir: str, ext: list
    subfolders, files = [], []

    if os.path.isfile(dir):
        files.append(dir)
        return subfolders, files

    for f in os.scandir(dir):
        if f.is_dir():
            subfolders.append(f.path)
        if f.is_file():
           files.append(f.path)

    for dir in list(subfolders):
        sf, f = run_fast_scandir(dir)
        subfolders.extend(sf)
        files.extend(f)

    return subfolders, files


path = sys.argv[1]

files = []
for path in sys.argv[1:]:
    print('file path : ', path)

    subfolders, f = run_fast_scandir(path)
    files.extend(f)

print('files ', files)

print('step 1 (parse)')
file_names = []

cap_files = list(filter(re.compile(".*SBE37.*\.cap$").match, files))
for fn in cap_files:
    print ('cap files', fn)
    filename = ocean_dp.parse.sbe37DD2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

cap_files = list(filter(re.compile(".*SBE16.*\.cap$").match, files))
for fn in cap_files:
    print ('SBE16 files', fn)
    filename = ocean_dp.parse.sbe16DD2netCDF.parse(fn)
    file_names.append((fn, filename))

cnv_files = list(filter(re.compile(".*\.cnv$").match, files))
for fn in cnv_files:
    print ('cnv files', fn)
    filename = ocean_dp.parse.sbeCNV2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

asc_files = list(filter(re.compile(".*\.asc$").match, files))
for fn in asc_files:
    print ('asc files', fn)
    filename = ocean_dp.parse.sbeASC2netCDF.sbe_asc_parse([fn])
    file_names.append((fn, filename[0]))

rbr_files = list(filter(re.compile(".*_eng.txt$").match, files))
for fn in rbr_files:
    print ('rbr files', fn)
    filename = ocean_dp.parse.rbr2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

rbr_files = list(filter(re.compile(".*RBR.*\.dat$").match, files))
for fn in rbr_files:
    print ('rbr files', fn)
    filename = ocean_dp.parse.rbrDAT2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

vemco_files = list(filter(re.compile(".*/A.*\.000$").match, files))
for fn in vemco_files:
    print ('vemco1 files', fn)
    filename = ocean_dp.parse.vemco2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

vemco_files = list(filter(re.compile(".*Asc.*\.txt$").match, files))
for fn in vemco_files:
    print ('vemco2 files', fn)
    filename = ocean_dp.parse.vemco2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

vemco_files = list(filter(re.compile(".*Minilog-.*\.csv$").match, files))
for fn in vemco_files:
    print ('vemco3 files', fn)
    filename = ocean_dp.parse.vemco2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

oddi_files = list(filter(re.compile(".*T.*\.DAT$").match, files))
for fn in oddi_files:
    print ('oddi files', fn)
    filename = ocean_dp.parse.starmon2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

asimet_files = list(filter(re.compile(".*/?.*L.*\.RAW$").match, files))
for fn in asimet_files:
    print ('asimet_files files', fn)
    filename = ocean_dp.parse.asimet_lgr2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

asimet_files = list(filter(re.compile(".*/L.*\.DAT$").match, files))
for fn in asimet_files:
    print ('asimet_files files', fn)
    filename = ocean_dp.parse.asimet_lgr2netCDF.parse([fn])
    file_names.append((fn, filename[0]))

print('files processed')
for f in file_names:
    print(f)
    if f[1]:
        shutil.move(f[1], os.path.join(os.path.dirname(f[1]), 'netCDF'))

# TODO: add salinity, density to files with CNDC, TEMP, (PRES) without them.....
