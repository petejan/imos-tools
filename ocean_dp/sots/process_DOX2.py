#!/usr/bin/python3

# raw2netCDF
# Copyright (C) 2019 Peter Jansen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.


#print('Python %s on %s' % (sys.version, sys.platform))
import re
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), '..'))

#print(sys.path)

from netCDF4 import Dataset

import glob

import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.qc.spike_test
import ocean_dp.qc.rate_of_change
import ocean_dp.qc.manual_by_date
import ocean_dp.qc.propogate_flags
import ocean_dp.processing.addPSAL
import ocean_dp.processing.add_density
import ocean_dp.processing.add_sigma_theta0_sm
import ocean_dp.processing.apply_scale_offset_attributes
from ocean_dp.processing.resampler import resample
import ocean_dp.file_name.imosNetCDFfileName
import ocean_dp.attribution.format_attributes

from ocean_dp.processing.extract_SBE16_v_to_optode import extract_optode
from ocean_dp.processing.extract_SBE16_v_to_SBE43 import extract_sbe43
from ocean_dp.processing.add_oxsol import add_oxsol
from ocean_dp.processing.correct_dox2 import oxygen
from ocean_dp.processing.add_doxs import add_doxs
from ocean_dp.attribution.add_optode_cal import add
from ocean_dp.processing.calc_optode_oxygen import add_optode_oxygen
import ocean_dp.parse.SOFS_1_csvRAW_2_netCDF as SOFS1
import ocean_dp.parse.optode2netCDF as SOFS
from ocean_dp.processing.calc_DOX_to_DOX2 import doxtodox2

from ocean_dp.processing.merge_resample import resample

import ocean_dp.file_name.find_file_with

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.processing.apply_scale_offset_attributes


import shutil

raw_ox_files = []

# SOFS-1
sofs1_files = glob.glob("SOFS1_SD.csv")
for f in sofs1_files:
    sofs1_f = SOFS1.parse(f)
    raw_ox_files.append(sofs1_f)

sofs_files = glob.glob("*-OptodeLine.txt")
for f in sofs_files:
    sofs_f = SOFS.parse([f])
    raw_ox_files.append(sofs_f)

sofs_files = glob.glob("SOFS-*-done.txt")
for f in sofs_files:
    sofs_f = SOFS.parse([f])
    raw_ox_files.append(sofs_f)


ox_files = []
for fn in raw_ox_files:
    filename = ocean_dp.attribution.addAttributes.add(fn, ['metadata/pulse-saz-sofs-flux-timeoffset.metadata.csv'])
    print('file-name', filename)
    filename = ocean_dp.processing.apply_scale_offset_attributes.apply_scale_offset([filename])

    filename = ocean_dp.attribution.addAttributes.add(fn,
                                                      ['metadata/pulse-saz-sofs-flux.metadata.csv',
                                                       'metadata/imos.metadata.csv',
                                                       'metadata/sots.metadata.csv',
                                                       'metadata/sofs.metadata.csv',
                                                       'metadata/asimet.metadata.csv',
                                                       'metadata/variable.metadata.csv'])

    filename = ocean_dp.attribution.addAttributes.add(fn, ['metadata/pulse-saz-sofs-flux-timeoffset.metadata.csv'])
    print('file-name', filename)
    filenames = ocean_dp.processing.apply_scale_offset_attributes.apply_scale_offset([filename])

    filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filenames[0])
    filename = ocean_dp.attribution.format_attributes.format_attributes(filename)

    ox_files.append(filename)

# Pulse SBE16 file processing
sbe16_files = glob.glob("**/IMOS*FV01*SBE16*.nc", recursive=True)

print("SBE16 files :", sbe16_files)

for f in sbe16_files:
    ox_files.append(extract_optode(f))

for f in sbe16_files:
    ox_files.append(extract_sbe43(f))

# do we need to add PSAL (for SOFS files)
psal_files = ocean_dp.file_name.find_file_with.find_variable(ox_files, 'PSAL')
for f in ox_files:
    if f not in psal_files:
        print("adding psal :", f)
        psal_f = glob.glob("IMOS*SBE37SM-*.nc")
        resample(f, psal_f[0])

# check for BPHASE
bphase_files = ocean_dp.file_name.find_file_with.find_variable(ox_files, 'BPHASE')

print('bphase files :', bphase_files)

for bf in bphase_files:
    cal_file = glob.glob("*calcoef*.txt")
    print("using cal file :", cal_file[0])
    add(bf, cal_file[0])
    add_optode_oxygen(bf)

odo_files = glob.glob("IMOS*ODO*.nc")

dox2_gr_files = ocean_dp.file_name.find_file_with.find_variable(odo_files, 'DOX2_quality_control_gr')

fn = 1
for f in odo_files:
    if f not in dox2_gr_files:
        print('add file :', f)
        tmp_fn = "ODO-NetCDF-Temp"+str(fn)+".nc"
        shutil.copyfile(f, tmp_fn)
        ox_files.append(tmp_fn)
        fn += 1

  # python3 ~/DWM/git/imos-tools/ocean_dp/processing/add_oxsol.py $f
  # python3 ~/DWM/git/imos-tools/ocean_dp/processing/correct_dox2.py $f
  # python3 ~/DWM/git/imos-tools/ocean_dp/processing/add_doxs.py $f
  # python3 ~/DWM/git/imos-tools/ocean_dp/qc/add_qc_flags.py $f
  # python3 ~/DWM/git/imos-tools/ocean_dp/qc/global_range.py $f DOX2 350 150 4
  # python3 ~/DWM/git/imos-tools/ocean_dp/qc/global_range.py $f DOX2 310 240 3
  # python3 ~/DWM/git/imos-tools/ocean_dp/qc/global_range.py $f DOXS 1.2 0.5 4
  # python3 ~/DWM/git/imos-tools/ocean_dp/qc/global_range.py $f DOXS 1.15 0.8 3
  # python3 ~/DWM/git/imos-tools/ocean_dp/qc/propogate_flags.py -DOXS $f
  # python3 ~/DWM/git/imos-tools/ocean_dp/qc/propogate_flags.py -DOX2 $f
  # python3 ~/DWM/git/imos-tools/ocean_dp/attribution/format_attributes.py $f
  # python3 ~/DWM/git/imos-tools/ocean_dp/file_name/imosNetCDFfileName.py $f

for f in ox_files:
    print()
    print('processing:', f)
    f1 = add_oxsol(f)
    f2 = oxygen(f1)
    if f2: # if there was a correction (DOX2_RAW in file), use the new file
        f1 = f2
    f2 = doxtodox2(f1)
    if f2: # if there was a DOX to DOX2 conversion, use the new file
        f1 = f2
    f1 = add_doxs(f1)

    f1_files = ocean_dp.qc.add_qc_flags.add_qc([f1])


    f1_files = ocean_dp.qc.global_range.global_range(f1_files, 'DOX2', 350, 150)
    #f1_files = ocean_dp.qc.global_range.global_range(f1_files, 'DOX2', 310, 240, qc_value=3)
    f1_files = ocean_dp.qc.global_range.global_range(f1_files, 'DOXS', 1.2, 0.5)
    #f1_files = ocean_dp.qc.global_range.global_range(f1_files, 'DOXS', 1.15, 0.8, qc_value=3)

    f1_files = ocean_dp.qc.propogate_flags.propogate(f1_files, 'DOXS')
    f1_files = ocean_dp.qc.propogate_flags.propogate(f1_files, 'DOX2')

    f1 = ocean_dp.attribution.format_attributes.format_attributes(f1_files[0])

    f1 = ocean_dp.file_name.imosNetCDFfileName.rename(f1)

    print('output file :', f1)
