import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), '..'))

from glob2 import glob

import ocean_dp.file_name.find_file_with
import ocean_dp.processing.pandas_pres_interp
import ocean_dp.processing.add_incoming_radiation
import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.qc.par_climate_range
import ocean_dp.qc.par_nearest_qc

import sys

files = []
for f in sys.argv[1:]:
    files.extend(glob(f))

par_files = ocean_dp.file_name.find_file_with.find_variable(files, 'PAR')

print('PAR files:')
for f in par_files:
    print(f)

qc_files = ocean_dp.qc.add_qc_flags.add_qc(par_files, "PAR")  # this resets the QC to 0

print('FV01 files:')
for f in qc_files:
    print(f)

ocean_dp.processing.add_incoming_radiation.add_solar(qc_files)

print('step 4 in/out water')
ocean_dp.qc.in_out_water.in_out_water(qc_files, "PAR")

print('step 8 global range')
ocean_dp.qc.global_range.global_range(qc_files, 'PAR', max=10000, min=-1.7)

print('step 9 global range, pbad 4500')
ocean_dp.qc.global_range.global_range(qc_files, 'PAR', max=4500, min=-1.7, qc_value=3)

print('step 10 climate qc')
ocean_dp.qc.par_climate_range.climate_range(qc_files, "PAR")

#
# #print('step 11 nearest')
# #ocean_dp.qc.par_nearest_qc.add_qc(fv01_files)
