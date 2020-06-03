import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import ocean_dp.attribution.addAttributes
import ocean_dp.file_name.find_file_with
import ocean_dp.processing.pandas_pres_interp
import ocean_dp.processing.add_incoming_radiation
import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.qc.par_climate_range
import ocean_dp.qc.par_nearest_qc

import psutil
import os
import sys

process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

path = sys.argv[1] + "/"

print ('file path : ', path)

all_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "IMOS*FV00*.nc"))
temp_files = ocean_dp.file_name.find_file_with.find_variable(all_files, 'TEMP')

print('TEMP files:')
for f in temp_files:
    print(f)

qc_files = ocean_dp.qc.add_qc_flags.add_qc(temp_files, "TEMP")  # this resets the QC to 0

all_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "IMOS*FV01*.nc"))
temp_files = ocean_dp.file_name.find_file_with.find_variable(all_files, 'TEMP')
fv01_files = ocean_dp.file_name.find_file_with.find_global(temp_files, 'file_version', 'Level 1 - Quality Controlled Data')

print('FV01 files:')
for f in fv01_files:
    print(f)

print('step 4 in/out water')
ocean_dp.qc.in_out_water.in_out_water(fv01_files, "TEMP")

print('step 8 global range')
ocean_dp.qc.global_range.global_range(fv01_files, 'TEMP', max=40, min=-1.7)

print('step 9 global range')
ocean_dp.qc.global_range.global_range(fv01_files, 'TEMP', max=25, min=0.8, qc_value=3)

# print('step 10 climate qc')
# ocean_dp.qc.par_climate_range.climate_range(fv01_files, "TEMP")
#
# #print('step 11 nearest')
# #ocean_dp.qc.par_nearest_qc.add_qc(fv01_files)
