import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import ocean_dp.file_name.find_file_with
import ocean_dp.qc.manual_by_date

import psutil
import os
import sys

process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

path = sys.argv[1] + "/"

print ('file path : ', path)

# pulse_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "IMOS*.nc"))
# par_files = ocean_dp.file_name.find_file_with.find_variable(pulse_files, 'PAR')
# epar_files = ocean_dp.file_name.find_file_with.find_variable(par_files, 'ePAR')
#
# print('PAR files:')
# for f in par_files:
#     print(f)
#
# par_files = ocean_dp.processing.add_incoming_radiation.add_solar(par_files)
#
# qc_files = ocean_dp.qc.add_qc_flags.add_qc(par_files, "PAR")  # this resets the QC to 0

pulse_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "IMOS*.nc"))
par_files = ocean_dp.file_name.find_file_with.find_variable(pulse_files, 'PAR')
fv01_files = ocean_dp.file_name.find_file_with.find_global(par_files, 'file_version', 'Level 1 - Quality Controlled Data')
fv01_files = ocean_dp.file_name.find_file_with.find_variable(fv01_files, 'ePAR')

print('FV01 files:')
for f in fv01_files:
    print(f)

p6_27 = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'deployment_code', 'Pulse-6-2009')
p6_27 = ocean_dp.file_name.find_file_with.find_global(p6_27, 'instrument_serial_number', '200341')

print('p6_0 files:')
for f in p6_27:
    print(f)

ocean_dp.qc.manual_by_date.maunal(p6_27, after_str='2009-09-22 00:00:00', var_name='PAR', flag=4, reason='sensor scaling incorrect, saturates')

p6_27 = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'deployment_code', 'Pulse-6-2009')
p6_27 = ocean_dp.file_name.find_file_with.find_global(p6_27, 'instrument_serial_number', '200664')

print('p6_27 files:')
for f in p6_27:
    print(f)

ocean_dp.qc.manual_by_date.maunal(p6_27, after_str='2010-01-02 00:00:00', var_name='PAR', flag=3, reason='bio-fouled, drops below deeper sensor')

p7_50 = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'deployment_code', 'Pulse-7-2010')
p7_50 = ocean_dp.file_name.find_file_with.find_global(p7_50, 'instrument_serial_number', '200665')

print('p7_50 files:')
for f in p7_50:
    print(f)

ocean_dp.qc.manual_by_date.maunal(p7_50, after_str='2011-02-01 00:00:00', var_name='PAR', flag=3, reason='bio-fouled, drops below deeper sensor')

p8_27 = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'deployment_code', 'Pulse-8-2011')
p8_27 = ocean_dp.file_name.find_file_with.find_global(p8_27, 'instrument_serial_number', '200664')

print('p8_27 files:')
for f in p8_27:
    print(f)

ocean_dp.qc.manual_by_date.maunal(p8_27, after_str='2011-10-01 00:00:00', var_name='PAR', flag=3, reason='bio-fouled, drops below deeper sensor')

p10_49 = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'deployment_code', 'Pulse-11-2015')
p10_49 = ocean_dp.file_name.find_file_with.find_global(p10_49, 'instrument_serial_number', '082V023')

print('p10_49 files:')
for f in p10_49:
    print(f)

ocean_dp.qc.manual_by_date.maunal(p10_49, after_str='2013-09-15 00:00:00', var_name='PAR', flag=3, reason='bio-fouled, drops below deeper sensor')

s1_0 = ocean_dp.file_name.find_file_with.find_global(fv01_files, 'deployment_code', 'SOFS-1-2010')
s1_0 = ocean_dp.file_name.find_file_with.find_global(s1_0, 'instrument_serial_number', 'Q40966')

print('s1_0 files:')
for f in s1_0:
    print(f)

ocean_dp.qc.manual_by_date.maunal(s1_0, after_str='2010-03-05 00:00:00', var_name='PAR', flag=4, reason='sensor scaling issue')
