import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import shutil

import ocean_dp.parse.mds5_to_netCDF
import ocean_dp.parse.defi_to_netCDF
import ocean_dp.parse.eco_par2netCDF

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.find_file_with
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName
import ocean_dp.processing.pandas_pres_interp
import ocean_dp.processing.add_incoming_radiation
import ocean_dp.processing.apply_scale_offset_attributes
import ocean_dp.processing.extract_SBE16_PAR
import ocean_dp.processing.eco_parcount_2_par

import ocean_dp.qc.add_qc_flags
import ocean_dp.qc.in_out_water
import ocean_dp.qc.global_range
import ocean_dp.qc.par_climate_range
import ocean_dp.qc.par_nearest_qc

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
cnv_files = glob.glob(os.path.join(path, "033*.Csv"))
for fn in cnv_files:
    print(fn)
    files.append(ocean_dp.parse.mds5_to_netCDF.parse([fn]))

cnv_files = glob.glob(os.path.join(path, "DEFI**.csv"))
for fn in cnv_files:
    print(fn)
    files.append(ocean_dp.parse.defi_to_netCDF.parse([fn]))

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
                                                      ['metadata/pulse-11.metadata.csv',
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

print('step SBE16 data')
sbe16_file = ocean_dp.processing.extract_SBE16_PAR.extract([os.path.join(path, 'IMOS_ABOS-SOTS_CPST_20150327_SOFS_FV01_Pulse-11-2015-SBE16plus-01606330-28m_END-20160329_C-20200428.nc')], sbe16_var='V1')
new_name = os.path.join(path, "../netCDF", os.path.basename(sbe16_file[0]))
print("new name", sbe16_file[0], new_name)
os.rename(sbe16_file[0], new_name)

pulse_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "../netCDF/IMOS*.nc"))
pulse_files = ocean_dp.file_name.find_file_with.find_global(pulse_files, 'deployment_code', 'Pulse-11-2015')
print('pulse files')
for f in pulse_files:
    print(f)

print('step ECO-PAR cal')
eco_par = ocean_dp.file_name.find_file_with.find_global(pulse_files, 'instrument_model', 'ECO-PARS')
print("eco par", eco_par)
eco_fn = ocean_dp.processing.eco_parcount_2_par.cal(eco_par)
pulse_files.extend(eco_fn)

pulse_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "../netCDF", "IMOS*FV00*.nc"))
p11 = ocean_dp.file_name.find_file_with.find_global(pulse_files, 'deployment_code', 'Pulse-11-2015')
p11 = ocean_dp.file_name.find_file_with.find_variable(p11, 'PAR')
print('p11 files:')
for f in p11:
    print(f)

ocean_dp.processing.add_incoming_radiation.add_solar(p11)


print(process.memory_info().rss)  # in bytes

