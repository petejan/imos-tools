import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import shutil

import ocean_dp.parse.mds5_to_netCDF
import ocean_dp.parse.eco_par2netCDF

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.site_instrument
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
import ocean_dp.qc.climate_range
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
cnv_files = glob.glob(os.path.join(path, "*.Csv"))
for fn in cnv_files:
    print(fn)
    files.append(ocean_dp.parse.mds5_to_netCDF.parse([os.path.join(path, fn)]))

log_files = glob.glob(os.path.join(path, "*.log"))
for fn in log_files:
    print(fn)
    files.append(ocean_dp.parse.eco_par2netCDF.eco_parse([os.path.join(path, fn)]))

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
                                                      ['metadata/pulse-7.metadata.csv',
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

    filenames = ocean_dp.processing.pandas_pres_interp.interpolator([filename], os.path.join(path, 'IMOS_ABOS-SOTS_PT_20100817_SOFS_FV02_Pulse-Aggregate-P_END-20110430_C-20200130.nc'))
    print('step 4 pressure interpolator : ', filename)

    filename = ocean_dp.processing.add_incoming_radiation.add_solar(filenames)
    print('step 5 add incoming radiation : ', filename)

print('step SBE16 data')
sbe16_file = ocean_dp.processing.extract_SBE16_PAR.extract([os.path.join(path, 'IMOS_ABOS-SOTS_CPST_20100817_SOFS_FV01_Pulse-7-2010-SBE16plus-01606331-31m_END-20110430_C-20200130.nc')], sbe16_var='V1')
new_name = os.path.join(path, "../netCDF", os.path.basename(sbe16_file[0]))
print("new name", sbe16_file[0], new_name)
os.rename(sbe16_file[0], new_name)
filename = ocean_dp.processing.add_incoming_radiation.add_solar([new_name])

print('step ECO-PAR cal')
fv00_files = glob.glob(os.path.join(path, "../netCDF/IMOS*X*FV00_Pulse-7-2010-ECO-PARS-PAR-135*.nc"))
ocean_dp.processing.eco_parcount_2_par.cal(fv00_files)

print('step 6 add qc flags')
fv00_files = glob.glob(os.path.join(path, "../netCDF/IMOS*X*FV00_Pulse-7-2010*.nc"))
ocean_dp.qc.add_qc_flags.add_qc(fv00_files, "PAR")

print('step 7 in/out water')
fv01_files = glob.glob(os.path.join(path, "../netCDF/IMOS*X*FV01_Pulse-7-2010*.nc"))
ocean_dp.qc.in_out_water.in_out_water(fv01_files, "PAR")

print('step 8 global range')
fv01_files = glob.glob(os.path.join(path, "../netCDF/IMOS*X*FV01_Pulse-7-2010*.nc"))
ocean_dp.qc.global_range.global_range(fv01_files, 'PAR', max=10000, min=-1.7)

print('step 9 global range, pbad 4500')
fv01_files = glob.glob(os.path.join(path, "../netCDF/IMOS*X*FV01_Pulse-7-2010*.nc"))
ocean_dp.qc.global_range.global_range(fv01_files, 'PAR', max=4500, min=-1.7)

print('step 10 climate qc')
fv01_files = glob.glob(os.path.join(path, "../netCDF/IMOS*X*FV01_Pulse-7-2010*.nc"))
ocean_dp.qc.climate_range.climate_range(fv01_files, "PAR")

print('step 11 nearest')
fv01_files = glob.glob(os.path.join(path, "../netCDF/IMOS_ABOS*X*FV01*Pulse-7*.nc"))
ocean_dp.qc.par_nearest_qc.add_qc(fv01_files)

#ocean_dp/qc/add_qc_flags.py -PAR netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV00_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc
#ocean_dp/qc/in_out_water.py -PAR netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV01_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc
#ocean_dp/qc/global_range.py netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV01_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc PAR 10000 -1.7
#ocean_dp/qc/global_range.py netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV01_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc PAR 4500 -1.7 3
#ocean_dp/qc/climate_range.py netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV01_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc PAR

#find netCDF -name "IMOS*FV01*.nc" -exec python3 imos-tools/ocean_dp/qc/climate_range.py {} PAR \;
#python3 imos-tools/ocean_dp/qc/par_nearest_qc.py

print(process.memory_info().rss)  # in bytes

