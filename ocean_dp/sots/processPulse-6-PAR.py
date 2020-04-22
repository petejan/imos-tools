import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import shutil

import ocean_dp.parse.mds5_to_netCDF

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.site_instrument
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName
import ocean_dp.processing.pandas_pres_interp
import ocean_dp.processing.add_incoming_radiation
import ocean_dp.processing.apply_scale_offset_attributes
import ocean_dp.processing.extract_SBE16_PAR

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
                                                      ['metadata/pulse-6.metadata.csv',
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

    filenames = ocean_dp.processing.pandas_pres_interp.interpolator([filename], '/Users/pete/cloudstor/SOTS-Temp-Raw-Data/Pulse-6-2009/netCDF/IMOS_ABOS-SOTS_CPT_20090928_SOFS_FV02_Pulse-Aggregate-PRES_END-20100318_C-20200227.nc')
    print('step 4 pressure interpolator : ', filename)

    filename = ocean_dp.processing.add_incoming_radiation.add_solar(filenames)
    print('step 5 add incoming radiation : ', filename)

sbe16_file = ocean_dp.processing.extract_SBE16_PAR.extract([os.path.join(path, 'IMOS_ABOS-SOTS_CPT_20090930_SOFS_FV01_Pulse-6-2009-SBE16plusV2-01606331-38m_END-20100325_C-20200227.nc')])
new_name = os.path.join(path, "../netCDF", os.path.basename(sbe16_file))
os.rename(sbe16_file, new_name)
filename = ocean_dp.processing.add_incoming_radiation.add_solar(new_name)

#ocean_dp/qc/add_qc_flags.py -PAR netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV00_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc
#ocean_dp/qc/in_out_water.py -PAR netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV01_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc
#ocean_dp/qc/global_range.py netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV01_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc PAR 10000 -1.7
#ocean_dp/qc/global_range.py netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV01_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc PAR 4500 -1.7 3
#ocean_dp/qc/climate_range.py netCDF/IMOS_ABOS-SOTS_FZX_20090922_SOFS_FV01_Pulse-6-2009-MDS-MKVL-200341-0m_END-20100323_C-20200417.nc PAR


print(process.memory_info().rss)  # in bytes

