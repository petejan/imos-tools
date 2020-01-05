#!/bin/zsh

datadir=data/temp-data/Pulse-7-2010

echo ${datadir}

python3 ocean_dp/parse/sbeCNV2netCDF.py ${datadir}/SBE37SM-RS232_03706962_2011_05_09.cnv
python3 ocean_dp/parse/sbeCNV2netCDF.py ${datadir}/SBE16plus_01606331_2011_04_30A.cnv
python3 ocean_dp/parse/rbr2netCDF.py   ${datadir}/014788_20110426_1347.txt
python3 ocean_dp/parse/rbr2netCDF.py   ${datadir}/014789_20110429_2327.txt

find ${datadir} -name "Asc*.000" -exec python3 ocean_dp/parse/vemco2netCDF.py {} \;

mkdir ${datadir}/netCDF
mv ${datadir}/*.nc ${datadir}/netCDF/

#find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/addAttributes.py {} metadata/Pulse-7.metadata.csv \;

find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/addAttributes.py {} metadata/Pulse-7.metadata.csv metadata/imos.metadata.csv metadata/sots.metadata.csv metadata/Pulse.metadata.csv metadata/variable.metadata.csv \;
find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/add_geospatial_attributes.py {} \;

find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/format_attributes.py {} \;
find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/file_name/imosNetCDFfileName.py {} \;
