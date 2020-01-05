#!/bin/zsh

datadir=data/temp-data/Pulse-6-2009

echo ${datadir}

python3 ocean_dp/parse/sbe37DD2netCDF.py ${datadir}/cat6962.cap
python3 ocean_dp/parse/sbe16DD2netCDF.py ${datadir}/Instrument_Data_Upload_25032010.csv
python3 ocean_dp/parse/rbr2netCDF.py   ${datadir}/RBR-Download-014788_eng.txt

find ${datadir} -name "Asc*.txt" -exec python3 ocean_dp/parse/vemco2netCDF.py {} \;

mkdir ${datadir}/netCDF
mv ${datadir}/*.nc ${datadir}/netCDF/

find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/addAttributes.py {} metadata/Pulse-6.metadata.csv metadata/imos.metadata.csv metadata/sots.metadata.csv metadata/Pulse.metadata.csv metadata/variable.metadata.csv \;
find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/add_geospatial_attributes.py {} \;

find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/format_attributes.py {} \;
find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/file_name/imosNetCDFfileName.py {} \;
