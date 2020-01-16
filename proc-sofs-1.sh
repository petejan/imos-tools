#!/bin/zsh

datadir=data/temp-data/SOFS-1-2010

echo ${datadir}

python3 ocean_dp/parse/sbe37DD2netCDF.py ${datadir}/Pulse-8-SBE37SM-6962.cap

python3 ocean_dp/parse/sbe16DD2netCDF.py ${datadir}/Pulse-8-SBE16-2012-07-21.cap
python3 ocean_dp/parse/sbeCNC2netCDF. py ${datadir}/SBE05600531_2012-08-07.cnv

python3 ocean_dp/parse/rbr2netCDF.py   ${datadir}/TDR-2050-014788_20120806_0311.hex
python3 ocean_dp/parse/rbr2netCDF.py   ${datadir}/TDR-2050-014789_20120806_0329.hex

find ${datadir} -name "Minilog-T_*.csv" -exec python3 ocean_dp/parse/vemco2netCDF.py {} \;

mkdir ${datadir}/netCDF
mv ${datadir}/*.nc ${datadir}/netCDF/

find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/addAttributes.py {} metadata/Pulse-6.metadata.csv metadata/imos.metadata.csv metadata/sots.metadata.csv metadata/Pulse.metadata.csv metadata/variable.metadata.csv \;
find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/add_geospatial_attributes.py {} \;

find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/attribution/format_attributes.py {} \;
find ${datadir}/netCDF -name "*.nc" -exec python3 ocean_dp/file_name/imosNetCDFfileName.py {} \;

python3 ocean_dp/aggregation/copyDataset.py -v PRES -f pulse-6-pres-agg.txt
