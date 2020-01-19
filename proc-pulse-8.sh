#!/bin/zsh

datadir=data/temp-data/Pulse-8-2011

echo ${datadir}

python3 ocean_dp/parse/sbe16DD2netCDF.py ${datadir}/Pulse-8-SBE16-2012-07-21.cap 

python3 ocean_dp/parse/sbe37DD2netCDF.py ${datadir}/Pulse-8-SBE37-6962-2012-07-21A.cap
python3 ocean_dp/parse/sbeCNV2netCDF.py ${datadir}/SBE05600531_2012-08-07.cnv

python3 ocean_dp/parse/rbr2netCDF.py   ${datadir}/TDR-2050-014788_20120806_0311_eng.txt
python3 ocean_dp/parse/rbr2netCDF.py   ${datadir}/TDR-2050-014789_20120806_0329_eng.txt

find ${datadir} -name "Minilog-T*.csv" -exec python3 ocean_dp/parse/vemco2netCDF.py {} \;

mkdir ${datadir}/netCDF
mv ${datadir}/*.nc ${datadir}/netCDF/

for file in $(find ${datadir}/netCDF -name "*.nc" -type f); 
  do echo "$file"; 

  python3 ocean_dp/attribution/addAttributes.py ${file} metadata/Pulse-8.metadata.csv metadata/imos.metadata.csv metadata/sots.metadata.csv metadata/Pulse.metadata.csv metadata/variable.metadata.csv
  python3 ocean_dp/attribution/add_geospatial_attributes.py ${file}

  python3 ocean_dp/attribution/format_attributes.py ${file}
  python3 ocean_dp/file_name/imosNetCDFfileName.py ${file}

done

#python3 ocean_dp/aggregation/copyDataset.py -v PRES -f pulse-6-pres-agg.txt
