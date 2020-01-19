#!/bin/zsh

datadir=data/temp-data/SOFS-1-2010

echo ${datadir}

python3 ocean_dp/parse/sbeASC2netCDF.py ${datadir}/SOFS-1-SBE37SM-1840-SS-7.asc
#python3 ocean_dp/processing/addPSAL.py ${datadir}/SOFS-1-SBE37SM-1840-SS-7.asc.nc
#python3 ocean_dp/processing/fill_pres_from_nominal.py ${datadir}/SOFS-1-SBE37SM-1840-SS-7.asc.nc

python3 ocean_dp/parse/sbe37DD2netCDF.py ${datadir}/SOFS-1-SBE37SM-2971-100m-3.asc
python3 ocean_dp/parse/sbeCNV2netCDF.py ${datadir}/sbe37sm-rs485_7409_2011_05_24.cnv

find ${datadir} -name "Asc-*.000" -exec python3 ocean_dp/parse/vemco2netCDF.py {} \;

mkdir -p ${datadir}/netCDF
mv ${datadir}/*.nc ${datadir}/netCDF/

for file in $(find ${datadir}/netCDF -name "*.nc" -type f); 
  do echo "$file"; 

  python3 ocean_dp/attribution/addAttributes.py ${file} metadata/sofs-1.metadata.csv metadata/imos.metadata.csv metadata/sots.metadata.csv metadata/sofs.metadata.csv metadata/variable.metadata.csv
  python3 ocean_dp/attribution/add_geospatial_attributes.py ${file}

  python3 ocean_dp/attribution/format_attributes.py ${file}
  python3 ocean_dp/file_name/imosNetCDFfileName.py ${file}

done

#python3 ocean_dp/aggregation/copyDataset.py -v PRES -f pulse-6-pres-agg.txt
