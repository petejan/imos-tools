#!/bin/sh

curl -o bgc_phyto_raw.csv "http://imos-data.s3-website-ap-southeast-2.amazonaws.com/IMOS/BGC_DB/harvested_from_CSIRO/bgc_phyto_raw.csv"
curl -o bgc_trip.csv "http://imos-data.s3-website-ap-southeast-2.amazonaws.com/IMOS/BGC_DB/harvested_from_CSIRO/bgc_trip.csv"
curl -o phytoinfo.csv "http://imos-data.s3-website-ap-southeast-2.amazonaws.com/IMOS/BGC_DB/harvested_from_CSIRO/phytoinfo.csv" 

sqlite3 phyto_bgc.sqlite ".import --csv bgc_phyto_raw.csv bgc_phyto_raw"
sqlite3 phyto_bgc.sqlite ".import --csv bgc_trip.csv bgc_trip"
sqlite3 phyto_bgc.sqlite ".import --csv phytoinfo.csv phytoinfo"

sqlite3 phyto_bgc.sqlite ".import --csv SOTS-RAS-deployments.csv SOTS_RAS"

sqlite3 phyto_bgc.sqlite "ALTER TABLE phytoinfo ADD WORMS_APHIA_ID TEXT"
sqlite3 phyto_bgc.sqlite "ALTER TABLE phytoinfo ADD FAMILY TEXT"
sqlite3 phyto_bgc.sqlite "ALTER TABLE phytoinfo ADD GENUS TEXT"
sqlite3 phyto_bgc.sqlite "ALTER TABLE phytoinfo ADD SPECIES TEXT"

python -m ocean_dp.taxa.phyto_sqlite_2_netCDF

python -m ocean_dp.attribution.addAttributes phyto_bgc.sqlite.nc metadata/imos.metadata.csv metadata/sots.metadata.csv metadata/variable.metadata.csv
python -m ocean_dp.attribution.addAttributes phyto_bgc.sqlite.nc phyto.metadata.csv
python -m ocean_dp.attribution.format_attributes phyto_bgc.sqlite.nc
python -m ocean_dp.file_name.imosNetCDFfileName phyto_bgc.sqlite.nc



