#/bin/sh

file=data/SBE37-1777-1000m.asc

python3 ocean_dp/parse/sbeASC2netCDF.py ${file}
python3 ocean_dp/processing/addPSAL.py ${file}.nc
python3 ocean_dp/attribution/addAttributes.py ${file}.nc metadata/imos.metadata.csv metadata/SAZ47-20-2018.metadata.csv metadata/SAZ47-20-2018-instrument.metadata.csv metadata/sots.metadata.csv metadata/variable.metadata.csv
python3 ocean_dp/attribution/add_geospatial_attributes.py ${file}.nc
python3 ocean_dp/plotting/plotNetCDF.py ${file}.nc

python3 ocean_dp/file_naming/imosNetCDFfileName.py ${file}.nc
