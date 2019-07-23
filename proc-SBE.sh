#/bin/sh

file=data/SBE37-1777-1000m.asc

python3 python/file-parsing/sbeASC2netCDF.py ${file}
python3 python/processing/addPSAL.py ${file}.nc
python3 python/attribution/addAttributes.py ${file}.nc metadata/imos.metadata.csv metadata/SAZ47-20-2018.metadata.csv metadata/SAZ47-20-2018-instrument.metadata.csv metadata/sots.metadata.csv metadata/variable.metadata.csv
python3 python/attribution/add_geospatial_attributes.py ${file}.nc
python3 python/plotting/plotNetCDF.py ${file}.nc

python3 python/file-naming/imosNetCDFfileName.py ${file}.nc
