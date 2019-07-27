#!/bin/sh

file=data/SAZ20-2018.aqd

python3 python/file-parsing/nortek2netCDF.py $file
python3 python/attribution/addAttributes.py $file.nc metadata/SAZ47-20-2018-instrument.metadata.csv metadata/imos.metadata.csv metadata/sots.metadata.csv metadata/SAZ47-20-2018.metadata.csv
python3 python/processing/add_mag_variation.py $file.nc 
python3 python/attribution/add_geospatial_attributes.py $file.nc

fn=$(python3 python/file-naming/imosNetCDFfileName.py $file.nc)

python3 python/processing/magnetic_to_true.py $fn
# python3 python/plotting/plotNetCDF.py $fn

echo $fn
