#!/bin/sh

file=data/SAZ20-2018.aqd

python3 ocean_dp/parse/nortek2netCDF.py $file
python3 ocean_dp/attribution/addAttributes.py $file.nc metadata/SAZ47-20.metadata.csv metadata/imos.metadata.csv metadata/SAZ47.metadata.csv metadata/variable.metadata.csv
python3 ocean_dp/processing/add_mag_variation.py $file.nc 
python3 ocean_dp/attribution/add_geospatial_attributes.py $file.nc

python3 ocean_dp/processing/findDeploymentRecoveryTimes.py $file.nc

fn=$(python3 ocean_dp/file_name/imosNetCDFfileName.py $file.nc)

python3 ocean_dp/processing/magnetic_to_true.py $fn

fn=$(python3 ocean_dp/qc/add_qc_flags.py $fn)
python3 ocean_dp/qc/in_out_water.py $fn
python3 ocean_dp/qc/finalize_qc.py $fn
python3 ocean_dp/attribution/format_attributes.py $fn 

# python3 ocean_dp/plotting/plotNetCDF.py $fn

#echo $fn
