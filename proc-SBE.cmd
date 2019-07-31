python ocean_dp\parse\sbeASC2netCDF.py data\SBE37-1777-1000m.asc
python ocean_dp\processing\addPSAL.py data\SBE37-1777-1000m.asc.nc
python ocean_dp\attribution\addAttributes.py data\SBE37-1777-1000m.asc.nc metadata\imos.metadata.csv metadata\SAZ47-20-2018.metadata.csv metadata\SAZ47-20-2018-instrument.metadata.csv metadata\sots.metadata.csv
