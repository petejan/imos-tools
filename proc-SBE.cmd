python python\file-parsing\sbeASC2netCDF.py data\SBE37-1777-1000m.asc
python python\processing\addPSAL.py data\SBE37-1777-1000m.asc.nc
python python\attribution\addAttributes.py data\SBE37-1777-1000m.asc.nc metadata\imos.metadata.csv metadata\SAZ47-20-2018.metadata.csv metadata\SAZ47-20-2018-instrument.metadata.csv metadata\sots.metadata.csv
