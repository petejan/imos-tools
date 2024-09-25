import pandas as pd
from cftime import num2date
from netCDF4 import Dataset, date2num, stringtoarr, stringtochar
import numpy as np

import sys
import os

from datetime import datetime, UTC
import sqlite3
from dateutil.parser import *
from dateutil.tz import *

# see http://cfconventions.org/cf-conventions/cf-conventions.html#taxon-names-and-identifiers

# dimension:
#   time = 100 ;
#   string80 = 80 ;
#   taxon = 2 ;
# variables:
#   float time(time);
#     time:standard_name = "time" ;
#     time:units = "days since 2019-01-01" ;
#   float abundance(time,taxon) ;
#     abundance:standard_name = "number_concentration_of_organisms_in_taxon_in_sea_water" ;
#     abundance:coordinates = "taxon_lsid taxon_name" ;
#   char taxon_name(taxon,string80) ;
#     taxon_name:standard_name = "biological_taxon_name" ;
#   char taxon_lsid(taxon,string80) ;
#     taxon_lsid:standard_name = "biological_taxon_lsid" ;
# data:
#   time = // 100 values ;
#   abundance = // 200 values ;
#   taxon_name = "Calanus finmarchicus", "Calanus helgolandicus" ;
#   taxon_lsid = "urn:lsid:marinespecies.org:taxname:104464", "urn:lsid:marinespecies.org:taxname:104466" ;

# this code implements a 'Discrete Sampling Geometries' format for the output file
#  http://cfconventions.org/cf-conventions/cf-conventions.html#_indexed_ragged_array_representation


# 2023-09-01
# AODN geo server http://geoserver-123.aodn.org.au/geoserver/ows?service=WFS&version=1.1.0&request=GetFeature&typeName=imos:bgc_phytoplankton_abundance_raw_data&outputFormat=csv
# is one line per sample, may counts per line
# http://pluto.it.csiro.au:8888/ords/orabn1/f?p=191 internal link

# Imos Site Code,Sots Year,Sots Deployment,Sample Number,Sample Date,Sample Time,Longitude,Latitude,Taxon Name,Family,Genus Name,Species Name,Taxon Eco Group,Aphia Id,Taxon Start Date,Cell Per Litre,Biovolume Um3 Per L,Sample Comments,Deployment Voyage,Retrieval Voyage,Deployment Date,Retrieval Date
# SOTS_RAS,2010,SOTS_RAS2010PULSE_7,2,12-SEP-10,12-SEP-10,142.2566,-46.9347333,Centric diatom < 10 µm,,,,Centric diatom,,29-SEP-08,105,20616.684,,SS2010_V07,SS2011_V01,12-SEP-10,17-APR-11
# SOTS_RAS,2010,SOTS_RAS2010PULSE_7,2,12-SEP-10,12-SEP-10,142.2566,-46.9347333,Ciliate 10-20 µm,,,,Ciliate,11,,26,,,SS2010_V07,SS2011_V01,12-SEP-10,17-APR-11
# SOTS_RAS,2010,SOTS_RAS2010PULSE_7,2,12-SEP-10,12-SEP-10,142.2566,-46.9347333,Ciliate 30-40 µm,,,,Ciliate,11,,26,,,SS2010_V07,SS2011_V01,12-SEP-10,17-APR-11
# SOTS_RAS,2010,SOTS_RAS2010PULSE_7,2,12-SEP-10,12-SEP-10,142.2566,-46.9347333,Dictyocha speculum,Dictyochaceae,Dictyocha,speculum,Silicoflagellate,157260,29-SEP-08,132,151845.611,,SS2010_V07,SS2011_V01,12-SEP-10,17-APR-11
# SOTS_RAS,2010,SOTS_RAS2010PULSE_7,2,12-SEP-10,12-SEP-10,142.2566,-46.9347333,Gymnodinioid dinoflagellate 10-20 µm,Gymnodiniaceae,,,Dinoflagellate,109392,29-SEP-08,263,193890.557,,SS2010_V07,SS2011_V01,12-SEP-10,17-APR-11
# SOTS_RAS,2010,SOTS_RAS2010PULSE_7,2,12-SEP-10,12-SEP-10,142.2566,-46.9347333,Leptocylindrus mediterraneus,Leptocylindraceae,Leptocylindrus,mediterraneus,Centric diatom,149230,29-SEP-08,26,245044.02,,SS2010_V07,SS2011_V01,12-SEP-10,17-APR-11


def create_obs_idx(ncOut, var, name):

    nc_out = ncOut.createVariable(name, 'S1', ('n'+name, 'strlen80',), zlib=True)
    nc_idx_out = ncOut.createVariable(name+'_INDEX', 'i4', ('OBS',), zlib=True, fill_value=-1)

    print('processing ', name, len(var))
    i = 0
    for f in var:
        #print(f[0][0])
        try:
            nc_out[i] = stringtoarr(f[0][0], 80, dtype='U')
            # s = np.array(f[0], 'S80')
            # nc_out[i] = stringtochar(f[0][0], encoding='utf-8')
            # nc_out[i] = f[0].encode('utf-8')
            # nc_out[i] = f[0]
        except UnicodeEncodeError:
            pass
        nc_idx_out[f[1].index] = i
        i += 1


fn = 'phyto.sqlite'

cnx = sqlite3.connect('phyto.sqlite')
phyto = pd.read_sql_query("SELECT * FROM PLANKTON_SOTS_PHYTOPLANKTON", cnx)

print(phyto)

print('number samples', len(phyto))

family = phyto.groupby(['FAMILY'])
genus = phyto.groupby(['GENUS'])
species = phyto.groupby(['SPECIES'])
taxon = phyto.groupby(['TAXON_NAME'])
taxon_group = phyto.groupby(['TAXON_ECO_GROUP'])

outputName = fn + '.nc'

ncOut = Dataset(outputName, 'w', format='NETCDF4')
#ncOut.comment = 'data downloaded from https://www.cmar.csiro.au/geoserver/ows?service=wfs&version=2.0.0&request=GetFeature&typeName=imos:PLANKTON_SOTS_PHYTOPLANKTON&srsName=EPSG%3A4326&sortby=imos:SAMPLE_TIME&outputFormat=csv'

oDim = ncOut.createDimension('OBS', len(phyto))
sDim = ncOut.createDimension('strlen80', 80)
fDim = ncOut.createDimension('nFAMILY', len(family))
gDim = ncOut.createDimension('nGENUS', len(genus))
sDim = ncOut.createDimension('nSPECIES', len(species))
tDim = ncOut.createDimension('nTAXON', len(taxon))
tgDim = ncOut.createDimension('nTAXON_GROUP', len(taxon_group))

ncTimesOut = ncOut.createVariable('TIME', 'd', ('OBS',), zlib=True)
ncTimesOut.long_name = "time"
ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
ncTimesOut.calendar = "gregorian"
ncTimesOut.axis = "T"

date_time = [parse(x) for x in phyto['SAMPLE_TIME']]
t_num = date2num(date_time, ncTimesOut.units)

ncTimesOut[:] = t_num

ncLat = ncOut.createVariable('LATITUDE', 'd', (), zlib=True)
ncLat.long_name = "latitude"
ncLat.units = "degrees_north"
ncLat.axis = "Y"
ncLat.reference_datum = "WGS84 geographic coordinate system"
ncLat.valid_max = 90
ncLat.valid_min = -90
ncLat[0] = -47.0

ncLon = ncOut.createVariable('LONGITUDE', 'd', (), zlib=True)
ncLon.long_name = "degrees_east"
ncLon.units = "degrees_east"
ncLon.axis = "X"
ncLon.reference_datum = "WGS84 geographic coordinate system"
ncLon.valid_max = 180
ncLon.valid_min = -180
ncLon[0] = 142.0

ncDNom = ncOut.createVariable('NOMINAL_DEPTH', 'd', (), zlib=True)
ncDNom.standard_name = "depth"
ncDNom.long_name = "nominal depth"
ncDNom.units = "m"
ncDNom.axis = "Z"
ncDNom.reference_datum = "sea surface"
ncDNom.positive = "down"
ncDNom.valid_max = 10000
ncDNom.valid_min = 0
ncDNom[0] = 0.0


nc_cell_Out = ncOut.createVariable('CELL', 'f4', ('OBS',), zlib=True, fill_value=np.nan)
nc_cell_Out.units = 'litre-1'
nc_cell_Out[:] = phyto['CELL_PER_LITRE'].values
nc_cell_Out.standard_name = 'number_concentration_of_biological_taxon_in_sea_water'

nc_bv_out = ncOut.createVariable('VOLUME', 'f4', ('OBS',), zlib=True, fill_value=np.nan)
nc_bv_out.units = 'm^3/litre'
nc_bv_out[:] = phyto['BIOVOLUME_UM3_PER_L'].values

#nc_aphia_id_out = ncOut.createVariable('APHIA_ID', 'i4', ('OBS', 'strlen80'), zlib=True, fill_value=-1)
nc_aphia_id_out = ncOut.createVariable('APHIA_ID', 'S1', ('OBS', 'strlen80'), zlib=True)
#nc_aphia_id_out.valid_min = int(0)
nc_aphia_id_out.comment = 'https://www.marinespecies.org/aphia.php'
aphia_id = phyto['WORMS_APHIA_ID'].values
#aphia_id[np.isnan(aphia_id)] = -1

# for i in range(len(aphia_id)):
#     if ~np.isnan(aphia_id[i]):
#         nc_aphia_id_out[i] = stringtoarr("urn:lsid:marinespecies.org:taxname:" + '{:.0f}'.format(aphia_id[i]), 80, dtype='U')
#     else:
#         nc_aphia_id_out[i] = stringtoarr("", 80, dtype='U')

create_obs_idx(ncOut, family, 'FAMILY')
create_obs_idx(ncOut, genus, 'GENUS')
create_obs_idx(ncOut, species, 'SPECIES')
create_obs_idx(ncOut, taxon, 'TAXON')
create_obs_idx(ncOut, taxon_group, 'TAXON_GROUP')

ncOut.variables['TAXON'].standard_name = 'biological_taxon_name'
ncOut.variables['APHIA_ID'].standard_name = 'biological_taxon_identifier' # these are not really WoRMS (AphidID) codes eg aphia:104464 , but should be

ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

# add timespan attributes
ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

# add creating and history entry
ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + os.path.basename(fn))

ncOut.setncattr("contributor_name", "Eriksen, Ruth")
ncOut.setncattr("contributor_role", "phytoplankton identification")

ncOut.geospatial_lat_max = -47.0
ncOut.geospatial_lat_min = -47.0

ncOut.geospatial_lon_min = 142.0
ncOut.geospatial_lon_max = 142.0

ncOut.close()


