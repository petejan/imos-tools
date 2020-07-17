import pandas as pd
from netCDF4 import Dataset, date2num, stringtoarr, stringtochar
import numpy as np

from datetime import datetime

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


def create_obs_idx(ncOut, var, name):

    nc_out = ncOut.createVariable(name, 'S1', ('n'+name, 'strlen80',), zlib=True)
    nc_idx_out = ncOut.createVariable(name+'_INDEX', 'i4', ('OBS',), zlib=True, fill_value=-1)

    print('processing ', name, len(var))
    i = 0
    for f in var:
        #print(f)
        try:
            nc_out[i] = stringtoarr(f[0], 80, dtype='U')
            # s = np.array(f[0], 'S80')
            #nc_out[i] = stringtochar(f[0], encoding='utf-8')
            # nc_out[i] = f[0].encode('utf-8')
            # nc_out[i] = f[0]
        except UnicodeEncodeError:
            pass
        nc_idx_out[f[1].index] = i
        i += 1


fn = 'PLANKTON_SOTS_PHYTOPLANKTON.csv'

phyto = pd.read_csv(fn)

family = phyto.groupby(['FAMILY'])
genus = phyto.groupby(['GENUS'])
species = phyto.groupby(['SPECIES'])
taxon = phyto.groupby(['TAXON_NAME'])
taxon_group = phyto.groupby(['TAXON_ECO_GROUP'])

outputName = fn + '.nc'

ncOut = Dataset(outputName, 'w', format='NETCDF4')
ncOut.comment = 'data downloaded from http://www.cmar.csiro.au/geoserver/wms?service=WFS&version=1.1.0&request=GetFeature&typeName=imos:PLANKTON_SOTS_PHYTOPLANKTON&srsName=EPSG%3A4326&sortby=imos:SAMPLE_TIME&outputFormat=csv'

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

date_time = [datetime.strptime(x, '%Y-%m-%dT%H:%MZ') for x in phyto.SAMPLE_TIME]
t_num = date2num(date_time, ncTimesOut.units)

ncTimesOut[:] = t_num

nc_cell_Out = ncOut.createVariable('CELL', 'f4', ('OBS',), zlib=True, fill_value=np.nan)
nc_cell_Out.units = 'litre-1'
nc_cell_Out[:] = phyto.CELL_PER_LITRE.values
nc_cell_Out.standard_name = 'number_concentration_of_biological_taxon_in_sea_water'

nc_bv_out = ncOut.createVariable('VOLUME', 'f4', ('OBS',), zlib=True, fill_value=np.nan)
nc_bv_out.units = 'm^3/litre'
nc_bv_out[:] = phyto.BIOVOLUME_UM3_PER_L.values

nc_caab_out = ncOut.createVariable('CAAB', 'i4', ('OBS',), zlib=True, fill_value=-1)
nc_caab_out.valid_min = np.int(0)
nc_caab_out.comment = 'https://www.cmar.csiro.au/caab/'
caab = phyto.CAAB_CODE.values
caab[np.isnan(caab)] = -1
nc_caab_out[:] = caab

create_obs_idx(ncOut, family, 'FAMILY')
create_obs_idx(ncOut, genus, 'GENUS')
create_obs_idx(ncOut, species, 'SPECIES')
create_obs_idx(ncOut, taxon, 'TAXON')
create_obs_idx(ncOut, taxon_group, 'TAXON_GROUP')

ncOut.variables['TAXON'].standard_name = 'biological_taxon_name'
ncOut.variables['CAAB'].standard_name = 'biological_taxon_identifier' # these are not really WoRMS (AphidID) codes eg aphia:104464 , but should be

ncOut.close()


