import os
import sys
from datetime import datetime, UTC

import numpy as np
from influxdb import InfluxDBClient
from netCDF4 import Dataset, date2num, stringtochar

from ocean_dp.processing.addPSAL import add_psal

client = InfluxDBClient('localhost', 8086, 'pete', 'password/usr/local/opt/influxdb/bin/influxd version', 'pyRTDP')
result = client.query('SELECT last("sst") AS "TEMP", last("scc") AS "CNDC", last("obpahse") AS "OXY_BPHASE", last("otemp") AS "OXY_TEMP" FROM "data" WHERE time > now() - 365d GROUP BY time(1h), "deployment_code"::tag fill(none)')

#print("Result: {0}".format(result))
points = result.get_points()
n = 0
time_dt = []
temp = []
cndc = []
for r in points:
    print(r)
    time_dt.append(datetime.strptime(r['time'], "%Y-%m-%dT%H:%M:%SZ"))
    if r['TEMP']:
        temp.append(float(r['TEMP']))
    else:
        temp.append(float('NaN'))
    if r['CNDC']:
        cndc.append(float(r['CNDC']))
    else:
        cndc.append(float('NaN'))
    n += 1

deployment_code = 'SOFS-15-2026'
lat = -46.79651
lon = 141.81913

file_name = 'IMOS_DWM-SOTS_CT_' + time_dt[0].strftime('%Y%m%d') + '_SOFS_FV00_' + deployment_code + '-ASIMET-realtime.nc'
ds = Dataset(file_name, 'w', format='NETCDF4_CLASSIC')

ds.abstract='The ' + deployment_code + ' mooring was deployed April 2025 at Lat '+f"{lat:.2f}"+', Lon '+f"{lon:.2f}"+'. Moored instruments are deployed by the IMOS Deep Water Mooring (DWM) Southern Ocean Time Series sub-facility for time-series observations of physical, biological, and chemical properties, in the Sub-Antarctic Zone southwest of Tasmania, with yearly servicing. The Southern Ocean Time Series (SOTS) Sub-Facility is responsible for the deployment of SOFS moorings. These time-series observations are crucial to resolving ecosystem processes that affect carbon cycling, ocean productivity and marine responses to climate variability and change, ocean acidification and other stresses.'
ds.acknowledgement='Any users of IMOS data are required to clearly acknowledge the source of the material derived from IMOS in the format: "Data was sourced from the Integrated Marine Observing System (IMOS) - IMOS is a national collaborative research infrastructure, supported by the Australian Government."'
ds.author='Jansen, Peter'
ds.author_email='peter.jansen@csiro.au'
ds.cdm_data_type='station'
ds.citation='The citation in a list of references is: IMOS [year-of-data-download], [Title], [data-access-url], accessed [date-of-access]'
ds.comment='The ' + deployment_code + ' mooring was deployed in May 2026 and due to be recovered May 2027 at Lat '+f"{lat:.2f}"+', Lon '+f"{lon:.2f}"+'.\nMoored instruments are deployed by the IMOS Deep Water Mooring (DWM) Southern Ocean Time Series sub-facility for time-series observations of physical, biological, and chemical properties, in the Sub-Antarctic Zone southwest of Tasmania, with yearly servicing.\nThe Southern Ocean Time Series (SOTS) Sub-Facility is responsible for the deployment of SOFS moorings.\nThese time-series observations are crucial to resolving ecosystem processes that affect carbon cycling, ocean productivity and marine responses to climate variability and change, ocean acidification and other stresses.'
ds.Conventions='CF-1.6,IMOS-1.4'
ds.data_centre='Australian Ocean Data Network (AODN)'
ds.data_centre_email='info@aodn.org.au'
ds.data_mode='R'
ds.deployment_code=deployment_code
ds.deployment_number='15'
ds.disclaimer='Data, products and services from IMOS are provided "as is" without any warranty as to fitness for a particular purpose.'
ds.distribution_statement='Data may be re-used, provided that related metadata explaining the data has been reviewed by the user, and the data is appropriately acknowledged. Data, products and services from IMOS are provided "as is" without any warranty as to fitness for a particular purpose.'
ds.featureType='timeSeries'
ds.file_version='Level 0 - Raw data'
ds.file_version_quality_control='Raw data is defined as unprocessed data and data products that have not undergone quality control. The data may be in engineering units or physical units, time and locations details can be in relative units and values can be pre-calibration measurements. Level 0 data is not suitable for public access within IMOS.'
ds.geospatial_lat_max=lat
ds.geospatial_lat_min=lat
ds.geospatial_lon_max=lon
ds.geospatial_lon_min=lon
ds.geospatial_vertical_max=2.0
ds.geospatial_vertical_min=2.0
ds.geospatial_vertical_positive='down'
ds.instrument_nominal_depth=-2.0
ds.instrument_serial_number='L22'
ds.license='http://creativecommons.org/licenses/by/4.0/'
ds.naming_authority='IMOS'
ds.netcdf_version='3.6'
ds.platform_code='SOFS'
ds.principal_investigator='Shadwick, Elizabeth; Shulz, Eric'
ds.project='Integrated Marine Observing System (IMOS)'
ds.site_code='SOTS'
ds.source='Mooring'
ds.standard_name_vocabulary='NetCDF Climate and Forecast (CF) Metadata Convention Standard Name Table 27'
ds.time_deployment_start='2026-05-28T03:00:00Z'
ds.title='' + deployment_code + ' Mooring Real Time Data'

ds.createDimension('TIME', n)
#ds.createDimension('name_strlen', 128)

ds.createVariable('TIME', np.float64, ('TIME', ))
ds.variables['TIME'].axis='T'
ds.variables['TIME'].calendar='gregorian'
ds.variables['TIME'].long_name='time'
ds.variables['TIME'].standard_name='time'
ds.variables['TIME'].units='days since 1950-01-01 00:00:00 UTC'
ds.variables['TIME'].valid_max=36525.0
ds.variables['TIME'].valid_min=18262.0
ds.variables['TIME'][:] = date2num(time_dt, calendar=ds.variables['TIME'].calendar, units=ds.variables['TIME'].units)

#ds.createVariable('station_name', "S1", ('name_strlen',))
#ds.variables['station_name'].long_name='station name'
#ds.variables['station_name'].cf_role='timeseries_id'
#ds.variables['station_name'][:] = stringtochar(np.array(deployment_code, dtype='str'), encoding='utf-8')

ds.createVariable('LONGITUDE', np.float64, ())
ds.variables['LONGITUDE'].axis='X'
ds.variables['LONGITUDE'].comment='anchor location'
ds.variables['LONGITUDE'].long_name='longitude'
ds.variables['LONGITUDE'].reference_datum='WGS84 coordinate reference system'
ds.variables['LONGITUDE'].standard_name='longitude'
ds.variables['LONGITUDE'].units='degrees_east'
ds.variables['LONGITUDE'].valid_max=180.0
ds.variables['LONGITUDE'].valid_min=-180.0
ds.variables['LONGITUDE'][:] = lon

ds.createVariable('LATITUDE', np.float64, ())
ds.variables['LATITUDE'].axis='Y'
ds.variables['LATITUDE'].comment='anchor location'
ds.variables['LATITUDE'].long_name='latitude'
ds.variables['LATITUDE'].reference_datum='WGS84 coordinate reference system'
ds.variables['LATITUDE'].standard_name='latitude'
ds.variables['LATITUDE'].units='degrees_north'
ds.variables['LATITUDE'].valid_max=90.0
ds.variables['LATITUDE'].valid_min=-90.0
ds.variables['LATITUDE'][:] = lat

ds.createVariable('NOMINAL_DEPTH', np.float64, ())
ds.variables['NOMINAL_DEPTH'].axis='Z'
ds.variables['NOMINAL_DEPTH'].long_name='nominal depth'
ds.variables['NOMINAL_DEPTH'].positive='down'
ds.variables['NOMINAL_DEPTH'].reference_datum='sea surface'
ds.variables['NOMINAL_DEPTH'].standard_name='depth'
ds.variables['NOMINAL_DEPTH'].units='metres'
ds.variables['NOMINAL_DEPTH'].valid_max=12000.0
ds.variables['NOMINAL_DEPTH'].valid_min=-5.0
ds.variables['NOMINAL_DEPTH'][:] = 2

ds.createVariable('CNDC', np.float64, ('TIME', ))
ds.variables['CNDC'].coordinates='TIME LATITUDE LONGITUDE NOMINAL_DEPTH'
ds.variables['CNDC'].long_name='sea_water_electrical_conductivity'
ds.variables['CNDC'].standard_name='sea_water_electrical_conductivity'
ds.variables['CNDC'].units='S m-1'
ds.variables['CNDC'].valid_max=50000.0
ds.variables['CNDC'].valid_min=0.0
ds.variables['CNDC'][:] = cndc

ds.createVariable('TEMP', np.float64, ('TIME', ))
ds.variables['TEMP'].coordinates='TIME LATITUDE LONGITUDE NOMINAL_DEPTH'
ds.variables['TEMP'].long_name='sea_water_temperature'
ds.variables['TEMP'].standard_name='sea_water_temperature'
ds.variables['TEMP'].units='Celsius'
ds.variables['TEMP'].valid_max=40.0
ds.variables['TEMP'].valid_min=-2.5
ds.variables['TEMP'][:] = temp

# add some file metadata to netCDF file
ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

# create the time coverage attributes
ds.setncattr("time_coverage_start", time_dt[0].strftime(ncTimeFormat))
ds.setncattr("time_coverage_end", time_dt[-1].strftime(ncTimeFormat))

# add creating and history entry
ds.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
ds.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from influxdb using " + os.path.basename(sys.argv[0]))

ds.close()

add_psal(file_name)