# processing SOTS data using ocean_dp

create a virtual python environment
```
> ~ % python3 -m venv venvs/ocean_db
active virtual enviroment                               
> ~ % source venvs/ocean_db/bin/activate
(ocean_db) > imos-tools % pip install --upgrade pip
```
install ocean_dp as module
```
(ocean_db) > ~ % cd GitHub/imos-tools 
(ocean_db) > imos-tools % pip install -e .
```
change to processing directory

```
cd ~/CSIRO/OneDrive\ -\ CSIRO/SOTS/2025-data-processing
mkdir processing
cd processing
```

get metadata from mooring deployment database

`> python -m ocean_dp.attribution.getDBmetadata SOFS-14-2025 >> ~/GitHub/imos-tools/metadata/pulse-saz-sofs-flux.metadata.csv` 

process all the cnv files
```
python -m ocean_dp.parse.sbeCNV2netCDF <path to files>/*.cnv
python -m ocean_dp.parse.sbeASC2netCDF <path to files>/*.asc
```
will place all files into the current directory

example output file, note instrument_model and instrument_serial_number
```
netcdf \4165.asc {
dimensions:
	TIME = 29728 ;
variables:
	double TIME(TIME) ;
		TIME:long_name = "time" ;
		TIME:units = "days since 1950-01-01 00:00:00 UTC" ;
		TIME:calendar = "gregorian" ;
		TIME:axis = "T" ;
	float TEMP(TIME) ;
		TEMP:_FillValue = NaNf ;
		TEMP:units = "degrees_Celsius" ;
		TEMP:calibration_comment = "temperature 13/05/2024" ;
		TEMP:calibration_TA0 = -0.0001053478 ;
		TEMP:calibration_TA1 = 0.0003058548 ;
		TEMP:calibration_TA2 = -4.393614e-06 ;
		TEMP:calibration_TA3 = 1.997618e-07 ;

// global attributes:
		:instrument = "Sea-Bird Electronics ; SBE 39" ;
		:instrument_model = "SBE 39" ;
		:instrument_serial_number = "4165" ;
		:instrument_sample_interval = 300. ;
		:time_coverage_start = "2025-03-23T00:00:00Z" ;
		:time_coverage_end = "2025-07-04T05:15:00Z" ;
		:date_created = "2025-07-10T02:57:06Z" ;
		:history = "2025-07-10 created from file 4165.asc" ;
}
```
edit metadata file so the instrument_model and instrument_serial_number match
add any deployment metadata that needs to be added to each file
```
GLOBAL,,SOFS-14-2015,,,,,,,,voyage_deployment,str,http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=SS2010_V02
GLOBAL,,SOFS-14-2015,,,,,,,,voyage_recovery,str,http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=SS2011_V01
GLOBAL,,SOFS-14-2015,,,,,,,,comment_recovery,str,Broke mooring 2025-

VAR,LATITUDE,SOFS-14-2025,,,2025-03-28,2026-05-01,LATITUDE,"()","()",,float64,"-46.9729700000",None
VAR,LONGITUDE,SOFS-14-2025,,,2025-03-28,2026-05-01,LONGITUDE,"()","()",,float64,"141.3544400000",None

GLOBAL,,SOFS-14-2025,,,2025-03-28,2026-05-01,,"","",site_nominal_depth,float64,"4624.00",None
GLOBAL,,SOFS-14-2025,,,2025-03-28,2026-05-01,,"","",time_deployment_start,str,"2025-03-28T00:00:00Z",None
GLOBAL,,SOFS-14-2025,,,2025-03-28,2026-05-01,,"","",time_deployment_end,str,"2025-07-03T00:00:00Z",None
```
now the netCDF files can be processed to FV00 IMOS files
`> python -m ocean_dp.sots.process_SOTS_toIMOS *.cnv.nc`
the message
```
Traceback (most recent call last):
  File "<frozen runpy>", line 198, in _run_module_as_main
  File "<frozen runpy>", line 88, in _run_code
  File "/Users/pete/GitHub/imos-tools/ocean_dp/sots/process_SOTS_toIMOS.py", line 49, in <module>
    filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/Users/pete/GitHub/imos-tools/ocean_dp/attribution/add_geospatial_attributes.py", line 29, in add_spatial_attr
    var_lat = ds.variables["LATITUDE"]
              ~~~~~~~~~~~~^^^^^^^^^^^^
KeyError: 'LATITUDE'
```
means that the instrument_serial_number/instrument_model don't match a deployment.

output file example
> ncdump -h IMOS_DWM-SOTS_CPST_20250303_SOFS_FV00_SOFS-14-2025-SBE37SMP-RS232-03722650-30m_END-20250708_C-20250710.nc
```
netcdf IMOS_DWM-SOTS_CPST_20250303_SOFS_FV00_SOFS-14-2025-SBE37SMP-RS232-03722650-30m_END-20250708_C-20250710 {
dimensions:
	TIME = 31003 ;
variables:
	double TIME(TIME) ;
		TIME:long_name = "time" ;
		TIME:units = "days since 1950-01-01 00:00:00 UTC" ;
		TIME:calendar = "gregorian" ;
		TIME:axis = "T" ;
		TIME:standard_name = "time" ;
		TIME:valid_max = 90000. ;
		TIME:valid_min = 0. ;
	float TEMP(TIME) ;
		TEMP:_FillValue = NaNf ;
		TEMP:comment = "Temperature [ITS-90, deg C]" ;
		TEMP:units = "degrees_Celsius" ;
		TEMP:calibration_SerialNumber = "22650" ;
		TEMP:calibration_CalibrationDate = "01-Dec-22" ;
		TEMP:calibration_A0 = -5.170214e-05 ;
		TEMP:calibration_A1 = 0.0002880995 ;
		TEMP:calibration_A2 = -2.688056e-06 ;
		TEMP:calibration_A3 = 1.546172e-07 ;
		TEMP:calibration_Slope = 1. ;
		TEMP:calibration_Offset = 0. ;
			TEMP:name = "sea_water_temperature" ;
		TEMP:standard_name = "sea_water_temperature" ;
		TEMP:long_name = "sea_water_temperature" ;
		TEMP:valid_min = -2.5f ;
		TEMP:valid_max = 40.f ;
		TEMP:reference_scale = "ITS-90" ;
		TEMP:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
	float CNDC(TIME) ;
		CNDC:_FillValue = NaNf ;
		CNDC:comment = "Conductivity [S/m]" ;
		CNDC:units = "S/m" ;
		CNDC:calibration_SerialNumber = "22650" ;
		CNDC:calibration_CalibrationDate = "01-Dec-22" ;
		CNDC:calibration_UseG_J = 1. ;
		CNDC:calibration_G = -1.015773 ;
		CNDC:calibration_H = 0.145086 ;
		CNDC:calibration_I = -0.0004886 ;
		CNDC:calibration_J = 5.598298e-05 ;
		CNDC:calibration_CPcor = -9.57e-08 ;
		CNDC:calibration_CTcor = 3.25e-06 ;
		CNDC:calibration_WBOTC = 4.395265e-07 ;
		CNDC:calibration_Slope = 1. ;
		CNDC:calibration_Offset = 0. ;
		CNDC:name = "Water conductivity" ;
		CNDC:standard_name = "sea_water_electrical_conductivity" ;
		CNDC:valid_min = 0.f ;
		CNDC:valid_max = 40.f ;
		CNDC:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		CNDC:long_name = "sea_water_electrical_conductivity" ;
	float PRES(TIME) ;
		PRES:_FillValue = NaNf ;
		PRES:comment = "Pressure, Strain Gauge [db]" ;
		PRES:units = "dbar" ;
		PRES:calibration_SerialNumber = "61447" ;
		PRES:calibration_CalibrationDate = "28-Nov-22" ;
		PRES:calibration_PA0 = 0.3529475 ;
		PRES:calibration_PA1 = 0.0160399 ;
		PRES:calibration_PA2 = -6.703851e-10 ;
		PRES:calibration_PTEMPA0 = -74.19641 ;
		PRES:calibration_PTEMPA1 = 0.05213812 ;
		PRES:calibration_PTEMPA2 = -6.364063e-07 ;
		PRES:calibration_PTCA0 = 524321.2 ;
		PRES:calibration_PTCA1 = 3.120528 ;
		PRES:calibration_PTCA2 = -0.09151171 ;
		PRES:calibration_PTCB0 = 25.06692 ;
		PRES:calibration_PTCB1 = 0.0003768844 ;
		PRES:calibration_PTCB2 = 0. ;
		PRES:calibration_Offset = 0. ;
		PRES:applied_offset = -10.1353f ;
		PRES:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		PRES:long_name = "sea_water_pressure_due_to_sea_water" ;
		PRES:standard_name = "sea_water_pressure_due_to_sea_water" ;
		PRES:valid_max = 12000.f ;
		PRES:valid_min = -15.f ;
	float PSAL(TIME) ;
		PSAL:_FillValue = NaNf ;
		PSAL:comment = "Salinity, Practical [PSU]" ;
		PSAL:units = "1" ;
		PSAL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		PSAL:long_name = "sea_water_practical_salinity" ;
		PSAL:standard_name = "sea_water_practical_salinity" ;
		PSAL:valid_max = 41.f ;
		PSAL:valid_min = 2.f ;
	float SIGMA_T0(TIME) ;
		SIGMA_T0:_FillValue = NaNf ;
		SIGMA_T0:comment = "Density [sigma-theta, kg/m^3]" ;
		SIGMA_T0:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		SIGMA_T0:units = "kg/m^3" ;
		SIGMA_T0:long_name = "sea_water_sigma_theta" ;
		SIGMA_T0:standard_name = "sea_water_sigma_theta" ;
		SIGMA_T0:reference_pressure = "0 dbar" ;
		SIGMA_T0:valid_max = 200.f ;
		SIGMA_T0:valid_min = 0.f ;
	float OXSOL(TIME) ;
		OXSOL:_FillValue = NaNf ;
		OXSOL:comment = "Oxygen Saturation, Garcia & Gordon [umol/kg]" ;
		OXSOL:units = "umol/kg" ;
		OXSOL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		OXSOL:long_name = "moles_of_oxygen_per_unit_mass_in_sea_water_at_saturation" ;
		OXSOL:valid_max = 400.f ;
		OXSOL:valid_min = 0.f ;
	double LATITUDE ;
		LATITUDE:axis = "Y" ;
		LATITUDE:long_name = "latitude" ;
		LATITUDE:reference_datum = "WGS84 geographic coordinate system" ;
		LATITUDE:standard_name = "latitude" ;
		LATITUDE:units = "degrees_north" ;
		LATITUDE:valid_max = 90. ;
		LATITUDE:valid_min = -90. ;
	double LONGITUDE ;
		LONGITUDE:axis = "X" ;
		LONGITUDE:long_name = "longitude" ;
		LONGITUDE:reference_datum = "WGS84 geographic coordinate system" ;
		LONGITUDE:standard_name = "longitude" ;
		LONGITUDE:units = "degrees_east" ;
		LONGITUDE:valid_max = 180. ;
		LONGITUDE:valid_min = -180. ;
	double NOMINAL_DEPTH ;
		NOMINAL_DEPTH:axis = "Z" ;
		NOMINAL_DEPTH:long_name = "nominal depth" ;
		NOMINAL_DEPTH:positive = "down" ;
		NOMINAL_DEPTH:reference_datum = "sea surface" ;
		NOMINAL_DEPTH:standard_name = "depth" ;
		NOMINAL_DEPTH:units = "m" ;
		NOMINAL_DEPTH:valid_max = 12000. ;
		NOMINAL_DEPTH:valid_min = -5. ;

// global attributes:
		:abstract = "Oceanographic and meteorological data from the Southern Ocean Time Series observatory in the Southern Ocean southwest of Tasmania" ;
		:acknowledgement = "Any users of IMOS data are required to clearly acknowledge the source of the material derived from IMOS in the format: \"Data was sourced from the Integrated Marine Observing System (IMOS) - IMOS is a national collaborative research infrastructure, supported by the Australian Government.\" If relevant, also credit other organisations involved in collection of this particular datastream (as listed in \'credit\' in the metadata record)." ;
		:author = "Jansen, Peter" ;
		:author_email = "peter.jansen@csiro.au" ;
		:citation = "The citation in a list of references is: \'IMOS [year-of-data-download], [Title], [data-access-URL], accessed [date-of-access].\'." ;
		:comment = "Geospatial vertical min/max information has been filled using the NOMINAL_DEPTH." ;
		:comment_recovery = "Broke mooring 2025-05-19 10:00 UTC at 302 m depth, top recovered 2025-07-03 23:25 UTC" ;
		:Conventions = "CF-1.6,IMOS-1.4" ;
		:data_centre = "Australian Ocean Data Network (AODN)" ;
		:data_centre_email = "info@aodn.org.au" ;
		:date_created = "2025-07-10T05:41:34Z" ;
		:deployment_code = "SOFS-14-2025" ;
		:disclaimer = "Data, products and services from IMOS are provided \"as is\" without any warranty as to fitness for a particular purpose." ;
		:featureType = "timeSeries" ;
		:file_version = "Level 0 - Raw data" ;
		:file_version_quality_control = "Raw data is defined as unprocessed data and data products that have not undergone quality control. The data may be in engineering physical units, time and location details can be in relative units and values can be pre-calibration measurements." ;
		:geospatial_lat_max = -46.97297 ;
		:geospatial_lat_min = -46.97297 ;
		:geospatial_lat_units = "degrees_north" ;
		:geospatial_lon_max = 141.35444 ;
		:geospatial_lon_min = 141.35444 ;
		:geospatial_lon_units = "degrees_east" ;
		:geospatial_vertical_max = 30. ;
		:geospatial_vertical_min = 30. ;
		:geospatial_vertical_positive = "down" ;
		:history = "2025-07-10 created from file SBE37SMP-RS232_03722650_2025_07_08.cnv\n",
			"2025-07-10 attributes added from file(s) [pulse-saz-sofs-flux.metadata.csv, imos.metadata.csv, sots.metadata.csv, asimet.metadata.csv, variable.metadata.csv]" ;
		:institution = "DWM-SOTS" ;
		:institution_references = "http://www.imos.org.au/aodn.html" ;
		:instrument = "Sea-Bird Electronics ; SBE37SMP-RS232" ;
		:instrument_model = "SBE37SMP-RS232" ;
		:instrument_nominal_depth = 30. ;
		:instrument_serial_number = "03722650" ;
		:keywords_vocabulary = "IMOS parameter names. See https://github.com/aodn/imos-toolbox/blob/master/IMOS/imosParameters.txt" ;
		:license = "http://creativecommons.org/licenses/by/4.0/" ;
		:naming_authority = "IMOS" ;
		:platform_code = "SOFS" ;
		:principal_investigator = "Shadwick, Elizabeth; Shulz, Eric" ;
		:principal_investigator_email = "elizabeth.shadwick@csiro.au" ;
		:project = "Integrated Marine Observing System (IMOS)" ;
		:references = "http://www.imos.org.au" ;
		:sea_bird_data_conversion_01_datcnv_date = "Jul 08 2025 14:39:45, 7.26.7.129 [datcnv_vars = 7]" ;
		:sea_bird_data_conversion_02_datcnv_in = "C:\\Users\\jan079\\OneDrive - CSIRO\\SOTS\\2025-processing\\SBE37SMP-RS232_03722650_2025_07_08.hex C:\\Users\\jan079\\OneDrive - CSIRO\\SOTS\\2025-processing\\SBE37SMP-RS232_03722650_2025_07_08.XMLCON" ;
		:sea_bird_data_conversion_03_datcnv_skipover = "0" ;
		:sea_bird_firmware_date = "Oct 23 2020 11:20:51" ;
		:sea_bird_firmware_version = "6.3.2" ;
		:sea_bird_manufacture_date = "28Nov2022" ;
		:site_code = "SOTS" ;
		:site_nominal_depth = 4624. ;
		:standard_name_vocabulary = "NetCDF Climate and Forecast (CF) Metadata Convention Standard Name Table 67" ;
		:time_coverage_end = "2025-07-08T04:05:01Z" ;
		:time_coverage_start = "2025-03-03T00:00:01Z" ;
		:time_deployment_end = "2025-07-03T00:00:00Z" ;
		:time_deployment_start = "2025-03-28T00:00:00Z" ;
		:title = "Oceanographic mooring data deployment of SOFS at latitude -47.0 longitude 141.4 depth  30 (m) instrument Sea-Bird Electronics ; SBE37SMP-RS232 serial 03722650" ;
		:voyage_deployment = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=IN2025_V02" ;
		:voyage_recovery = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=IN2025_V07" ;
		:wmo_platform_code = "58450" ;
}
```
