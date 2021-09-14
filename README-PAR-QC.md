# PAR
PAR data processing and QC

## processing RAW files

	python3 parse/eco_raw2netCDF.py --dev RAW/PARSB-439.dev RAW/PARSB-439.RAW      

DEV file example
```
ECO     PARSB-439       
Created on:     09/05/14        

COLUMNS=3               
DATE=1          
TIME=2          
PAR=3           
im=     1.3589  
a1=     2922    
a0=     4289    
```

Makes file RAW/PARSB-439.RAW.nc

## add SOTS addAttributes

	python3 sots/process_SOTS_toIMOS.py RAW/PARSB-439.RAW.nc


```
VAR_ATT,PAR,,,,,,,,,coordinates,str,TIME LATITUDE LONGITUDE NOMINAL_DEPTH
VAR_ATT,PAR,,,,,,,,,valid_max,float32,10000
VAR_ATT,PAR,,,,,,,,,valid_min,float32,-2
VAR_ATT,PAR,,,,,,,,,units,str,umol/m^2/s
VAR_ATT,PAR,,,,,,,,,standard_name,str,surface_downwelling_photosynthetic_photon_flux_in_air
VAR_ATT,PAR,,,,,,,,,long_name,str,surface_downwelling_photosynthetic_photon_flux_in_air

VAR_ATT,PAR,,,,,,,,,sensor_SeaVoX_L22_code,str,SDN:L22::TOOL0193
VAR_ATT,PAR,,,,,,,,,comment_sensor_type,str,cosine sensor

VAR,NOMINAL_DEPTH,SOFS-8-2019,PARSB,439,2019-03-18,2020-08-01,NOMINAL_DEPTH,"()","()",,float64,"30.00"
GLOBAL,,,PARSB,439,2019-03-18,2020-08-01,,"","",deployment_code,str,"SOFS-8-2019"
```
Also possible to add calibration constants
```
VAR_ATT,PAR_COUNT,SOFS-7-2018,ECO-PARS,unknown,,,,,,comment_attached_instrument,str,Wet-LABS ; PARS-419
VAR_ATT,PAR_COUNT,SOFS-7-2018,ECO-PARS,unknown,,,,,,calibration_PAR_Im,float64,1.3589
VAR_ATT,PAR_COUNT,SOFS-7-2018,ECO-PARS,unknown,,,,,,calibration_PAR_digital_A1,float64,2923
VAR_ATT,PAR_COUNT,SOFS-7-2018,ECO-PARS,unknown,,,,,,calibration_PAR_digital_A0,float64,4266
VAR_ATT,PAR_COUNT,SOFS-7-2018,ECO-PARS,unknown,,,,,,calibration_PAR_date,str,9/5/2014
VAR_ATT,PAR_COUNT,SOFS-7-2018,ECO-PARS,unknown,,,,,,calibration_PAR_facility,str,Satlantic
VAR_ATT,PAR_COUNT,SOFS-7-2018,ECO-PARS,unknown,,,,,,calibration_PAR_SN,str,PARSB-419
```

## QC data

	python3 sots/processPAR.py RAW/IMOS_DWM-SOTS_F_20190115_SOFS_FV00_SOFS-8-2019-PARSB-439-30m_END-20201001_C-20210914.nc

*nix

netcdf IMOS_DWM-SOTS_F_20190115_SOFS_FV01_SOFS-8-2019-PARSB-439-30m_END-20201001_C-20210914 {
dimensions:
	TIME = 68205 ;
variables:
	double TIME(TIME) ;
		TIME:long_name = "time" ;
		TIME:units = "days since 1950-01-01 00:00:00 UTC" ;
		TIME:calendar = "gregorian" ;
		TIME:axis = "T" ;
		TIME:standard_name = "time" ;
		TIME:valid_max = 90000. ;
		TIME:valid_min = 0. ;
	float PAR(TIME) ;
		PAR:calibration_Im = 1.3589 ;
		PAR:calibration_a1 = 2922. ;
		PAR:calibration_a0 = 4289. ;
		PAR:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		PAR:valid_max = 10000.f ;
		PAR:valid_min = -2.f ;
		PAR:units = "umol/m^2/s" ;
		PAR:standard_name = "surface_downwelling_photosynthetic_photon_flux_in_air" ;
		PAR:long_name = "surface_downwelling_photosynthetic_photon_flux_in_air" ;
		PAR:sensor_SeaVoX_L22_code = "SDN:L22::TOOL0193" ;
		PAR:comment_sensor_type = "cosine sensor" ;
		PAR:ancillary_variables = "PAR_quality_control PAR_quality_control_loc PAR_quality_control_gr PAR_quality_control_cl" ;
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
	byte PAR_quality_control(TIME) ;
		PAR_quality_control:_FillValue = 99b ;
		PAR_quality_control:long_name = "quality flag for surface_downwelling_photosynthetic_photon_flux_in_air" ;
		PAR_quality_control:standard_name = "surface_downwelling_photosynthetic_photon_flux_in_air status_flag" ;
		PAR_quality_control:quality_control_conventions = "IMOS standard flags" ;
		PAR_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		PAR_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		PAR_quality_control:comment = "maximum of all flags" ;
	float ALT(TIME) ;
		ALT:_FillValue = NaNf ;
		ALT:units = "degree" ;
		ALT:long_name = "sun_altitude" ;
		ALT:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		ALT:comment = "using http://docs.pysolar.org/en/latest/ v0.8 get_altitude" ;
	float SOLAR(TIME) ;
		SOLAR:_FillValue = NaNf ;
		SOLAR:units = "W/m2" ;
		SOLAR:long_name = "incoming_solar_radiation" ;
		SOLAR:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		SOLAR:comment = "using http://docs.pysolar.org/en/latest/ v0.8 extraterrestrial_irrad() with incoming = 1361 W/m^2" ;
	float ePAR(TIME) ;
		ePAR:_FillValue = NaNf ;
		ePAR:units = "umol/m^2/s" ;
		ePAR:long_name = "incoming_solar_radiation converted to PAR (x2.114) attenuated by depth" ;
		ePAR:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		ePAR:comment = "using http://docs.pysolar.org/en/latest/ v0.8 extraterrestrial_irrad() with incoming = 1361 W/m^2, x 2.114, kd = 0.04" ;
	byte PAR_quality_control_loc(TIME) ;
		PAR_quality_control_loc:_FillValue = 99b ;
		PAR_quality_control_loc:long_name = "in/out of water flag for surface_downwelling_photosynthetic_photon_flux_in_air" ;
		PAR_quality_control_loc:units = "1" ;
		PAR_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte PAR_quality_control_gr(TIME) ;
		PAR_quality_control_gr:_FillValue = 99b ;
		PAR_quality_control_gr:long_name = "global_range flag for surface_downwelling_photosynthetic_photon_flux_in_air" ;
		PAR_quality_control_gr:units = "1" ;
		PAR_quality_control_gr:comment = "Test 4. gross range test" ;
	byte PAR_quality_control_cl(TIME) ;
		PAR_quality_control_cl:_FillValue = 99b ;
		PAR_quality_control_cl:long_name = "climate flag for surface_downwelling_photosynthetic_photon_flux_in_air" ;
		PAR_quality_control_cl:units = "1" ;
		PAR_quality_control_cl:comment = "Test 7. climatology test" ;
		PAR_quality_control_cl:comment_note = "cosine sensor, par < (2 * SOLAR) + 15" ;

// global attributes:
		:abstract = "Oceanographic and meteorological data from the Southern Ocean Time Series observatory in the Southern Ocean southwest of Tasmania" ;
		:acknowledgement = "Any users of IMOS data are required to clearly acknowledge the source of the material derived from IMOS in the format: \"Data was sourced from the Integrated Marine Observing System (IMOS) - IMOS is a national collaborative research infrastructure, supported by the Australian Government.\" If relevant, also credit other organisations involved in collection of this particular datastream (as listed in \'credit\' in the metadata record)." ;
		:author = "Jansen, Peter" ;
		:author_email = "peter.jansen@csiro.au" ;
		:citation = "The citation in a list of references is: \'IMOS [year-of-data-download], [Title], [data-access-URL], accessed [date-of-access]\'." ;
		:comment = "Geospatial vertical min/max information has been filled using the NOMINAL_DEPTH." ;
		:Conventions = "CF-1.6,IMOS-1.4" ;
		:data_centre = "Australian Ocean Data Network (AODN)" ;
		:data_centre_email = "info@aodn.org.au" ;
		:date_created = "2021-09-14T09:58:35Z" ;
		:deployment_code = "SOFS-8-2019" ;
		:disclaimer = "Data, products and services from IMOS are provided \"as is\" without any warranty as to fitness for a particular purpose." ;
		:featureType = "timeSeries" ;
		:file_version_quality_control = "Raw data is defined as unprocessed data and data products that have not undergone quality control. The data may be in engineering physical units, time and location details can be in relative units and values can be pre-calibration measurements." ;
		:geospatial_lat_max = -46.89335 ;
		:geospatial_lat_min = -46.89335 ;
		:geospatial_lat_units = "degrees_north" ;
		:geospatial_lon_max = 142.34464 ;
		:geospatial_lon_min = 142.34464 ;
		:geospatial_lon_units = "degrees_east" ;
		:geospatial_vertical_max = 30. ;
		:geospatial_vertical_min = 30. ;
		:geospatial_vertical_positive = "down" ;
		:institution = "DWM-SOTS" ;
		:institution_references = "http://www.imos.org.au/aodn.html" ;
		:instrument = "WetLABs ; PARSB" ;
		:instrument_calibration_date = "09/05/14" ;
		:instrument_model = "PARSB" ;
		:instrument_nominal_depth = 30. ;
		:instrument_serial_number = "439" ;
		:keywords_vocabulary = "IMOS parameter names. See https://github.com/aodn/imos-toolbox/blob/master/IMOS/imosParameters.txt" ;
		:license = "http://creativecommons.org/licenses/by/4.0/" ;
		:naming_authority = "IMOS" ;
		:platform_code = "SOFS" ;
		:principal_investigator = "Trull, Tom; Shulz, Eric; Shadwick, Elizabeth" ;
		:principal_investigator_email = "tom.trull@csiro.au; eshulz@bom.gov.au; elizabeth.shadwick@csiro.au" ;
		:project = "Integrated Marine Observing System (IMOS)" ;
		:references = "http://www.imos.org.au" ;
		:site_code = "SOTS" ;
		:site_nominal_depth = 4624. ;
		:standard_name_vocabulary = "NetCDF Climate and Forecast (CF) Metadata Convention Standard Name Table 67" ;
		:time_coverage_end = "2020-10-01T00:47:02Z" ;
		:time_coverage_start = "2019-01-15T22:00:38Z" ;
		:time_deployment_end = "2020-09-04T22:18:23Z" ;
		:time_deployment_start = "2019-03-18T10:00:00Z" ;
		:title = "Oceanographic mooring data deployment of SOFS at latitude -46.9 longitude 142.3 depth  30 (m) instrument WetLABs ; PARSB serial 439" ;
		:file_version = "Level 1 - Quality Controlled Data" ;
		:history = "2021-09-14 created from file RAW/PARSB-439.RAW\n2021-09-14 : attributes added from file(s) [metadata/pulse-saz-sofs-flux.metadata.csv, metadata/imos.metadata.csv, metadata/sots.metadata.csv, metadata/sofs.metadata.csv, metadata/asimet.metadata.csv, metadata/variable.metadata.csv]\n2021-09-14 : quality_control variables added.\n2021-09-14 : added incoming radiation\n2021-09-14 : in/out marked 3840\n2021-09-14 PAR global range min = -1.7 max = 10000 marked 0.0\n2021-09-14 PAR global range min = -1.7 max = 4500 marked 0.0\n2021-09-14 PAR climate range, marked 0" ;
}
