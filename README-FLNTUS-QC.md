# FLNTUS chlorophyll and backscatter
FLNTUS instrument data processing and QC

## processing RAW files

	python3 ocean_dp/parse/eco_raw2netCDF.py --dev RAW/FLNTUS-1172_NTU.dev RAW/FLNTUS-1172.raw

DEV file example
```
ECO     FLNTUS-1172     
Created on:     10/10/18        
                
COLUMNS=7               
N/U=1           
N/U=2           
N/U=3           
Chl=4   0.0071  49
N/U=5           
NTU=6   0.0024  50
N/U=7           
```

Makes file RAW/FLNTUS-1172.raw.nc

## add SOTS addAttributes

	python3 sots/process_SOTS_toIMOS.py RAW/FLNTUS-1172.raw.nc


```
GLOBAL,,,FLNTUS,1172,2019-03-18,2020-08-01,,"","",deployment_code,str,"SOFS-8-2019"
GLOBAL,,,FLNTUS,1215,2019-03-18,2020-08-01,,"","",deployment_code,str,"SOFS-8-2019"

VAR,NOMINAL_DEPTH,SOFS-8-2019,FLNTUS,1172,2019-03-18,2020-08-01,NOMINAL_DEPTH,"()","()",,float64,"1.00"
VAR,NOMINAL_DEPTH,SOFS-8-2019,FLNTUS,1215,2019-03-18,2020-08-01,NOMINAL_DEPTH,"()","()",,float64,"30.00"

VAR_ATT,CPHL,,,,,,,,,units,str,ug/l
VAR_ATT,CPHL,,,,,,,,,coordinates,str,TIME LATITUDE LONGITUDE NOMINAL_DEPTH
VAR_ATT,CPHL,,,,,,,,,standard_name,str,mass_concentration_of_chlorophyll_in_sea_water
VAR_ATT,CPHL,,,,,,,,,long_name,str,mass_concentration_of_inferred_chlorophyll_from_relative_fluorescence_units_in_sea_water
VAR_ATT,CPHL,,,,,,,,,valid_max,float32,40
VAR_ATT,CPHL,,,,,,,,,valid_min,float32,-1

VAR_ATT,TURB,,,,,,,,,coordinates,str,TIME LATITUDE LONGITUDE NOMINAL_DEPTH
VAR_ATT,TURB,,,,,,,,,standard_name,str,sea_water_turbidity
VAR_ATT,TURB,,,,,,,,,long_name,str,sea_water_turbidity
VAR_ATT,TURB,,,,,,,,,units,str,1
VAR_ATT,TURB,,,,,,,,,valid_max,float32,50
VAR_ATT,TURB,,,,,,,,,valid_min,float32,-1

VAR_ATT,ECO_FLNTUS_CHL,,,,,,,,,name,str,Eco-FLNTUS CHL counts
VAR_ATT,ECO_FLNTUS_CHL,,,,,,,,,long_name,str,wetlabs_flntus_chl_counts
VAR_ATT,ECO_FLNTUS_CHL,,,,,,,,,valid_min,str,0.
VAR_ATT,ECO_FLNTUS_CHL,,,,,,,,,valid_max,str,4130.
VAR_ATT,ECO_FLNTUS_CHL,,,,,,,,,coordinates,str,TIME DEPTH LATITUDE LONGITUDE

VAR_ATT,ECO_FLNTUS_TURB,,,,,,,,,name,str,Eco-FLNTUS Turbidity counts
VAR_ATT,ECO_FLNTUS_TURB,,,,,,,,,long_name,str,wetlabs_flntus_turb_counts
VAR_ATT,ECO_FLNTUS_TURB,,,,,,,,,valid_min,str,0.
VAR_ATT,ECO_FLNTUS_TURB,,,,,,,,,valid_max,str,4130.
VAR_ATT,ECO_FLNTUS_TURB,,,,,,,,,coordinates,str,TIME DEPTH LATITUDE LONGITUDE

-- SOFS-8 sensors time/date was set in dd/mm/yy format where as the instrument is mm/dd/yy format, which made a 145 day error

VAR_ATT,TIME,SOFS-8-2019,FLNTUS,1172,2019-03-18,2020-08-01,,,,comment_scale_offset,str,1.0 -145
VAR_ATT,TIME,SOFS-8-2019,FLNTUS,1215,2019-03-18,2020-08-01,,,,comment_scale_offset,str,1.0 -145
```
## Add temperature, salinity to file
	python3 ~/DWM/git/imos-tools/ocean_dp/processing/merge_resample.py RAW/IMOS_DWM-SOTS_R_20190207_SOFS_FV00_SOFS-8-2019-FLNTUS-1215-30m_END-20200922_C-20210916.nc RAW/IMOS_DWM-SOTS_COPST_20190123_SOFS_FV01_SOFS-8-2019-SBE37SMP-ODO-RS232-03720126-30m_END-20200909_C-20210811.nc

## QC data

using the matlab routines at 

run 
	QC_sens_read
	QC_sens_routine2
	QC_sens_routine3
	outputNetCDF_FLNTUSdata

rename the file with a IMOS file name

	python3 ocean_dp/file_name/imosNetCDFfileName.py SOFS_8_2019-1172-FLNTUSdata.nc

```
netcdf IMOS_DWM-SOTS_BSTU_20190207_SOFS_FV01_SOFS-8-2019-FLNTUS-1172-1m_END-20201002_C-20210916 {
dimensions:
	TIME = 11347 ;
variables:
	double TIME(TIME) ;
		TIME:standard_name = "time" ;
		TIME:long_name = "time of measurement" ;
		TIME:units = "days since 1950-01-01T00:00:00 UTC" ;
		TIME:axis = "T" ;
		TIME:valid_min = 10957. ;
		TIME:valid_max = 54787. ;
		TIME:calendar = "gregorian" ;
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
	float CPHL(TIME) ;
		CPHL:standard_name = "mass_concentration_of_chlorophyll_in_sea_water" ;
		CPHL:long_name = "mass_concentration_of_inferred_chlorophyll_from_relative_fluorescence_units_in_sea_water" ;
		CPHL:units = "ug/L" ;
		CPHL:valid_min = -1.f ;
		CPHL:valid_max = 40.f ;
		CPHL:ancillary_variables = "CPHL_quality_control" ;
		CPHL:comment_equation = "chl-a (mg/m-3) = scale_factor * (count - dark_count)" ;
		CPHL:comment_qc_gross_range = "data < 0 or data > 2000" ;
		CPHL:comment_qc_spike_test = "window_size_for_running_median = 25 hours; abs(single_data_point - running_median) > 3 * stddev(whole_time_series)" ;
		CPHL:comment_qc_climatology = "chl-a < 0 or chl-a > 10 mg/m-3" ;
		CPHL:comment_qc_flat_line = "window_size = 24 hours; abs(diff(data in window)) == 0" ;
		CPHL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
	byte CPHL_quality_control(TIME) ;
		CPHL_quality_control:standard_name = "mass_concentration_of_chlorophyll_in_sea_water status_flag" ;
		CPHL_quality_control:long_name = "quality flag for CPHL" ;
		CPHL_quality_control:quality_control_conventions = "IMOS standard flags" ;
		CPHL_quality_control:valid_min = 0b ;
		CPHL_quality_control:valid_max = 9b ;
		CPHL_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		CPHL_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
	float BB(TIME) ;
		BB:long_name = "optical backscatter" ;
		BB:units = "mm-1" ;
		BB:valid_min = 0.f ;
		BB:valid_max = 1.f ;
		BB:ancillary_variables = "BB_quality_control" ;
		BB:comment_equation_1 = "turb (NTU)= scale factor*(counts-dark counts)" ;
		BB:comment_equation_2 = "B (m-1 sr-1)=turb (NTU)*0.002727 ... wetlabs" ;
		BB:comment_equation_3 = "bbp (m-1) = 2 * pi * Xp * (B - Bsw) ... Zhang et al. (2009)" ;
		BB:comment_equation_4 = "where Xp angle taken as 142 deg, 1.17 ... Sullivan and Twardowski, 2009" ;
		BB:comment_qc_climatology = "BB < 0 or BB > 0.01 m-1" ;
		BB:comment_qc_flat_line = "window_size = 24 hours; abs(diff(data in window)) == 0" ;
		BB:comment_qc_param_flat_line = "abs(diff(all_data_in_burst)) == 0" ;
		BB:comment_qc_spike_test = "window_size_for_running_median = 25 hours; abs(single_data_point - running_median) > 3 * stddev(whole_time_series)" ;
		BB:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
	byte BB_quality_control(TIME) ;
		BB_quality_control:long_name = "quality flag for BB" ;
		BB_quality_control:quality_control_conventions = "IMOS standard flags" ;
		BB_quality_control:valid_min = 0b ;
		BB_quality_control:valid_max = 9b ;
		BB_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		BB_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
	float ECO_FLNTUS_CPHL(TIME) ;
		ECO_FLNTUS_CPHL:units = "1" ;
		ECO_FLNTUS_CPHL:CH_DIGITAL_DARK_COUNT = 49. ;
		ECO_FLNTUS_CPHL:CH_DIGITAL_SCALE_FACTOR = 0.0071 ;
		ECO_FLNTUS_CPHL:name = "Eco-FLNTUS CHL counts" ;
		ECO_FLNTUS_CPHL:long_name = "wetlabs_flntus_chl_counts" ;
		ECO_FLNTUS_CPHL:valid_min = 0.f ;
		ECO_FLNTUS_CPHL:valid_max = 4130.f ;
		ECO_FLNTUS_CPHL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
	float ECO_FLNTUS_TURB(TIME) ;
		ECO_FLNTUS_TURB:units = "1" ;
		ECO_FLNTUS_TURB:TURB_DIGITAL_DARK_COUNT = 50. ;
		ECO_FLNTUS_TURB:TURB_DIGITAL_SCALE_FACTOR = 0.0024 ;
		ECO_FLNTUS_TURB:name = "Eco-FLNTUS Turbidity counts" ;
		ECO_FLNTUS_TURB:long_name = "wetlabs_flntus_turb_counts" ;
		ECO_FLNTUS_TURB:valid_min = 0.f ;
		ECO_FLNTUS_TURB:valid_max = 4130.f ;
		ECO_FLNTUS_TURB:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
	float TEMP(TIME) ;
		TEMP:sensor_model = "SBE37SM-RS485" ;
		TEMP:sensor_serial_number = "03715728" ;
		TEMP:comment = "Temperature [ITS-90, deg C]" ;
		TEMP:units = "degrees_Celsius" ;
		TEMP:calibration_SerialNumber = "15728" ;
		TEMP:calibration_CalibrationDate = "30-Jun-18" ;
		TEMP:calibration_A0 = -9.807067e-05 ;
		TEMP:calibration_A1 = 0.0002985251 ;
		TEMP:calibration_A2 = -3.569583e-06 ;
		TEMP:calibration_A3 = 1.777242e-07 ;
		TEMP:calibration_Slope = 1. ;
		TEMP:calibration_Offset = 0. ;
		TEMP:name = "sea_water_temperature" ;
		TEMP:standard_name = "sea_water_temperature" ;
		TEMP:long_name = "sea_water_temperature" ;
		TEMP:valid_min = -2.5f ;
		TEMP:valid_max = 40.f ;
		TEMP:reference_scale = "ITS-90" ;
		TEMP:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
	float PSAL(TIME) ;
		PSAL:sensor_model = "SBE37SM-RS485" ;
		PSAL:sensor_serial_number = "03715728" ;
		PSAL:comment = "Salinity, Practical [PSU], moored pressure = 1.00000e+000 [dbar]" ;
		PSAL:units = "1" ;
		PSAL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		PSAL:long_name = "sea_water_practical_salinity" ;
		PSAL:standard_name = "sea_water_practical_salinity" ;
		PSAL:valid_max = 41.f ;
		PSAL:valid_min = 2.f ;

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
		:date_created = "2021-09-16T06:00:48Z" ;
		:deployment_code = "SOFS-8-2019" ;
		:disclaimer = "Data, products and services from IMOS are provided \"as is\" without any warranty as to fitness for a particular purpose." ;
		:featureType = "timeSeries" ;
		:geospatial_lat_max = -46.89335 ;
		:geospatial_lat_min = -46.89335 ;
		:geospatial_lat_units = "degrees_north" ;
		:geospatial_lon_max = 142.34464 ;
		:geospatial_lon_min = 142.34464 ;
		:geospatial_lon_units = "degrees_east" ;
		:geospatial_vertical_max = 1. ;
		:geospatial_vertical_min = 1. ;
		:geospatial_vertical_positive = "down" ;
		:institution = "DWM-SOTS" ;
		:institution_references = "http://www.imos.org.au/aodn.html" ;
		:instrument = "WetLABs ; FLNTUS" ;
		:instrument_calibration_date = "10/10/18" ;
		:instrument_model = "FLNTUS" ;
		:instrument_nominal_depth = 1. ;
		:instrument_serial_number = "1172" ;
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
		:time_coverage_end = "2020-10-02T03:49:24Z" ;
		:time_coverage_start = "2019-02-07T05:39:16Z" ;
		:time_deployment_end = "2020-09-04T22:18:23Z" ;
		:time_deployment_start = "2019-03-18T10:00:00Z" ;
		:title = "Oceanographic mooring data deployment of SOFS at latitude -46.9 longitude 142.3 depth   1 (m) instrument WetLABs ; FLNTUS serial 1172" ;
		:history = "2021-09-16 created from file FLNTUS/SOFS-8/FLNTUS-1172.raw\n",
			"2021-09-16 : attributes added from file(s) [metadata/pulse-saz-sofs-flux.metadata.csv, metadata/imos.metadata.csv, metadata/sots.metadata.csv, metadata/sofs.metadata.csv, metadata/asimet.metadata.csv, metadata/variable.metadata.csv]\n",
			"2021-09-16 : attributes added from file(s) [metadata/pulse-saz-sofs-flux-timeoffset.metadata.csv]\n",
			"2021-09-16 : scale, offset variable TIME\n",
			"2021-09-16 added data from IMOS_DWM-SOTS_CST_20190123_SOFS_FV01_SOFS-8-2019-SBE37SM-RS485-03715728-1m_END-20200918_C-20210811.nc interpolated to this time\n",
			"2021-09-16 data QC" ;
		:file_version = "Level 1 - Quality Controlled data" ;
		:file_version_quality_control = "Quality controlled data have been through quality assurance procedures such as automated routines and sensor calibration and/or a level of visual inspection and flag of obvious errors. The data are in physical units using standard SI metric units with calibration and other pre- processing routines applied, all time and location values are in absolute coordinates to comply with standards and datum. Data includes flags for each measurement to indicate the estimated quality of the measurement. Metadata exists for the data or for the higher level dataset that the data belongs to. This is the standard IMOS data level and is what should be made available to AODN and to the IMOS community." ;
}

```
