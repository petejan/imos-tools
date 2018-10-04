# imos-tools
tools for processing imos data

##retreive list of file from imos thredds server

python python\catalog.py ABOS/DA/EAC2000 > EAC-2000\EAC-2000-url.txt

##download all files in windows

for /f "delims=" %u in (EAC-2000\EAC-2000-url.txt) do curl -OLs "%u"

*nix

xargs -n 1 curl -Os

##create a combined netCDF file, this creates a file with all input variable in it (TEMP, PSAL, DEPTH, PRES_REL)

python python/copyDataset.py IMOS_ABOS-DA_STZ_20150515T000001Z_EAC2000_FV01_EAC2000-2016-SBE37SMP-140_END-20161110T221930Z_C-20170703T055824Z.nc IMOS_ABOS-DA_STZ_20150515T000001Z_EAC2000_FV01_EAC2000-2016-SBE37SMP-205_END-20161110T224850Z_C-20170703T055825Z.nc 

###to just select TEMP in the output, copies TEMP and ancillary variables along with LATITUDE, LONGITUDE, TIME, NOMINAL_DEPTH, DEPTH

python python/copyDataset.py -v TEMP IMOS_ABOS-DA_STZ_20150515T000001Z_EAC2000_FV01_EAC2000-2016-SBE37SMP-140_END-20161110T221930Z_C-20170703T055824Z.nc IMOS_ABOS-DA_STZ_20150515T000001Z_EAC2000_FV01_EAC2000-2016-SBE37SMP-205_END-20161110T224850Z_C-20170703T055825Z.nc 

###output file:

	dimensions:
		OBS = 157233 ;
		instrument = 2 ;
		strlen = 256 ;
	variables:
		double TIME(OBS) ;
			TIME:axis = "T" ;
			TIME:calendar = "gregorian" ;
			TIME:comment = "csiroManualQC adjusted time for a linear drift of 31 seconds." ;
			TIME:long_name = "time" ;
			TIME:standard_name = "time" ;
			TIME:units = "days since 1950-01-01 00:00:00 UTC" ;
			TIME:valid_max = 90000. ;
			TIME:valid_min = 0. ;
		ubyte instrument_index(OBS) ;
			instrument_index:long_name = "which instrument this obs is for" ;
			instrument_index:instance_dimension = "instrument" ;
		char source_file(instrument, strlen) ;
			source_file:long_name = "source file for this instrument" ;
		byte PSAL_quality_control(OBS) ;
			PSAL_quality_control:_FillValue = 99b ;
			PSAL_quality_control:flag_meanings = "No_QC_performed Good_data Probably_good_data Bad_data_that_are_potentially_correctable Bad_data Value_changed Not_used Not_used Not_used Missing_value" ;
			PSAL_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 5b, 6b, 7b, 8b, 9b ;
			PSAL_quality_control:long_name = "quality flag for sea_water_practical_salinity" ;
			PSAL_quality_control:quality_control_conventions = "IMOS standard flags" ;
			PSAL_quality_control:quality_control_global = "B" ;
			PSAL_quality_control:quality_control_global_conventions = "Argo reference table 2a (see http://www.cmar.csiro.au/argo/dmqc/user_doc/QC_flags.html), applied on data in position only (between global attributes time_deployment_start and time_deployment_end)" ;
			PSAL_quality_control:standard_name = "sea_water_practical_salinity status_flag" ;
		byte TEMP_quality_control(OBS) ;
			TEMP_quality_control:_FillValue = 99b ;
			TEMP_quality_control:flag_meanings = "No_QC_performed Good_data Probably_good_data Bad_data_that_are_potentially_correctable Bad_data Value_changed Not_used Not_used Not_used Missing_value" ;
			TEMP_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 5b, 6b, 7b, 8b, 9b ;
			TEMP_quality_control:long_name = "quality flag for sea_water_temperature" ;
			TEMP_quality_control:quality_control_conventions = "IMOS standard flags" ;
			TEMP_quality_control:quality_control_global = "B" ;
			TEMP_quality_control:quality_control_global_conventions = "Argo reference table 2a (see http://www.cmar.csiro.au/argo/dmqc/user_doc/QC_flags.html), applied on data in position only (between global attributes time_deployment_start and time_deployment_end)" ;
			TEMP_quality_control:standard_name = "sea_water_temperature status_flag" ;
		byte PRES_REL_quality_control(OBS) ;
			PRES_REL_quality_control:_FillValue = 99b ;
			PRES_REL_quality_control:flag_meanings = "No_QC_performed Good_data Probably_good_data Bad_data_that_are_potentially_correctable Bad_data Value_changed Not_used Not_used Not_used Missing_value" ;
			PRES_REL_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 5b, 6b, 7b, 8b, 9b ;
			PRES_REL_quality_control:long_name = "quality flag for sea_water_pressure_due_to_sea_water" ;
			PRES_REL_quality_control:quality_control_conventions = "IMOS standard flags" ;
			PRES_REL_quality_control:quality_control_global = "B" ;
			PRES_REL_quality_control:quality_control_global_conventions = "Argo reference table 2a (see http://www.cmar.csiro.au/argo/dmqc/user_doc/QC_flags.html), applied on data in position only (between global attributes time_deployment_start and time_deployment_end)" ;
			PRES_REL_quality_control:standard_name = "sea_water_pressure_due_to_sea_water status_flag" ;
		float NOMINAL_DEPTH(instrument) ;
			NOMINAL_DEPTH:axis = "Z" ;
			NOMINAL_DEPTH:long_name = "instrument nominal depth" ;
			NOMINAL_DEPTH:positive = "down" ;
			NOMINAL_DEPTH:reference_datum = "sea surface" ;
			NOMINAL_DEPTH:standard_name = "depth" ;
			NOMINAL_DEPTH:units = "m" ;
			NOMINAL_DEPTH:valid_max = 12000.f ;
			NOMINAL_DEPTH:valid_min = -5.f ;
		double LONGITUDE(instrument) ;
			LONGITUDE:axis = "X" ;
			LONGITUDE:long_name = "longitude" ;
			LONGITUDE:reference_datum = "WGS84 coordinate reference system" ;
			LONGITUDE:standard_name = "longitude" ;
			LONGITUDE:units = "degrees_east" ;
			LONGITUDE:valid_max = 180. ;
			LONGITUDE:valid_min = -180. ;
		float TEMP(OBS) ;
			TEMP:ancillary_variables = "TEMP_quality_control" ;
			TEMP:coordinates = "TIME LATITUDE LONGITUDE DEPTH NOMINAL_DEPTH" ;
			TEMP:_FillValue = 999999.f ;
			TEMP:long_name = "sea_water_temperature" ;
			TEMP:standard_name = "sea_water_temperature" ;
			TEMP:units = "degrees_Celsius" ;
			TEMP:valid_max = 40.f ;
			TEMP:valid_min = -2.5f ;
		float PSAL(OBS) ;
			PSAL:ancillary_variables = "PSAL_quality_control" ;
			PSAL:coordinates = "TIME LATITUDE LONGITUDE DEPTH NOMINAL_DEPTH" ;
			PSAL:_FillValue = 999999.f ;
			PSAL:long_name = "sea_water_practical_salinity" ;
			PSAL:standard_name = "sea_water_practical_salinity" ;
			PSAL:units = "1" ;
			PSAL:valid_max = 41.f ;
			PSAL:valid_min = 2.f ;
		float DEPTH(OBS) ;
			DEPTH:ancillary_variables = "DEPTH_quality_control" ;
			DEPTH:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
			DEPTH:_FillValue = 999999.f ;
			DEPTH:long_name = "actual depth" ;
			DEPTH:positive = "down" ;
			DEPTH:reference_datum = "sea surface" ;
			DEPTH:standard_name = "depth" ;
			DEPTH:units = "m" ;
			DEPTH:valid_max = 12000.f ;
			DEPTH:valid_min = -5.f ;
		byte DEPTH_quality_control(OBS) ;
			DEPTH_quality_control:_FillValue = 99b ;
			DEPTH_quality_control:flag_meanings = "No_QC_performed Good_data Probably_good_data Bad_data_that_are_potentially_correctable Bad_data Value_changed Not_used Not_used Not_used Missing_value" ;
			DEPTH_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 5b, 6b, 7b, 8b, 9b ;
			DEPTH_quality_control:long_name = "quality flag for depth" ;
			DEPTH_quality_control:quality_control_conventions = "IMOS standard flags" ;
			DEPTH_quality_control:quality_control_global = "B" ;
			DEPTH_quality_control:quality_control_global_conventions = "Argo reference table 2a (see http://www.cmar.csiro.au/argo/dmqc/user_doc/QC_flags.html), applied on data in position only (between global attributes time_deployment_start and time_deployment_end)" ;
			DEPTH_quality_control:standard_name = "depth status_flag" ;
		float PRES_REL(OBS) ;
			PRES_REL:ancillary_variables = "PRES_REL_quality_control" ;
			PRES_REL:comment = "In-situ measurement, sea surface = 0 dbar" ;
			PRES_REL:coordinates = "TIME LATITUDE LONGITUDE DEPTH NOMINAL_DEPTH" ;
			PRES_REL:_FillValue = 999999.f ;
			PRES_REL:long_name = "sea_water_pressure_due_to_sea_water" ;
			PRES_REL:standard_name = "sea_water_pressure_due_to_sea_water" ;
			PRES_REL:units = "dbar" ;
			PRES_REL:valid_max = 12000.f ;
			PRES_REL:valid_min = -15.f ;
		double LATITUDE(instrument) ;
			LATITUDE:axis = "Y" ;
			LATITUDE:long_name = "latitude" ;
			LATITUDE:reference_datum = "WGS84 coordinate reference system" ;
			LATITUDE:standard_name = "latitude" ;
			LATITUDE:units = "degrees_north" ;
			LATITUDE:valid_max = 90. ;
			LATITUDE:valid_min = -90. ;
		int TIMESERIES(instrument) ;
			TIMESERIES:cf_role = "timeseries_id" ;
			TIMESERIES:long_name = "EAC2000-9188" ;

