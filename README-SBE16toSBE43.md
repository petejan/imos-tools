# SBE43 data
Create SBE16 data files, then extract the SBE43 RAW data and calculate the disolved oxygen

## processing RAW files

    python3 ocean_dp/parse/sbeCNV2netCDF.py data/SBE16plus_01606330_2016_03_30.cnv
or

    python3 ocean_dp/parse/sbe16DD2netCDF.py data/Pulse-10-SBE16-6330-download.cap     

Makes file data/...nc

## add SOTS addAttributes

	python3 sots/process_SOTS_toIMOS.py data/*.nc

SBE43 calibration constants to add to the V0 variable
```
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_SerialNumber,str,"1635"
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_CalibrationDate,str,"24-Sep-11"
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_Soc,str,0.5202
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_offset,str,-0.5032
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_A,str,-0.002596
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_B,str,0.00010752
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_C,str,-2.2866e-06
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_D0,str,2.5826
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_D1,str,0.00019263
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_D2,str,-0.04648
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_E,str,0.036
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_Tau20,str,5.56
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_H1,str,-0.033
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_H2,str,5000.
VAR_ATT,V0,Pulse-9-2012,SBE16plusV2,unknown,,,,,,calibration_H3,str,1450.
```

## QC SBE16 data
add practical salinity if needed

    python3 ocean_dp/processing/addPSAL.py data/IMOS*FV00*.nc

qc the files

    python3 ocean_dp/sots/process_to_qc_and_resample.py data/IMOS*FV00*.nc

```

netcdf IMOS_DWM-SOTS_CPST_20120619_Pulse_FV01_Pulse-9-2012-SBE16plusV2-01606331-38m_END-20130427_C-20211025 {
dimensions:
	TIME = 5429 ;
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
		TEMP:units = "degrees_Celsius" ;
		TEMP:instrument_uncertainty = 0.005f ;
		TEMP:name = "sea_water_temperature" ;
		TEMP:standard_name = "sea_water_temperature" ;
		TEMP:long_name = "sea_water_temperature" ;
		TEMP:valid_min = -2.5f ;
		TEMP:valid_max = 40.f ;
		TEMP:reference_scale = "ITS-90" ;
		TEMP:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		TEMP:ancillary_variables = "TEMP_quality_control TEMP_quality_control_loc TEMP_quality_control_gr TEMP_quality_control_spk TEMP_quality_control_roc TEMP_quality_control_man" ;
	float CNDC(TIME) ;
		CNDC:_FillValue = NaNf ;
		CNDC:units = "S/m" ;
		CNDC:instrument_uncertainty = 0.0005f ;
		CNDC:name = "Water conductivity" ;
		CNDC:standard_name = "sea_water_electrical_conductivity" ;
		CNDC:long_name = "sea_water_electrical_conductivity" ;
		CNDC:valid_min = 0.f ;
		CNDC:valid_max = 50000.f ;
		CNDC:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		CNDC:ancillary_variables = "CNDC_quality_control CNDC_quality_control_loc CNDC_quality_control_gr CNDC_quality_control_man" ;
	float PRES(TIME) ;
		PRES:_FillValue = NaNf ;
		PRES:units = "dbar" ;
		PRES:instrument_uncertainty = 2.f ;
		PRES:applied_offset = -10.1353f ;
		PRES:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		PRES:long_name = "sea_water_pressure_due_to_sea_water" ;
		PRES:standard_name = "sea_water_pressure_due_to_sea_water" ;
		PRES:valid_max = 12000.f ;
		PRES:valid_min = -15.f ;
		PRES:ancillary_variables = "PRES_quality_control PRES_quality_control_loc PRES_quality_control_man" ;
	float V0(TIME) ;
		V0:_FillValue = NaNf ;
		V0:units = "V" ;
		V0:calibration_SerialNumber = "1635" ;
		V0:calibration_CalibrationDate = "24-Sep-11" ;
		V0:calibration_Soc = "0.5202" ;
		V0:calibration_offset = "-0.5032" ;
		V0:calibration_A = "-0.002596" ;
		V0:calibration_B = "0.00010752" ;
		V0:calibration_C = "-2.2866e-06" ;
		V0:calibration_D0 = "2.5826" ;
		V0:calibration_D1 = "0.00019263" ;
		V0:calibration_D2 = "-0.04648" ;
		V0:calibration_E = "0.036" ;
		V0:calibration_Tau20 = "5.56" ;
		V0:calibration_H1 = "-0.033" ;
		V0:calibration_H2 = "5000." ;
		V0:calibration_H3 = "1450." ;
		V0:ancillary_variables = "V0_quality_control V0_quality_control_loc V0_quality_control_man" ;
	float V1(TIME) ;
		V1:_FillValue = NaNf ;
		V1:units = "V" ;
		V1:comment_attached_instrument = "Wet-LABS ; PARS-135" ;
		V1:calibration_PAR_Im = 1.3589 ;
		V1:calibration_PAR_analog_A1 = 0.8946 ;
		V1:calibration_PAR_analog_A0 = 1.3285 ;
		V1:calibration_PAR_date = "Mar. 27, 2009" ;
		V1:calibration_PAR_facility = "Wet-LABS" ;
		V1:calibration_PAR_SN = "PARS-135" ;
		V1:ancillary_variables = "V1_quality_control V1_quality_control_loc V1_quality_control_man" ;
	float V2(TIME) ;
		V2:_FillValue = NaNf ;
		V2:units = "V" ;
		V2:ancillary_variables = "V2_quality_control V2_quality_control_loc V2_quality_control_man" ;
	float V3(TIME) ;
		V3:_FillValue = NaNf ;
		V3:units = "V" ;
		V3:ancillary_variables = "V3_quality_control V3_quality_control_loc V3_quality_control_man" ;
	float V4(TIME) ;
		V4:_FillValue = NaNf ;
		V4:units = "V" ;
		V4:long_name = "optode_bphase_voltage" ;
		V4:calibration_scale_offset = "12 10" ;
		V4:sensor_serial_number = "1158" ;
		V4:ancillary_variables = "V4_quality_control V4_quality_control_loc V4_quality_control_man" ;
	float V5(TIME) ;
		V5:_FillValue = NaNf ;
		V5:units = "V" ;
		V5:long_name = "optode_temp_voltage" ;
		V5:calibration_scale_offset = "9 -5" ;
		V5:sensor_serial_number = "1158" ;
		V5:ancillary_variables = "V5_quality_control V5_quality_control_loc V5_quality_control_man" ;
	float TOTAL_GAS_PRESSURE(TIME) ;
		TOTAL_GAS_PRESSURE:_FillValue = NaNf ;
		TOTAL_GAS_PRESSURE:units = "millibars" ;
		TOTAL_GAS_PRESSURE:ancillary_variables = "TOTAL_GAS_PRESSURE_quality_control TOTAL_GAS_PRESSURE_quality_control_loc TOTAL_GAS_PRESSURE_quality_control_man" ;
	float TEMP_GTD(TIME) ;
		TEMP_GTD:_FillValue = NaNf ;
		TEMP_GTD:units = "degrees_Celsius" ;
		TEMP_GTD:ancillary_variables = "TEMP_GTD_quality_control TEMP_GTD_quality_control_loc TEMP_GTD_quality_control_man" ;
	float PSAL(TIME) ;
		PSAL:_FillValue = NaNf ;
		PSAL:units = "1" ;
		PSAL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		PSAL:long_name = "sea_water_practical_salinity" ;
		PSAL:standard_name = "sea_water_practical_salinity" ;
		PSAL:valid_max = 41.f ;
		PSAL:valid_min = 2.f ;
		PSAL:ancillary_variables = "PSAL_quality_control PSAL_quality_control_loc PSAL_quality_control_gr PSAL_quality_control_spk PSAL_quality_control_roc PSAL_quality_control_man" ;
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
	byte TEMP_quality_control(TIME) ;
		TEMP_quality_control:_FillValue = 99b ;
		TEMP_quality_control:long_name = "quality flag for sea_water_temperature" ;
		TEMP_quality_control:standard_name = "sea_water_temperature status_flag" ;
		TEMP_quality_control:quality_control_conventions = "IMOS standard flags" ;
		TEMP_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		TEMP_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		TEMP_quality_control:comment = "maximum of all flags" ;
	byte CNDC_quality_control(TIME) ;
		CNDC_quality_control:_FillValue = 99b ;
		CNDC_quality_control:long_name = "quality flag for sea_water_electrical_conductivity" ;
		CNDC_quality_control:standard_name = "sea_water_electrical_conductivity status_flag" ;
		CNDC_quality_control:quality_control_conventions = "IMOS standard flags" ;
		CNDC_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		CNDC_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		CNDC_quality_control:comment = "maximum of all flags" ;
	byte PRES_quality_control(TIME) ;
		PRES_quality_control:_FillValue = 99b ;
		PRES_quality_control:long_name = "quality flag for sea_water_pressure_due_to_sea_water" ;
		PRES_quality_control:standard_name = "sea_water_pressure_due_to_sea_water status_flag" ;
		PRES_quality_control:quality_control_conventions = "IMOS standard flags" ;
		PRES_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		PRES_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		PRES_quality_control:comment = "maximum of all flags" ;
	byte V0_quality_control(TIME) ;
		V0_quality_control:_FillValue = 99b ;
		V0_quality_control:quality_control_conventions = "IMOS standard flags" ;
		V0_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		V0_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		V0_quality_control:comment = "maximum of all flags" ;
	byte V1_quality_control(TIME) ;
		V1_quality_control:_FillValue = 99b ;
		V1_quality_control:quality_control_conventions = "IMOS standard flags" ;
		V1_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		V1_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		V1_quality_control:comment = "maximum of all flags" ;
	byte V2_quality_control(TIME) ;
		V2_quality_control:_FillValue = 99b ;
		V2_quality_control:quality_control_conventions = "IMOS standard flags" ;
		V2_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		V2_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		V2_quality_control:comment = "maximum of all flags" ;
	byte V3_quality_control(TIME) ;
		V3_quality_control:_FillValue = 99b ;
		V3_quality_control:quality_control_conventions = "IMOS standard flags" ;
		V3_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		V3_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		V3_quality_control:comment = "maximum of all flags" ;
	byte V4_quality_control(TIME) ;
		V4_quality_control:_FillValue = 99b ;
		V4_quality_control:long_name = "quality flag for optode_bphase_voltage" ;
		V4_quality_control:quality_control_conventions = "IMOS standard flags" ;
		V4_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		V4_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		V4_quality_control:comment = "maximum of all flags" ;
	byte V5_quality_control(TIME) ;
		V5_quality_control:_FillValue = 99b ;
		V5_quality_control:long_name = "quality flag for optode_temp_voltage" ;
		V5_quality_control:quality_control_conventions = "IMOS standard flags" ;
		V5_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		V5_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		V5_quality_control:comment = "maximum of all flags" ;
	byte TOTAL_GAS_PRESSURE_quality_control(TIME) ;
		TOTAL_GAS_PRESSURE_quality_control:_FillValue = 99b ;
		TOTAL_GAS_PRESSURE_quality_control:quality_control_conventions = "IMOS standard flags" ;
		TOTAL_GAS_PRESSURE_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		TOTAL_GAS_PRESSURE_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		TOTAL_GAS_PRESSURE_quality_control:comment = "maximum of all flags" ;
	byte TEMP_GTD_quality_control(TIME) ;
		TEMP_GTD_quality_control:_FillValue = 99b ;
		TEMP_GTD_quality_control:quality_control_conventions = "IMOS standard flags" ;
		TEMP_GTD_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		TEMP_GTD_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		TEMP_GTD_quality_control:comment = "maximum of all flags" ;
	byte PSAL_quality_control(TIME) ;
		PSAL_quality_control:_FillValue = 99b ;
		PSAL_quality_control:long_name = "quality flag for sea_water_practical_salinity" ;
		PSAL_quality_control:standard_name = "sea_water_practical_salinity status_flag" ;
		PSAL_quality_control:quality_control_conventions = "IMOS standard flags" ;
		PSAL_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		PSAL_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		PSAL_quality_control:comment = "maximum of all flags" ;
	byte TEMP_quality_control_loc(TIME) ;
		TEMP_quality_control_loc:_FillValue = 99b ;
		TEMP_quality_control_loc:long_name = "in/out of water flag for sea_water_temperature" ;
		TEMP_quality_control_loc:units = "1" ;
		TEMP_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte CNDC_quality_control_loc(TIME) ;
		CNDC_quality_control_loc:_FillValue = 99b ;
		CNDC_quality_control_loc:long_name = "in/out of water flag for sea_water_electrical_conductivity" ;
		CNDC_quality_control_loc:units = "1" ;
		CNDC_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte PRES_quality_control_loc(TIME) ;
		PRES_quality_control_loc:_FillValue = 99b ;
		PRES_quality_control_loc:long_name = "in/out of water flag for sea_water_pressure_due_to_sea_water" ;
		PRES_quality_control_loc:units = "1" ;
		PRES_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte V0_quality_control_loc(TIME) ;
		V0_quality_control_loc:_FillValue = 99b ;
		V0_quality_control_loc:units = "1" ;
		V0_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte V1_quality_control_loc(TIME) ;
		V1_quality_control_loc:_FillValue = 99b ;
		V1_quality_control_loc:units = "1" ;
		V1_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte V2_quality_control_loc(TIME) ;
		V2_quality_control_loc:_FillValue = 99b ;
		V2_quality_control_loc:units = "1" ;
		V2_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte V3_quality_control_loc(TIME) ;
		V3_quality_control_loc:_FillValue = 99b ;
		V3_quality_control_loc:units = "1" ;
		V3_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte V4_quality_control_loc(TIME) ;
		V4_quality_control_loc:_FillValue = 99b ;
		V4_quality_control_loc:long_name = "in/out of water flag for optode_bphase_voltage" ;
		V4_quality_control_loc:units = "1" ;
		V4_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte V5_quality_control_loc(TIME) ;
		V5_quality_control_loc:_FillValue = 99b ;
		V5_quality_control_loc:long_name = "in/out of water flag for optode_temp_voltage" ;
		V5_quality_control_loc:units = "1" ;
		V5_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte TOTAL_GAS_PRESSURE_quality_control_loc(TIME) ;
		TOTAL_GAS_PRESSURE_quality_control_loc:_FillValue = 99b ;
		TOTAL_GAS_PRESSURE_quality_control_loc:units = "1" ;
		TOTAL_GAS_PRESSURE_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte TEMP_GTD_quality_control_loc(TIME) ;
		TEMP_GTD_quality_control_loc:_FillValue = 99b ;
		TEMP_GTD_quality_control_loc:units = "1" ;
		TEMP_GTD_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte PSAL_quality_control_loc(TIME) ;
		PSAL_quality_control_loc:_FillValue = 99b ;
		PSAL_quality_control_loc:long_name = "in/out of water flag for sea_water_practical_salinity" ;
		PSAL_quality_control_loc:units = "1" ;
		PSAL_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
	byte TEMP_quality_control_gr(TIME) ;
		TEMP_quality_control_gr:_FillValue = 99b ;
		TEMP_quality_control_gr:long_name = "global_range flag for sea_water_temperature" ;
		TEMP_quality_control_gr:units = "1" ;
		TEMP_quality_control_gr:comment = "Test 4. gross range test" ;
	byte TEMP_quality_control_spk(TIME) ;
		TEMP_quality_control_spk:_FillValue = 99b ;
		TEMP_quality_control_spk:long_name = "spike flag for sea_water_temperature" ;
		TEMP_quality_control_spk:units = "1" ;
		TEMP_quality_control_spk:comment = "Test 6. spike test" ;
	byte TEMP_quality_control_roc(TIME) ;
		TEMP_quality_control_roc:_FillValue = 99b ;
		TEMP_quality_control_roc:long_name = "rate_of_change flag for sea_water_temperature" ;
		TEMP_quality_control_roc:units = "1" ;
		TEMP_quality_control_roc:comment = "Test 7. rate of change test" ;
	byte CNDC_quality_control_gr(TIME) ;
		CNDC_quality_control_gr:_FillValue = 99b ;
		CNDC_quality_control_gr:long_name = "global_range flag for sea_water_electrical_conductivity" ;
		CNDC_quality_control_gr:units = "1" ;
		CNDC_quality_control_gr:comment = "Test 4. gross range test" ;
	byte PSAL_quality_control_gr(TIME) ;
		PSAL_quality_control_gr:_FillValue = 99b ;
		PSAL_quality_control_gr:long_name = "global_range flag for sea_water_practical_salinity" ;
		PSAL_quality_control_gr:units = "1" ;
		PSAL_quality_control_gr:comment = "Test 4. gross range test" ;
	byte PSAL_quality_control_spk(TIME) ;
		PSAL_quality_control_spk:_FillValue = 99b ;
		PSAL_quality_control_spk:long_name = "spike flag for sea_water_practical_salinity" ;
		PSAL_quality_control_spk:units = "1" ;
		PSAL_quality_control_spk:comment = "Test 6. spike test" ;
	byte PSAL_quality_control_roc(TIME) ;
		PSAL_quality_control_roc:_FillValue = 99b ;
		PSAL_quality_control_roc:long_name = "rate_of_change flag for sea_water_practical_salinity" ;
		PSAL_quality_control_roc:units = "1" ;
		PSAL_quality_control_roc:comment = "Test 7. rate of change test" ;
	byte TEMP_quality_control_man(TIME) ;
		TEMP_quality_control_man:_FillValue = 99b ;
		TEMP_quality_control_man:long_name = "manual flag for sea_water_temperature" ;
		TEMP_quality_control_man:units = "1" ;
		TEMP_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte CNDC_quality_control_man(TIME) ;
		CNDC_quality_control_man:_FillValue = 99b ;
		CNDC_quality_control_man:long_name = "manual flag for sea_water_electrical_conductivity" ;
		CNDC_quality_control_man:units = "1" ;
		CNDC_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte PRES_quality_control_man(TIME) ;
		PRES_quality_control_man:_FillValue = 99b ;
		PRES_quality_control_man:long_name = "manual flag for sea_water_pressure_due_to_sea_water" ;
		PRES_quality_control_man:units = "1" ;
		PRES_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte V0_quality_control_man(TIME) ;
		V0_quality_control_man:_FillValue = 99b ;
		V0_quality_control_man:units = "1" ;
		V0_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte V1_quality_control_man(TIME) ;
		V1_quality_control_man:_FillValue = 99b ;
		V1_quality_control_man:units = "1" ;
		V1_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte V2_quality_control_man(TIME) ;
		V2_quality_control_man:_FillValue = 99b ;
		V2_quality_control_man:units = "1" ;
		V2_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte V3_quality_control_man(TIME) ;
		V3_quality_control_man:_FillValue = 99b ;
		V3_quality_control_man:units = "1" ;
		V3_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte V4_quality_control_man(TIME) ;
		V4_quality_control_man:_FillValue = 99b ;
		V4_quality_control_man:long_name = "manual flag for optode_bphase_voltage" ;
		V4_quality_control_man:units = "1" ;
		V4_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte V5_quality_control_man(TIME) ;
		V5_quality_control_man:_FillValue = 99b ;
		V5_quality_control_man:long_name = "manual flag for optode_temp_voltage" ;
		V5_quality_control_man:units = "1" ;
		V5_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte TOTAL_GAS_PRESSURE_quality_control_man(TIME) ;
		TOTAL_GAS_PRESSURE_quality_control_man:_FillValue = 99b ;
		TOTAL_GAS_PRESSURE_quality_control_man:units = "1" ;
		TOTAL_GAS_PRESSURE_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte TEMP_GTD_quality_control_man(TIME) ;
		TEMP_GTD_quality_control_man:_FillValue = 99b ;
		TEMP_GTD_quality_control_man:units = "1" ;
		TEMP_GTD_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;
	byte PSAL_quality_control_man(TIME) ;
		PSAL_quality_control_man:_FillValue = 99b ;
		PSAL_quality_control_man:long_name = "manual flag for sea_water_practical_salinity" ;
		PSAL_quality_control_man:units = "1" ;
		PSAL_quality_control_man:comment = "manual, by date, start 2012-12-29, battery failed" ;

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
		:date_created = "2021-10-25T01:37:45Z" ;
		:deployment_code = "Pulse-9-2012" ;
		:disclaimer = "Data, products and services from IMOS are provided \"as is\" without any warranty as to fitness for a particular purpose." ;
		:featureType = "timeSeries" ;
		:geospatial_lat_max = -46.84932 ;
		:geospatial_lat_min = -46.84932 ;
		:geospatial_lat_units = "degrees_north" ;
		:geospatial_lon_max = 142.39855 ;
		:geospatial_lon_min = 142.39855 ;
		:geospatial_lon_units = "degrees_east" ;
		:geospatial_vertical_max = 38.5 ;
		:geospatial_vertical_min = 38.5 ;
		:geospatial_vertical_positive = "down" ;
		:institution = "DWM-SOTS" ;
		:institution_references = "http://www.imos.org.au/aodn.html" ;
		:instrument = "Sea-Bird Electronics ; SBE16plusV2" ;
		:instrument_model = "SBE16plusV2" ;
		:instrument_nominal_depth = 38.5 ;
		:instrument_serial_number = "01606331" ;
		:keywords_vocabulary = "IMOS parameter names. See https://github.com/aodn/imos-toolbox/blob/master/IMOS/imosParameters.txt" ;
		:license = "http://creativecommons.org/licenses/by/4.0/" ;
		:naming_authority = "IMOS" ;
		:platform_code = "Pulse" ;
		:principal_investigator = "Trull, Tom; Shulz, Eric; Shadwick, Elizabeth" ;
		:principal_investigator_email = "tom.trull@csiro.au; eshulz@bom.gov.au; elizabeth.shadwick@csiro.au" ;
		:project = "Integrated Marine Observing System (IMOS)" ;
		:site_code = "SOTS" ;
		:site_nominal_depth = 4300. ;
		:standard_name_vocabulary = "NetCDF Climate and Forecast (CF) Metadata Convention Standard Name Table 67" ;
		:time_coverage_end = "2013-04-27T01:03:28Z" ;
		:time_coverage_start = "2012-06-19T22:29:01Z" ;
		:time_deployment_end = "2013-05-05T01:15:00Z" ;
		:time_deployment_start = "2012-07-17T07:00:00Z" ;
		:title = "Oceanographic mooring data deployment of Pulse at latitude -46.8 longitude 142.4 depth  38 (m) instrument Sea-Bird Electronics ; SBE16plusV2 serial 01606331" ;
		:voyage_deployment = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=SS2012_V03" ;
		:voyage_recovery = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=SS2013_V03" ;
		:file_version = "Level 1 - Quality Controlled Data" ;
	    :file_version_quality_control = "Quality controlled data have been through quality assurance procedures such as automated routines and sensor calibration and/or a level of visual inspection and flag of obvious errors. The data are in physical units using standard SI metric units with calibration and other pre- processing routines applied, all time and location values are in absolute coordinates to comply with standards and datum. Data includes flags for each measurement to indicate the estimated quality of the measurement. Metadata exists for the data or for the higher level dataset that the data belongs to. This is the standard IMOS data level and is what should be made available to AODN and to the IMOS community." ;		
		:history = "2021-10-25 created from file /Users/pete/DWM/SBE16_data/Pulse-9-SBE16-6331-RAS-data.cap\n2021-10-25 attributes added from file(s) [metadata/pulse-saz-sofs-flux.metadata.csv, metadata/imos.metadata.csv, metadata/sots.metadata.csv, metadata/sofs.metadata.csv, metadata/asimet.metadata.csv, metadata/variable.metadata.csv]\n2021-10-25 : quality_control variables added.\n2021-10-25 : in/out marked 776\n2021-10-25 TEMP global range min = -2 max = 30 marked 0.0\n2021-10-25 TEMP global range min = 5 max = 16 marked 0.0\n2021-10-25 TEMP spike height = 2 marked 0\n2021-10-25 TEMP max rate = 80 marked 0\n2021-10-25 CNDC global range min = 3 max = 4.5 marked 0.0\n2021-10-25 PSAL global range min = 2 max = 41 marked 0.0\n2021-10-25 PSAL global range min = 34 max = 35.5 marked 190.0\n2021-10-25 PSAL spike height = 0.4 marked 0\n2021-10-25 PSAL max rate = 30 marked 0\n2021-10-25 manual QC, marked 687 with flag=4, start 2012-12-29 12:30:00, battery failed" ;
		:references = "http://www.imos.org.au; Jansen P, Weeding B, Shadwick EH and Trull TW (2020). Southern Ocean Time Series (SOTS) Quality Assessment and Control Report Temperature Records Version 1.0. CSIRO, Australia. DOI: 10.26198/gfgr-fq47 (https://doi.org/10.26198/gfgr-fq47); Jansen P, Shadwick E and Trull TW (2021). Southern Ocean Time Series (SOTS) Quality Assessment and Control Report Salinity Records Version 1.0. CSIRO, Australia." ;
}
```

# Extract the SBE43 data

    python3 ocean_dp/processing/extract-SBE16-v-to-SBE43.py data/IMOS*FV01*.nc

rename to IMOS file

    find data -name "Pulse*SBE43.nc" -exec python3 ocean_dp/file_name/imosNetCDFfileName.py {} \;  

Output file

```
netcdf IMOS_DWM-SOTS_OPST_20090930_Pulse_FV01_Pulse-6-2009-SBE43-431634-38m_END-20100325_C-20211025 {
dimensions:
	TIME = 4228 ;
variables:
	float TIME(TIME) ;
		TIME:_FillValue = NaNf ;
		TIME:long_name = "time" ;
		TIME:units = "days since 1950-01-01 00:00:00 UTC" ;
		TIME:calendar = "gregorian" ;
		TIME:axis = "T" ;
		TIME:standard_name = "time" ;
		TIME:valid_max = 90000. ;
		TIME:valid_min = 0. ;
	float PRES(TIME) ;
		PRES:_FillValue = NaNf ;
		PRES:units = "dbar" ;
		PRES:instrument_uncertainty = 2.f ;
		PRES:applied_offset = -10.1353f ;
		PRES:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		PRES:long_name = "sea_water_pressure_due_to_sea_water" ;
		PRES:standard_name = "sea_water_pressure_due_to_sea_water" ;
		PRES:valid_max = 12000.f ;
		PRES:valid_min = -15.f ;
		PRES:ancillary_variables = "PRES_quality_control PRES_quality_control_loc" ;
	double NOMINAL_DEPTH ;
		NOMINAL_DEPTH:axis = "Z" ;
		NOMINAL_DEPTH:long_name = "nominal depth" ;
		NOMINAL_DEPTH:positive = "down" ;
		NOMINAL_DEPTH:reference_datum = "sea surface" ;
		NOMINAL_DEPTH:standard_name = "depth" ;
		NOMINAL_DEPTH:units = "m" ;
		NOMINAL_DEPTH:valid_max = 12000. ;
		NOMINAL_DEPTH:valid_min = -5. ;
	double LATITUDE ;
		LATITUDE:axis = "Y" ;
		LATITUDE:long_name = "latitude" ;
		LATITUDE:reference_datum = "WGS84 geographic coordinate system" ;
		LATITUDE:standard_name = "latitude" ;
		LATITUDE:units = "degrees_north" ;
		LATITUDE:valid_max = 90. ;
		LATITUDE:valid_min = -90. ;
	byte TEMP_quality_control(TIME) ;
		TEMP_quality_control:_FillValue = 99b ;
		TEMP_quality_control:long_name = "quality flag for sea_water_temperature" ;
		TEMP_quality_control:standard_name = "sea_water_temperature status_flag" ;
		TEMP_quality_control:quality_control_conventions = "IMOS standard flags" ;
		TEMP_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		TEMP_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		TEMP_quality_control:comment = "maximum of all flags" ;
	float V0(TIME) ;
		V0:_FillValue = NaNf ;
		V0:units = "V" ;
		V0:calibration_SerialNumber = "1634" ;
		V0:calibration_CalibrationDate = "15-May-09p" ;
		V0:calibration_Soc = "0.4263" ;
		V0:calibration_offset = "-0.4382" ;
		V0:calibration_A = "-0.0025795" ;
		V0:calibration_B = "0.00016628" ;
		V0:calibration_C = "-3.1959e-06" ;
		V0:calibration_D0 = "2.5826" ;
		V0:calibration_D1 = "0.00019263" ;
		V0:calibration_D2 = "-0.04648" ;
		V0:calibration_E = "0.036" ;
		V0:calibration_Tau20 = "5.01" ;
		V0:calibration_H1 = "-0.033" ;
		V0:calibration_H2 = "5000." ;
		V0:calibration_H3 = "1450." ;
		V0:ancillary_variables = "V0_quality_control V0_quality_control_loc" ;
	double LONGITUDE ;
		LONGITUDE:axis = "X" ;
		LONGITUDE:long_name = "longitude" ;
		LONGITUDE:reference_datum = "WGS84 geographic coordinate system" ;
		LONGITUDE:standard_name = "longitude" ;
		LONGITUDE:units = "degrees_east" ;
		LONGITUDE:valid_max = 180. ;
		LONGITUDE:valid_min = -180. ;
	byte PRES_quality_control(TIME) ;
		PRES_quality_control:_FillValue = 99b ;
		PRES_quality_control:long_name = "quality flag for sea_water_pressure_due_to_sea_water" ;
		PRES_quality_control:standard_name = "sea_water_pressure_due_to_sea_water status_flag" ;
		PRES_quality_control:quality_control_conventions = "IMOS standard flags" ;
		PRES_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		PRES_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		PRES_quality_control:comment = "maximum of all flags" ;
	float PSAL(TIME) ;
		PSAL:_FillValue = NaNf ;
		PSAL:units = "1" ;
		PSAL:standard_name = "sea_water_practical_salinity" ;
		PSAL:long_name = "sea_water_practical_salinity" ;
		PSAL:valid_max = 40.f ;
		PSAL:valid_min = -1.f ;
		PSAL:comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html" ;
		PSAL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		PSAL:ancillary_variables = "PSAL_quality_control PSAL_quality_control_loc PSAL_quality_control_gr PSAL_quality_control_spk PSAL_quality_control_roc" ;
	byte PSAL_quality_control(TIME) ;
		PSAL_quality_control:_FillValue = 99b ;
		PSAL_quality_control:long_name = "quality flag for sea_water_practical_salinity" ;
		PSAL_quality_control:standard_name = "sea_water_practical_salinity status_flag" ;
		PSAL_quality_control:quality_control_conventions = "IMOS standard flags" ;
		PSAL_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		PSAL_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		PSAL_quality_control:comment = "maximum of all flags" ;
	byte V0_quality_control(TIME) ;
		V0_quality_control:_FillValue = 99b ;
		V0_quality_control:quality_control_conventions = "IMOS standard flags" ;
		V0_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		V0_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		V0_quality_control:comment = "maximum of all flags" ;
	float TEMP(TIME) ;
		TEMP:_FillValue = NaNf ;
		TEMP:units = "degrees_Celsius" ;
		TEMP:instrument_uncertainty = 0.005f ;
		TEMP:name = "sea_water_temperature" ;
		TEMP:standard_name = "sea_water_temperature" ;
		TEMP:long_name = "sea_water_temperature" ;
		TEMP:valid_min = -2.5f ;
		TEMP:valid_max = 40.f ;
		TEMP:reference_scale = "ITS-90" ;
		TEMP:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
		TEMP:ancillary_variables = "TEMP_quality_control TEMP_quality_control_loc TEMP_quality_control_gr TEMP_quality_control_spk TEMP_quality_control_roc" ;
	float DOX2_SBE(TIME) ;
		DOX2_SBE:_FillValue = NaNf ;
		DOX2_SBE:standard_name = "moles_of_oxygen_per_unit_mass_in_sea_water" ;
		DOX2_SBE:valid_min = 0.f ;
		DOX2_SBE:valid_max = 400.f ;
		DOX2_SBE:units = "umol/kg" ;
		DOX2_SBE:equation_1 = "Ox(umol/kg)=Soc.(V+Voffset).(1+A.T+B.T^2+V.T^3).OxSOL(T,S).exp(E.P/K) ... SeaBird (AN64)" ;
		DOX2_SBE:comment = "OxSOL in umol/kg" ;
	float DOX2(TIME) ;
		DOX2:_FillValue = NaNf ;
		DOX2:standard_name = "moles_of_oxygen_per_unit_mass_in_sea_water" ;
		DOX2:valid_min = 0.f ;
		DOX2:valid_max = 400.f ;
		DOX2:units = "umol/kg" ;
		DOX2:equation_1 = "Ox(umol/kg)=Soc.(V+Voffset).(1+A.T+B.T^2+V.T^3).OxSOL(T,S).exp(E.P/K) ... SeaBird (AN64)" ;
		DOX2:comment = "OxSOL in umol/kg" ;
	byte DOX2_quality_control(TIME) ;
		DOX2_quality_control:_FillValue = 99b ;
		DOX2_quality_control:standard_name = "moles_of_oxygen_per_unit_mass_in_sea_water status_flag" ;
		DOX2_quality_control:quality_control_conventions = "IMOS standard flags" ;
		DOX2_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
		DOX2_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
		DOX2_quality_control:comment = "maximum of all flags" ;
	float OXSOL(TIME) ;
		OXSOL:_FillValue = NaNf ;
		OXSOL:units = "umol/kg" ;
		OXSOL:comment = "calculated using gsw-python https://teos-10.github.io/GSW-Python/index.html function gsw.O2sol_SP_pt" ;

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
		:date_created = "2021-10-25T02:23:23Z" ;
		:deployment_code = "Pulse-6-2009" ;
		:disclaimer = "Data, products and services from IMOS are provided \"as is\" without any warranty as to fitness for a particular purpose." ;
		:featureType = "timeSeries" ;
		:geospatial_lat_max = -46.3224 ;
		:geospatial_lat_min = -46.3224 ;
		:geospatial_lat_units = "degrees_north" ;
		:geospatial_lon_max = 140.6776 ;
		:geospatial_lon_min = 140.6776 ;
		:geospatial_lon_units = "degrees_east" ;
		:geospatial_vertical_max = 37.5 ;
		:geospatial_vertical_min = 37.5 ;
		:geospatial_vertical_positive = "down" ;
		:institution = "DWM-SOTS" ;
		:institution_references = "http://www.imos.org.au/aodn.html" ;
		:instrument = "Sea-Bird Electronics ; SBE43" ;
		:instrument_model = "SBE43" ;
		:instrument_nominal_depth = 37.5 ;
		:instrument_serial_number = "431634" ;
		:keywords_vocabulary = "IMOS parameter names. See https://github.com/aodn/imos-toolbox/blob/master/IMOS/imosParameters.txt" ;
		:license = "http://creativecommons.org/licenses/by/4.0/" ;
		:naming_authority = "IMOS" ;
		:platform_code = "Pulse" ;
		:principal_investigator = "Trull, Tom; Shulz, Eric; Shadwick, Elizabeth" ;
		:principal_investigator_email = "tom.trull@csiro.au; eshulz@bom.gov.au; elizabeth.shadwick@csiro.au" ;
		:project = "Integrated Marine Observing System (IMOS)" ;
		:site_code = "SOTS" ;
		:site_nominal_depth = 4300. ;
		:standard_name_vocabulary = "NetCDF Climate and Forecast (CF) Metadata Convention Standard Name Table 67" ;
		:time_coverage_end = "2010-03-25T03:00:00Z" ;
		:time_coverage_start = "2009-09-30T00:00:00Z" ;
		:time_deployment_end = "2010-03-18T02:05:00Z" ;
		:time_deployment_start = "2009-09-28T00:00:00Z" ;
		:title = "Oceanographic mooring data deployment of Pulse at latitude -46.3 longitude 140.7 depth  38 (m) instrument Sea-Bird Electronics ; SBE16plusV2 serial 01606331" ;
		:voyage_deployment = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=SS2009_V04" ;
		:voyage_recovery = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=SS2010_V02" ;
		:file_version = "Level 1 - Quality Controlled Data" ;
		:file_version_quality_control = "Quality controlled data have been through quality assurance procedures such as automated routines and sensor calibration and/or a level of visual inspection and flag of obvious errors. The data are in physical units using standard SI metric units with calibration and other pre- processing routines applied, all time and location values are in absolute coordinates to comply with standards and datum. Data includes flags for each measurement to indicate the estimated quality of the measurement. Metadata exists for the data or for the higher level dataset that the data belongs to. This is the standard IMOS data level and is what should be made available to AODN and to the IMOS community." ;
		:references = "http://www.imos.org.au; Jansen P, Weeding B, Shadwick EH and Trull TW (2020). Southern Ocean Time Series (SOTS) Quality Assessment and Control Report Temperature Records Version 1.0. CSIRO, Australia. DOI: 10.26198/gfgr-fq47 (https://doi.org/10.26198/gfgr-fq47); Jansen P, Shadwick E and Trull TW (2021). Southern Ocean Time Series (SOTS) Quality Assessment and Control Report Salinity Records Version 1.0. CSIRO, Australia." ;
		:history = "2021-10-25 created from file /Users/pete/DWM/SBE16_data/SBE16-Instrument_Data_Upload_25032010.cap\n2021-10-25 attributes added from file(s) [metadata/pulse-saz-sofs-flux.metadata.csv, metadata/imos.metadata.csv, metadata/sots.metadata.csv, metadata/sofs.metadata.csv, metadata/asimet.metadata.csv, metadata/variable.metadata.csv]\n2021-10-25 added PSAL from TEMP, CNDC, PRES\n2021-10-25 : quality_control variables added.\n2021-10-25 : in/out marked 169\n2021-10-25 TEMP global range min = -2 max = 30 marked 0.0\n2021-10-25 TEMP global range min = 5 max = 16 marked 0.0\n2021-10-25 TEMP spike height = 2 marked 0\n2021-10-25 TEMP max rate = 80 marked 0\n2021-10-25 CNDC global range min = 3 max = 4.5 marked 0.0\n2021-10-25 PSAL global range min = 2 max = 41 marked 0.0\n2021-10-25 PSAL global range min = 34 max = 35.5 marked 1.0\n2021-10-25 PSAL spike height = 0.4 marked 1\n2021-10-25 PSAL max rate = 30 marked 0\n2021-10-25 calculate DOX2 IMOS_DWM-SOTS_CPT_20090930_Pulse_FV01_Pulse-6-2009-SBE16plusV2-01606331-38m_END-20100325_C-20211025.nc\n2021-10-25 calculate DOX2 from file IMOS_DWM-SOTS_OPST_20090930_Pulse_FV01_Pulse-6-2009-SBE43-431634-38m_END-20100325_C-20211025.nc" ;
}
```