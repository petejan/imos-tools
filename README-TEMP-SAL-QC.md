# Temperature and Salinity
Temperature and Salinity data processing and QC

## processing RAW files

	python3 ocean_dp\parse\sbeCNV2netCDF.py data\SOFS-9-2020\*.cnv      

Makes files data/SOFS-9-2020/SBE...cnv.nc

## add SOTS addAttributes

	python3 sots/process_SOTS_toIMOS.py data/SOFS-9-2020/*.nc


SOFS-9 specific metadata attributes

```
GLOBAL,,,SBE37SM-RS485,03707408,2020-08-31,2021-04-24,,"","",deployment_code,str,"SOFS-9-2020"
GLOBAL,,,SBE37SM-RS485,03707409,2020-08-31,2021-04-24,,"","",deployment_code,str,"SOFS-9-2020"
GLOBAL,,,SBE37SMP-ODO-RS232,03714700,2020-08-31,2021-04-24,,"","",deployment_code,str,"SOFS-9-2020"
GLOBAL,,,SBE37SMP-ODO-RS232,03715969,2020-08-31,2021-04-24,,"","",deployment_code,str,"SOFS-9-2020"
GLOBAL,,,SBE37SMP-ODO-RS232,03715970,2020-08-31,2021-04-24,,"","",deployment_code,str,"SOFS-9-2020"
GLOBAL,,,SBE37SMP-ODO-RS232,03715971,2020-08-31,2021-04-24,,"","",deployment_code,str,"SOFS-9-2020"
GLOBAL,,,SBE37SMP-ODO-RS232,03715972,2020-08-31,2021-04-24,,"","",deployment_code,str,"SOFS-9-2020"
GLOBAL,,,SBE37SMP-ODO-RS232,03720127,2020-08-31,2021-04-24,,"","",deployment_code,str,"SOFS-9-2020"

VAR,LATITUDE,SOFS-9-2020,,,2020-08-31,2021-04-24,LATITUDE,"()","()",,float64,"-46.9847600000"
VAR,LONGITUDE,SOFS-9-2020,,,2020-08-31,2021-04-24,LONGITUDE,"()","()",,float64,"141.8116900000"

GLOBAL,,SOFS-9-2020,,,2020-08-31,2021-04-24,,"","",site_nominal_depth,float64,"4624.00"
GLOBAL,,SOFS-9-2020,,,2020-08-31,2021-04-24,,"","",time_deployment_start,str,"2020-09-01T03:36:38Z"
GLOBAL,,SOFS-9-2020,,,2020-08-31,2021-04-24,,"","",time_deployment_end,str,"2021-04-24T00:00:00Z"

GLOBAL,,SOFS-9-2020,,,,,,,,voyage_deployment,str,http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=IN2020_V09
GLOBAL,,SOFS-9-2020,,,,,,,,voyage_recovery,str,http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=IN2021_V02

GLOBAL,,SOFS-9-2020,SBE37SMP-ODO-RS232,03714700,,,,,,comment_ctd_pre_dip,str,sensor pre-dipped on IN2020_V09 CTD #8
GLOBAL,,SOFS-9-2020,SBE37SMP-ODO-RS232,03714700,,,,,,comment_ctd_post_dip,str,sensor post-dipped on IN2021_V02 CTD #6
GLOBAL,,SOFS-9-2020,SBE37SMP-ODO-RS232,03715970,,,,,,comment_ctd_pre_dip,str,sensor pre-dipped on IN2020_V09 CTD #8
GLOBAL,,SOFS-9-2020,SBE37SMP-ODO-RS232,03715970,,,,,,comment_ctd_post_dip,str,sensor post-dipped on IN2021_V02 CTD #6
GLOBAL,,SOFS-9-2020,SBE37SMP-ODO-RS232,03715971,,,,,,comment_ctd_pre_dip,str,sensor pre-dipped on IN2020_V09 CTD #8
GLOBAL,,SOFS-9-2020,SBE37SMP-ODO-RS232,03715971,,,,,,comment_ctd_post_dip,str,sensor post-dipped on IN2021_V02 CTD #6
GLOBAL,,SOFS-9-2020,SBE37SMP-ODO-RS232,03715972,,,,,,comment_ctd_pre_dip,str,sensor pre-dipped on IN2020_V09 CTD #8
GLOBAL,,SOFS-9-2020,SBE37SMP-ODO-RS232,03715972,,,,,,comment_ctd_post_dip,str,sensor post-dipped on IN2021_V02 CTD #6

GLOBAL,,SOFS-9-2020,SBE37SM-RS485,03707409,,,,,,comment_ctd_pre_dip,str,sensor pre-dipped on IN2020_V09 CTD #8
GLOBAL,,SOFS-9-2020,SBE37SM-RS485,03707409,,,,,,comment_ctd_post_dip,str,sensor post-dipped on IN2021_V02 CTD #6

VAR,NOMINAL_DEPTH,SOFS-9-2020,SBE37SM-RS485,03707408,2020-08-31,2021-04-24,NOMINAL_DEPTH,"()","()",,float64,"1.00"
VAR,NOMINAL_DEPTH,SOFS-9-2020,SBE37SM-RS485,03707409,2020-08-31,2021-04-24,NOMINAL_DEPTH,"()","()",,float64,"1.00"
VAR,NOMINAL_DEPTH,SOFS-9-2020,SBE37SMP-ODO-RS232,03714700,2020-08-31,2021-04-24,NOMINAL_DEPTH,"()","()",,float64,"200.00"
VAR,NOMINAL_DEPTH,SOFS-9-2020,SBE37SMP-ODO-RS232,03715969,2020-08-31,2021-04-24,NOMINAL_DEPTH,"()","()",,float64,"30.00"
VAR,NOMINAL_DEPTH,SOFS-9-2020,SBE37SMP-ODO-RS232,03715970,2020-08-31,2021-04-24,NOMINAL_DEPTH,"()","()",,float64,"125.00"
VAR,NOMINAL_DEPTH,SOFS-9-2020,SBE37SMP-ODO-RS232,03715971,2020-08-31,2021-04-24,NOMINAL_DEPTH,"()","()",,float64,"300.00"
VAR,NOMINAL_DEPTH,SOFS-9-2020,SBE37SMP-ODO-RS232,03715972,2020-08-31,2021-04-24,NOMINAL_DEPTH,"()","()",,float64,"510.00"

```

## QCd data

	python3 sots/process_to_qc_smooth.py data/SOFS-9-2020/IMOS*.nc

```

netcdf IMOS_DWM-SOTS_COPST_20200327_SOFS_FV01_SOFS-9-2020-SBE37SMP-ODO-RS232-03714700-200m_END-20210425_C-20211010 {
dimensions:
        TIME = 15083 ;
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
                TEMP:calibration_SerialNumber = "14700" ;
                TEMP:calibration_CalibrationDate = "04-May-18" ;
                TEMP:calibration_A0 = -0.0001087568 ;
                TEMP:calibration_A1 = 0.0003064198 ;
                TEMP:calibration_A2 = -4.386211e-06 ;
                TEMP:calibration_A3 = 1.98623e-07 ;
                TEMP:calibration_Slope = 1. ;
                TEMP:calibration_Offset = 0. ;
                TEMP:name = "sea_water_temperature" ;
                TEMP:standard_name = "sea_water_temperature" ;
                TEMP:long_name = "sea_water_temperature" ;
                TEMP:valid_min = -2.5f ;
                TEMP:valid_max = 40.f ;
                TEMP:reference_scale = "ITS-90" ;
                TEMP:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                TEMP:ancillary_variables = "TEMP_quality_control TEMP_quality_control_loc TEMP_quality_control_gr TEMP_quality_control_spk TEMP_quality_control_roc" ;
        float PRES(TIME) ;
                PRES:_FillValue = NaNf ;
                PRES:comment = "Pressure, Strain Gauge [db]" ;
                PRES:units = "dbar" ;
                PRES:calibration_SerialNumber = "4436175" ;
                PRES:calibration_CalibrationDate = "02-May-18" ;
                PRES:calibration_PA0 = 0.0004448772 ;
                PRES:calibration_PA1 = 0.002725439 ;
                PRES:calibration_PA2 = 6.552032e-11 ;
                PRES:calibration_PTEMPA0 = 205.1934 ;
                PRES:calibration_PTEMPA1 = -0.06343555 ;
                PRES:calibration_PTEMPA2 = -5.16947e-06 ;
                PRES:calibration_PTCA0 = 524371.2 ;
                PRES:calibration_PTCA1 = 3.15176 ;
                PRES:calibration_PTCA2 = -0.133066 ;
                PRES:calibration_PTCB0 = 24.86487 ;
                PRES:calibration_PTCB1 = 0.000375 ;
                PRES:calibration_PTCB2 = 0. ;
                PRES:calibration_Offset = 0. ;
                PRES:applied_offset = -10.1353f ;
                PRES:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                PRES:long_name = "sea_water_pressure_due_to_sea_water" ;
                PRES:standard_name = "sea_water_pressure_due_to_sea_water" ;
                PRES:valid_max = 12000.f ;
                PRES:valid_min = -15.f ;
                PRES:ancillary_variables = "PRES_quality_control PRES_quality_control_loc" ;
        float CNDC(TIME) ;
                CNDC:_FillValue = NaNf ;
                CNDC:comment = "Conductivity [S/m]" ;
                CNDC:units = "S/m" ;
                CNDC:calibration_SerialNumber = "14700" ;
                CNDC:calibration_CalibrationDate = "04-May-18" ;
                CNDC:calibration_UseG_J = 1. ;
                CNDC:calibration_G = -0.9826711 ;
                CNDC:calibration_H = 0.166543 ;
                CNDC:calibration_I = -0.0002383773 ;
                CNDC:calibration_J = 4.514524e-05 ;
                CNDC:calibration_CPcor = -9.57e-08 ;
                CNDC:calibration_CTcor = 3.25e-06 ;
                CNDC:calibration_WBOTC = 2.741821e-07 ;
                CNDC:calibration_Slope = 1. ;
                CNDC:calibration_Offset = 0. ;
                CNDC:name = "Water conductivity" ;
                CNDC:standard_name = "sea_water_electrical_conductivity" ;
                CNDC:long_name = "sea_water_electrical_conductivity" ;
                CNDC:valid_min = 0.f ;
                CNDC:valid_max = 50000.f ;
                CNDC:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                CNDC:ancillary_variables = "CNDC_quality_control CNDC_quality_control_loc CNDC_quality_control_gr" ;
        float PSAL(TIME) ;
                PSAL:_FillValue = NaNf ;
                PSAL:comment = "Salinity, Practical [PSU]" ;
                PSAL:units = "1" ;
                PSAL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                PSAL:long_name = "sea_water_practical_salinity" ;
                PSAL:standard_name = "sea_water_practical_salinity" ;
                PSAL:valid_max = 41.f ;
                PSAL:valid_min = 2.f ;
                PSAL:ancillary_variables = "PSAL_quality_control PSAL_quality_control_loc PSAL_quality_control_gr PSAL_quality_control_spk PSAL_quality_control_roc" ;
        float DENSITY(TIME) ;
                DENSITY:_FillValue = NaNf ;
                DENSITY:comment = "Density [density, kg/m^3]" ;
                DENSITY:units = "kg/m^3" ;
                DENSITY:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                DENSITY:long_name = "sea_water_density" ;
                DENSITY:standard_name = "sea_water_density" ;
                DENSITY:valid_max = 1200.f ;
                DENSITY:valid_min = 0.f ;
                DENSITY:ancillary_variables = "DENSITY_quality_control DENSITY_quality_control_loc" ;
        float DOX(TIME) ;
                DOX:_FillValue = NaNf ;
                DOX:comment = "Oxygen, SBE 63 [ml/l]" ;
                DOX:units = "ml/l" ;
                DOX:calibration_SerialNumber = "1378" ;
                DOX:calibration_CalibrationDate = "14-Apr-18" ;
                DOX:calibration_A0 = 1.0513 ;
                DOX:calibration_A1 = -0.0015 ;
                DOX:calibration_A2 = 0.3562581 ;
                DOX:calibration_B0 = -0.2447496 ;
                DOX:calibration_B1 = 1.587353 ;
                DOX:calibration_C0 = 0.1058147 ;
                DOX:calibration_C1 = 0.004529342 ;
                DOX:calibration_C2 = 6.31179e-05 ;
                DOX:calibration_TA0 = 0.0007288568 ;
                DOX:calibration_TA1 = 0.0002431142 ;
                DOX:calibration_TA2 = 1.4399e-06 ;
                DOX:calibration_TA3 = 7.74929e-08 ;
                DOX:calibration_pcor = 0.011 ;
                DOX:calibration_Slope = 1. ;
                DOX:calibration_Offset = 0. ;
                DOX:ancillary_variables = "DOX_quality_control DOX_quality_control_loc" ;
        float DOX2(TIME) ;
                DOX2:_FillValue = NaNf ;
                DOX2:comment = "Oxygen, SBE 63 [umol/kg]" ;
                DOX2:units = "umol/kg" ;
                DOX2:calibration_SerialNumber = "1378" ;
                DOX2:calibration_CalibrationDate = "14-Apr-18" ;
                DOX2:calibration_A0 = 1.0513 ;
                DOX2:calibration_A1 = -0.0015 ;
                DOX2:calibration_A2 = 0.3562581 ;
                DOX2:calibration_B0 = -0.2447496 ;
                DOX2:calibration_B1 = 1.587353 ;
                DOX2:calibration_C0 = 0.1058147 ;
                DOX2:calibration_C1 = 0.004529342 ;
                DOX2:calibration_C2 = 6.31179e-05 ;
                DOX2:calibration_TA0 = 0.0007288568 ;
                DOX2:calibration_TA1 = 0.0002431142 ;
                DOX2:calibration_TA2 = 1.4399e-06 ;
                DOX2:calibration_TA3 = 7.74929e-08 ;
                DOX2:calibration_pcor = 0.011 ;
                DOX2:calibration_Slope = 1. ;
                DOX2:calibration_Offset = 0. ;
                DOX2:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                DOX2:long_name = "moles_of_oxygen_per_unit_mass_in_sea_water" ;
                DOX2:standard_name = "moles_of_oxygen_per_unit_mass_in_sea_water" ;
                DOX2:valid_max = 400.f ;
                DOX2:valid_min = 0.f ;
                DOX2:ancillary_variables = "DOX2_quality_control DOX2_quality_control_loc" ;
        float OXSOL(TIME) ;
                OXSOL:_FillValue = NaNf ;
                OXSOL:comment = "Oxygen Saturation, Garcia & Gordon [umol/kg]" ;
                OXSOL:units = "umol/kg" ;
                OXSOL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                OXSOL:long_name = "oxygen_solubility_per_unit_mass_in_seawater" ;
                OXSOL:valid_max = 400.f ;
                OXSOL:valid_min = 0.f ;
                OXSOL:ancillary_variables = "OXSOL_quality_control OXSOL_quality_control_loc" ;
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
        byte PRES_quality_control(TIME) ;
                PRES_quality_control:_FillValue = 99b ;
                PRES_quality_control:long_name = "quality flag for sea_water_pressure_due_to_sea_water" ;
                PRES_quality_control:standard_name = "sea_water_pressure_due_to_sea_water status_flag" ;
                PRES_quality_control:quality_control_conventions = "IMOS standard flags" ;
                PRES_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
                PRES_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
                PRES_quality_control:comment = "maximum of all flags" ;
        byte CNDC_quality_control(TIME) ;
                CNDC_quality_control:_FillValue = 99b ;
                CNDC_quality_control:long_name = "quality flag for sea_water_electrical_conductivity" ;
                CNDC_quality_control:standard_name = "sea_water_electrical_conductivity status_flag" ;
                CNDC_quality_control:quality_control_conventions = "IMOS standard flags" ;
                CNDC_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
                CNDC_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
                CNDC_quality_control:comment = "maximum of all flags" ;
        byte PSAL_quality_control(TIME) ;
                PSAL_quality_control:_FillValue = 99b ;
                PSAL_quality_control:long_name = "quality flag for sea_water_practical_salinity" ;
                PSAL_quality_control:standard_name = "sea_water_practical_salinity status_flag" ;
                PSAL_quality_control:quality_control_conventions = "IMOS standard flags" ;
                PSAL_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
                PSAL_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
                PSAL_quality_control:comment = "maximum of all flags" ;
        byte DENSITY_quality_control(TIME) ;
                DENSITY_quality_control:_FillValue = 99b ;
                DENSITY_quality_control:long_name = "quality flag for sea_water_density" ;
                DENSITY_quality_control:standard_name = "sea_water_density status_flag" ;
                DENSITY_quality_control:quality_control_conventions = "IMOS standard flags" ;
                DENSITY_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
                DENSITY_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
                DENSITY_quality_control:comment = "maximum of all flags" ;
        byte DOX_quality_control(TIME) ;
                DOX_quality_control:_FillValue = 99b ;
                DOX_quality_control:quality_control_conventions = "IMOS standard flags" ;
                DOX_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
                DOX_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
                DOX_quality_control:comment = "maximum of all flags" ;
        byte DOX2_quality_control(TIME) ;
                DOX2_quality_control:_FillValue = 99b ;
                DOX2_quality_control:long_name = "quality flag for moles_of_oxygen_per_unit_mass_in_sea_water" ;
                DOX2_quality_control:standard_name = "moles_of_oxygen_per_unit_mass_in_sea_water status_flag" ;
                DOX2_quality_control:quality_control_conventions = "IMOS standard flags" ;
                DOX2_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
                DOX2_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
                DOX2_quality_control:comment = "maximum of all flags" ;
        byte OXSOL_quality_control(TIME) ;
                OXSOL_quality_control:_FillValue = 99b ;
                OXSOL_quality_control:long_name = "quality flag for oxygen_solubility_per_unit_mass_in_seawater" ;
                OXSOL_quality_control:quality_control_conventions = "IMOS standard flags" ;
                OXSOL_quality_control:flag_values = 0b, 1b, 2b, 3b, 4b, 6b, 7b, 9b ;
                OXSOL_quality_control:flag_meanings = "unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value" ;
                OXSOL_quality_control:comment = "maximum of all flags" ;
        byte TEMP_quality_control_loc(TIME) ;
                TEMP_quality_control_loc:_FillValue = 99b ;
                TEMP_quality_control_loc:long_name = "in/out of water flag for sea_water_temperature" ;
                TEMP_quality_control_loc:units = "1" ;
                TEMP_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
        byte PRES_quality_control_loc(TIME) ;
                PRES_quality_control_loc:_FillValue = 99b ;
                PRES_quality_control_loc:long_name = "in/out of water flag for sea_water_pressure_due_to_sea_water" ;
                PRES_quality_control_loc:units = "1" ;
                PRES_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
        byte CNDC_quality_control_loc(TIME) ;
                CNDC_quality_control_loc:_FillValue = 99b ;
                CNDC_quality_control_loc:long_name = "in/out of water flag for sea_water_electrical_conductivity" ;
                CNDC_quality_control_loc:units = "1" ;
                CNDC_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
        byte PSAL_quality_control_loc(TIME) ;
                PSAL_quality_control_loc:_FillValue = 99b ;
                PSAL_quality_control_loc:long_name = "in/out of water flag for sea_water_practical_salinity" ;
                PSAL_quality_control_loc:units = "1" ;
                PSAL_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
        byte DENSITY_quality_control_loc(TIME) ;
                DENSITY_quality_control_loc:_FillValue = 99b ;
                DENSITY_quality_control_loc:long_name = "in/out of water flag for sea_water_density" ;
                DENSITY_quality_control_loc:units = "1" ;
                DENSITY_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
        byte DOX_quality_control_loc(TIME) ;
                DOX_quality_control_loc:_FillValue = 99b ;
                DOX_quality_control_loc:units = "1" ;
                DOX_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
        byte DOX2_quality_control_loc(TIME) ;
                DOX2_quality_control_loc:_FillValue = 99b ;
                DOX2_quality_control_loc:long_name = "in/out of water flag for moles_of_oxygen_per_unit_mass_in_sea_water" ;
                DOX2_quality_control_loc:units = "1" ;
                DOX2_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
        byte OXSOL_quality_control_loc(TIME) ;
                OXSOL_quality_control_loc:_FillValue = 99b ;
                OXSOL_quality_control_loc:long_name = "in/out of water flag for oxygen_solubility_per_unit_mass_in_seawater" ;
                OXSOL_quality_control_loc:units = "1" ;
                OXSOL_quality_control_loc:comment = "data flagged not deployed (6) when out of water" ;
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

// global attributes:
                :abstract = "Oceanographic and meteorological data from the Southern Ocean Time Series observatory in the Southern Ocean southwest of Tasmania" ;
                :acknowledgement = "Any users of IMOS data are required to clearly acknowledge the source of the material derived from IMOS in the format: \"Data was sourced from the Integrated Marine Observing System (IMOS) - IMOS is a national collaborative research infrastructure, supported by the Australian Government.\" If relevant, also credit other organisations involved in collection of this particular datastream (as listed in \'credit\' in the metadata record)." ;
                :author = "Jansen, Peter" ;
                :author_email = "peter.jansen@csiro.au" ;
                :citation = "The citation in a list of references is: \'IMOS [year-of-data-download], [Title], [data-access-URL], accessed [date-of-access]\'." ;
                :comment = "Geospatial vertical min/max information has been filled using the NOMINAL_DEPTH." ;
                :comment_ctd_post_dip = "sensor post-dipped on IN2021_V02 CTD #6" ;
                :comment_ctd_pre_dip = "sensor pre-dipped on IN2020_V09 CTD #8" ;
                :Conventions = "CF-1.6,IMOS-1.4" ;
                :data_centre = "Australian Ocean Data Network (AODN)" ;
                :data_centre_email = "info@aodn.org.au" ;
                :date_created = "2021-10-10T22:47:35Z" ;
                :deployment_code = "SOFS-9-2020" ;
                :disclaimer = "Data, products and services from IMOS are provided \"as is\" without any warranty as to fitness for a particular purpose." ;
                :featureType = "timeSeries" ;
                :file_version_quality_control = "Raw data is defined as unprocessed data and data products that have not undergone quality control. The data may be in engineering physical units, time and location details can be in relative units and values can be pre-calibration measurements." ;
                :firmware_date = "May 20 2013 08:04:08" ;
                :firmware_version = "2.3.1" ;
                :geospatial_lat_max = -46.98476 ;
                :geospatial_lat_min = -46.98476 ;
                :geospatial_lat_units = "degrees_north" ;
                :geospatial_lon_max = 141.81169 ;
                :geospatial_lon_min = 141.81169 ;
                :geospatial_lon_units = "degrees_east" ;
                :geospatial_vertical_max = 200. ;
                :geospatial_vertical_min = 200. ;
                :geospatial_vertical_positive = "down" ;
                :institution = "DWM-SOTS" ;
                :institution_references = "http://www.imos.org.au/aodn.html" ;
                :instrument = "Sea-Bird Electronics ; SBE37SMP-ODO-RS232" ;
                :instrument_model = "SBE37SMP-ODO-RS232" ;
                :instrument_nominal_depth = 200. ;
                :instrument_serial_number = "03714700" ;
                :keywords_vocabulary = "IMOS parameter names. See https://github.com/aodn/imos-toolbox/blob/master/IMOS/imosParameters.txt" ;
                :license = "http://creativecommons.org/licenses/by/4.0/" ;
                :manufacture_date = "31-MAY-2016" ;
                :naming_authority = "IMOS" ;
                :platform_code = "SOFS" ;
                :principal_investigator = "Trull, Tom; Shulz, Eric; Shadwick, Elizabeth" ;
                :principal_investigator_email = "tom.trull@csiro.au; eshulz@bom.gov.au; elizabeth.shadwick@csiro.au" ;
                :project = "Integrated Marine Observing System (IMOS)" ;
                :sea_bird_data_conversion_01_datcnv_date = "Apr 25 2021 19:10:49, 7.26.7.129 [datcnv_vars = 9]" ;
                :sea_bird_data_conversion_02_datcnv_in = "C:\\Users\\jan079\\SBE37SMP-ODO-RS232_03714700_2021_04_25.hex C:\\Users\\jan079\\SBE37SMP-ODO-RS232_03714700_2021_04_25.XMLCON" ;
                :sea_bird_data_conversion_03_datcnv_skipover = "0" ;
                :site_code = "SOTS" ;
                :site_nominal_depth = 4624. ;
                :standard_name_vocabulary = "NetCDF Climate and Forecast (CF) Metadata Convention Standard Name Table 67" ;
                :time_coverage_end = "2021-04-25T03:01:01Z" ;
                :time_coverage_start = "2020-03-27T00:00:01Z" ;
                :time_deployment_end = "2021-04-24T00:00:00Z" ;
                :time_deployment_start = "2020-09-01T03:36:38Z" ;
                :title = "Oceanographic mooring data deployment of SOFS at latitude -47.0 longitude 141.8 depth 200 (m) instrument Sea-Bird Electronics ; SBE37SMP-ODO-RS232 serial 03714700" ;
                :voyage_deployment = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=IN2020_V09" ;
                :voyage_recovery = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=IN2021_V02" ;
                :file_version = "Level 1 - Quality Controlled Data" ;
                :history = "2021-10-10 created from file SBE37SMP-ODO-RS232_03714700_2021_04_25.cnv\n2021-10-10 attributes added from file(s) [metadata/pulse-saz-sofs-flux.metadata.csv, metadata/imos.metadata.csv, metadata/sots.metadata.csv, metadata/sofs.metadata.csv, metadata/asimet.metadata.csv, metadata/variable.metadata.csv]\n2021-10-10 : quality_control variables added.\n2021-10-10 : in/out marked 3811\n2021-10-10 TEMP global range min = -2 max = 30 marked 0.0\n2021-10-10 TEMP global range min = 5 max = 16 marked 0.0\n2021-10-10 TEMP spike height = 2 marked 0\n2021-10-10 TEMP max rate = 80 marked 0\n2021-10-10 CNDC global range min = 3 max = 4.5 marked 0.0\n2021-10-10 PSAL global range min = 2 max = 41 marked 0.0\n2021-10-10 PSAL global range min = 34 max = 35.5 marked 0.0\n2021-10-10 PSAL spike height = 0.4 marked 1\n2021-10-10 PSAL max rate = 30 marked 0" ;
                :references = "http://www.imos.org.au; Jansen P, Weeding B, Shadwick EH and Trull TW (2020). Southern Ocean Time Series (SOTS) Quality Assessment and Control Report Temperature Records Version 1.0. CSIRO, Australia. DOI: 10.26198/gfgr-fq47 (https://doi.org/10.26198/gfgr-fq47)" ;
}
```

## Re-sample

    python3 ocean_dp\processing\resampler.py data\SOFS-9-2020\IMOS*.nc

This creates the variable

        float TEMP_SAMPLE_TIME_DIFF(TIME) ;        
                SAMPLE_TIME_DIFF:_FillValue = NaNf ;
                SAMPLE_TIME_DIFF:comment = "seconds to actual sample timestamp" ;

Which is the time to the nearest good (or probably good) sample

## Combine into sqllite database

This can include any other data sources (PAR, FLNTUS, pCO2, ...)

    python3 ocean_dp\dbms\load_sqlite_db.py data\SOFS-9-2020\resample\IMOS*.nc

## Create hourly sampled netCDF gridded file

    python3 ocean_dp\dbms\create_from_sqlite_db.py SOFS-9-2020.sqlite

````
ncdump -h SOFS-9-2020.sqlite.nc
netcdf SOFS-9-2020.sqlite {
dimensions:
        FILE_NAME = 4 ;
        strlen = 256 ;
        INSTANCE_CNDC = 4 ;
        INSTANCE_DENSITY = 4 ;
        INSTANCE_DOX2 = 4 ;
        INSTANCE_PRES = 4 ;
        INSTANCE_PSAL = 4 ;
        INSTANCE_TEMP = 4 ;
        TIME = 5636 ;
variables:
        char FILE_NAME(FILE_NAME, strlen) ;
        char INSTRUMENT(FILE_NAME, strlen) ;
        int IDX_FILE_NAME(FILE_NAME) ;
        int DEPTH_FILE_NAME(FILE_NAME) ;
        int IDX_CNDC(INSTANCE_CNDC) ;
        int IDX_DENSITY(INSTANCE_DENSITY) ;
        int IDX_DOX2(INSTANCE_DOX2) ;
        int IDX_PRES(INSTANCE_PRES) ;
        int IDX_PSAL(INSTANCE_PSAL) ;
        int IDX_TEMP(INSTANCE_TEMP) ;
        float DEPTH_CNDC(INSTANCE_CNDC) ;
        float DEPTH_DENSITY(INSTANCE_DENSITY) ;
        float DEPTH_DOX2(INSTANCE_DOX2) ;
        float DEPTH_PRES(INSTANCE_PRES) ;
        float DEPTH_PSAL(INSTANCE_PSAL) ;
        float DEPTH_TEMP(INSTANCE_TEMP) ;
        double TIME(TIME) ;
                TIME:long_name = "time" ;
                TIME:units = "days since 1950-01-01 00:00:00 UTC" ;
                TIME:calendar = "gregorian" ;
                TIME:axis = "T" ;
        float CNDC(INSTANCE_CNDC, TIME) ;
                CNDC:_FillValue = NaNf ;
                CNDC:calibration_CPcor = -9.57e-08 ;
                CNDC:calibration_CTcor = 3.25e-06 ;
                CNDC:calibration_Offset = 0. ;
                CNDC:calibration_Slope = 1. ;
                CNDC:calibration_UseG_J = 1. ;
                CNDC:comment = "Conductivity [S/m]" ;
                CNDC:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                CNDC:long_name = "sea_water_electrical_conductivity" ;
                CNDC:name = "Water conductivity" ;
                CNDC:standard_name = "sea_water_electrical_conductivity" ;
                CNDC:units = "S/m" ;
                CNDC:valid_max = 50000.f ;
                CNDC:valid_min = 0.f ;
        float DENSITY(INSTANCE_DENSITY, TIME) ;
                DENSITY:_FillValue = NaNf ;
                DENSITY:comment = "Density [density, kg/m^3]" ;
                DENSITY:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                DENSITY:long_name = "sea_water_density" ;
                DENSITY:standard_name = "sea_water_density" ;
                DENSITY:units = "kg/m^3" ;
                DENSITY:valid_max = 1200.f ;
                DENSITY:valid_min = 0.f ;
        float DOX2(INSTANCE_DOX2, TIME) ;
                DOX2:_FillValue = NaNf ;
                DOX2:calibration_A0 = 1.0513 ;
                DOX2:calibration_A1 = -0.0015 ;
                DOX2:calibration_Offset = 0. ;
                DOX2:calibration_Slope = 1. ;
                DOX2:calibration_pcor = 0.011 ;
                DOX2:comment = "Oxygen, SBE 63 [umol/kg]" ;
                DOX2:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                DOX2:long_name = "moles_of_oxygen_per_unit_mass_in_sea_water" ;
                DOX2:standard_name = "moles_of_oxygen_per_unit_mass_in_sea_water" ;
                DOX2:units = "umol/kg" ;
                DOX2:valid_max = 400.f ;
                DOX2:valid_min = 0.f ;
        float PRES(INSTANCE_PRES, TIME) ;
                PRES:_FillValue = NaNf ;
                PRES:applied_offset = -10.1353f ;
                PRES:calibration_Offset = 0. ;
                PRES:calibration_PTCB2 = 0. ;
                PRES:comment = "Pressure, Strain Gauge [db]" ;
                PRES:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                PRES:long_name = "sea_water_pressure_due_to_sea_water" ;
                PRES:standard_name = "sea_water_pressure_due_to_sea_water" ;
                PRES:units = "dbar" ;
                PRES:valid_max = 12000.f ;
                PRES:valid_min = -15.f ;
        float PSAL(INSTANCE_PSAL, TIME) ;
                PSAL:_FillValue = NaNf ;
                PSAL:comment = "Salinity, Practical [PSU]" ;
                PSAL:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                PSAL:long_name = "sea_water_practical_salinity" ;
                PSAL:standard_name = "sea_water_practical_salinity" ;
                PSAL:units = "1" ;
                PSAL:valid_max = 41.f ;
                PSAL:valid_min = 2.f ;
        float TEMP(INSTANCE_TEMP, TIME) ;
                TEMP:_FillValue = NaNf ;
                TEMP:calibration_Offset = 0. ;
                TEMP:calibration_Slope = 1. ;
                TEMP:comment = "Temperature [ITS-90, deg C]" ;
                TEMP:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                TEMP:long_name = "sea_water_temperature" ;
                TEMP:name = "sea_water_temperature" ;
                TEMP:reference_scale = "ITS-90" ;
                TEMP:standard_name = "sea_water_temperature" ;
                TEMP:units = "degrees_Celsius" ;
                TEMP:valid_max = 40.f ;
                TEMP:valid_min = -2.5f ;

// global attributes:
                :Conventions = "CF-1.6,IMOS-1.4" ;
                :abstract = "Oceanographic and meteorological data from the Southern Ocean Time Series observatory in the Southern Ocean southwest of Tasmania" ;
                :acknowledgement = "Any users of IMOS data are required to clearly acknowledge the source of the material derived from IMOS in the format: \"Data was sourced from the Integrated Marine Observing System (IMOS) - IMOS is a national collaborative research infrastructure, supported by the Australian Government.\" If relevant, also credit other organisations involved in collection of this particular datastream (as listed in \'credit\' in the metadata record)." ;
                :author = "Jansen, Peter" ;
                :author_email = "peter.jansen@csiro.au" ;
                :citation = "The citation in a list of references is: \'IMOS [year-of-data-download], [Title], [data-access-URL], accessed [date-of-access]\'." ;
                :comment = "Geospatial vertical min/max information has been filled using the NOMINAL_DEPTH." ;
                :comment_ctd_post_dip = "sensor post-dipped on IN2021_V02 CTD #6" ;
                :comment_ctd_pre_dip = "sensor pre-dipped on IN2020_V09 CTD #8" ;
                :data_centre = "Australian Ocean Data Network (AODN)" ;
                :data_centre_email = "info@aodn.org.au" ;
                :date_created = "2021-10-10T22:56:12Z" ;
                :deployment_code = "SOFS-9-2020" ;
                :disclaimer = "Data, products and services from IMOS are provided \"as is\" without any warranty as to fitness for a particular purpose." ;
                :featureType = "timeSeries" ;
                string :file_version = "Level 2 ÔÇô Derived Products" ;
                :file_version_quality_control = "Raw data is defined as unprocessed data and data products that have not undergone quality control. The data may be in engineering physical units, time and location details can be in relative units and values can be pre-calibration measurements." ;
                :firmware_date = "May 20 2013 08:04:08" ;
                :firmware_version = "2.3.1" ;
                :geospatial_lat_max = -46.98476 ;
                :geospatial_lat_min = -46.98476 ;
                :geospatial_lat_units = "degrees_north" ;
                :geospatial_lon_max = 141.81169 ;
                :geospatial_lon_min = 141.81169 ;
                :geospatial_lon_units = "degrees_east" ;
                :geospatial_vertical_positive = "down" ;
                :institution = "DWM-SOTS" ;
                :institution_references = "http://www.imos.org.au/aodn.html" ;
                :instrument = "Sea-Bird Electronics ; SBE37SMP-ODO-RS232" ;
                :instrument_model = "SBE37SMP-ODO-RS232" ;
                :keywords_vocabulary = "IMOS parameter names. See https://github.com/aodn/imos-toolbox/blob/master/IMOS/imosParameters.txt" ;
                :license = "http://creativecommons.org/licenses/by/4.0/" ;
                :naming_authority = "IMOS" ;
                :platform_code = "SOFS" ;
                :principal_investigator = "Trull, Tom; Shulz, Eric; Shadwick, Elizabeth" ;
                :principal_investigator_email = "tom.trull@csiro.au; eshulz@bom.gov.au; elizabeth.shadwick@csiro.au" ;
                :project = "Integrated Marine Observing System (IMOS)" ;
                :references = "http://www.imos.org.au; Jansen P, Weeding B, Shadwick EH and Trull TW (2020). Southern Ocean Time Series (SOTS) Quality Assessment and Control Report Temperature Records Version 1.0. CSIRO, Australia. DOI: 10.26198/gfgr-fq47 (https://doi.org/10.26198/gfgr-fq47)" ;
                :sea_bird_data_conversion_03_datcnv_skipover = "0" ;
                :site_code = "SOTS" ;
                :site_nominal_depth = 4624. ;
                :standard_name_vocabulary = "NetCDF Climate and Forecast (CF) Metadata Convention Standard Name Table 67" ;
                :time_coverage_end = "2021-04-23T23:00:00Z" ;
                :time_coverage_start = "2020-09-01T04:00:00Z" ;
                :time_deployment_end = "2021-04-24T00:00:00Z" ;
                :time_deployment_start = "2020-09-01T03:36:38Z" ;
                :voyage_deployment = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=IN2020_V09" ;
                :voyage_recovery = "http://www.cmar.csiro.au/data/trawler/survey_details.cfm?survey=IN2021_V02" ;
}

````

rename this to an IMOS FV02 file

## create QC variable counts

## create temperature Water fall plot

## create Salinity Stacked plot

