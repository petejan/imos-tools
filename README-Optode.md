# Processing optode bphase/temperature 

## create netCDF from csv file
```
python ~/GitHub/imos-tools/ocean_dp/parse/csv2netCDF.py '-model=Optode 4831' -serial=509 SOFS-13-optode.csv
```
file should contain BPHASE and OTEMP variables

## add calibration to BPHASE for conversion to oyxgen
```
python ~/GitHub/imos-tools/ocean_dp/attribution/add_optode_cal.py SOFS-13-optode.csv.nc CSV_calcoef_T-opt_4831_509_CMAR_cal-B32svs.txt
```

## add PSAL and water TEMP to file
use the FV00 file here as merging the FV01 file creates _quality_control variables, which fail the conformance check for FV00 files. 
```
python ~/GitHub/imos-tools/ocean_dp/processing/merge_resample.py -VARS TEMP,PSAL SOFS-13-optode.csv.nc IMOS_DWM-SOTS_CPST_20240313_SOFS_FV01_SOFS-13-2024-SBE37SM-RS485-03708765-1m_END-20250415_C-20250529.nc
```
File should now also have PSAL and TEMP

## calculate oyxgen
```
python ~/GitHub/imos-tools/ocean_dp/processing/calc_optode_oxygen.py SOFS-13-optode.csv.nc
```
File should now have DOX2_RAW

## correct DOX2_RAW for salinity and pressure
```
python ~/GitHub/imos-tools/ocean_dp/processing/correct_dox2.py SOFS-13-optode.csv.nc 
```

## add SOTS attributes
```
python ~/GitHub/imos-tools/ocean_dp/sots/process_SOTS_toIMOS.py SOFS-13-optode.csv.nc
```
