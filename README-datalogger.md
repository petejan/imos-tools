# Processing data logger files

# GPS data
## create GPS netCDF from TXT files
```
python -m ocean_dp.parse.dataloggergps2netCDF datalogger/*.TXT
```
file should contain XPOS and YPOS

## add SOTS attributes
```
python -m ocean_dp.sots.process_SOTS_toIMOS LOG00241-gps.TXT.nc
```

# MRU data
## process the MRU data
```
python -m ocean_dp.parse.datalogger2netCDF datalogger/*.TXT
```

## add SOTS attributes
```
python -m ocean_dp.sots.process_SOTS_toIMOS LOG00241.TXT.nc
```

## add calculate waves from the MRU data
```
python -m ocean_dp.processing.mru_2_waves IMOS_DWM-SOTS_OST_20250302_SOFS_FV00_SOFS-14-2025-Optode-4831-506-1m_END-20250706_C-20261105.nc
```

# Oxygen
## create netCDF from datalogger files
```
python -m ocean_dp.parse.optode2netCDF datalogger/*.TXT
```
file should contain BPHASE and OTEMP variables

## add calibration to BPHASE for conversion to oyxgen
```
python -m ocean_dp.attribution.add_optode_cal LOG00241.TXT-optode.nc ~/CSIRO/OneDrive\ -\ CSIRO/SOTS/2025-data-processing/SOFS-14-2025/CSV_calcoef_T-opt_4831_506_CMAR_cal-B32svs.txt
```

## add PSAL and water TEMP to file
use the FV00 file here as merging the FV01 file creates _quality_control variables, which fail the conformance check for FV00 files. 
```
python -m ocean_dp.processing.merge_resample -VARS TEMP,PSAL LOG00241.TXT-optode.nc IMOS_DWM-SOTS_CPST_20250303_SOFS_FV01_SOFS-14-2025-SBE37SM-RS485-03720164-0m_END-20250710_C-20251028.nc
```
File should now also have PSAL and TEMP

## calculate oyxgen
```
python -m ocean_dp.processing.calc_optode_oxygen LOG00241.TXT-optode.nc 
```
File should now have DOX2_RAW

## correct DOX2_RAW for salinity and pressure
```
python -m ocean_dp.processing.correct_dox2 LOG00241.TXT-optode.nc
```

## add SOTS attributes
```
python ~/GitHub/imos-tools/ocean_dp/sots/process_SOTS_toIMOS.py SOFS-13-optode.csv.nc
```
