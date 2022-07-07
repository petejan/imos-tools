# File gridding
Temperature and Salinity data processing and QC

collect files to go into the gridding, eg

copy ..\..\cloudstor\SOTS-Temp-Raw-Data\netCDF\IMOS*FV01*SOFS-10*.nc .

## Re-sample

	mkdir resample
    python3 ocean_dp\processing\down_sample.py IMOS*.nc

This creates the netCDF file like

```
netcdf IMOS_DWM-SOTS_COPST_20210420_SOFS_FV02_SOFS-10-2021-SBE37SMP-ODO-RS232-03709513-125m-mean_END-20220512_C-20220706 {
dimensions:
        TIME = 9299 ;
variables:
        double TIME(TIME) ;
                TIME:long_name = "time" ;
                TIME:units = "days since 1950-01-01 00:00:00 UTC" ;
                TIME:calendar = "gregorian" ;
                TIME:axis = "T" ;
                TIME:standard_name = "time" ;
                TIME:valid_max = 90000. ;
                TIME:valid_min = 0. ;
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
        double LONGITUDE ;
                LONGITUDE:axis = "X" ;
                LONGITUDE:long_name = "longitude" ;
                LONGITUDE:reference_datum = "WGS84 geographic coordinate system" ;
                LONGITUDE:standard_name = "longitude" ;
                LONGITUDE:units = "degrees_east" ;
                LONGITUDE:valid_max = 180. ;
                LONGITUDE:valid_min = -180. ;
        float CNDC(TIME) ;
                CNDC:_FillValue = NaNf ;
                CNDC:comment = "Conductivity [S/m]" ;
                CNDC:units = "S/m" ;
                CNDC:calibration_SerialNumber = "9513" ;
                CNDC:calibration_CalibrationDate = "17-Nov-20" ;
                CNDC:calibration_UseG_J = 1. ;
                CNDC:calibration_G = -0.9750336 ;
                CNDC:calibration_H = 0.1390885 ;
                CNDC:calibration_I = -0.0003036082 ;
                CNDC:calibration_J = 4.247957e-05 ;
                CNDC:calibration_CPcor = -9.57e-08 ;
                CNDC:calibration_CTcor = 3.25e-06 ;
                CNDC:calibration_WBOTC = 4.084195e-07 ;
                CNDC:calibration_Slope = 1. ;
                CNDC:calibration_Offset = 0. ;
                CNDC:name = "Water conductivity" ;
                CNDC:standard_name = "sea_water_electrical_conductivity" ;
                CNDC:long_name = "sea_water_electrical_conductivity" ;
                CNDC:valid_min = 0.f ;
                CNDC:valid_max = 40.f ;
                CNDC:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH" ;
                CNDC:ancillary_variables = "CNDC_quality_control CNDC_standard_error CNDC_number_of_observations" ;
        float CNDC_standard_error(TIME) ;
                CNDC_standard_error:_FillValue = NaNf ;
                CNDC_standard_error:comment = "sample bin standard deviation" ;
        float CNDC_number_of_observations(TIME) ;
                CNDC_number_of_observations:_FillValue = NaNf ;
                CNDC_number_of_observations:comment = "number of samples" ;
        byte CNDC_quality_control(TIME) ;
                CNDC_quality_control:_FillValue = 99b ;
                CNDC_quality_control:comment = "maximum of quality flags of input data" ;
				
```

This can be used directly in MatLAB,

```
	fn = 'SOFS-10-2021.sqlite.nc'; % contains the file to use
	var = 'TEMP'; % just for convienence
	
    % time variable
    time = ncread(fn, 'TIME') + datetime(1950,1,1);       

    % pressure variable (PRES_ALL has presssure for every instrument)
    pres = ncread(fn, 'PRES_ALL');   

	temp = ncread(fn, var);     
	var_name = ncreadatt(fn, var, 'long_name');
	var_units = ncreadatt(fn, var, 'units');
	
	temp_idx = ncread(fn, ['IDX_' var]);
	nominal_depth = ncread(fn, 'NOMINAL_DEPTH');
	temp_depth = nominal_depth(temp_idx+1);

	pres_temp = pres(:, temp_idx+1); % contains a pressure at each timestamp
	
	instrument = ncread(fn, 'INSTRUMENT');
	instrument_temp = instrument(:,temp_idx+1)'; % contains the instrument the data came from
```

## Combine into sqllite database

This can include any other data sources (PAR, FLNTUS, pCO2, ...)

    python3 ocean_dp\dbms\load_sqlite_db.py resample\IMOS*.nc

## Create hourly sampled netCDF gridded file

    python3 ocean_dp\dbms\create_from_sqlite_db.py SOFS-9-2020.sqlite
	
## add pressures to file for each instrument

	python3 ocean_dp\processing\calc_depth.py SOFS-10-2021.sqlite.nc

## add mix-layer depth calculation

python3 ocean_dp\processing\calc_mld.py SOFS-10-2021.sqlite.nc

## bin data to WOA depth bins

```

files = dir("*.sqlite.nc"); % get list of files to use

figure(1); clf;

std_level = [0:5:100, 125:25:500, 550:50:2000, 2100:100:5500];
std_level_gap = diff(std_level);
std_level(1) = 0.5;
edges = std_level - [0 std_level_gap]/2;
edges(1) = -10;

% create storage for output variables
time_flat = [];
temp_flat = [];
pres_flat = [];
mld = [];
time_n = [];

var='PSAL';

for a = 1:numel(files)
    
    fn = [files(a).folder '/' files(a).name];
    disp(files(a).name);

    % time variable
    time = ncread(fn, 'TIME') + datetime(1950,1,1);       

    % pressure variable (PRES_ALL has presssure for every instrument)
    pres = ncread(fn, 'PRES_ALL');   

    try
        % temperature variable

        temp = ncread(fn, var);     
        var_name = ncreadatt(fn, var, 'long_name');
        var_units = ncreadatt(fn, var, 'units');
        
        temp_idx = ncread(fn, ['IDX_' var]);
        nominal_depth = ncread(fn, 'NOMINAL_DEPTH');
        temp_depth = nominal_depth(temp_idx+1);

        % create a flat array of time, temperature and pressure
        % so we have a vector of times, temperature, pressure observations
        time_mat = repmat(time, size(temp_depth, 1), 1);
        time_flat = [time_flat; reshape(time_mat, 1, [])'];

        temp_flat = [temp_flat; reshape(temp, [] ,1)];

        pres_temp = pres(:, temp_idx+1);
        pres_flat = [pres_flat; reshape(pres_temp, [] ,1)];

        mld_n = nan(size(time));
        try
            mld_n = ncread(fn, 'MLD');
        catch
        end
        mld = [mld; mld_n];
        time_n = [time_n; time];
    catch
    end
    
end

time_grid = min(time_flat):hours(24):max(time_flat);

%pres_flat(pres_flat>4500) = nan;

% find bin for each depth
d_disc = discretize(pres_flat, edges);

% find bin for each time
t_disc = discretize(time_flat, time_grid);

% mask only good current data, within time and depth grid
msk = not(isnan(d_disc) | isnan(t_disc));% | isnan(temp_flat));

% create a vector of subscripts within the time/depth_grid that the sample goes into
subs = [t_disc(msk) d_disc(msk)];
% find mean within each bin
temp_binned = accumarray(subs, temp_flat(msk), [size(time_grid,2) size(edges,2)], @mean, nan);

pres_binned = accumarray(subs, pres_flat(msk), [size(time_grid,2) size(edges,2)], @mean, nan);

```

	

