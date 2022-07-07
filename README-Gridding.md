# File gridding
Temperature and Salinity data processing and QC

collect files to go into the gridding, eg
```
copy ..\..\cloudstor\SOTS-Temp-Raw-Data\netCDF\IMOS*FV01*SOFS-10*.nc .
```

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
This can be used directly in MatLAB

```matlab
    % time variable
    time = ncread(fn, 'TIME') + datetime(1950,1,1);       

    % read the temperature from the file and some of its metadata
    temp = ncread(fn, 'TEMP');     
    temp_qc = ncread(fn, 'TEMP_quality_control');     
    temp_name = ncreadatt(fn, 'TEMP', 'long_name');
    temp_units = ncreadatt(fn, 'TEMP', 'units');
	
    msk = temp_qc <= 2; % create a mask for only good data
	
    plot(time(msk), temp(msk)); % plot only good data
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

This can be used directly in MatLAB,

```matlab
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


## bin data to WOA depth bins

```matlab

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

### example with vertical filling and plot

```matlab
% take list of time gridded files, 
% grid into 'standard' depth levels, and standard times

files = dir("SOFS*.sqlite.nc");

figure(1); clf;
figure(2); clf;

% setup standard level grid
std_level = [0:5:100, 125:25:500, 550:50:2000, 2100:100:5500];
std_level_gap = diff(std_level);
std_level(1) = 0.5;
edges = std_level - [0 std_level_gap]/2;
edges(1) = -10;

% storage of output flat arrays
time_flat = [];
temp_flat = [];
pres_flat = [];

mld = [];

time_n = [];

var='TEMP';

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

time_grid = min(time_flat):hours(24)*10:max(time_flat);

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

% create a scatter plot of the binned data
figure(1); clf
for i=1:numel(std_level)
    if ~all(isnan(temp_binned(:, i)))
        scatter(time_grid, std_level(i)'-zeros(size(temp_binned(:, i))), 5, temp_binned(:, i), 'filled'); hold on
    end
end
f=figure(1);
axis 'ij'
if (strcmp(var, 'TEMP'))
    colormap('jet');
end
a=gca;
a.YTick=[10 20 50 100 200 500 1000 2000 5000];
a.YAxis.MinorTickValues = std_level;
grid on
ylim([0.4 6000]);
%a.YScale='log';
a.FontSize=8;

ylabel('pressure (dbar)');
xlim([datetime(2009,1,1) datetime(2022,10,1)])
f.Units = 'pixels';
f.Position(3:4) = [800 600];
c = colorbar;
c.Label.String = [strrep(var_name, '_', ' ') ' (' strrep(var_units, 'degrees_', '\circ ') ')'];
ylim([0 550]);
caxis([8 15]);

% create a binned mix-layer-depth
t_mld_disc = discretize(time_n, time_grid);
msk_mld = not(isnan(mld) | isnan(t_mld_disc));
mld_binned = accumarray(t_mld_disc(msk_mld), mld(msk_mld), [size(time_grid, 2) 1], @mean, nan);

plot(time_grid, mld_binned, 'Color', [0.5 0.5 0.5]);

% interpolate vertcially each timestep
interp_level = 5:10:500;
temp_interp = nan([size(interp_level, 2) size(temp_binned, 1)]);
for i=1:size(temp_binned,1)
    good_data = ~isnan(temp_binned(i, :));
    if (sum(good_data) > 2)
        temp_interp(:, i) = interp1(std_level(good_data), temp_binned(i, good_data), interp_level, 'linear');
    end
end

% create the vertical filled plot with contours
f=figure(2); clf
colormap('jet');
imagesc(datenum(time_grid), interp_level, temp_interp, 'AlphaData',~isnan(temp_interp)); axis 'ij'
datetick('x', 'keeplimits');
hold on
contour(datenum(time_grid), interp_level, temp_interp, [9 10 11 12], 'k');
c = colorbar;
c.Label.String = [strrep(var_name, '_', ' ') ' (' strrep(var_units, 'degrees_', '\circ ') ')'];
ylabel('pressure (dbar)');
f.Units = 'pixels';
f.Position(3:4) = [1800 600];
caxis([8 15]);

%print( figure(2), '-dpng', 'SOTS-Gridded-10d-Temp-contour.png' , '-r600');

%print( figure(1), '-dpng', ['SOTS-' var '-scatter-griddedx2.png'] , '-r600');

% out_file = 'SOFS-Gridded-All.nc';
% 
% mySchema.Name   = '/';
% mySchema.Format = 'classic';
% mySchema.Dimensions(1).Name   = 'TIME';
% mySchema.Dimensions(1).Length = n_time;
% mySchema.Dimensions(2).Name   = 'DEPTH';
% mySchema.Dimensions(2).Length = n_depth;
% 
% mySchema.Variables(1).Name   = 'TIME';
% mySchema.Variables(1).Dimensions(1).Name   = 'TIME';
% mySchema.Variables(1).Dimensions(1).Length   = n_time;
% mySchema.Variables(1).Datatype   = 'double';
% mySchema.Variables(2).Name   = 'DEPTH';
% mySchema.Variables(2).Dimensions(1).Name   = 'DEPTH';
% mySchema.Variables(2).Dimensions(1).Length   = n_depth;
% mySchema.Variables(2).Datatype   = 'double';
% mySchema.Variables(3).Name   = 'TEMP';
% mySchema.Variables(3).Dimensions(1).Name   = 'TIME';
% mySchema.Variables(3).Dimensions(1).Length   = n_time;
% mySchema.Variables(3).Dimensions(2).Name   = 'DEPTH';
% mySchema.Variables(3).Dimensions(2).Length   = n_depth;
% mySchema.Variables(3).Datatype   = 'single';
% 
% ncwriteschema(out_file, mySchema);
% 
% 
% ncwrite(out_file, 'TIME', datenum(time_grid) - datenum(1950,1,1));
% ncwrite(out_file, 'DEPTH', std_level);
% ncwrite(out_file, 'TEMP', temp_binned);
	
```
