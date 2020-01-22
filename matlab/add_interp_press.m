% SOTS Pressure interpolator

% This code imports pressure data from an aggregated file (constructed by
% P.Jansen), and creates interpolated pressure records for FV00 raw
% instrument files - firstly by interpolating along time series of pressure
% readings in the aggregate file to find pressures at each time in a
% particular FV00 file, and secondly by interpolating down nominal depths at
% each timestamp to find a pressure value for each FV00 timestamp.

% Ben Weeding - ben.weeding.26@gmail.com

%% Load the filenames

fv00_files = dir('*FV00*.nc');
agg_files = dir('*Aggregate*.nc');

%% Load the pressure data

agg_pres = ncread(agg_files.name,'PRES');
agg_pres_info = ncinfo(agg_files.name, 'PRES');
agg_instrument_index = ncread(agg_files.name,'instrument_index');
agg_nominal_depth  = ncread(agg_files.name,'NOMINAL_DEPTH');
agg_time = ncread(agg_files.name,'TIME');

%% Create a scattered interpolant from the aggregate data

% This was an error, as it interpolated in 2D space rather than twice in 1D


% Subsampled every 10 points for speed of execution at this point
%scat_interp_pres = scatteredInterpolant(agg_time(1:10:end),agg_nominal_depth(agg_instrument_index(1:10:end)+1),agg_pres(1:10:end));

%% Interpolate the pressure and write the data into the FV00 file

% Loop through each of the fv00 files
for i=1:length(fv00_files)
    
    disp(fv00_files(i).name)
    
    % Extract the content from the FV00 file
    fv00_contents = ncinfo(fv00_files(i).name);
    
    % Check if the FV00 file contains pressure data, run the interpolation
    % code if not
    
    if (sum(contains({fv00_contents.Variables(:).Name}, 'PRES')) == 0)
        
        % Load the FV00 data requiring pressure        
        %'days since 1950-01-01 00:00:00 UTC' for minilog T        
        fv00_time = ncread(fv00_files(i).name,'TIME');        
        fv00_depth = ncread(fv00_files(i).name,'NOMINAL_DEPTH');
  
        % Interpolate the agg pressure records at each nominal depth to
        % provide pressure values at each timestamp in the current FV00
        % file
        
        interp_agg_pres = nan(length(agg_nominal_depth),length(fv00_time));
        
        % Loop through each nominal depth in the aggregate file, and get pressure for the FV00 file's time       
        for j = 1:length(agg_nominal_depth)
            
            % Select the relevant time and pressures
            
            time_selection = agg_time(agg_instrument_index == (j-1));            
            pres_selection = agg_pres(agg_instrument_index == (j-1));
                        
            % Interpolate along each nominal depth

            interp_agg_pres(j,:) = interp1(time_selection,pres_selection,fv00_time);
        end
        
        % At each timestamp in the FV00 record, interpolate a pressure
        % value based on the FV00 nominal depth, and the interpolated
        % pressures in interp_agg_pres. Using spline interpolation here to 
        % deal with NaN values that occur in failed pressure sensors.
        pres_interp_dummy = nan(size(fv00_time));      
        
        for l = 1:length(fv00_time)               
            
            pres_interp_dummy(l) = interp1(agg_nominal_depth,interp_agg_pres(:,l),fv00_depth,'spline');                
        
        end            
        
        pres_interp = pres_interp_dummy;
        
        % Create an FV01 version of the current FV00 file
        
        % Create the new FV01 file name
        
        fv01_name = strrep(fv00_files(i).name,'FV00','FV01');        
        fv01_name(end-10:end-3)=datestr(now,'yyyymmdd');
        
        % Write the FV00 data into the FV01 file        
        ncwriteschema(fv01_name, fv00_contents);
        
        % copy variable data to new file
        for v = fv00_contents.Variables
            ncwrite(fv01_name, v.Name, ncread(fv00_files(i).name, v.Name));
        end
        
        % Modify the global attributes of the file to record processing,
        % and add to the file history
        
        ncwriteatt(fv01_name,'/','file_version','Level 1 - partially processed');
        hist = ncreadatt(fv00_files(i).name, '/', 'history');
        ncwriteatt(fv01_name,'/','history',[hist newline datestr(now,'yyyy-mm-dd') ' : Added interpolated pressure from ' agg_files.name]);
        
        % Add and populate a PRES variable to the FV01 file
        nccreate(fv01_name, 'PRES', 'Dimensions',{'TIME',size(pres_interp,1)}, 'FillValue',NaN);
        ncwrite(fv01_name, 'PRES', pres_interp);
                
        % copy attributes from agg file to output file
        pres_atts = agg_pres_info.Attributes; % get all attribtes from the aggregate file
        for k=1:length(pres_atts)
            if (strcmp(pres_atts(k).Name, '_FillValue') == 0)
                ncwriteatt(fv01_name, 'PRES', pres_atts(k).Name, pres_atts(k).Value);
            end
        end
        
        % Add the relevant attributes to the PRES variable, including a
        % comment noting that the data has been linearly interpolated        
        ncwriteatt(fv01_name, 'PRES', 'comment','pressure data has been linearly interpolated from surrounding pressure sensors');
        
    end    
end