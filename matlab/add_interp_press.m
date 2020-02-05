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

% Here we prevent the use of bad data from Pulse 8

if strfind(fv00_files(1).name,'Pulse-8')
             
    agg_pres(agg_instrument_index==2 & agg_time+datenum(1950,1,1,0,0,0) >= datenum('30-01-2012 05:00','dd-mm-yyyy HH:MM'))=NaN;
   
    %agg_pres(agg_instrument_index==2)=NaN;
    
end


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
        
        interp_agg_pres = nan(length(agg_nominal_depth)+1,length(fv00_time));
        
        % Include a row of zeros to set surface depth as 0 dbar
        
        interp_agg_pres(1,:) = zeros(size(fv00_time));
        
        agg_nominal_depth_with_0 = [0; agg_nominal_depth];
        
        % Loop through each nominal depth in the aggregate file, and get pressure for the FV00 file's time       
        for j = 1:(length(agg_nominal_depth))
            
            % Select the relevant time and pressures
            
            time_selection = agg_time(agg_instrument_index == (j-1));            
            pres_selection = agg_pres(agg_instrument_index == (j-1));
                        
            % Interpolate along each nominal depth

            interp_agg_pres(j+1,:) = interp1(time_selection,pres_selection,fv00_time);
        end   
        
        % Sort the nominal depths and pressures
        
        [agg_nominal_depth_with_0,sort_idx] = sort(agg_nominal_depth_with_0);
        
        interp_agg_pres = interp_agg_pres(sort_idx,:);
        
        
        % Linearly interpolate at each timestamp to replace NaN values
        
        interp_agg_pres = fillmissing(interp_agg_pres,'linear','SamplePoints',agg_nominal_depth_with_0);
        
        
        % At each timestamp in the FV00 record, interpolate a pressure
        % value based on the FV00 nominal depth, and the interpolated
        % pressures in interp_agg_pres. 
        pres_interp_dummy = nan(size(fv00_time));     
        
       
        for l = 1:length(fv00_time) 
            
            if sum(~isnan(interp_agg_pres(:,l))) > 1
            
                pres_interp_dummy(l) = interp1(agg_nominal_depth_with_0,interp_agg_pres(:,l),fv00_depth);                
        
            end
            
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
                
        % Add quality control variables to the FV01 file, assigning 8 to
        % interpolated data in line with Argo
        for v = fv00_contents.Variables
            
            if ~isempty(v.Dimensions)
            
                nccreate(fv01_name, v.Name + "_quality_control",'Dimensions',{v.Dimensions.Name,v.Dimensions.Length},'FillValue',99);
                
                ncwriteatt(fv01_name,v.Name + "_quality_control",'long_name',"quality_code for"+v.Name);
                
                ncwriteatt(fv01_name,v.Name,'ancillary_variables',v.Name + "_quality_control");
                
                if contains(v.Name,'PRES')
                    
                    ncwrite(fv01_name, v.Name + "_quality_control",8*ones(size(fv00_time)));
                    
                end
                
            end
            
        end
        
            
            
        % copy attributes from agg file to output file
        pres_atts = agg_pres_info.Attributes; % get all attribtes from the aggregate file
        for k=1:length(pres_atts)
            if (strcmp(pres_atts(k).Name, '_FillValue') == 0)
                ncwriteatt(fv01_name, 'PRES', pres_atts(k).Name, pres_atts(k).Value);
            end
        end
        
        % Add the relevant attributes to the PRES variable, including a
        % comment noting that the data has been linearly interpolated        
        ncwriteatt(fv01_name, 'PRES', 'comment','pressure data has been interpolated from surrounding pressure sensors');
        
    else
        
        % Load the FV00 data containing pressure        
        %'days since 1950-01-01 00:00:00 UTC' for minilog T        
        fv00_time = ncread(fv00_files(i).name,'TIME');        
        fv00_depth = ncread(fv00_files(i).name,'NOMINAL_DEPTH');
        fv00_pres = ncread(fv00_files(i).name,'PRES');
        
        % Remove bad data in pulse 8
        
        if strfind(fv00_files(i).name,'Pulse-8-2011-SBE16plusV2-01606330-34m')
            
            fv00_pres(4442:end) = NaN;
            
        end
  
        % Interpolate the agg pressure records at each nominal depth to
        % provide pressure values at each timestamp in the current FV00
        % file
        
        interp_agg_pres = nan(length(agg_nominal_depth)+1,length(fv00_time));
        
        % Include a row of zeros to set surface depth as 0 dbar
        
        interp_agg_pres(1,:) = zeros(size(fv00_time));
        
        agg_nominal_depth_with_0 = [0; agg_nominal_depth];
        
        % Loop through each nominal depth in the aggregate file, and get pressure for the FV00 file's time       
        for j = 1:(length(agg_nominal_depth))
            
            % Select the relevant time and pressures
            
            time_selection = agg_time(agg_instrument_index == (j-1));            
            pres_selection = agg_pres(agg_instrument_index == (j-1));
                        
            % Interpolate along each nominal depth

            interp_agg_pres(j+1,:) = interp1(time_selection,pres_selection,fv00_time);
        end   
        
        % Sort the nominal depths and pressures
        
        [agg_nominal_depth_with_0,sort_idx] = sort(agg_nominal_depth_with_0);
        
        interp_agg_pres = interp_agg_pres(sort_idx,:);
        
        
        % Linearly interpolate at each timestamp to replace NaN values
        
        interp_agg_pres = fillmissing(interp_agg_pres,'linear','SamplePoints',agg_nominal_depth_with_0);
        
        for j = 1:length(fv00_pres)
            
            if isnan(fv00_pres(j))
                
                fv00_pres(j) = interp_agg_pres(agg_nominal_depth_with_0==fv00_depth,j);
                
            end
            
        end
        
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
        ncwriteatt(fv01_name,'/','history',[hist newline datestr(now,'yyyy-mm-dd') ' : Filled missing pressure with interpolated pressure from ' agg_files.name]);
        
        % Add and populate a PRES variable to the FV01 file
        %nccreate(fv01_name, 'PRES', 'Dimensions',{'TIME',size(fv00_pres,1)}, 'FillValue',NaN);
        ncwrite(fv01_name, 'PRES', fv00_pres);
                
        % copy attributes from agg file to output file
        pres_atts = agg_pres_info.Attributes; % get all attribtes from the aggregate file
        for k=1:length(pres_atts)
            if (strcmp(pres_atts(k).Name, '_FillValue') == 0)
                ncwriteatt(fv01_name, 'PRES', pres_atts(k).Name, pres_atts(k).Value);
            end
        end
        
        % Add the relevant attributes to the PRES variable, including a
        % comment noting that the data has been linearly interpolated        
        ncwriteatt(fv01_name, 'PRES', 'comment','originally missing pressure data has been interpolated from surrounding pressure sensors');
        
        
    end    
end