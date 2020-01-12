

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
scat_interp_pres = scatteredInterpolant(agg_time,agg_nominal_depth(agg_instrument_index+1),agg_pres);

%% Interpolate the pressure and write the data into the FV00 file

% Loop through each of the fv00 files

for i=1:length(fv00_files)
    % Extract the content from the FV00 file
    fv00_contents = ncinfo(fv00_files(i).name);

    % Check if the FV00 file contains pressure data, run the interpolation
    % code if not

    try
        ncinfo(fv00_files(i).name,'PRES');
        "Contains pres"

    catch
        "Doesn't contain press"

        % Load the FV00 data requiring pressure

        %'days since 1950-01-01 00:00:00 UTC' for minilog T

        fv00_time = ncread(fv00_files(i).name,'TIME');
        fv00_depth = ncread(fv00_files(i).name,'NOMINAL_DEPTH');

        % Interpolate the pressure
        pres_interp = scat_interp_pres(fv00_time,fv00_depth*ones(size(fv00_time)));
        
        %
        % Create an FV01 version of the current FV00 file
        %

        % Create the new FV01 file name

        fv01_name = strrep(fv00_files(i).name,'FV00','FV01');
        fv01_name(end-10:end-3)=datestr(now,'yyyymmdd');

        % Write the FV00 data into the FV01 file

        ncwriteschema(fv01_name,fv00_contents);

        % Add and populate a PRES variable to the FV01 file

        nccreate(fv01_name,'PRES');
        ncwrite(fv01_name,'PRES',scat_interp_pres);

        % Add the relevant attributes to the PRES variable, including a
        % comment noting that the data has been linearly interpolated

        % copy attributes from agg file to output file
        pres_atts = agg_pres_info.Attributes; % get all attribtes from the aggregate file
        for k=1:length(pres_atts)

            ncwriteatt(fv01_name, 'PRES', pres_atts(k).Name, pres_atts(k).Value);

        end

        ncwriteatt(fv01_name, 'PRES', 'comment','pressure data has been linearly interpolated from surrounding pressure sensors');

    end
end
