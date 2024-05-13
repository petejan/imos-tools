% load all the flntus data

% load \Users\jan079\Desktop\mooring_data.mat

fields = fieldnames(cleandat_level1);
nel = numel(fields);

tmax = 0;
tmin = datenum(2020,1,1);

for i=1:nel
    t = cleandat_level1.(fields{i}).time;
    dv = datevec(t);
    doy = t - datenum(dv(:,1),1,1) + 1;

    disp(horzcat(fields{i}, ' ', datestr(max(t)), ' ', datestr(min(t)), ' ',  cleandat_level1.(fields{i}).serial_no))

    cnts = cleandat_level1.(fields{i}).fl_cnts(cleandat_level1.(fields{i}).bb_qc<2);
    fntus = (cnts - cleandat_level1.(fields{i}).fl_dark_cnts) .* cleandat_level1.(fields{i}).fl_scale_factor;
    
    outname = horzcat((fields{i}), '-', regexprep(cleandat_level1.(fields{i}).serial_no, '[; ]', '-'), '-FLNTUSdata.nc');

    delete(outname);

    source_file = cleandat_level1.(fields{i}).source_file;
    finfo = ncinfo(source_file);

    timestart = datetime(ncreadatt(source_file, '/', 'time_deployment_start'), 'InputFormat', 'yyyy-MM-dd''T''HH:mm:ss''Z''');
    timeend = datetime(ncreadatt(source_file, '/', 'time_deployment_end'), 'InputFormat', 'yyyy-MM-dd''T''HH:mm:ss''Z''');

    dep_msk = t >= datenum(timestart) & t < datenum(timeend);
           
    nccreate(outname, 'TIME', 'Dimensions', {'TIME',numel(t)}, 'format', 'netcdf4_classic');

    % copy variables LATITUDE, LONGITUDE, NOMINAL_DEPTH
    for var_n=1:numel(finfo.Variables)
        if numel(intersect(finfo.Variables(var_n).Name, {'LATITUDE', 'LONGITUDE', 'NOMINAL_DEPTH'})) >= 1
            nccreate(outname, finfo.Variables(var_n).Name, 'format', 'netcdf4_classic');
            for att_n=1:numel(finfo.Variables(var_n).Attributes)
                ncwriteatt(outname, finfo.Variables(var_n).Name, finfo.Variables(var_n).Attributes(att_n).Name, finfo.Variables(var_n).Attributes(att_n).Value);
            end
            v = ncread(source_file, finfo.Variables(var_n).Name);
            ncwrite(outname, finfo.Variables(var_n).Name, v);
        end
    end
    
    
    nccreate(outname, 'CPHL', 'Dimensions', {'TIME',numel(t)}, 'format', 'netcdf4_classic', 'datatype', 'single');
    nccreate(outname, 'CPHL_quality_control', 'Dimensions', {'TIME',numel(t)}, 'format', 'netcdf4_classic', 'datatype', 'int8');
    nccreate(outname, 'BB', 'Dimensions', {'TIME',numel(t)}, 'format', 'netcdf4_classic', 'datatype', 'single');
    nccreate(outname, 'BB_quality_control', 'Dimensions', {'TIME',numel(t)}, 'format', 'netcdf4_classic', 'datatype', 'int8');

    ncwrite(outname, 'TIME', t - datenum(1950,1,1));
	ncwriteatt(outname, 'TIME', 'standard_name', 'time') ;
	ncwriteatt(outname, 'TIME', 'long_name', 'time of measurement');
	ncwriteatt(outname, 'TIME', 'units' ,'days since 1950-01-01T00:00:00 UTC') ;
    ncwriteatt(outname, 'TIME', 'axis', 'T') ;
	ncwriteatt(outname, 'TIME', 'valid_min', 10957);
	ncwriteatt(outname, 'TIME', 'valid_max', 54787);
	ncwriteatt(outname, 'TIME', 'calendar', 'gregorian') ;
    
    ncwrite(outname, 'CPHL', cleandat_level1.(fields{i}).fl_chl_a);
    ncwriteatt(outname, 'CPHL', 'standard_name', 'mass_concentration_of_chlorophyll_in_sea_water');
    ncwriteatt(outname, 'CPHL', 'long_name', 'mass_concentration_of_inferred_chlorophyll_from_relative_fluorescence_units_in_sea_water');
    ncwriteatt(outname, 'CPHL', 'units', 'ug/L');
    ncwriteatt(outname, 'CPHL', 'valid_min', single(-1));
    ncwriteatt(outname, 'CPHL', 'valid_max', single(40));
    ncwriteatt(outname, 'CPHL', 'ancillary_variables', 'CPHL_quality_control');
    ncwriteatt(outname, 'CPHL', 'comment_equation', 'chl-a (mg/m-3) = scale_factor * (count - dark_count)');
    ncwriteatt(outname, 'CPHL', 'comment_qc_gross_range', 'data < 0 or data > 2000');
    ncwriteatt(outname, 'CPHL', 'comment_qc_spike_test', 'window_size_for_running_median = 25 hours; abs(single_data_point - running_median) > 3 * stddev(whole_time_series)');
    ncwriteatt(outname, 'CPHL', 'comment_qc_climatology', 'chl-a < 0 or chl-a > 10 mg/m-3');
    ncwriteatt(outname, 'CPHL', 'comment_qc_flat_line', 'window_size = 24 hours; abs(diff(data in window)) == 0');
    ncwriteatt(outname, 'CPHL', 'coordinates', 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH');
    
    
    fl_qc = cleandat_level1.(fields{i}).fl_qc;
    fl_qc(~dep_msk) = 6;
    ncwrite(outname, 'CPHL_quality_control', fl_qc);
	ncwriteatt(outname, 'CPHL_quality_control', 'standard_name','mass_concentration_of_chlorophyll_in_sea_water status_flag') ;
	ncwriteatt(outname, 'CPHL_quality_control', 'long_name','quality flag for CPHL') ;
    ncwriteatt(outname, 'CPHL_quality_control', 'quality_control_conventions','IMOS standard flags');
	ncwriteatt(outname, 'CPHL_quality_control', 'valid_min', int8(0)) ;
	ncwriteatt(outname, 'CPHL_quality_control', 'valid_max', int8(9));
	ncwriteatt(outname, 'CPHL_quality_control', 'flag_values',int8([0, 1, 2, 3, 4, 6, 7, 9]));
	ncwriteatt(outname, 'CPHL_quality_control', 'flag_meanings','unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value' );
       
    ncwrite(outname, 'BB', cleandat_level1.(fields{i}).bb_bbp);
    ncwriteatt(outname, 'BB', 'long_name', 'optical backscatter');
    ncwriteatt(outname, 'BB', 'units', 'm-1');
    ncwriteatt(outname, 'BB', 'valid_min', single(0));
    ncwriteatt(outname, 'BB', 'valid_max', single(1));

    ncwriteatt(outname, 'BB', 'ancillary_variables', 'BB_quality_control');    

    ncwriteatt(outname, 'BB', 'comment_equation_1', 'turb (NTU)= scale factor*(counts-dark counts)') ;
	ncwriteatt(outname, 'BB', 'comment_equation_2', 'B (m-1 sr-1)=turb (NTU)*0.002727 ... wetlabs') ;
	ncwriteatt(outname, 'BB', 'comment_equation_3', 'bbp (m-1) = 2 * pi * Xp * (B - Bsw) ... Zhang et al. (2009)') ;
    ncwriteatt(outname, 'BB', 'comment_equation_4', 'where Xp angle taken as 142 deg, 1.17 ... Sullivan and Twardowski, 2009') ;
	ncwriteatt(outname, 'BB', 'comment_qc_climatology', 'BB < 0 or BB > 0.01 m-1');
	ncwriteatt(outname, 'BB', 'comment_qc_flat_line', 'window_size = 24 hours; abs(diff(data in window)) == 0') ;
	ncwriteatt(outname, 'BB', 'comment_qc_param_flat_line', 'abs(diff(all_data_in_burst)) == 0') ;
	ncwriteatt(outname, 'BB', 'comment_qc_spike_test', 'window_size_for_running_median = 25 hours; abs(single_data_point - running_median) > 3 * stddev(whole_time_series)') ;
    ncwriteatt(outname, 'BB', 'coordinates', 'TIME LATITUDE LONGITUDE NOMINAL_DEPTH');
    
    bb_qc = cleandat_level1.(fields{i}).bb_qc;
    bb_qc(~dep_msk) = 6;
    
    ncwrite(outname, 'BB_quality_control', bb_qc);
	ncwriteatt(outname, 'BB_quality_control', 'long_name','quality flag for BB') ;
    ncwriteatt(outname, 'BB_quality_control', 'quality_control_conventions','IMOS standard flags');
	ncwriteatt(outname, 'BB_quality_control', 'valid_min', int8(0)) ;
	ncwriteatt(outname, 'BB_quality_control', 'valid_max', int8(9));
	ncwriteatt(outname, 'BB_quality_control', 'flag_values',int8([0, 1, 2, 3, 4, 6, 7, 9]));
	ncwriteatt(outname, 'BB_quality_control', 'flag_meanings','unknown good_data probably_good_data probably_bad_data bad_data not_deployed interpolated missing_value' );

    % copy global attributes
    for att_n=1:numel(finfo.Attributes)
        if strcmp(finfo.Attributes(att_n).Name, 'history') ~= 0
            h = finfo.Attributes(att_n).Value;
            h_new = [h char(10) datestr(datetime, 'yyyy-mm-dd') ' data QC'];
            ncwriteatt(outname, '/', 'history', h_new);
        else
            ncwriteatt(outname, '/', finfo.Attributes(att_n).Name, finfo.Attributes(att_n).Value);
        end
    end
    
    ncwriteatt(outname, '/', 'file_version', 'Level 1 - Quality Controlled data') ;
    ncwriteatt(outname, '/', 'file_version_quality_control', 'Quality controlled data have been through quality assurance procedures such as automated routines and sensor calibration and/or a level of visual inspection and flag of obvious errors. The data are in physical units using standard SI metric units with calibration and other pre- processing routines applied, all time and location values are in absolute coordinates to comply with standards and datum. Data includes flags for each measurement to indicate the estimated quality of the measurement. Metadata exists for the data or for the higher level dataset that the data belongs to. This is the standard IMOS data level and is what should be made available to AODN and to the IMOS community.');
    
    % copy variables PSAL, TEMP
    for var_n=1:numel(finfo.Variables)
        var = finfo.Variables(var_n);
        if numel(intersect(var.Name, {'PSAL', 'TEMP', 'ECO_FLNTUS_CPHL', 'ECO_FLNTUS_TURB'})) >= 1
            nccreate(outname, var.Name, 'Dimensions', {'TIME', numel(t)}, 'format', 'netcdf4_classic', 'datatype', 'single');
            for att_n=1:numel(var.Attributes)
                if isa(var.Attributes(att_n).Value, 'single')
                    ncwriteatt(outname, var.Name, var.Attributes(att_n).Name, single(var.Attributes(att_n).Value));
                else
                    ncwriteatt(outname, var.Name, var.Attributes(att_n).Name, var.Attributes(att_n).Value);
                end
            end
            if strcmp(finfo.Variables(var_n).Name, 'PSAL') ~= 0
                ncwrite(outname, 'PSAL', single(cleandat_level1.(fields{i}).sal));
            end
            if strcmp(finfo.Variables(var_n).Name, 'TEMP') ~= 0
                ncwrite(outname, 'TEMP', single(cleandat_level1.(fields{i}).temp));
            end
            if strcmp(finfo.Variables(var_n).Name, 'ECO_FLNTUS_CPHL') ~= 0
                ncwrite(outname, 'ECO_FLNTUS_CPHL', single(cleandat_level1.(fields{i}).fl_cnts));
            end
            if strcmp(finfo.Variables(var_n).Name, 'ECO_FLNTUS_TURB') ~= 0
                ncwrite(outname, 'ECO_FLNTUS_TURB', single(cleandat_level1.(fields{i}).bb_cnts));
            end
        end
    end
end
