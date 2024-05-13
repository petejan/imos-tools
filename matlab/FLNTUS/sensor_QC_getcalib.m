% get the calibration data from the netcdf files
% great fun that is ;-) 
% CS, 23.5.2017

clear;
close all;
clc;

% find the files
search_path = '/Users/cs118/data/sensor_QC/fromPete_most_recent';

cd(search_path);

files = files_w_ext(search_path,'nc');
    
for i = 1:length(files)              
    i
    filename = files{i};
    
    % variable to get all calib info from: chl (this is arbitrary) 
    var = 'ECO_FLNTUS_CHL';
    
     ncid = netcdf.open(filename,'nowrite');
     varid = netcdf.inqVarID(ncid,var);
     
     % unfortunately, the attributes I need are not always in the same
     % place behind the variable, so I need a loop and strfind to proceed
     
     % first, figure out how many attributes I have to loop through: 
     [varname,xtype,dimids,natts] = netcdf.inqVar(ncid,varid);
     
     for n = 1:natts-1          % the minus 1 fixes some attribute issue and does no harm
        
         attname = netcdf.inqAttName(ncid,varid,n);
         
         test1 = strfind(attname,'CH_DIGITAL_DARK_COUNT');
         test2 = strfind(attname,'CH_DIGITAL_SCALE_FACTOR');
         test3 = strfind(attname,'TURB_DIGITAL_DARK_COUNT');
         test4 = strfind(attname,'TURB_DIGITAL_SCALE_FACTOR');
         
         if ~isempty(test1)            % proceed if the string was found
             dat.fl_dark_cnts = ncreadatt(filename,var,attname);
         elseif ~isempty(test2)            % proceed if the string was found
             dat.fl_scale_factor = ncreadatt(filename,var,attname);
         elseif ~isempty(test3)            % proceed if the string was found
             dat.bb_dark_cnts = ncreadatt(filename,var,attname);
         elseif ~isempty(test4)            % proceed if the string was found
             dat.bb_scale_factor = ncreadatt(filename,var,attname);
         else
             continue
         end
         
         dat.serial_no = ncreadatt(filename,'/','instrument_serial_number');
         
     end
     
        % then name the structure properly ==========================
        % using the deployment_code
        % from the Global Attributes in the netcdf file
        new_file = ncreadatt(filename,'/','deployment_code');
        
        % replace any minuses with an underscore ;-)
        dah = strfind(new_file,'-');
        new_file(dah) = '_';
        
        % collect into the structure:
        allcalibs.(new_file) = dat;
                                  
        clear dat new_file dah
      
end

% % and save the data
% load mooring_data.mat
% save mooring_data.mat allcalibs -append
