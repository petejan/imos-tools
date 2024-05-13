% takes all the burst data, takes the medians, saves the std, min, max, 
% and mean, and puts it all into a matlab structure from where I can 
% administer further QC testing
% preserves the QC flags and shortens the time vector according to the
% burst medians
% data that's not coming in bursts will just be saved as is in the new
% structure
% bursts treated as separate as soon as they are > 1 minute apart

% CS, 18.5.2017

clear;
close all;


% find the files
%search_path = '/Users/cs118/data/sensor_QC/fromPete_wTS';

%cd(search_path);

%files = files_w_ext(search_path,'nc');
 
%files{1} = '/Users/pete/DWM/git/imos-tools/FLNTUS/SOFS-8/IMOS_DWM-SOTS_B_20190207_SOFS_FV00_SOFS-8-2019-FLNTUS-1172-1m_END-20201002_C-20210916.nc';
%files{1} = '/Users/pete/DWM/git/imos-tools/FLNTUS/SOFS-8/IMOS_DWM-SOTS_B_20190207_SOFS_FV00_SOFS-8-2019-FLNTUS-1215-30m_END-20200922_C-20210916.nc';
files{1} = 'IMOS_DWM-SOTS_BCPST_20230503_SOFS_FV00_SOFS-12-2023-FLNTUS-1215-30m_END-20240413_C-20240513.nc';
%files{1} = 'IMOS_DWM-SOTS_BCST_20230503_SOFS_FV00_SOFS-12-2023-FLNTUS-1172-1m_END-20240429_C-20240513.nc';

%files{1} = 'data/IMOS_DWM-SOTS_R_20190207_SOFS_FV00_SOFS-8-2019-FLNTUS-1172-1m_END-20201002_C-20210916.nc';
%files{1} = 'data/IMOS_DWM-SOTS_R_20190207_SOFS_FV00_SOFS-8-2019-FLNTUS-1215-30m_END-20200922_C-20210916.nc';

for i = 1:length(files)              

    filename = files{i};
    i                   % so I know the program is doing something
    
    % pull out variables of interest:
    time = ncread(filename, 'TIME') + datenum(1950,1,1);
    
    fl_cnts = ncread(filename, 'ECO_FLNTUS_CPHL');
    %fl_qc = ncread(filename,'ECO_FLNTUS_CHL_quality_control');
    fl_qc = ones(size(fl_cnts));
   
    bb_cnts = ncread(filename, 'ECO_FLNTUS_TURB');
    %bb_qc = ncread(filename, 'ECO_FLNTUS_TURB_quality_control');
    bb_qc = ones(size(bb_cnts));
    
    % also pull out sal and temp for bbp calculation, added 13.6.2017
    sal = ncread(filename, 'PSAL');
    temp = ncread(filename, 'TEMP');

%     % use only the clean products 
%     clean_time = time(bb_qc<2);                 % days
%     clean_fl = fl_cnts(bb_qc<2);                % counts
%     clean_bb = bb_cnts(bb_qc<2);                % counts

    % =================================================================
    % then look at the time vector to see whether we're 
    % dealing with burst measurements or not:
    
    % take the difference between time points
    diff_t = diff(time);                        % used to be clean_time
    % convert from days to minutes
    diff_t =  diff_t.*24.*60;
    
    % take the median to see what we're dealing with
    med = median(diff_t);
    
    % if measurements are apart by more than a minute, treat them as 
    % separate and do no further investigation 
    if med > 1
        
        disp('no bursts detected')
        
        % save the relevant data into the mat file
        % first into a dummy structure, dat
        dat.fl_cnts = fl_cnts;
        dat.bb_cnts = bb_cnts;
        dat.time = time;
        dat.bb_qc = bb_qc;
        dat.fl_qc = fl_qc;
        dat.sal = sal;
        dat.temp = temp;
        
        % then name the structure properly by using the deployment_code
        % from the Global Attributes in the netcdf file
        new_file = ncreadatt(filename,'/','deployment_code');
        
        % replace any minuses with an underscore 
        new_file = strrep(new_file, '-', '_');
       
        % but apparently this is the better way of doing it:
        alldat.(new_file) = dat;
        
        clear dat new_file
        
        continue;
        
    else    % take the medians of burst measurements
            % and preserve the std as well as # of obs info
            % shorten the time vector and qc flags accordingly
            
           % =========================================================
           % MEDIAN
           
           % find the separations between bursts
           ind = find (diff_t > 1);

           % now get the medians etc.
           bb_std = zeros(length(ind),1);
           fl_std = zeros(length(ind),1);

           bb_md = zeros(length(ind),1);
           fl_md = zeros(length(ind),1);
           
           bb_mean = zeros(length(ind),1);
           fl_mean = zeros(length(ind),1);
           
           bb_min = zeros(length(ind),1);
           fl_min = zeros(length(ind),1);
           
           bb_max = zeros(length(ind),1);
           fl_max = zeros(length(ind),1);

           OO_obs = zeros(length(ind),1);
           
           s_time = zeros(length(ind),1);
           s_fl_qc = zeros(length(ind),1);
           s_bb_qc = zeros(length(ind),1);
           
           s_sal = zeros(length(ind),1);
           s_temp = zeros(length(ind),1);

           cnt1 = 1;
           fl_cnt = 0;
           bb_cnt = 0;
           for l=1:length(ind)

               % get the burst std
               bb_std(l,1) = std(bb_cnts(cnt1:ind(l)));
               fl_std(l,1) = std(fl_cnts(cnt1:ind(l))); 

               % the burst median
               bb_md(l,1) = median(bb_cnts(cnt1:ind(l)));
               fl_md(l,1) = median(fl_cnts(cnt1:ind(l)));
               
               % the burst mean
               bb_mean(l,1) = mean(bb_cnts(cnt1:ind(l)));
               fl_mean(l,1) = mean(fl_cnts(cnt1:ind(l)));
               
               % the burst min
               bb_min(l,1) = min(bb_cnts(cnt1:ind(l)));
               fl_min(l,1) = min(fl_cnts(cnt1:ind(l)));
               
               % the burst max
               bb_max(l,1) = max(bb_cnts(cnt1:ind(l)));
               fl_max(l,1) = max(fl_cnts(cnt1:ind(l)));

               % the # of obs per burst, same for bb and fl
               OO_obs(l,1) = length(bb_cnts(cnt1:ind(l)));

               % contract the time vector into a shorter one
               s_time(l,1) = mean(time(cnt1:ind(l)), "omitnan");
               
               % contract T and S
               s_sal(l,1) = mean(sal(cnt1:ind(l)),"omitnan");
               s_temp(l,1) = mean(temp(cnt1:ind(l)), "omitnan");

               % contract the QC vectors
               % preserve the highest flag observed for each burst
               s_fl_qc(l,1) = max(fl_qc(cnt1:ind(l)), [], "omitnan");
               s_bb_qc(l,1) = max(bb_qc(cnt1:ind(l)), [], "omitnan");
               
               % add a flat line test for bursts =====================
               delta_fl = abs(diff(fl_cnts(cnt1:ind(l))));
               delta_bb = abs(diff(bb_cnts(cnt1:ind(l))));
               
               if sum(delta_fl) == 0
                    s_fl_qc(l,1) = 4;
                    fl_cnt= fl_cnt+1;
%                     % controlplot
%                     plot(time(cnt1:ind(l)),fl_cnts(cnt1:ind(l)),'x')
%                     ylabel('fluo')
%                     pause
               end
               
               if sum(delta_bb) == 0
                    s_bb_qc(l,1) = 4;
                    s_fl_qc(l,1) = 4;       % when bb is flat line, flag fluo as well
                    bb_cnt = bb_cnt+1;
%                     % controlplot
%                     plot(time(cnt1:ind(l)),bb_cnts(cnt1:ind(l)),'x')
%                     ylabel('bb')
%                     pause
               end
              

               cnt1 = ind(l)+1;
           end
           
           fl_flags(i,1) = fl_cnt;                       % display number of points flagged
           bb_flags(i,1) = bb_cnt;                       % display number of points flagged
            
            
%        %% controlplot 1: 
%        % check fluo and bb before and after median was taken, and with and
%        % without the QF_flags
%        
       subplot(2,1,1)
       plot(time,bb_cnts,'.k')
       hold on
       plot(time(bb_qc<2),bb_cnts(bb_qc<2),'.r')
       % plot(s_time(s_bb_qc==5),bb_md(s_bb_qc==5),'g.')
       % axis([s_time(1000) s_time(1000)+50 0 4000])         % zoom
       hold off
       
       subplot(2,1,2)
       plot(time,fl_cnts,'.k')
       hold on
       plot(time(fl_qc<2),fl_cnts(fl_qc<2),'.r')
       % plot(s_time(s_fl_qc==5),fl_md(s_fl_qc==5),'g.')
       % axis([s_time(1000) s_time(1000)+50 0 1000])           % zoom
       hold off
       
       % pause
     
        
        % save the relevant data into the mat file
        % first into a dummy structure, dat, where all the variables also
        % get renamed to be the same between deployments
        
        dat.fl_cnts = fl_md;
        dat.bb_cnts = bb_md;
        dat.time = s_time;
        dat.bb_qc = s_bb_qc;
        dat.fl_qc = s_fl_qc;
        dat.obs_per_burst = OO_obs;
        dat.bb_burst_std = bb_std;
        dat.bb_burst_mean = bb_mean;
        dat.bb_burst_min = bb_min;
        dat.bb_burst_max = bb_max;
        dat.fl_burst_std = fl_std;
        dat.fl_burst_mean = fl_mean;
        dat.fl_burst_min = fl_min;
        dat.fl_burst_max = fl_max;
        dat.sal = s_sal;
        dat.temp = s_temp;
        dat.source_file = filename;
        
        % sensor calibration
        dat.fl_dark_cnts = ncreadatt(filename,'ECO_FLNTUS_CPHL','CH_DIGITAL_DARK_COUNT');
        dat.fl_scale_factor = ncreadatt(filename,'ECO_FLNTUS_CPHL','CH_DIGITAL_SCALE_FACTOR');

        dat.bb_dark_cnts = ncreadatt(filename,'ECO_FLNTUS_TURB','TURB_DIGITAL_DARK_COUNT');
        dat.bb_scale_factor = ncreadatt(filename,'ECO_FLNTUS_TURB','TURB_DIGITAL_SCALE_FACTOR');

        dat.serial_no = ncreadatt(filename,'/','instrument_serial_number');

        
        % then name the structure properly by using the deployment_code
        % from the Global Attributes in the netcdf file
        new_file = ncreadatt(filename,'/','deployment_code');
        
        % replace any minuses with an underscore ;-)
        new_file = strrep(new_file, '-', '_');
        
        % collect into the structure:
        alldat.(new_file) = dat;
        
       clear s_time fl_md bb_md s_fl_qc s_bb_qc dat new_file s_sal s_temp 
       
    end
    
end

% then save
save mooring_data.mat alldat
