% now go into the cleaning routine for the optical data
% start at the count level
% work off the data in the mat file 
% CS, May 2017

% load the mat file
clear;
close all;


% find the file
%search_path = '/Users/cs118/data/sensor_QC';

%cd(search_path);
load mooring_data.mat

% set colors for the plots
cols = get(gca,'Colororder');      

names  = fieldnames(alldat);

for n = 1:length(names)
  
   
   %% GROSS RANGE TEST
   
   % for bb, exclude data < 0 and > 2000
   % flag only as suspect, i.e. flag = 3
   % make sure I don't overwrite any higher flags
   range_bb1 = find(alldat.(names{n}).bb_cnts > 2000);
   range_bb2 = find(alldat.(names{n}).bb_cnts < 0);
   
   range_bb_all = unique([range_bb1; range_bb2]);      % combine the two
   
   low_bb_flags = find(alldat.(names{n}).bb_qc < 4);       % find indices of all low flags
   % then find entries common to both the gross range and low flag vector
   safe_bb_flag1 = intersect(low_bb_flags, range_bb_all);     
   alldat.(names{n}).bb_qc(safe_bb_flag1) = 3;
  
%    % a more stringent attempt to get at data that are high AND noisy and
%    % are not necessarily caught by the spike test:
%    alldat.(names{n}).bb_qc(moving(alldat.(names{n}).bb_cnts,15,'median') > 3000) = 5;
   
   % now the same for fluo
   range_fl1 = find(alldat.(names{n}).fl_cnts > 2000);
   range_fl2 = find(alldat.(names{n}).fl_cnts < 0);
   
   range_fl_all = unique([range_fl1; range_fl2; range_bb_all]);      % combine the two
   % and also add the bb_flag for this specific test
   
   low_fl_flags = find(alldat.(names{n}).fl_qc < 4);       % find indices of all low flags
   % then find entries common to both the gross range and low flag vector
   safe_fl_flag1 = intersect(low_fl_flags, range_fl_all);     
   alldat.(names{n}).fl_qc(safe_fl_flag1) = 3;
   
   bb_range(n,1) = length(range_bb_all);            % how many were flagged?
   fl_range(n,1) = length(unique([range_fl1; range_fl2]));
    
   %% FLAT LINE TEST
   
   % start flagging when data from an entire day are the same 
   % flag the first day of obs as 2 ("not evaluated") unless they already have 
   % a higher flag than that
   % also check that the time gap for comparisons is not > 26 hours or so
  
   % go with a moving window and compare within that window
   % (with the indexing approach I was going insane) ;-)
   
   % BACKSCATTER ======================================================
   bb_win = 24;     % sets the window size to be evaluated 
                   % minimum # of equal counts in sequence that I'm concerned about
                
   bb_time_gap = 26;  % max time gap allowed between first and last obs in a window; in hours 
   
   % give the data at position <del a flag=2, i.e. "not evaluated" - unless
   % they already have a flag higher than that
   flag = alldat.(names{n}).bb_qc(1:bb_win-1 );
   for h = 1:length(flag)
       if flag < 2
            alldat.(names{n}).bb_qc(h) = 2;
       end
   end
  
   % now the loop with the sliding window
   cnt_bb = 0;
   for g = 1:length(alldat.(names{n}).bb_cnts) - bb_win
       dat = alldat.(names{n}).bb_cnts(g:g+bb_win-1);            % data of interest in the set window
       delta = abs(diff(dat));              % difference between data points
       delta_t = alldat.(names{n}).time(g+bb_win-1) - alldat.(names{n}).time(g);        % time gap
       if (delta_t.*24) > bb_time_gap               % if the time gap is above threshold
           if alldat.(names{n}).bb_qc(g+bb_win-1) < 3     % and if there isn't already a higher flag
                alldat.(names{n}).bb_qc(g+bb_win-1) = 2;   % label as "not evaluated"
           end
       elseif sum(delta) == 0                   % if the data are all the same, i.e. sum(diff) = 0
           if alldat.(names{n}).bb_qc(g+bb_win-1) < 4     % and if there isn't already a higher flag  
                alldat.(names{n}).bb_qc(g+bb_win-1) = 4;      % then flag as bad or suspect
                cnt_bb = cnt_bb+1;
           end
       else
           continue
       end
   end
   bb_flat(n,1) = cnt_bb;            % how many were flagged?
              
   clear flag cnt_bb dat delta
   
   % FLUORESCENCE ======================================================
   fl_win = 24;     % sets the window size to be evaluated 
                   % minimum # of equal counts in sequence that I'm concerned about
                
   fl_time_gap = 26;  % max time gap allowed between first and last obs in a window; in hours 
   
   % give the data at position <del a flag=2, i.e. "not evaluated" - unless
   % they already have a flag higher than that
   flag = alldat.(names{n}).fl_qc(1:fl_win-1 );
   for h = 1:length(flag)
       if flag < 2
            alldat.(names{n}).fl_qc(h) = 2;
       end
   end
   
    % now the loop with the sliding window
    cnt_fl = 0;
   for g = 1:length(alldat.(names{n}).fl_cnts) - fl_win
       dat = alldat.(names{n}).fl_cnts(g:g+fl_win-1);            % data of interest in the set window
       delta = abs(diff(dat));              % difference between data points
       delta_t = alldat.(names{n}).time(g+fl_win-1) - alldat.(names{n}).time(g);
       if (delta_t.*24) > fl_time_gap               % if the time gap is above threshold
           if alldat.(names{n}).fl_qc(g+fl_win-1) < 3     % and if there isn't already a higher flag
                alldat.(names{n}).fl_qc(g+fl_win-1) = 2;   % label as "not evaluated"
           end
       elseif sum(delta) == 0                   % if the data are all the same, i.e. sum(diff) = 0
           if alldat.(names{n}).fl_qc(g+fl_win-1) < 4     % and if there isn't already a higher flag  
                alldat.(names{n}).fl_qc(g+fl_win-1) = 4;      % then flag as suspect or bad
                cnt_fl = cnt_fl+1;
           end
       else
           continue
       end
   end
   fl_flat(n,1) = cnt_fl;                 % how many were flagged?
   
   clear flag cnt_fl dat delta
   
   %% SPIKE TEST
   
   % work with a moving median that has a reasonably large window size
   % then define spikes as outliers outside of 3 x std or sth. like that
   
   
   % BACKSCATTER =============================================
   % flag both negative and positive spikes as suspect
   % neither is expected to be indicative of a particle
   
   bb_med = moving(alldat.(names{n}).bb_cnts,25,'median');                      % running median
   bb_std=std(alldat.(names{n}).bb_cnts(alldat.(names{n}).bb_qc<2));             % clean std
   
   bb_spike_id1 = find((alldat.(names{n}).bb_cnts - bb_med) > 3.*bb_std);        % positive spikes
   bb_spike_id2 = find((alldat.(names{n}).bb_cnts - bb_med) < -3.*bb_std);        % negative spikes
   
   % combine the two:
   bb_spike_flag = unique([bb_spike_id1; bb_spike_id2]);
   % safeguard so I don't overwrite higher flags with lower ones:
   low_bb_flag_ind = find(alldat.(names{n}).bb_qc < 4);       % find indices of all low flags
   % then find entries common to both the spike vector and low flag vector
   safe_bb_flag = intersect(low_bb_flag_ind, bb_spike_flag);     
   
   alldat.(names{n}).bb_qc(safe_bb_flag) = 3;     % then apply the flag
    
   bb_spike(n,1) = length(bb_spike_flag);           % how many were flagged?
   
   % FLUORESCENCE ======================================================
   % same treatment as bb
   
   fl_med = moving(alldat.(names{n}).fl_cnts,25,'median');      % running median
   fl_std = std(alldat.(names{n}).fl_cnts(alldat.(names{n}).fl_qc<2));        % clean std
   
   fl_spike_id = find(abs(alldat.(names{n}).fl_cnts - fl_med) > 3.* fl_std);
   % fl_spike_id2 = find(abs(alldat.(names{n}).fl_cnts - fl_med) > 2.* fl_std);
   
   % safeguard so I don't overwrite higher flags with lower ones:
   low_fluo_flag_ind = find(alldat.(names{n}).fl_qc < 4);       % find indices of all low flags
   % then find entries common to both the spike vector and low flag vector
   safe_fluo_flag = intersect(low_fluo_flag_ind, fl_spike_id);     
   
   alldat.(names{n}).fl_qc(safe_fluo_flag) = 3;     % then apply the flag 
   
   fl_spike(n,1) = length(fl_spike_id);           % how many were flagged?
   
%    %% Attenuated signal test
%    % see if I can catch those "flat lines" in the fluorescence
%    % take moving stds and if they're smaller than some threshold, flag the
%    % data; possibility to define more than one threshold and flag
%    % THIS IS CATCHING WAY TOO MUCH GOOD DATA
%    
% %    bb_run_std = moving(alldat.(names{n}).bb_cnts,25,'nanstd');
% %    fl_run_std = moving(alldat.(names{n}).fl_cnts,25,'nanstd');
% %    
% %    med_bb_std = nanmedian(bb_run_std(alldat.(names{n}).bb_qc<2))
% %    med_fl_std = nanmedian(fl_run_std(alldat.(names{n}).fl_qc<2))
% %    
% %    at_fl = find(fl_run_std < 0.2.*med_fl_std);      % 0.2
% %    at_bb = find(bb_run_std < 0.2.*med_bb_std);      % 0.2
% %    
% %    alldat.(names{n}).fl_qc(at_fl) = 5;
% %    alldat.(names{n}).bb_qc(at_bb) = 5;
% %    

% %    %% BB NOISE LEVEL TEST
% %    % My own invention but looking at the data I feel it's worth a shot
% %    % see notes for why I've abandoned this avenue (May 23, 2017)
% %    
% %    bb_std = moving(alldat.(names{n}).bb_cnts,25,'std');    
% %    
% %    subplot(2,1,1)
% %    plot(alldat.(names{n}).time,alldat.(names{n}).bb_cnts,'.r')
% %    subplot(2,1,2)
% %    plot(alldat.(names{n}).time, bb_std,'.r')
% %    pause
%     
     %% controlplot 1: 
       % check fluo and bb with and without the QF flags
       
       subplot(2,1,1)
       plot(alldat.(names{n}).time,alldat.(names{n}).bb_cnts,'.k')
       hold on
       plot(alldat.(names{n}).time(alldat.(names{n}).bb_qc<2),...
           alldat.(names{n}).bb_cnts(alldat.(names{n}).bb_qc<2),'.','Color',[cols(1,:)])
       plot(alldat.(names{n}).time(alldat.(names{n}).bb_qc==3),...
           alldat.(names{n}).bb_cnts(alldat.(names{n}).bb_qc==3),'.','Color',[cols(2,:)])
%        plot(alldat.(names{n}).time(alldat.(names{n}).bb_qc==5),...
%            alldat.(names{n}).bb_cnts(alldat.(names{n}).bb_qc==5),'.r')
       % plot(alldat.(names{n}).time,bb_med,'b.')       % plot the median
       % axis([alldat.(names{n}).time(1000) alldat.(names{n}).time(1000)+50 0 200])         % zoom
       hold off 
       ylabel('bb counts')
        datetick('x',3)             % 12
       
       % fix the title syntax
       title(strrep(names{n}, '_', '-'))
      
       subplot(2,1,2)
       plot(alldat.(names{n}).time,alldat.(names{n}).fl_cnts,'.k')
       hold on
       plot(alldat.(names{n}).time(alldat.(names{n}).fl_qc<2),...
           alldat.(names{n}).fl_cnts(alldat.(names{n}).fl_qc<2),'.','Color',[cols(1,:)])
       plot(alldat.(names{n}).time(alldat.(names{n}).fl_qc==3),...
           alldat.(names{n}).fl_cnts(alldat.(names{n}).fl_qc==3),'.','Color',[cols(2,:)])
%        plot(alldat.(names{n}).time(alldat.(names{n}).fl_qc==5),...
%            alldat.(names{n}).fl_cnts(alldat.(names{n}).fl_qc==5),'.r')

       % plot(alldat.(names{n}).time,fl_med,'b.')       % add the median
       % axis([alldat.(names{n}).time(1000) alldat.(names{n}).time(1000)+50 0 1000])           % zoom
       hold off
       xlabel('time')
       ylabel('fluo counts')
       datetick('x',3)          % 12
       
       %pause
     
end


% rename the data structure to "cleandat_level1", to indicate the first cleaning
% level that has been achieved

cleandat_level1 = alldat;
save mooring_data.mat cleandat_level1 -append

% % save images
% cd('/Users/cs118/figures/sensor_QC')
% orient landscape

