% apply fluorescence and bb factory calibrations
% then do a climatology QC test and stop there (for now)
% in the future: possibility to get to regional calibrations also
% (e.g. using approach and factors from Boss SOCCOM Bio-optics primer)
% commented modifications allow export (print) control plots
% CS, 25.5.2017


% load the mat file
clear;
close all;
%clc;

% find the files
%search_path = '/Users/cs118/data/sensor_QC';

%cd(search_path);
load mooring_data.mat

names_dat  = fieldnames(cleandat_level1);

% set colors for the plots
cols = get(gca,'Colororder');      

% % change directory to save the plots
% cd ('/Users/cs118/figures/sensor_QC')

% loop through the deployments
for n = 1:length(names_dat)
     
    
    % calibrate fluo:
    chl_fluo_fact = (cleandat_level1.(names_dat{n}).fl_cnts - cleandat_level1.(names_dat{n}).fl_dark_cnts)...
        .* cleandat_level1.(names_dat{n}).fl_scale_factor;         % ug L-1

    % calibrate bb:
    bb_NTU_fact = (cleandat_level1.(names_dat{n}).bb_cnts - cleandat_level1.(names_dat{n}).bb_dark_cnts)...
        .* cleandat_level1.(names_dat{n}).bb_scale_factor;         % NTU
    
    % adjust the bb calibration to get to the volume scattering function
    % (VSF); conversion factor at 700 nm (as per Dave Stahlke email):
    f700 = 0.002727; 
    beta700 = bb_NTU_fact .* f700;          % m-1 sr-1
    
    % calculate seawater contribution
    % for this I need salinity and temperature,
    % then use betasw_ZHH2009.m from Bozena/Xiadong:
    % [betasw,beta90sw,bsw]= betasw_ZHH2009(lambda,Tc,theta,S,delta)
    sal = abs(cleandat_level1.(names_dat{n}).sal);      % some neg. # for out of water screw with the function
    temp = cleandat_level1.(names_dat{n}).temp;
    for k= 1:length(sal)
        betasw(k,1) = betasw_ZHH2009(700,temp(k,1),142,sal(k,1));
    end
    
    % subtract the seawater contribution from the total VSF
    beta_p = beta700 - betasw;              % m-1 sr-1
    
    % Xp value from ANFOG April 2016 publication, for theta = 142 and
    % taking the mean between the two references provided:
    Xp = 1.17;
    
    % and finally, the particle backscattering coefficient:
    bbp = 2.*pi.*beta_p.*Xp;                % (m-1)
    
    %% run a climatology test =============================================
    % set the limits of what's reasonable
    chl_min = 0;
    chl_max = 10;
    
    bb_min = 0;
    bb_max = 0.01;           
    
    % then find the suspect data points
    fl_ind1 = find(chl_fluo_fact < chl_min);
    fl_ind2 = find(chl_fluo_fact > chl_max);
    fl_ind =  unique([fl_ind1; fl_ind2]);
    
    bb_ind1 = find(bbp < bb_min);
    bb_ind2 = find(bbp > bb_max);
    bb_ind = unique([bb_ind1; bb_ind2]);
    
    bb_flags(n,1) = length(bb_ind);            % how many? 
    fl_flags(n,1) = length(fl_ind);            % how many?
    
    % now make sure I don't overwrite any higher flags
    low_bb_flags = find(cleandat_level1.(names_dat{n}).bb_qc < 4);       % find indices of all low flags
    % then find entries common to both the spike vector and low flag vector
    safe_bb_flag = intersect(low_bb_flags, bb_ind);     
    cleandat_level1.(names_dat{n}).bb_qc(safe_bb_flag) = 3;     % then apply the flag 
   
    % now make sure I don't overwrite any higher flags
    low_fluo_flags = find(cleandat_level1.(names_dat{n}).fl_qc < 4);       % find indices of all low flags
    % then find entries common to both the spike vector and low flag vector
    safe_fluo_flag = intersect(low_fluo_flags, fl_ind);     
    cleandat_level1.(names_dat{n}).fl_qc(safe_fluo_flag) = 3;     % then apply the flag 
    
    
%     %% check the bb/chl ratio
%     bb_chl_rat = bbp./chl_fluo_fact;
%     
%     ratio_ind = find(bb_chl_rat > 0.03);
%     
%     cleandat_level1.(names_dat{n}).bb_qc(ratio_ind) = 6;
%     cleandat_level1.(names_dat{n}).fl_qc(ratio_ind) = 6;
    
    
    %% write one combined flag for fluo and bb that is the most conservative
    
    qc_comb = cleandat_level1.(names_dat{n}).bb_qc;
    for l = 1:length(qc_comb)
        if cleandat_level1.(names_dat{n}).fl_qc(l,1) > qc_comb(l,1)
            qc_comb(l,1) = cleandat_level1.(names_dat{n}).fl_qc(l,1);
        end
    end
    
    % "good data" remaining in each deployment
    good(n,1) = length(qc_comb==1);
    
    
    %% now apply "good data" and "probably good data" flags to the data 
    % that are not flagged bad or suspect
    
    cleandat_level1.(names_dat{n}).fl_qc(cleandat_level1.(names_dat{n}).fl_qc<3) = 2;
    cleandat_level1.(names_dat{n}).bb_qc(cleandat_level1.(names_dat{n}).bb_qc<3) = 2;
    
    cleandat_level1.(names_dat{n}).fl_qc(qc_comb<2) = 1;
    cleandat_level1.(names_dat{n}).bb_qc(qc_comb<2) = 1;
    
    % and add the calibrated values to the structure
    cleandat_level1.(names_dat{n}).fl_chl_a = chl_fluo_fact;
    cleandat_level1.(names_dat{n}).bb_bbp = bbp;
    
    %% then plot the clean, calibrated data only ==========================
    
    % define good time limits first
    min_time = min(cleandat_level1.(names_dat{n}).time(cleandat_level1.(names_dat{n}).fl_qc<3));
    max_time = max(cleandat_level1.(names_dat{n}).time(cleandat_level1.(names_dat{n}).fl_qc<3));
    
    % plot bb
    subplot(4,1,1)
    plot(cleandat_level1.(names_dat{n}).time(cleandat_level1.(names_dat{n}).bb_qc==2),...
        bbp(cleandat_level1.(names_dat{n}).bb_qc==2),'.','Color',[cols(2,:)])
    hold on
    plot(cleandat_level1.(names_dat{n}).time(cleandat_level1.(names_dat{n}).bb_qc==1),...
        bbp(cleandat_level1.(names_dat{n}).bb_qc==1),'.','Color',[cols(1,:)])
    ylabel('b_b_p (m^{-1})')
     datetick('x',12)
     xlim([min_time max_time])
     hold off
    
     % fix the title syntax
     title(strrep(names_dat{n}, '_', '-'))
    
     % plot fluo
    subplot(4,1,2)
    plot(cleandat_level1.(names_dat{n}).time(cleandat_level1.(names_dat{n}).fl_qc==2),...
        chl_fluo_fact(cleandat_level1.(names_dat{n}).fl_qc==2),'.','Color',[cols(2,:)])
    hold on
    plot(cleandat_level1.(names_dat{n}).time(cleandat_level1.(names_dat{n}).fl_qc==1),...
        chl_fluo_fact(cleandat_level1.(names_dat{n}).fl_qc==1),'.','Color',[cols(1,:)])
%     plot(cleandat_level1.(names_dat{n}).time(fl_ind),...
%         chl_fluo_fact(fl_ind),'.r')
%     plot(cleandat_level1.(names_dat{n}).time(ratio_ind),...
%         chl_fluo_fact(ratio_ind),'.g')
    ylabel('chl-a (\mug L^{-1})')
    datetick('x',12)
    xlim([min_time max_time])
    xlabel('Date')
    hold off
    
    % plot the ratio between the two
    subplot(4,1,3)
    deploy_time = cleandat_level1.(names_dat{n}).time(qc_comb<2)...
        -cleandat_level1.(names_dat{n}).time(1);
    scatter(chl_fluo_fact(qc_comb<2),...
        bbp(qc_comb<2),13,deploy_time,'filled')
    ylabel('b_b_p (m^{-1})')
    xlabel('chl-a (\mug L^{-1})')
    c = colorbar;
    ylabel(c,'Days since deploy')
    ylim([0 0.005])
    xlim([0 5])
    
    
    subplot(4,1,4)
    plot(cleandat_level1.(names_dat{n}).time(qc_comb<2),...
        bbp(qc_comb<2)./chl_fluo_fact(qc_comb<2),'.')
    datetick('x',12)
    xlim([min_time max_time])
    xlabel('Date')
    ylabel('b_b_p / chl-a')
    ylim([0 0.04])
    % refline(0,0.03)
    

% hist(cleandat_level1.(names_dat{n}).fl_qc)

% %% or make one plot with all the deployments together, with the most stringent flag
% 
% t = cleandat_level1.(names_dat{n}).time(qc_comb<2);
% dv = datevec(t);
% doy = t - datenum(dv(:,1),1,1) + 1;
% 
% plot(doy, chl_fluo_fact(qc_comb<2),'.'); grid
% hold on

%%
%     orient tall
%     print(names_dat{n},'-dpng') 
%     
    %pause
    clear beta700 betasw bbp beta_p sal temp qc_comb ratio_ind
 
end

% grid on;
% xlabel('doy'); ylabel('chl-a (ug/l)');
% hold off

% save the calibrated data and new flags
save mooring_data.mat cleandat_level1 -append
