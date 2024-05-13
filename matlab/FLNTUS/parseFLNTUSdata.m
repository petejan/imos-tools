% load all the flntus data and make DOY plot

% load \Users\jan079\Desktop\mooring_data.mat

fields = fieldnames(cleandat_level1);
nel = numel(fields);

tmax = 0;
tmin = datenum(2020,1,1);
figure(1); clf; hold on

for i=1:nel
    t = cleandat_level1.(fields{i}).time(cleandat_level1.(fields{i}).fl_qc<3);
    dv = datevec(t);
    doy = t - datenum(dv(:,1),1,1) + 1;

    cnts = cleandat_level1.(fields{i}).fl_cnts(cleandat_level1.(fields{i}).fl_qc<3);
    fntus = (cnts - allcalibs.(fields{i}).fl_dark_cnts) .* allcalibs.(fields{i}).fl_scale_factor;
    
    tmax = max(tmax, max(t));
    tmin = min(tmin, min(t));
    disp(horzcat(fields{i}, ' ', datestr(max(t)), ' ', datestr(min(t)), ' ',  allcalibs.(fields{i}).serial_no))
    p = plot(t , fntus,'.'); grid
    p.DisplayName = strrep(horzcat(fields{i},' ',allcalibs.(fields{i}).serial_no ),'_','-');
end

grid on;
xlabel('doy'); ylabel('chl-a (ug/l)');
ylim([0 10]);

xlim([tmin tmax]);
datetick('x', 'keeplimits');

legend('show')