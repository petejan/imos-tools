% load all the flntus data

% load \Users\jan079\Desktop\mooring_data.mat

fields = fieldnames(cleandat_level1);
nel = numel(fields);

tmax = 0;
tmin = datenum(2020,1,1);

for i=1:nel
    t = cleandat_level1.(fields{i}).time(cleandat_level1.(fields{i}).bb_qc<2);
    dv = datevec(t);
    doy = t - datenum(dv(:,1),1,1) + 1;

    cnts = cleandat_level1.(fields{i}).fl_cnts(cleandat_level1.(fields{i}).bb_qc<2);
    fntus = (cnts - allcalibs.(fields{i}).fl_dark_cnts) .* allcalibs.(fields{i}).fl_scale_factor;

    
end
