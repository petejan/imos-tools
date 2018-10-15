% cd ~/ABOS/git/java-ocean-data-delivery/ABOS/

%file = 'IMOS_ABOS-DA_STZ_20150523Z_EAC2000_FV01_EAC2000-Aggregate-PSAL_END-20161109Z_C-20180930Z.nc';
%file = 'IMOS_ABOS-DA_STZ_20150522_EAC3200_FV01_EAC3200-Aggregate-TEMP_END-20161106_C-20181012.nc';
file = 'IMOS_ABOS-DA_ETVZ_20150522_EAC3200_FV01_EAC3200-Aggregate-UCUR_END-20161106_C-20181012.nc';
%file = 'IMOS_ANMN-NRS_ACESTZ_20080812Z_NRSKAI_FV01_NRSKAI-Aggregate-TEMP_END-20180307Z_C-20180930Z.nc';

instrument = ncread(file, 'instrument_index');

plotVar = 'UCUR';

ucur = ncread(file, 'UCUR');
vcur = ncread(file, 'VCUR');

var = sqrt(ucur .^ 2 + vcur .^ 2);
%var = ncread(file, plotVar);

var_unit = ncreadatt(file, plotVar, 'units');
var_name = ncreadatt(file, plotVar, 'long_name');
%var_pos = ncreadatt(file, plotVar, 'positive');
time = ncread(file, 'TIME') + datetime(1950,1,1);

varQCname = ncreadatt(file, plotVar, 'ancillary_variables');
varQC = ncread(file, varQCname);

depth = ncread(file, 'DEPTH');
depth_unit = ncreadatt(file, plotVar, 'units');
depth_name = ncreadatt(file, plotVar, 'long_name');

nom_depth = ncread(file, 'NOMINAL_DEPTH');

figure(1);
clf
hold on
n = 1;
for i = min(instrument):max(instrument)
    plot(time(instrument==i & varQC <= 1), var(instrument==i & varQC <= 1))
    tmin(n) = min(time(instrument==i & varQC <= 1));
    tmax(n) = max(time(instrument==i & varQC <= 1));
    n = n + 1;
end
grid on

% create time bin

t0 = dateshift(min(tmin),'start','hour');
t1 = dateshift(max(tmax),'end','hour');
t = t0:hours(1):t1;

n = 1;
for i = min(instrument):max(instrument)
    subs = floor((datenum(time(instrument==i & varQC <= 1)) - datenum(t0))*24)+1;
    v(n,:) = accumarray(subs, var(instrument==i & varQC <= 1), size(t'), @mean, NaN);
    d(n,:) = accumarray(subs, depth(instrument==i & varQC <= 1), size(t'), @mean, NaN);
    n = n + 1;
end

% add the adcp data to the end of v, d

file='EAC3200/IMOS_ABOS-DA_AETVZ_20150515T000000Z_EAC3200_FV01_EAC3200-2016-WORKHORSE-ADCP-700_END-20161108T055726Z_C-20170703T055605Z.nc';

ucur = ncread(file, 'UCUR');
vcur = ncread(file, 'VCUR');

var = sqrt(ucur .^ 2 + vcur .^ 2);

time = ncread(file, 'TIME') + datetime(1950,1,1);

varQCname = ncreadatt(file, plotVar, 'ancillary_variables');
varQC = ncread(file, varQCname);

depth = ncread(file, 'DEPTH');
depth_unit = ncreadatt(file, plotVar, 'units');
depth_name = ncreadatt(file, plotVar, 'long_name');

nnom_depth = double(ncread(file, 'NOMINAL_DEPTH'));
has = -double(ncread(file, 'HEIGHT_ABOVE_SENSOR'));

nom_depth = [nom_depth' has'+nnom_depth];

for i = 1:size(has,1)
    subs = floor((datenum(time(varQC(i,:) <= 1)) - datenum(t0))*24)+1;
    v(n,:) = accumarray(subs, var(i, varQC(i,:) <= 1), size(t'), @mean, NaN);
    d(n,:) = accumarray(subs, has(i) + depth(varQC(i, :) <= 1), size(t'), @mean, has(i) + nnom_depth);
    n = n + 1;
end

figure(2)
plot(t, d)
grid on
ylabel([var_name ' (' var_unit ')'], 'Interpreter', 'none')

% create depth interpolation

dq = 200:20:3200;
[nom_depth_sort, nom_depth_idx] = sort(nom_depth);

vq = NaN * ones(size(d,2), size(dq,2));
for n = 1:size(d,2)
    if (sum(isnan(d(:,n))) == 0)
        vq(n,:) = interp1(d(nom_depth_idx, n), v(nom_depth_idx, n), dq, 'linear', NaN);
    else
        vq(n,:) = NaN * ones(size(dq));
    end
end

figure(4)
plot(mean(vq, 'omitnan'), dq)
grid on
axis 'ij'

figure(3);
imagesc(datenum(t), dq, vq')
datetick('x', 'keeplimits');
%axis 'ij'

