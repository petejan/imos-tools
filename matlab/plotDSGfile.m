% cd ~/ABOS/git/java-ocean-data-delivery/ABOS/

%file = 'IMOS_ABOS-DA_STZ_20150523Z_EAC2000_FV01_EAC2000-Aggregate-PSAL_END-20161109Z_C-20180930Z.nc';
file = 'IMOS_ABOS-DA_STZ_20150522_EAC3200_FV01_EAC3200-Aggregate-TEMP_END-20161106_C-20181012.nc';
%file = 'IMOS_ANMN-NRS_ACESTZ_20080812Z_NRSKAI_FV01_NRSKAI-Aggregate-TEMP_END-20180307Z_C-20180930Z.nc';

instrument = ncread(file, 'instrument_index');

plotVar = 'TEMP';

var = ncread(file, plotVar);
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
ylabel([var_name ' (' var_unit ')'], 'Interpreter', 'none')
datetick('x', 'keeplimits');