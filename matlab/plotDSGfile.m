% cd ~/ABOS/git/java-ocean-data-delivery/ABOS/

%file = 'IMOS_ABOS-DA_STZ_20150523Z_EAC2000_FV01_EAC2000-Aggregate-PSAL_END-20161109Z_C-20180930Z.nc';
file = 'IMOS_ABOS-DA_AETVZ_20150523_EAC2000_FV01_EAC2000-Aggregate-TEMP_END-20161109_C-20181004.nc';
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

depth = ncread(file, 'NOMINAL_DEPTH');

figure(1);
clf
hold on
for i = 0:max(instrument)
    plot(time(instrument==i & varQC <= 1), var(instrument==i & varQC <= 1))
end

grid on
ylabel([var_name ' (' var_unit ')'], 'Interpreter', 'none')
datetick('x', 'keeplimits');
%axis 'ij'

