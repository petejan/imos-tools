% cd ~/ABOS/git/java-ocean-data-delivery/ABOS/

file = 'EAC-2000/IMOS_ABOS-DA_STZ_20150515T000001Z_EAC2000_FV01_EAC2000-2016-SBE37SMP-1850_END-20161110T232004Z_C-20170703T055706Z.nc';

var = ncread(file, 'TEMP');
var_unit = ncreadatt(file, 'TEMP', 'units');
var_name = ncreadatt(file, 'TEMP', 'long_name');
time = ncread(file, 'TIME') + datetime(1950,1,1);

depth = ncread(file, 'NOMINAL_DEPTH');

figure(1);
clf
hold on
plot(time, var)

grid on
ylabel([var_name ' (' var_unit ')'], 'Interpreter', 'none')
datetick('x', 'keeplimits');

