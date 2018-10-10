% cd ~/ABOS/git/java-ocean-data-delivery/ABOS/

%file = 'IMOS_ABOS-DA_STZ_20150523Z_EAC2000_FV01_EAC2000-Aggregate-PSAL_END-20161109Z_C-20180930Z.nc';
file = 'EAC3200/IMOS_ABOS-DA_STZ_20150522_EAC3200_FV01_EAC3200-Aggregate-TEMP_END-20161106_C-20181010.nc';
%file = 'EAC3200/IMOS_ABOS-DA_ETVZ_20150522_EAC3200_FV01_EAC3200-Aggregate-LATITUDE_END-20161106_C-20181010.nc';
%file = 'IMOS_ANMN-NRS_ACESTZ_20080812Z_NRSKAI_FV01_NRSKAI-Aggregate-TEMP_END-20180307Z_C-20180930Z.nc';

instrument = ncread(file, 'instrument_index');

plotVar = 'DEPTH';

%ucur = ncread(file, 'UCUR');
%vcur = ncread(file, 'VCUR');

%var = sqrt(ucur .^ 2 + vcur .^ 2);
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
    figure(1)
    %plot(time(instrument==i & varQC <= 1), (var(instrument==i & varQC <= 1) + depth(i+1) / -1000))    
    plot(time(instrument==i & varQC <= 1), (var(instrument==i & varQC <= 1) + depth(i+1) / -1000))    
    figure(2);
    h = histogram(var(instrument==i & varQC <= 1) - depth(i+1), 'Normalization', 'cdf' , 'DisplayStyle', 'stair');
    figure(3);
    plot(h.Values, h.BinEdges(1:end-1)); grid on; hold on
end

figure(1)
grid on
%ylabel([var_name ' (' var_unit ')'], 'Interpreter', 'none')
ylabel(['horizontal current + depth/1000 (' var_unit ')'], 'Interpreter', 'none')
datetick('x', 'keeplimits');
ylim([-4 0]);
%axis 'ij'

figure(3);
xlabel('probability');
ylabel('depth (m)');

