% cd ~/ABOS/git/java-ocean-data-delivery/ABOS/

%file = 'IMOS_ABOS-DA_STZ_20150523Z_EAC2000_FV01_EAC2000-Aggregate-PSAL_END-20161109Z_C-20180930Z.nc';
%file = 'EAC3200/IMOS_ABOS-DA_STZ_20150522_EAC3200_FV01_EAC3200-Aggregate-TEMP_END-20161106_C-20181010.nc';
%file = 'EAC3200/IMOS_ABOS-DA_ETVZ_20150522_EAC3200_FV01_EAC3200-Aggregate-LATITUDE_END-20161106_C-20181010.nc';
%file = 'IMOS_ANMN-NRS_ACESTZ_20080812Z_NRSKAI_FV01_NRSKAI-Aggregate-TEMP_END-20180307Z_C-20180930Z.nc';
files{1} = 'EAC3200/IMOS_ABOS-DA_AETVZ_20150515T000000Z_EAC3200_FV01_EAC3200-2016-WORKHORSE-ADCP-169_END-20161108T045439Z_C-20170710T005112Z.nc';
files{2} = 'EAC3200/IMOS_ABOS-DA_AETVZ_20150515T000000Z_EAC3200_FV01_EAC3200-2016-WORKHORSE-ADCP-170_END-20161108T055302Z_C-20170703T055425Z.nc';
files{3} = 'EAC3200/IMOS_ABOS-DA_AETVZ_20150515T000000Z_EAC3200_FV01_EAC3200-2016-WORKHORSE-ADCP-700_END-20161108T055726Z_C-20170703T055605Z.nc';

figure(1); clf
figure(2); clf
figure(3); clf

for fn = files
    
    file = fn{1};
    disp(file)
    disp(ncreadatt(file, '/', 'instrument_serial_number'));

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

    nDepth = ncread(file, 'NOMINAL_DEPTH');
    nDepthPlus = ncreadatt(file, 'NOMINAL_DEPTH', 'positive');
    nDepthUp = strcmp(nDepthPlus, 'down') * 2 - 1;

    depth = ncread(file, 'DEPTH');
    dist = ncread(file, 'HEIGHT_ABOVE_SENSOR');

    distPlus = ncreadatt(file, 'HEIGHT_ABOVE_SENSOR', 'positive');
    distUp = strcmp(distPlus, 'down') * 2 - 1;
    depthPlus = ncreadatt(file, 'DEPTH', 'positive');
    depthUp = strcmp(depthPlus, 'down') * 2 - 1;

    cellDepth = -(repmat(dist * distUp, 1, size(depth,1))' - repmat(depth * distUp, 1, size(dist,1) ));

    nDepths = nDepth * nDepthUp + dist * distUp;

    figure(1)
    plot(time, cellDepth(:,1)); grid on; hold on
    plot(time, cellDepth(:,end), ':'); grid on; hold on
    %axis 'ij'

    figure(2)
    plot(time, var(1,:)' - nDepths(1)/100); grid on; hold on
    plot(time, var - repmat(nDepths/100, 1, size(var,2)), '.'); grid on; hold on

    figure(3)
    plot(var(1,:), cellDepth(:,1),'.'); grid on; hold on

end

%file = 'EAC3200/IMOS_ABOS-DA_ETVZ_20150522_EAC3200_FV01_EAC3200-Aggregate_END-20161106_C-20181010.nc';
file = 'EAC3200/IMOS_ABOS-DA_ETVZ_20150522_EAC3200_FV01_EAC3200-Aggregate-UCUR_END-20161106_C-20181011.nc';

%file = 'IMOS_ANMN-NRS_ACESTZ_20080812Z_NRSKAI_FV01_NRSKAI-Aggregate-TEMP_END-20180307Z_C-20180930Z.nc';

instrument = ncread(file, 'instrument_index');

ucur = ncread(file, 'UCUR');
vcur = ncread(file, 'VCUR');

var = sqrt(ucur .^ 2 + vcur .^ 2);
var_unit = ncreadatt(file, plotVar, 'units');
var_name = ncreadatt(file, plotVar, 'long_name');
%var_pos = ncreadatt(file, plotVar, 'positive');
time = ncread(file, 'TIME') + datetime(1950,1,1);

varQCname = ncreadatt(file, plotVar, 'ancillary_variables');
varQC = ncread(file, varQCname);

depth = ncread(file, 'NOMINAL_DEPTH');

figure(2); hold on
for i = min(instrument):max(instrument)
    figure(2)
    plot(time(instrument==i & varQC <= 1), -(var(instrument==i & varQC <= 1) - (depth(i+1) / -100)))
end


