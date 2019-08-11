
files{1}='IMOS_ABOS-SOTS_AETVZ_20100907T120000Z_SAZ47_FV01_SAZ47-13-2010-Aquadopp-Current-Meter-1100_END-20110815T030000Z_C-20160609T034640Z.nc';
files{2}='IMOS_ABOS-SOTS_AETVZ_20110801T000000Z_SAZ47_FV01_SAZ47-14-2011-Aquadopp-Current-Meter-1217_END-20130130T223000Z_C-20160609T041721Z.nc';
files{3}='IMOS_ABOS-SOTS_AETVZ_20120720T000000Z_SAZ47_FV01_SAZ47-15-2012-Aquadopp-Current-Meter-1215_END-20131030T024000Z_C-20160609T041846Z.nc';
files{4}='IMOS_ABOS-SOTS_AETVZ_20130428T120000Z_SAZ47_FV01_SAZ47-16-2013-Aquadopp-Current-Meter-1200_END-20150407T065841Z_C-20160427T230606Z.nc';
files{5}='IMOS_ABOS-SOTS_AETVZ_20150321T000000Z_SAZ47_FV01_SAZ47-17-2015-Aquadopp-Current-Meter-1100_END-20160323T034000Z_C-20160427T231412Z.nc';
files{6}='IMOS_ABOS-SOTS_AETVZ_20160314T120000Z_SAZ47_FV01_SAZ47-18-2016-Aquadopp-Current-Meter-1200_END-20170323T010000Z_C-20170401T052741Z.nc';
files{7}='IMOS_ABOS-SOTS_AETVZ_20170317_SAZ47_FV01_SAZ47-19-2017-Aquadopp-Current-Meter-1200_END-20180311_C-20180504.nc';

for i=1:size(files,2)
%for i=7:7
    file = files{i};
    ttl = ncreadatt(file, '/', 'deployment_code');
    dpt = ncreadatt(file, '/', 'instrument_nominal_depth');
    inst = ncreadatt(file, '/', 'instrument');

    TIME = ncread(file, 'TIME') + datetime(1950,1,1);

    VCUR = ncread(file, 'VCUR');
    VCURunits = ncreadatt(file, 'VCUR', 'units');
    qc_v = ncreadatt(file, 'VCUR', 'ancillary_variables');
    VCUR_qc = ncread(file, qc_v);
    UCUR = ncread(file, 'UCUR');
    qc_u = ncreadatt(file, 'UCUR', 'ancillary_variables');
    UCUR_qc = ncread(file, qc_u);

    WCUR = ncread(file, 'WCUR');

    PRES = ncread(file, 'PRES_REL');
    PRES_u = ncreadatt(file, 'PRES_REL', 'ancillary_variables');
    PRES_qc = ncread(file, PRES_u);

    VCUR_qc(PRES<1000) = 5;
    PRES_qc(PRES<1000) = 5;

    roll = ncread(file, 'ROLL');
    pitch = ncread(file, 'PITCH');
    tilt=acosd(cosd(roll).*cosd(pitch));

    cspd = sqrt(VCUR.^2 + UCUR.^2);

    edges = 0:0.01:0.5;
    N = histcounts(cspd(VCUR_qc<=1), edges, 'Normalization', 'cdf');

    figure(4)

    edg_tilt = 0:0.2:20;
    %Ntilt = histcounts(tilt(VCUR_qc<=1), edg_tilt, 'Normalization', 'pdf');
    Ntilt = histcounts(tilt(VCUR_qc<=1), edg_tilt, 'Normalization', 'probability');

    plot(edg_tilt(1:end-1), Ntilt);
    ylabel('probability'); xlabel('tilt (deg)');
    xlim([0 8]);

    grid(); title(sprintf('%s : %s @ %4.0f m',  ttl, inst, dpt))
    [un ni] = unique(N);
    nintypct = interp1(N(ni), edges(ni), 0.9, 'pchip');

    figure(5);
    plot(TIME(VCUR_qc<=1), cspd(VCUR_qc<=1)); grid on; %datetick('x', 'keeplimits');
    title(sprintf('%s : %s @ %4.0f m',  ttl, inst, dpt))
    ylabel(['current (' VCURunits ')']);

    figure(1)
    plot(edges(1:end-1),N)

    % histogram(cspd(cspd_qc==0), 'DisplayStyle', 'stair', 'Normalization', 'cdf'); 

    grid on; title(sprintf('%s @ %4.0f m - 90%% value %5.3f (%s)',  ttl, dpt, nintypct, VCURunits));
    ylabel('cumulative probability'); xlabel(['current (' VCURunits ')']);
    hold on; plot(nintypct, 0.9, '*'); hold off

    mode_pres = mode(PRES(PRES_qc<=1));
    figure(2);
    plot(TIME(PRES_qc <= 1), PRES(PRES_qc <= 1));
    ylabel('pressure (dbar)');title(sprintf('%s : mode %6.1f m : mean %6.1f',  ttl, mode_pres, mean(PRES(PRES_qc<=1))))
    grid on; axis 'ij'

    filename = sprintf([ttl '-Current-' num2str(dpt) 'm-Figures.ps']);
    delete(filename);
    figures = findall(0,'type','figure'); 
    for f = 1:numel(figures)
          fig = figures(f);

          print( fig, '-dpsc2', filename, '-append');
    end

end