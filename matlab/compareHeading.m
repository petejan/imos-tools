file1='data/IMOS_ABOS-SOTS_AETVZ_20180303_SAZ47_FV01_SAZ47-20-2018-Aquadopp-Current-Meter-AQD-5961-1200m_END-20190322_C-20190529.nc';

file2='data/IMOS_ABOS-SOTS_AETVZ_20180303_SAZ47_FV01_SAZ47-20-2018-Aquadopp-Current-Meter-AQD-5961-1200m_END-20190322_C-20190719.nc';

time1 = ncread(file1, 'TIME') + datetime(1950,1,1);
hdr1 = ncread(file1, 'HEADING');

time2 = ncread(file1, 'TIME') + datetime(1950,1,1);
hdr2 = ncread(file1, 'HEADING');

plot(time1, hdr1); grid on