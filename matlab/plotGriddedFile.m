file='bin.nc';
temp = ncread(file, 'TEMP');
bin=ncread(file, 'BIN');
time = ncread(file, 'TIME') + datetime(1950,1,1);

b=imagesc(datenum(time), bin, temp);
set(b,'AlphaData',~isnan(temp))
colorbar
datetick('x', 'yyyy-mm-dd',  'keeplimits');
grid on