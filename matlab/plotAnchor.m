
file='~/cloudstor/Documents/ANCHOR03.VEC.nc';

time=ncread(file, 'TIME') + datetime(1950,1,1);
ana1 = ncread(file, 'ANALOG1');
pres = ncread(file, 'PRES');

d0 = mean(ana1(time > datetime(2019,03,27,22,14,0) & time < datetime(2019,03,27,22,26,0)));
d1 = mean(ana1(time > datetime(2019,03,29,7,0,0) & time < datetime(2019,03,29,7,30,0)));
n=size(time,1);

offset=d0:(d1-d0)/(n-1):d1;
ana_offset = ana1 - offset';

d2 = mean(ana_offset(time > datetime(2019,03,28,15,0,0) & time < datetime(2019,03,28,15,30,0)));

offset=d0:(d1-d0)/(n-1):d1;

ip2 = find(time > datetime(2019,3,28,0,48,0),1 );
ip1 = find(time > datetime(2019,3,28,0,38,0),1 );
pslope = (pres(ip2) - pres(ip1))/(ip2 - ip1);

td = seconds(time(ip2)-time(ip1));
speed = (pres(ip2)-pres(ip1))/td;

ia1 = find(time > datetime(2019,3,28,21,0,0),1 );
ia2 = find(time > datetime(2019,3,28,21,30,0),1 );
ta = seconds(time(ia2) - time(ia1));
speed_a = (pres(ia2)-pres(ia1))/ta;

figure(1); clf; hold on
sh(1) = subplot(2,1,1);
plot(time, ana_offset * 200 / d2)
grid on
ylim([-100 1000])

sh(2) = subplot(2,1,2);
plot(time, pres)
grid on; axis 'ij'

linkaxes(sh, 'x')



