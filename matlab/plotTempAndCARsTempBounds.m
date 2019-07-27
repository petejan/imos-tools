% cd ~/ABOS/git/raw2netCDF

file='data/IMOS_ABOS-SOTS_COSTZ_20180801_SOFS_FV01_SOFS-7.5-2018-SBE37SMP-ODO-RS232-03715969-30m_END-20190327_C-20190606.nc';
time=ncread(file, "TIME") + datetime(1950,1,1);
temp=ncread(file, "TEMP");
temp_qc = ncread(file, 'TEMP_quality_control');
tmask = (temp_qc == 0);
temp_car = ncread(file, 'TEMP_CARS');
temp_car_std = ncread(file, 'TEMP_STD_CARS');

figure(1); clf
sh(1) = subplot(4,1,1);
plot(time(tmask), [temp(tmask) temp_car(tmask)]);
hold on
plot(time(tmask), temp_car(tmask) + 2 * temp_car_std(tmask))
plot(time(tmask), temp_car(tmask) - 2 * temp_car_std(tmask))
grid on
legend('measured', 'CARS temp', '+2 std', '-2 std');
title('SBE37 SOFS Temperature');

psal=ncread(file, "PSAL");
psal_car = ncread(file, 'PSAL_CARS');
psal_car_std = ncread(file, 'PSAL_STD_CARS');

sh(2) = subplot(4,1,2);
plot(time(tmask), [psal(tmask) psal_car(tmask)]);
hold on
plot(time(tmask), psal_car(tmask) + 2 * psal_car_std(tmask))
plot(time(tmask), psal_car(tmask) - 2 * psal_car_std(tmask))
grid on
legend('measured', 'CARS psal', '+2 std', '-2 std');
title('SBE37 SOFS PSAL');

subplot(4,1,4);
plot(temp(tmask), psal(tmask), '.')
hold on
plot(temp_car(tmask) + 2 * temp_car_std(tmask), psal_car(tmask) + 2 * psal_car_std(tmask), '.')
plot(temp_car(tmask), psal_car(tmask), '.')
grid on

sh(3) = subplot(4,1,3);
pres=ncread(file, "PRES_REL");
plot(time(tmask), pres(tmask)); axis 'ij'
title("pressure"); ylabel("pressure (dbar)");
grid on

linkaxes(sh, 'x')

