from datetime import datetime, UTC
import os
import sys

import gsw.conversions
import numpy as np
import seabirdscientific.instrument_data as id
import seabirdscientific.cal_coefficients as cal
import seabirdscientific.conversion as conv
import xmltodict
from cftime import date2num
from glob2 import glob

from netCDF4 import Dataset
from numpy.ma.core import ones_like
from seabirdscientific.conversion import potential_density_from_t_c_p

def prase_hex(hex_file, xml_file):

    # open the xml file and parse to dict
    with open(xmlcon_file, 'r', encoding='utf-8') as file:
        data_dict = xmltodict.parse(file.read())
    #print(data_dict)

    sensor_dict = data_dict['SBE_InstrumentConfiguration']['Instrument']['SensorArray']['Sensor']

    # find sensor index for temperature, pressure, conductivity and oxygen
    temp_idx = None
    pres_idx = None
    cndc_idx = None
    dox_idx = None
    i = 0
    for s in sensor_dict:
        print(s.keys())
        if 'TemperatureSensor' in s.keys():
            temp_id = s['@SensorID']
            temp_idx = i
            print('temp_id', i, temp_id)
        if 'PressureSensor' in s.keys():
            pres_idx = i
            print('pres_idx', pres_idx)
        if 'ConductivitySensor' in s.keys():
            cndc_idx = i
            print('cndc_idx', cndc_idx)
        if 'OxygenSensor' in s.keys():
            dox_idx = i
            print('dox_idx', dox_idx)
        i += 1

    # read hex file, add sensors found in xmlcon file
    sensor_list = []
    if temp_idx is not None:
        sensor_list.append(id.Sensors.Temperature)
    if cndc_idx is not None:
        sensor_list.append(id.Sensors.Conductivity)
    if pres_idx is not None:
        sensor_list.append(id.Sensors.Pressure)
    if dox_idx is not None:
        sensor_list.append(id.Sensors.SBE63)

    raw_data = id.read_hex_file(filepath=hex_file, instrument_type=id.InstrumentType.SBE37SM, enabled_sensors=sensor_list)

    # create a output netCDF file
    outputName = os.path.basename(hex_file) + ".nc"

    print("output file : %s" % outputName)

    dataset = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    # record instrument config in netCDF metadata
    dataset.instrument_model = data_dict['SBE_InstrumentConfiguration']['Instrument']['DeviceType']
    dataset.instrument = "Sea-Bird Electronics ; " + dataset.instrument_model
    dataset.instrument_serial_number = '037' + sensor_dict[temp_idx]['TemperatureSensor']['SerialNumber'].zfill(5)

    # create the time array
    time_dim = dataset.createDimension('TIME', len(raw_data))
    times = dataset.createVariable('TIME', np.float64, ('TIME',))
    times.long_name = "time"
    times.units = "days since 1950-01-01 00:00:00 UTC"
    times.calendar = "gregorian"
    times.axis = "T"

    time_dt = [x.to_pydatetime() for x in raw_data['date time']]
    times[:] = date2num(time_dt, calendar=times.calendar, units=times.units)

    print(raw_data)

    mid_point = int(len(raw_data) / 2)

    # for sensors found in xmlcon file, convert and add a netCDF variable, also add calibration attribute

    # temperature data
    if temp_idx is not None:

        tCal = cal.TemperatureCoefficients

        tCal.a0 = float(sensor_dict[temp_idx]['TemperatureSensor']['A0'])
        tCal.a1 = float(sensor_dict[temp_idx]['TemperatureSensor']['A1'])
        tCal.a2 = float(sensor_dict[temp_idx]['TemperatureSensor']['A2'])
        tCal.a3 = float(sensor_dict[temp_idx]['TemperatureSensor']['A3'])

        print(tCal.a0, tCal.a1, tCal.a2, tCal.a3)

        temperature = conv.convert_temperature(temperature_counts_in=raw_data["temperature"].values, coefs=tCal, standard='ITS90', units='C', use_mv_r=False)
        print('temperature=', temperature[mid_point])

        # create the temperature
        var_temp = dataset.createVariable('TEMP', np.float32, ('TIME',))
        var_temp[:] = temperature

        tcal_vals = sensor_dict[temp_idx]['TemperatureSensor']
        for v, k in enumerate(tcal_vals):
            print('tcal_vals[', v, ']=', tcal_vals[k])
            if not k.startswith('@'):
                var_temp.setncattr('calibration_'+k, tcal_vals[k])

    # use moored pressure if no pressure sensor
    moored_pressure = None
    if pres_idx is None:
        moored_pressure = float(data_dict['SBE_InstrumentConfiguration']['Instrument']['MooredPressure'])
        print('moored pressure', moored_pressure)
        pressure = moored_pressure * ones_like(temperature)

    # pressure data
    if pres_idx is not None:
        pCal = cal.PressureCoefficients

        pCal.pa0 = float(sensor_dict[pres_idx]['PressureSensor']['PA0'])
        pCal.pa1 = float(sensor_dict[pres_idx]['PressureSensor']['PA1'])
        pCal.pa2 = float(sensor_dict[pres_idx]['PressureSensor']['PA2'])
        pCal.ptca0 = float(sensor_dict[pres_idx]['PressureSensor']['PTCA0'])
        pCal.ptca1 = float(sensor_dict[pres_idx]['PressureSensor']['PTCA1'])
        pCal.ptca2 = float(sensor_dict[pres_idx]['PressureSensor']['PTCA2'])
        pCal.ptcb0 = float(sensor_dict[pres_idx]['PressureSensor']['PTCB0'])
        pCal.ptcb1 = float(sensor_dict[pres_idx]['PressureSensor']['PTCB1'])
        pCal.ptcb2 = float(sensor_dict[pres_idx]['PressureSensor']['PTCB2'])

        pCal.ptempa0 = float(sensor_dict[pres_idx]['PressureSensor']['PTEMPA0'])
        pCal.ptempa1 = float(sensor_dict[pres_idx]['PressureSensor']['PTEMPA1'])
        pCal.ptempa2 = float(sensor_dict[pres_idx]['PressureSensor']['PTEMPA2'])

        pressure = conv.convert_pressure(pressure_count=raw_data["pressure"].values, compensation_voltage=raw_data["temperature compensation"].values, coefs=pCal, units='dbar')
        print('pressure=', pressure[mid_point])

        # create the pressure
        var_pres = dataset.createVariable('PRES', np.float32, ('TIME',))
        var_pres[:] = pressure

        pcal_vals = sensor_dict[pres_idx]['PressureSensor']
        for v, k in enumerate(pcal_vals):
            print('pcal_vals[', v, ']=', pcal_vals[k])
            if not k.startswith('@'):
                var_pres.setncattr('calibration_'+k, pcal_vals[k])

    # conductivity data
    if cndc_idx is not None:

        cCal = cal.ConductivityCoefficients

        cCal.g = float(sensor_dict[cndc_idx]['ConductivitySensor']['Coefficients'][1]['G'])
        cCal.h = float(sensor_dict[cndc_idx]['ConductivitySensor']['Coefficients'][1]['H'])
        cCal.i = float(sensor_dict[cndc_idx]['ConductivitySensor']['Coefficients'][1]['I'])
        cCal.j = float(sensor_dict[cndc_idx]['ConductivitySensor']['Coefficients'][1]['J'])
        cCal.cpcor = float(sensor_dict[cndc_idx]['ConductivitySensor']['Coefficients'][1]['CPcor'])
        cCal.ctcor = float(sensor_dict[cndc_idx]['ConductivitySensor']['Coefficients'][1]['CTcor'])
        cCal.wbotc = float(sensor_dict[cndc_idx]['ConductivitySensor']['Coefficients'][1]['WBOTC'])

        conductivity = conv.convert_conductivity(conductivity_count=raw_data["conductivity"].values, temperature=temperature, pressure=pressure, coefs=cCal)
        print('conductivity=', conductivity[mid_point])

        # create the conductivity
        var_cndc = dataset.createVariable('CNDC', np.float32, ('TIME',))
        var_cndc[:] = conductivity

        # calculate salinity from conductivity, temperature and pressure
        salinity = gsw.conversions.SP_from_C(C=conductivity*10, t=temperature, p=pressure)
        print('salinity=', salinity[mid_point])

        # create the salinity
        var_psal = dataset.createVariable('PSAL', np.float32, ('TIME',))
        var_psal[:] = salinity
        if moored_pressure is not None:
            var_psal.comment_pressure = 'using moored pressure ' + str(moored_pressure) + ' dbar for salinity calculation'

        var_cndc.setncattr('calibration_SerialNumber', sensor_dict[cndc_idx]['ConductivitySensor']['SerialNumber'])
        var_cndc.setncattr('calibration_CalibrationDate', sensor_dict[cndc_idx]['ConductivitySensor']['CalibrationDate'])

        ccal_vals = sensor_dict[cndc_idx]['ConductivitySensor']['Coefficients'][1]
        for v, k in enumerate(ccal_vals):
            print('ccal_vals[', v, ']=', ccal_vals[k])
            if not k.startswith('@'):
                var_cndc.setncattr('calibration_'+k, ccal_vals[k])

    # oxygen data
    if dox_idx is not None:

        oCal = cal.Oxygen63Coefficients

        oCal.a0 = float(sensor_dict[dox_idx]['OxygenSensor']['A0'])
        oCal.a1 = float(sensor_dict[dox_idx]['OxygenSensor']['A1'])
        oCal.a2 = float(sensor_dict[dox_idx]['OxygenSensor']['A2'])
        oCal.b0 = float(sensor_dict[dox_idx]['OxygenSensor']['B0'])
        oCal.b1 = float(sensor_dict[dox_idx]['OxygenSensor']['B1'])
        oCal.c0 = float(sensor_dict[dox_idx]['OxygenSensor']['C0'])
        oCal.c1 = float(sensor_dict[dox_idx]['OxygenSensor']['C1'])
        oCal.c2 = float(sensor_dict[dox_idx]['OxygenSensor']['C2'])
        oCal.e = float(sensor_dict[dox_idx]['OxygenSensor']['pcor'])

        otCal = cal.Thermistor63Coefficients
        otCal.ta0 = float(sensor_dict[dox_idx]['OxygenSensor']['TA0'])
        otCal.ta1 = float(sensor_dict[dox_idx]['OxygenSensor']['TA1'])
        otCal.ta2 = float(sensor_dict[dox_idx]['OxygenSensor']['TA2'])
        otCal.ta3 = float(sensor_dict[dox_idx]['OxygenSensor']['TA3'])

        p_density = potential_density_from_t_c_p(temperature=temperature, pressure=pressure, conductivity=conductivity*10)
        print('p_density=', p_density[mid_point])

        temperature_ox = conv.convert_sbe63_thermistor(instrument_output=raw_data["SBE63 oxygen temperature"].values, coefs=otCal)
        print('temperature_ox=', temperature_ox[mid_point])

        oxygen = conv.convert_sbe63_oxygen(raw_oxygen_phase=raw_data["SBE63 oxygen phase"].values, thermistor=raw_data["SBE63 oxygen temperature"].values, pressure=pressure, salinity=salinity, coefs=oCal, thermistor_coefs=otCal, thermistor_units="volts")
        oxygen_umol_kg = conv.convert_oxygen_to_umol_per_kg(ox_values=oxygen, potential_density=p_density)
        print('oxygen=', oxygen[mid_point], 'umol/kg', oxygen_umol_kg[mid_point])

        # create the oxygen
        var_dox = dataset.createVariable('DOX2', np.float32, ('TIME',))
        var_dox[:] = oxygen_umol_kg

        ocal_vals = sensor_dict[dox_idx]['OxygenSensor']
        for v, k in enumerate(ocal_vals):
            print('ocal_vals[', v, ']=', ocal_vals[k])
            if not k.startswith('@'):
                var_dox.setncattr('calibration_'+k, ocal_vals[k])

    # add some file metadata to netCDF file
    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    # create the time coverage attributes
    dataset.setncattr("time_coverage_start", raw_data.iloc[0]['date time'].to_pydatetime().strftime(ncTimeFormat))
    dataset.setncattr("time_coverage_end", raw_data.iloc[-1]['date time'].to_pydatetime().strftime(ncTimeFormat))

    # add creating and history entry
    dataset.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
    dataset.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + os.path.basename(hex_file))

    dataset.close()

    return outputName


if __name__ == "__main__":

    # process command line arguments
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))
    files.sort()

    hex_file = ''
    xmlcon_file = ''
    for f in range(0, len(files)):
        if files[f].endswith('.hex'):
            hex_file = files[f]
        if files[f].endswith('.xmlcon'):
            xmlcon_file = files[f]

        if os.path.basename(hex_file).replace(".hex","") == os.path.basename(xmlcon_file).replace(".xmlcon",""):
            prase_hex(hex_file, xmlcon_file)
            hex_file = ''
            xmlcon_file = ''

