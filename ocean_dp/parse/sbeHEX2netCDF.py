#!/usr/bin/python3

# Copyright (C) 2019 Peter Jansen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# process seabird SBE37SM hex files into netCDF files, needs a xmlcon file for the calibration coefficients

from datetime import datetime, UTC
import os
import sys
from glob2 import glob

import numpy as np
from numpy.ma.core import ones_like

import gsw.conversions
import seabirdscientific.instrument_data as id
import seabirdscientific.cal_coefficients as cal
import seabirdscientific.conversion as conv
from seabirdscientific.conversion import potential_density_from_t_c_p

import xmltodict

from cftime import date2num
from netCDF4 import Dataset


def add_cal_to_netCDF_var(var, cal_xml):
    for v, k in enumerate(cal_xml):
        print(var.name, '[', k, ']=', cal_xml[k])
        if not k.startswith('@'):
            if k.startswith('SerialNumber') or k.startswith('CalibrationDate'):
                var.setncattr('calibration_' + k, cal_xml[k])
            else:
                var.setncattr('calibration_' + k, float(cal_xml[k]))


def prase_hex(hex_file, xml_file):

    # open the xml file and parse to dict
    with open(xmlcon_file, 'r', encoding='utf-8') as file:
        data_dict = xmltodict.parse(file.read())
    #print(data_dict)

    sensor_dict = data_dict['SBE_InstrumentConfiguration']['Instrument']['SensorArray']['Sensor']

    # find sensor index for temperature, pressure, conductivity and oxygen
    temp_cal_xml = None
    pres_cal_xml = None
    cndc_cal_xml = None
    dox_cal_xml = None
    for s in sensor_dict:
        print(s.keys())
        if 'TemperatureSensor' in s.keys():
            temp_id = s['@SensorID']
            temp_cal_xml = s['TemperatureSensor']
            print('temp_id', temp_id)
        if 'PressureSensor' in s.keys():
            pres_id = s['@SensorID']
            pres_cal_xml = s['PressureSensor']
            print('pres_id', pres_id)
        if 'ConductivitySensor' in s.keys():
            cndc_id = s['@SensorID']
            cndc_cal_xml = s['ConductivitySensor']
            print('cndc_id', cndc_id)
        if 'OxygenSensor' in s.keys():
            dox_id = s['@SensorID']
            dox_cal_xml = s['OxygenSensor']
            print('dox_id', dox_id)

    # read hex file, add sensors found in xmlcon file
    sensor_list = []
    if temp_cal_xml is not None:
        sensor_list.append(id.Sensors.Temperature)
    if cndc_cal_xml is not None:
        sensor_list.append(id.Sensors.Conductivity)
    if pres_cal_xml is not None:
        sensor_list.append(id.Sensors.Pressure)
    if dox_cal_xml is not None:
        sensor_list.append(id.Sensors.SBE63)

    raw_data = id.read_hex_file(filepath=hex_file, instrument_type=id.InstrumentType.SBE37SM, enabled_sensors=sensor_list)

    # create a output netCDF file
    outputName = os.path.basename(hex_file) + ".nc"

    print("output file : %s" % outputName)

    dataset = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    # record instrument config in netCDF metadata
    dataset.instrument_model = data_dict['SBE_InstrumentConfiguration']['Instrument']['DeviceType']
    dataset.instrument = "Sea-Bird Electronics ; " + dataset.instrument_model
    # make the SN look like 037xxxxx
    dataset.instrument_serial_number = '037' + temp_cal_xml['SerialNumber'].zfill(5)

    # create the time array
    time_dim = dataset.createDimension('TIME', len(raw_data))
    times = dataset.createVariable('TIME', np.float64, ('TIME',))
    times.long_name = "time"
    times.units = "days since 1950-01-01 00:00:00 UTC"
    times.calendar = "gregorian"
    times.axis = "T"

    # data frame stores date time as np.datetime64, convert to datetime, then to datenum based on time variable calendar and units
    time_dt = [x.to_pydatetime() for x in raw_data['date time']]
    times[:] = date2num(time_dt, calendar=times.calendar, units=times.units)

    print(raw_data)

    mid_point = int(len(raw_data) / 2)

    # for sensors found in xmlcon file, convert and add a netCDF variable, also add calibration attribute

    # temperature data
    if temp_cal_xml is not None:

        tCal = cal.TemperatureCoefficients
        tCal.a0 = float(temp_cal_xml['A0'])
        tCal.a1 = float(temp_cal_xml['A1'])
        tCal.a2 = float(temp_cal_xml['A2'])
        tCal.a3 = float(temp_cal_xml['A3'])

        temperature = conv.convert_temperature(temperature_counts_in=raw_data["temperature"].values, coefs=tCal, standard='ITS90', units='C', use_mv_r=False)
        print('temperature=', temperature[mid_point])

        # create the temperature
        var_temp = dataset.createVariable('TEMP', np.float32, ('TIME',))
        var_temp[:] = temperature
        var_temp.units = 'degrees_celsius'

        add_cal_to_netCDF_var(var_temp, temp_cal_xml)

    # use moored pressure if no pressure sensor
    moored_pressure = None
    if pres_cal_xml is None:
        moored_pressure = float(data_dict['SBE_InstrumentConfiguration']['Instrument']['MooredPressure'])
        print('moored pressure', moored_pressure)
        pressure = moored_pressure * ones_like(temperature)

    # pressure data
    if pres_cal_xml is not None:

        pCal = cal.PressureCoefficients
        pCal.pa0 = float(pres_cal_xml['PA0'])
        pCal.pa1 = float(pres_cal_xml['PA1'])
        pCal.pa2 = float(pres_cal_xml['PA2'])
        pCal.ptca0 = float(pres_cal_xml['PTCA0'])
        pCal.ptca1 = float(pres_cal_xml['PTCA1'])
        pCal.ptca2 = float(pres_cal_xml['PTCA2'])
        pCal.ptcb0 = float(pres_cal_xml['PTCB0'])
        pCal.ptcb1 = float(pres_cal_xml['PTCB1'])
        pCal.ptcb2 = float(pres_cal_xml['PTCB2'])

        pCal.ptempa0 = float(pres_cal_xml['PTEMPA0'])
        pCal.ptempa1 = float(pres_cal_xml['PTEMPA1'])
        pCal.ptempa2 = float(pres_cal_xml['PTEMPA2'])

        pressure = conv.convert_pressure(pressure_count=raw_data["pressure"].values, compensation_voltage=raw_data["temperature compensation"].values, coefs=pCal, units='dbar')
        print('pressure=', pressure[mid_point])

        # create the pressure
        var_pres = dataset.createVariable('PRES', np.float32, ('TIME',))
        var_pres[:] = pressure
        var_pres.units = 'dbar'

        add_cal_to_netCDF_var(var_pres, pres_cal_xml)

    # conductivity data
    if cndc_cal_xml is not None:

        # select 2nd equation <UseG_J>1</UseG_J>, <Coefficients equation="1" >
        cCal = cal.ConductivityCoefficients
        cCal.g = float(cndc_cal_xml['Coefficients'][1]['G'])
        cCal.h = float(cndc_cal_xml['Coefficients'][1]['H'])
        cCal.i = float(cndc_cal_xml['Coefficients'][1]['I'])
        cCal.j = float(cndc_cal_xml['Coefficients'][1]['J'])
        cCal.cpcor = float(cndc_cal_xml['Coefficients'][1]['CPcor'])
        cCal.ctcor = float(cndc_cal_xml['Coefficients'][1]['CTcor'])
        cCal.wbotc = float(cndc_cal_xml['Coefficients'][1]['WBOTC'])

        conductivity = conv.convert_conductivity(conductivity_count=raw_data["conductivity"].values, temperature=temperature, pressure=pressure, coefs=cCal)
        print('conductivity=', conductivity[mid_point])

        # create the conductivity
        var_cndc = dataset.createVariable('CNDC', np.float32, ('TIME',))
        var_cndc[:] = conductivity
        var_cndc.units = 'S/m'

        var_cndc.setncattr('calibration_SerialNumber', cndc_cal_xml['SerialNumber'])
        var_cndc.setncattr('calibration_CalibrationDate', cndc_cal_xml['CalibrationDate'])

        add_cal_to_netCDF_var(var_cndc, cndc_cal_xml['Coefficients'][1])

        # calculate salinity from conductivity, temperature and pressure
        salinity = gsw.conversions.SP_from_C(C=conductivity*10, t=temperature, p=pressure)
        print('salinity=', salinity[mid_point])

        # create the salinity
        var_psal = dataset.createVariable('PSAL', np.float32, ('TIME',))
        var_psal[:] = salinity
        if moored_pressure is not None:
            var_psal.comment_pressure = 'using moored pressure ' + str(moored_pressure) + ' dbar for salinity calculation'
        var_psal.units = '1'

        # create the sigma-t0
        p_density = potential_density_from_t_c_p(temperature=temperature, pressure=pressure, conductivity=conductivity*10)
        print('p_density=', p_density[mid_point])

        var_sigma_t0 = dataset.createVariable('SIGMA-T0', np.float32, ('TIME',))
        var_sigma_t0[:] = p_density
        var_sigma_t0.units = 'kg/m^3'

    # oxygen data
    if dox_cal_xml is not None:

        oCal = cal.Oxygen63Coefficients
        oCal.a0 = float(dox_cal_xml['A0'])
        oCal.a1 = float(dox_cal_xml['A1'])
        oCal.a2 = float(dox_cal_xml['A2'])
        oCal.b0 = float(dox_cal_xml['B0'])
        oCal.b1 = float(dox_cal_xml['B1'])
        oCal.c0 = float(dox_cal_xml['C0'])
        oCal.c1 = float(dox_cal_xml['C1'])
        oCal.c2 = float(dox_cal_xml['C2'])
        oCal.e = float(dox_cal_xml['pcor'])

        otCal = cal.Thermistor63Coefficients
        otCal.ta0 = float(dox_cal_xml['TA0'])
        otCal.ta1 = float(dox_cal_xml['TA1'])
        otCal.ta2 = float(dox_cal_xml['TA2'])
        otCal.ta3 = float(dox_cal_xml['TA3'])

        temperature_ox = conv.convert_sbe63_thermistor(instrument_output=raw_data["SBE63 oxygen temperature"].values, coefs=otCal)
        print('temperature_ox=', temperature_ox[mid_point])

        # create the oxygen temperature
        var_dox_temp = dataset.createVariable('TEMP_DOX', np.float32, ('TIME',))
        var_dox_temp[:] = temperature_ox
        var_dox_temp.units = 'degrees_celsius'

        oxygen = conv.convert_sbe63_oxygen(raw_oxygen_phase=raw_data["SBE63 oxygen phase"].values, thermistor=raw_data["SBE63 oxygen temperature"].values, pressure=pressure, salinity=salinity, coefs=oCal, thermistor_coefs=otCal, thermistor_units="volts")
        #oxygen = conv.convert_sbe63_oxygen(raw_oxygen_phase=raw_data["SBE63 oxygen phase"].values, thermistor=temperature, pressure=pressure, salinity=salinity, coefs=oCal, thermistor_coefs=None, thermistor_units="C")
        print('oxygen ml/l=', oxygen[mid_point])

        # create the oxygen
        var_dox = dataset.createVariable('DOX', np.float32, ('TIME',))
        var_dox[:] = oxygen
        var_dox.units = 'ml/l'

        # add calibration values to netCDF file
        add_cal_to_netCDF_var(var_dox, dox_cal_xml)

        oxygen_umol_kg = conv.convert_oxygen_to_umol_per_kg(ox_values=oxygen, potential_density=p_density)
        print('oxygen umol/kg=', oxygen_umol_kg[mid_point])

        # create the oxygen
        var_dox2 = dataset.createVariable('DOX2', np.float32, ('TIME',))
        var_dox2[:] = oxygen_umol_kg
        var_dox2.units = 'umol/kg'

        # add calibration values to netCDF file
        add_cal_to_netCDF_var(var_dox2, dox_cal_xml)

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

    # sort files, sorts hex file followed by xmlcon file
    files.sort()

    hex_found = False
    xml_found = False
    match_found = False

    # select matching hex and xmlcon files, probably should be done in two passes
    hex_file = ''
    xmlcon_file = ''
    for f in range(0, len(files)):
        if files[f].endswith('.hex'):
            hex_file = files[f]
            hex_found = True
        if files[f].endswith('.xmlcon'):
            xmlcon_file = files[f]
            xml_found = True

        if os.path.splitext(hex_file)[0] == os.path.splitext(xmlcon_file)[0]:
            prase_hex(hex_file, xmlcon_file)
            match_found = True
            hex_file = ''
            xmlcon_file = ''

    if not match_found:
        print('No matching files')
        if not hex_found:
            print('No hex files')
        if not xml_found:
            print('No xmlcon files')