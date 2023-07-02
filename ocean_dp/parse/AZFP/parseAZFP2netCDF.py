#!/usr/bin/python3

# raw2netCDF
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

import sys

import datetime
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct
import xml.etree.ElementTree as ET


def parse_azfp(files):
    output_name = files[1] + ".nc"

    print("output file : %s" % output_name)

    config_xml_tree = ET.parse(files[1])
    root = config_xml_tree.getroot()
    print(root)

    for sn in root.findall('.//AZFP_Version/SerialNumber'):
        print (sn.tag, sn.text)

    instrument_serialnumber = sn.text
    instrument_model = 'AZFP'

    frequencies = []
    for f in root.findall('.//LogAcousticCoefficients/Frequencies/Frequency'):
        fn = f.findall('kHz')[0].text
        tvr = float(f.findall('TVR')[0].text)
        vtx = float(f.findall('VTX0')[0].text)
        bp = float(f.findall('BP')[0].text)
        el = float(f.findall('EL')[0].text)
        ds = float(f.findall('DS')[0].text)
        frequencies.append({'f': fn, 'tvr': tvr, 'vtx': vtx, 'bp': bp, 'el': el, 'ds': ds})

    print (frequencies)

    for f in root.findall('.//ULS5_LogConfiguration/AG_Tilt'):
        X_a = float(f.findall('X_a')[0].text)
        X_b = float(f.findall('X_b')[0].text)
        X_c = float(f.findall('X_c')[0].text)
        X_d = float(f.findall('X_d')[0].text)
        tilt_x_cal = {'a': X_a, 'b': X_b, 'c': X_c, 'd': X_d}
        Y_a = float(f.findall('Y_a')[0].text)
        Y_b = float(f.findall('Y_b')[0].text)
        Y_c = float(f.findall('Y_c')[0].text)
        Y_d = float(f.findall('Y_d')[0].text)
        tilt_y_cal = {'a': Y_a, 'b': Y_b, 'c': Y_c, 'd': Y_d}

    for f in root.findall('.//ULS5_LogConfiguration/Analog_Temperature'):
        ka = float(f.findall('ka')[0].text)
        kb = float(f.findall('kb')[0].text)
        kc = float(f.findall('kc')[0].text)
        a = float(f.findall('A')[0].text)
        b = float(f.findall('B')[0].text)
        c = float(f.findall('C')[0].text)
        temp_cal = {'ka': ka, 'kb': kb, 'kc': kc, 'a': a, 'b': b, 'c': c}

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut = Dataset(output_name, 'w', format='NETCDF4_CLASSIC')

    ncOut.instrument = 'ASL Environmental Sciences ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = instrument_serialnumber

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME") # create unlimited
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=False)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"

    # // setup the one-way water range attenuation in dB/m
    # alpha[0] = 0.0101;
    # alpha[1] = 0.0399;
    # alpha[2] = 0.0548;
    # alpha[3] = 0.1156;
    #
    # // hacked values based on 55046 data from SOFS-4 to make average constant with distance
    # alpha[0] = -0.0101;
    # alpha[1] = -0.025;
    # alpha[2] = -0.03;
    # alpha[3] = -0.065;
    #
    # // From http://resource.npl.co.uk/acoustics/techguides/seaabsorption/ t = 10C, depth = 0.05, sal = 35, ph=8, (Ainslie and McColm 1998)
    alpha = {}
    alpha[0] = 10.298 / 1000
    alpha[1] = 40.583 / 1000
    alpha[2] = 55.674 / 1000
    alpha[3] = 116.741 / 1000

    # read the data, and write to netCDF file
    sample_n = 0
    ch_var = {}
    ch_range_var = {}
    ch_range = {}
    ch_const = {}

    for f in files[2:]:
        with open(f, "rb") as binary_file:
            data = binary_file.read(2)
            while data:
                #print("hdr ", data)
                if data == b'\xfd\x02':
                    #print(data)
                    data = binary_file.read(10)
                    packet = struct.unpack(">hHhI", data)
                    d = ['burst', 'serial', 'status', 'interval']
                    pack_dict = dict(zip(d, packet))
                    #print(pack_dict)

                    data = binary_file.read(14)
                    packet = struct.unpack(">hhhhhhh", data)
                    d = ['year', 'month', 'day', 'hour', 'min', 'second', 'hun_seconds']
                    time_dict = dict(zip(d, packet))
                    #print(time_dict)

                    t = datetime.datetime(time_dict['year'], time_dict['month'], time_dict['day'],
                                          time_dict['hour'], time_dict['min'], time_dict['second'],
                                          time_dict['hun_seconds'] * 10 * 1000) # convert hundreth of seconds to microseconds
                    print(t)

                    ts = date2num(t, calendar=ncTimesOut.calendar, units=ncTimesOut.units)
                    ncTimesOut[sample_n] = ts

                    data = binary_file.read(32)
                    packet = struct.unpack(">4H4h4h4h", data)
                    d = ['rate1', 'rate2', 'rate3', 'rate4', 'lock1', 'lock2', 'lock3', 'lock4',
                         'bin1', 'bin2', 'bin3', 'bin4', 'samples1', 'sample2', 'samples3', 'samples4']
                    samples_dict = dict(zip(d, packet))
                    #print(samples_dict)

                    data = binary_file.read(5*2)
                    packet = struct.unpack((">hhhHH"), data)
                    d = ['pings', 'npings', 'seconds', 'first_ping', 'last_ping']
                    ping_dict = dict(zip(d, packet))
                    #print(ping_dict)

                    data = binary_file.read(9)
                    packet = struct.unpack((">ihbbb"), data)
                    d = ['data_type', 'data_error', 'phase', 'overrun', 'channels']
                    data_type_dict = dict(zip(d, packet))
                    print(data_type_dict)

                    data = binary_file.read(7+12*2)
                    packet = struct.unpack((">4B3b4h4h4h"), data)
                    d = ['gain_1', 'gain_2', 'gain_3', 'gain_4', 'spare_1', 'spare_2', 'spare_3',
                         'pulse_len_1', 'pulse_len_2', 'pulse_len_3', 'pulse_len_4',
                         'board_1', 'board_2', 'board_3', 'board_4',
                         'freq_1', 'freq_2', 'freq_3', 'freq_4']
                    pings_dict = dict(zip(d, packet))
                    #print(pings_dict)

                    # sensors
                    data = binary_file.read(8*2)
                    packet = struct.unpack((">8H"), data)
                    d = ['sensors', 'tilt_x', 'tilt_y', 'battery', 'pressure', 'temperature', 'ad6', 'ad7']
                    sensors_dict = dict(zip(d, packet))
                    #print(sensors_dict)

                    # read sensor data
                    if sample_n == 0:
                        tilt_x_var = ncOut.createVariable("TILT_X", "f4", ("TIME",), fill_value=np.nan)
                        tilt_y_var = ncOut.createVariable("TILT_Y", "f4", ("TIME",), fill_value=np.nan)
                        battery_var = ncOut.createVariable("BAT", "f4", ("TIME",), fill_value=np.nan)
                        battery_var.long_name = 'instrument battery voltage'
                        pressure_var = ncOut.createVariable("PRES", "f4", ("TIME",), fill_value=np.nan)
                        pressure_var.comment = 'not used, no sensor fitted'
                        temperature_var = ncOut.createVariable("TEMP", "f4", ("TIME",), fill_value=np.nan)
                        temperature_var.comment = 'instrument temperature'


                    tilt = sensors_dict['tilt_x']
                    tilt_x_var[sample_n] = tilt_x_cal['a'] + tilt_x_cal['b'] * tilt + tilt_x_cal['c'] * tilt**2 + tilt_x_cal['d'] * tilt**3
                    tilt = sensors_dict['tilt_y']
                    tilt_y_var[sample_n] = tilt_y_cal['a'] + tilt_y_cal['b'] * tilt + tilt_y_cal['c'] * tilt**2 + tilt_y_cal['d'] * tilt**3

                    battery_var[sample_n] = 6.5 * 2.5 * sensors_dict['battery']/65536

                    pressure_var[sample_n] = sensors_dict['pressure'] # Not Used ?

                    v = 2.5 * sensors_dict['temperature'] / 65536

                    r = (temp_cal['ka'] + temp_cal['kb'] * v ) / (temp_cal['kc'] - v)
                    lnr = np.log(r)
                    temperature_var[sample_n] = (1/(temp_cal['a'] + temp_cal['b'] * lnr + temp_cal['c'] * (lnr ** 3))) - 273.15

                    # public double getVolts(int i)
                    # {
                    #     return 2.5 * ((double)i) / 65536;
                    # }
                    # public double getBattery()
                    # {
                    #     return 6.5 * getVolts(battery);
                    # }
                    # public double getTemp()
                    # {
                    #     double v = getVolts(temperature);
                    #     double r = (calTempK[0] + calTempK[1] * v ) / (calTempK[2] - v);
                    #     double lnr = Math.log(r);
                    #     return (1/(calTemp[0] + calTemp[1] * lnr + calTemp[2] * Math.pow(lnr, 3))) - 273.15;
                    # }
                    #
                    # public double getTiltX()
                    # {
                    #     double v = tiltx; // manual says volts, but the AD counts gives the correct value (?)
                    #     return calTiltX[0] + calTiltX[1] * v + calTiltX[2] * Math.pow(v, 2) + calTiltX[3] * Math.pow(v, 3);
                    # }
                    # public double getTiltY()
                    # {
                    #     double v = tilty; // manual says volts, but the AD counts gives the correct value (?)
                    #     return calTiltX[0] + calTiltY[1] * v + calTiltY[2] * Math.pow(v, 2) + calTiltY[3] * Math.pow(v, 3);
                    # }
                    #
                    # public double[] getSV(int ch)
                    # {
                    # 	// Sv = ELmax –2.5/ds + N/(26214.ds) – TVR – 20.logVTX + 20.logR + 2.α.R – 10log(1/2c.t.Ψ)
                    #     double[] sv = new double[bins[ch]];
                    #     double r;
                    #
                    #     double c = el[ch] - 2.5/ds[ch] - tvr[ch] - 20 * Math.log10(vtx[ch]) - 10 * Math.log10(bp[ch] * sos * (pulseLen[ch]*1.e-6) / 2.0);
                    #
                    #     for(int i=0;i<bins[ch];i++)
                    #     {
                    #         r = sos * (i + 1) / rate[ch] / 2; // range
                    #         // sv = const + data (dB) + range_spread + range_water_attenuation
                    #         sv[i] = c + data[ch][i]/(26214 * ds[ch])  + 20 * Math.log10(r) + 2 * alpha[ch] * r ; // 26214 = 65536/2.5
                    #
                    #     }
                    #     return sv;
                    # }

                    for i in range(0, 4):
                        # create a dimension for each frequency and phase, as different frequencies and phases have different number of samples
                        if sample_n == 0:
                            ncOut.createDimension("RANGE%d" % (i+1), samples_dict['bin%d' % (i+1)])
                            ch_var[i] = ncOut.createVariable("Sv_%d" % (i+1), "f4", ("TIME", "RANGE%d" % (i+1)), fill_value=np.nan, zlib=False)
                            # sampling configuration
                            ch_var[i].sample_rate = float(samples_dict['rate%d' % (i+1)]) # samples / second
                            ch_var[i].frequency = float(pings_dict['freq_%d' % (i+1)])
                            ch_var[i].pulse_length = float(pings_dict['pulse_len_%d' % (i+1)])
                            ch_var[i].gain = float(pings_dict['gain_%d' % (i+1)])
                            # channel calibration
                            ch_var[i].tx_voltage_response = float(frequencies[i]['tvr'])
                            ch_var[i].voltage_tx = float(frequencies[i]['vtx'])
                            ch_var[i].beam_pattern = float(frequencies[i]['bp'])
                            ch_var[i].echo_level = float(frequencies[i]['el'])
                            ch_var[i].detector_sensitivity = float(frequencies[i]['ds'])
                            # fixed values
                            ch_var[i].speed_of_sound = float(1500)
                            ch_var[i].alpha = float(alpha[i])
                            # range variable
                            ch_range[i] = ch_var[i].speed_of_sound * (np.arange(samples_dict['bin%d' % (i+1)])+1) / (ch_var[i].sample_rate) / 2
                            ch_range_var[i] = ncOut.createVariable("RANGE%d" % (i+1), "f4", ("RANGE%d" % (i+1), ), fill_value=np.nan)
                            ch_range_var[i][:] = ch_range[i]
                            # offset constant part
                            ch_const[i] = ch_var[i].echo_level - 2.5 / ch_var[i].detector_sensitivity - ch_var[i].tx_voltage_response - 20 * np.log10(ch_var[i].voltage_tx) - 10 * np.log10(ch_var[i].beam_pattern * ch_var[i].speed_of_sound * ch_var[i].pulse_length * 1e-6 / 2)

                            ch_var[i].comment = "Sv = ELmax - 2.5/ds + N(26214.ds) - TVR - 20.log(VTX) + 20.Log(R) + 2.alpha.R - 10.log(speed.t.beam/2)"

                        # read data, save to netCDF variables
    
                        #Sv = ELmax –2.5/ds + N/(26214.ds) – TVR – 20.logVTX + 20.logR + 2.α.R – 10log(1/2c.t.Ψ)
                        #  EL = ELmax – 2.5/a + N/(26214·a)
                        #  Sv = ELmax – 2.5/a + N/(26214·a) – SL + 20·logR + 2·α·R – 10log(½c·τ·ψ)
                        #  SL = sound transmission level
                        #  α = absorption coefficient (dB/m)
                        #  τ = transmit pulse length (s)
                        #  c = sound speed (m/s)
                        #  ψ = equivalent solid angle of the transducer beam pattern (sr).
                        #
                        #         sv[i] = c + data[ch][i]/(26214 * ds[ch])  + 20 * Math.log10(r) + 2 * alpha[ch] * r ; // 26214 = 65536/2.5
    
                        data = binary_file.read(2 * samples_dict['bin%d' % (i+1)])
                        ch_data = struct.unpack(">%dH" % samples_dict['bin%d' % (i+1)], data)
                        ch_var[i][sample_n, :] = ch_const[i] + ch_data/(26214 * ch_var[i].detector_sensitivity) + 20 * np.log10(ch_range[i]) + 2 * ch_var[i].alpha * ch_range[i]

                    sample_n += 1

                    # print(data_raw)
                    # break

                data = binary_file.read(2)

    return output_name


if __name__ == "__main__":

    # arguments are <mooring> <xml file> <files....(or zip file)>
    parse_azfp(sys.argv)
