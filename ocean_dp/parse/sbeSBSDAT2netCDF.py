#!/usr/bin/python3
import struct
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
import re

from datetime import datetime, UTC

from netCDF4 import Dataset, date2num
import numpy as np

from bs4 import BeautifulSoup
import bs4

# add attributes from sbedat file to a seaphox netcdf file

def parse_sbs(sbs_file):

    fp = open(sbs_file, errors='ignore')
    soup = BeautifulSoup(fp, features="xml")

    #
    # build the netCDF file
    #

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = sbs_file + ".nc"
    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    ncOut.instrument = 'Sea-Bird Electronics ; Deep SeapHox2'

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME")
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=False)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"

    var_temp_r = ncOut.createVariable("TEMP_RAW", "f4", ("TIME",), zlib=False)
    var_temp = ncOut.createVariable("TEMP", "f4", ("TIME",), zlib=False)
    var_pres_r = ncOut.createVariable("PRES_RAW", "f4", ("TIME",), zlib=False)
    var_pres = ncOut.createVariable("PRES", "f4", ("TIME",), zlib=False)
    var_cndc_r = ncOut.createVariable("COND_RAW", "f4", ("TIME",), zlib=False)
    var_cndc = ncOut.createVariable("CNDC", "f4", ("TIME",), zlib=False)
    var_ophase = ncOut.createVariable("OXPHASE_RAW", "f4", ("TIME",), zlib=False)
    var_otemp = ncOut.createVariable("OXTEMP_RAW", "f4", ("TIME",), zlib=False)
    var_ph_v = ncOut.createVariable("VEXT_PH", "f4", ("TIME",), zlib=False)
    var_ph_cv = ncOut.createVariable("pH_COUNTER_V", "f4", ("TIME",), zlib=False)
    var_ph_b = ncOut.createVariable("pH_BASE_I", "f4", ("TIME",), zlib=False)
    var_ph_ci = ncOut.createVariable("PH_COUNTER_I", "f4", ("TIME",), zlib=False)
    var_hum = ncOut.createVariable("IHUMID", "f4", ("TIME",), zlib=False)
    var_itemp = ncOut.createVariable("ITEMP", "f4", ("TIME",), zlib=False)
    var_flags = ncOut.createVariable("FLAGS", "i4", ("TIME",), zlib=False)

    cals = soup.find_all('Calibration')

    for c in cals:
        # print(c)
        # print(c.attrs)

        for vals in c.contents:
            if isinstance(vals, bs4.element.Tag):
                print(c.attrs['id'], ':', vals.name, '=', vals.getText())
                if vals.name.startswith('SerialNum') or vals.name == 'CalDate':
                    out = vals.getText()
                else:
                    out = float(vals.getText())
                if c.attrs['id'] == 'Oxygen':
                    var_ophase.setncattr('calibration_' + vals.name, out)
                if c.attrs['id'] == 'Pressure':
                    var_pres.setncattr('calibration_' + vals.name, out)
                if c.attrs['id'] == 'Conductivity':
                    var_cndc.setncattr('calibration_' + vals.name, out)
                if c.attrs['id'] == 'Temperature':
                    var_temp.setncattr('calibration_' + vals.name, out)
                if c.attrs['id'] == 'pH':
                    var_ph_v.setncattr('calibration_' + vals.name, out)

        print()

    hardware_data = soup.find_all('HardwareData')
    for h in hardware_data:
        print(h.attrs)
        print(h.attrs['DeviceType'])

        ncOut.instrument_model = h.attrs['DeviceType']
        ncOut.instrument_serial_number = h.attrs['SerialNumber']

    firmware_version = soup.find('FirmwareVersion')
    for f in firmware_version:
        print('Firmware Version', f.getText())

        ncOut.instrument_firmware_version = f.getText()

    fb = open(sbs_file, 'rb')
    s = fb.read()
    raw_data = s.find(b'InstrumentRawData')
    print("file search ", raw_data)

    a0 = var_temp.calibration_A0
    a1 = var_temp.calibration_A1
    a2 = var_temp.calibration_A2
    a3 = var_temp.calibration_A3

    pa0 = var_pres.calibration_PA0
    pa1 = var_pres.calibration_PA1
    pa2 = var_pres.calibration_PA2
    ptca0 = var_pres.calibration_PTCA0
    ptca1 = var_pres.calibration_PTCA1
    ptca2 = var_pres.calibration_PTCA2
    ptcb0 = var_pres.calibration_PTCB0
    ptcb1 = var_pres.calibration_PTCB1
    ptcb2 = var_pres.calibration_PTCB2
    ptempa0 = var_pres.calibration_PTEMPA0
    ptempa1 = var_pres.calibration_PTEMPA1
    ptempa2 = var_pres.calibration_PTEMPA2

    g = var_cndc.calibration_G
    h = var_cndc.calibration_H
    cndc_i = var_cndc.calibration_I
    j = var_cndc.calibration_J
    pCor = var_cndc.calibration_PCOR
    tCor = var_cndc.calibration_TCOR
    wbotc = var_cndc.calibration_WBOTC

    data_blocks = b''
    data_blocks_len = 0
    while raw_data != -1:
        count_start = s.find(b'<Bytes>', raw_data)
        count_end = s.find(b'</Bytes>', raw_data)
        raw_data = s.find(b'<Data>', raw_data)
        if raw_data != -1:
            if count_start != -1:
                bytes_to_read = int(s[count_start+7:count_end])
                print('bytes to read', count_start, count_end, bytes_to_read)
                print("file data ", raw_data, s[raw_data:raw_data+6])
                data_block = s[raw_data+6:raw_data+6+bytes_to_read]
                data_blocks += data_block
                data_blocks_len += bytes_to_read
                #print(data_block)
            raw_data += 6

    sample = 0
    for i in range(0, data_blocks_len, 38):
        temp_raw = int.from_bytes(data_blocks[i:i+3], 'big', signed=False)
        temp = np.log(temp_raw)
        temp = 1 / (a0 + (temp * (a1 + temp * (a2 + temp * a3)))) - 273.15
        #print(data_blocks[i:i + 3], temp_raw, temp)
        var_temp[sample] = temp
        var_temp_r[sample] = temp_raw

        pres_raw = int.from_bytes(data_blocks[i + 6:i + 9], 'big', signed=False)
        #pressTemp = ptempa0 + temp * (ptempa1 + (temp * ptempa2))
        pressTemp = temp
        pval = pres_raw - ptca0 - (pressTemp * (ptca1 + (pressTemp * ptca2)))
        pval = pval * ptcb0 / (ptcb0 + (pressTemp * (ptcb1 + (pressTemp * ptcb2))))

        pressure = pa0 + pval * (pa1 + (pval * pa2))
        pressure = 0.6894759 * (pressure - 14.7)
        #print(data_blocks[i+6:i + 9], pres_raw, pressure)
        var_pres_r[sample] = pres_raw
        var_pres[sample] = pressure

        cndc_raw = int.from_bytes(data_blocks[i + 3:i + 6], 'big', signed=False)
        localCFreq = cndc_raw/256
        localCFreq = localCFreq * np.sqrt(1.0 + wbotc * temp)
        localCFreq /= 1000.0
        cond = g + localCFreq * localCFreq * (h + localCFreq * (cndc_i + (localCFreq * j)))
        cond = cond / (1.0 + (temp * tCor) + (pressure * pCor))

        #print(data_blocks[i+3:i + 6], cndc_raw, cond)
        var_cndc_r[sample] = cndc_raw
        var_cndc[sample] = cond

        ptemp = int.from_bytes(data_blocks[i + 9:i + 10], 'big', signed=False)
        ox_phase = int.from_bytes(data_blocks[i + 11:i + 14], 'big', signed=False)
        ox_temp = int.from_bytes(data_blocks[i + 14:i + 17], 'big', signed=False)
        ph_ext_v = int.from_bytes(data_blocks[i + 17:i + 20], 'big', signed=False)
        ph_counter_v = int.from_bytes(data_blocks[i + 20:i + 23], 'big', signed=False)
        ph_base_i = int.from_bytes(data_blocks[i + 23:i + 26], 'big', signed=False)
        ph_counter_i = int.from_bytes(data_blocks[i + 26:i + 29], 'big', signed=False)
        int_hum = (int.from_bytes(data_blocks[i + 29:i + 32], 'big', signed=False) & 0xfff000) >> 12
        int_temp = int.from_bytes(data_blocks[i + 29:i + 32], 'big', signed=False) & 0x00fff

        print(data_blocks[i + 29:i + 32].hex())
        var_otemp[sample] = ox_temp
        var_ophase[sample] = ox_phase
        var_ph_b[sample] = ph_base_i
        var_ph_cv[sample] = 2.5 * (ph_counter_v/8388608 - 1)
        var_ph_v[sample] = 2.5 * (ph_ext_v/8388608 - 1)
        var_ph_ci[sample] = ph_counter_i

        var_hum[sample] = int_hum/22.1429 - 14.6484
        var_itemp[sample] = int_temp/23.2222 - 47.0947

        time_raw = int.from_bytes(data_blocks[i + 32:i + 36], 'big', signed=False)  # time in seconds since 2000-01-01 00:00:00
        ts = datetime.fromtimestamp(time_raw + 946684800, tz=UTC)
        ncTimesOut[sample] = date2num(ts, units=ncTimesOut.units)

        flags = int.from_bytes(data_blocks[i + 36:i + 38], 'big', signed=False)
        var_flags[sample] = flags

        print(time_raw, ts, temp, pressure, cond, int_hum, int_temp)

        sample += 1

    fb.close()

    try:
        hist = ncOut.history + "\n"
    except AttributeError:
        hist = ""

    ncOut.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " attributes from " + sbs_file)

    ncOut.close()

    return


if __name__ == "__main__":
    sbs_file = sys.argv[1]

    parse_sbs(sbs_file)
