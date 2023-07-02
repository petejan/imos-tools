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

header_decoder = {'keys': ['spare', 'dataTypes'],
                  'unpack': "<BB"}
fixed_decoder = {'keys': ['cpuVER', 'cpuREV', 'sysConfig', 'read', 'lag_len', 'num_beam', 'num_cells',
                          'pings_per_ensemble', 'cell_length', 'blank_after_tx', 'profile_mode', 'low_corr',
                          'code_reps', 'gd_min', 'error_vel_max', 'tpp_min', 'tpp_sec', 'tpp_hun_sec', 'coord_trans',
                          'head_alignment', 'head_bias', 'sensor_source', 'sensor_available', 'bin1_dist',
                          'xmit_pulse_len', 'ref_layer', 'false_target_thresh', 'spare', 'tx_lag_dist', 'cpu_board',
                          'system_bandwidth', 'system_power', 'spare2', 'inst_serial', 'beam_angle'],
                 'unpack': "<BBHBBBBHHHBBBBHBBBBHHBBHHHBBHQHBBLB"}
variable_decoder = {'keys': ['ensemble_no', 'year', 'month', 'day', 'hour', 'minute', 'second', 'hsec',
                             'ensemble_msb', 'result', 'speed_of_sound', 'depth_of_trans', 'heading', 'pitch',
                             'roll', 'salinity', 'temperature', 'mpt_min', 'mpt_sec', 'mpt_hsec', 'hdg_stdev',
                             'pitch_stdev', 'roll_stdev', 'adc0', 'adc1', 'adc2', 'adc3', 'adc4', 'adc5', 'adc6', 'adc7',
                             'error_status', 'spare1', 'pressure', 'press_variance', 'spare2', 'rtc_cen',
                             'rtc_year', 'rtc_month', 'rtc_day', 'rtc_hour', 'rtc_min', 'rtc_sec', 'rtc_hsec'],
                    'unpack': "<H7BB4H4hBBBBBB8BIHiiB8B"}

inst_system_decoder = {}
inst_system_decoder[0] = '75kHz'
inst_system_decoder[1] = '150kHz'
inst_system_decoder[2] = '300kHz'
inst_system_decoder[3] = '600kHz'
inst_system_decoder[4] = '1200kHz'
inst_system_decoder[5] = '2400kHz'

inst_coords_decoder = {}
inst_coords_decoder[0] = 'Beam'
inst_coords_decoder[1] = 'Instrument'
inst_coords_decoder[2] = 'Ship'
inst_coords_decoder[3] = 'Earth'

def main(files):
    filepath = files[1]
    ts_start = None

    times = []
    head = []
    pitch = []
    roll = []
    pressure = []
    pressure_var = []
    speed_of_sound = []

    velocity = []
    corr_mag = []
    echo_int = []
    pct_good = []
    status = []

    ad1 = []

    number_ensambles_read = 0

    with open(filepath, "rb") as binary_file:
        data = binary_file.read(2)
        while data:
            # print("hdr ", data)
            if data == b'\x7f\x7f':

                data = binary_file.read(2)
                (ensemble_len,) = struct.unpack("<H", data)

                #print("length ", ensemble_len)
                ensemble = binary_file.read(ensemble_len-4)

                cksum = binary_file.read(2)
                #print("checksum ", cksum)

                header = struct.unpack(header_decoder["unpack"], ensemble[0:2])
                header_decoded = dict(zip(header_decoder['keys'], header))
                #print("header ", header_decoded)

                n = 2
                addrs = [0 for x in range(0, header_decoded["dataTypes"])]
                for i in range(0, header_decoded["dataTypes"]):
                    addr_data = ensemble[n:n+2]
                    addrs[i] = struct.unpack("<H", addr_data)[0]
                    #print("addr ", addrs[i])
                    n += + 2

                while n < (ensemble_len - 4):
                    data = ensemble[n:n+2]
                    n += 2
                    #print("data hdr ", data)
                    if data == b'\x00\x00':  # fixed header
                        data = ensemble[n:n+57]
                        n += 57
                        fixed = struct.unpack(fixed_decoder["unpack"], data)
                        fixed_decoded = dict(zip(fixed_decoder['keys'], fixed))
                        #print("fixed ", fixed_decoded)

                        num_cells = fixed_decoded['num_cells']
                        num_beams = fixed_decoded['num_beam']

                    elif data == b'\x80\x00':  # variable header
                        data = ensemble[n:n+63]
                        n += 63
                        variable = struct.unpack(variable_decoder["unpack"], data)
                        variable_decoded = dict(zip(variable_decoder['keys'], variable))
                        #print("variable header ", variable_decoded)

                        ts = datetime.datetime(year=variable_decoded['rtc_cen']*100 + variable_decoded['rtc_year'],
                                               month=variable_decoded['rtc_month'], day=variable_decoded['rtc_day'],
                                               hour=variable_decoded['rtc_hour'], minute=variable_decoded['rtc_min'],
                                               second=variable_decoded['rtc_sec'],
                                               microsecond=variable_decoded['rtc_hsec']*1000*10)

                        #print("ts = ", ts)
                        if not ts_start:
                            ts_start = ts
                        times.append(ts)

                        head.append(variable_decoded['heading']*0.01)
                        pitch.append(variable_decoded['pitch']*0.01)
                        roll.append(variable_decoded['roll']*0.01)
                        pressure.append(variable_decoded['pressure']/1000)
                        pressure_var.append(variable_decoded['press_variance']/1000)

                        ad1.append(variable_decoded['adc1']*2092719/1000000)

                    if data == b'\x00\x01':  # velocity data
                        data = ensemble[n:n+(num_beams*2)*num_cells]
                        velocity.append(struct.unpack("<%dh" % (num_beams*num_cells), data))
                        n += len(data)
                    elif data == b'\x00\x02':  # correlation mag
                        data = ensemble[n:n+num_beams*num_cells]
                        corr_mag.append(struct.unpack("<%db" % (num_beams*num_cells), data))
                        n += len(data)
                    elif data == b'\x00\x03':  # echo intensity
                        data = ensemble[n:n+num_beams*num_cells]
                        echo_int.append(struct.unpack("<%db" % (num_beams*num_cells), data))
                        n += len(data)
                    elif data == b'\x00\x04':  # percent good
                        data = ensemble[n:n+num_beams*num_cells]
                        pct_good.append(struct.unpack("<%db" % (num_beams*num_cells), data))
                        n += len(data)
                    elif data == b'\x00\x05':  # status data
                        data = ensemble[n:n+num_beams*num_cells]
                        status.append(struct.unpack("<%db" % (4*num_cells), data))
                        n += len(data)

                if number_ensambles_read % 1000 == 0:
                    print("number ensambles read", number_ensambles_read)

            number_ensambles_read += 1

            data = binary_file.read(2)

    print("file start time", ts_start)
    print("file end time  ", ts)

    # create the netCDF file
    outputName = filepath + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    inst_system = fixed_decoded['sysConfig'] & 0x7
    inst_system_text = inst_system_decoder[inst_system]
    print("system ", inst_system_text)

    # add global attributes
    instrument_model = 'WORKHORSE ' + inst_system_text
    instrument_serialnumber = fixed_decoded['inst_serial']

    ncOut.instrument = 'RDI ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = str(instrument_serialnumber)

    coord_sys = (fixed_decoded['coord_trans'] >> 3) & 0x03
    ncOut.data_coordinates = inst_coords_decoder[coord_sys]

    ncOut.number_beams = np.int32(num_beams)
    ncOut.number_cells = np.int32(num_cells)
    ncOut.cell_size_cm = np.int32(fixed_decoded['cell_length'])
    ncOut.blank_after_tx_cm = np.int32(fixed_decoded['blank_after_tx'])
    ncOut.dist_centre_bin1_cm = np.int32(fixed_decoded['bin1_dist'])
    ncOut.xmit_pulse_length_cm = np.int32(fixed_decoded['xmit_pulse_len'])

    # add time variable

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME", number_ensambles_read)
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"
    ncTimesOut[:] = date2num(times, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

    cellDim = ncOut.createDimension("CELL", fixed_decoded["num_cells"])

    var_head = ncOut.createVariable("HEADING_MAG", "f4", ("TIME",), zlib=True)
    var_head[:] = head
    var_head.units = 'degrees'
    var_pitch = ncOut.createVariable("PITCH", "f4", ("TIME",), zlib=True)
    var_pitch[:] = pitch
    var_pitch.units = 'degrees'
    var_roll = ncOut.createVariable("ROLL", "f4", ("TIME",), zlib=True)
    var_roll[:] = roll
    var_roll.units = 'degrees'
    var_press = ncOut.createVariable("PRES", "f4", ("TIME",), zlib=True)
    var_press[:] = pressure
    var_press.units = 'dbar'
    var_press.applied_offset = np.float(-10.1353)
    var_press_v = ncOut.createVariable("PRES_VAR", "f4", ("TIME",), zlib=True)
    var_press_v[:] = pressure_var
    var_press_v.units = 'dbar'
    var_txv = ncOut.createVariable("TX_VOLT", "f4", ("TIME",), zlib=True)
    var_txv[:] = ad1
    var_txv.units = 'V'

    var_vel = ncOut.createVariable("UCUR", "f4", ("TIME", "CELL"), zlib=True)
    v = np.array(velocity).reshape([number_ensambles_read, num_cells, fixed_decoded['num_beam']])
    var_vel[:] = v[:, :, 0]/1000
    var_vel.units = 'm/s'
    var_vel = ncOut.createVariable("VCUR", "f4", ("TIME", "CELL"), zlib=True)
    v = np.array(velocity).reshape([number_ensambles_read, num_cells, fixed_decoded['num_beam']])
    var_vel[:] = v[:, :, 1]/1000
    var_vel.units = 'm/s'
    var_vel = ncOut.createVariable("WCUR", "f4", ("TIME", "CELL"), zlib=True)
    v = np.array(velocity).reshape([number_ensambles_read, num_cells, fixed_decoded['num_beam']])
    var_vel[:] = v[:, :, 2]/1000
    var_vel.units = 'm/s'
    var_vel = ncOut.createVariable("ECUR", "f4", ("TIME", "CELL"), zlib=True)
    v = np.array(velocity).reshape([number_ensambles_read, num_cells, fixed_decoded['num_beam']])
    var_vel[:] = v[:, :, 3]/1000
    var_vel.units = 'm/s'

    for x in fixed_decoded:
        print("fixed value ", x)
        ncOut.setncattr("fixed_" + x, np.int32(fixed_decoded[x]))
    for x in variable_decoded:
        print("variable value ", x)
        ncOut.setncattr("variable_" + x, np.int32(variable_decoded[x]))


    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", ts.strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.utcnow().strftime("%Y-%m-%d") + " created from file " + filepath)

    return outputName


if __name__ == "__main__":
    main(sys.argv)
