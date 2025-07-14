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
from datetime import UTC
from netCDF4 import num2date, date2num
from netCDF4 import Dataset
import numpy as np
import struct
import traceback

header_decoder = {'keys': ['spare', 'dataTypes'],
                  'unpack': "<BB"}
fixed_decoder = {'keys': ['cpuVER', 'cpuREV', 'sysConfig', 'read', 'lag_len', 'num_beam', 'num_cells',
                          'pings_per_ensemble', 'cell_length', 'blank_after_tx', 'profile_mode', 'low_corr',
                          'code_reps', 'gd_min', 'error_vel_max', 'tpp_min', 'tpp_sec', 'tpp_hun_sec', 'coord_trans',
                          'head_alignment', 'head_bias', 'sensor_source', 'sensor_available', 'bin1_dist',
                          'xmit_pulse_len', 'ref_layer', 'false_target_thresh', 'spare', 'tx_lag_dist', 'cpu_board',
                          'system_bandwidth', 'system_power', 'spare2', 'inst_serial', 'beam_angle'],
                 'unpack': "<BBHBBBBHHHBBBBHBBBBHHBBHHHBBHQHBBLB"}

# should we create a netCDF variable for every variable here?
variable_decoder = {'keys': ['ensemble_no', 'year', 'month', 'day', 'hour', 'minute', 'second', 'hsec',
                             'ensemble_msb', 'result', 'speed_of_sound', 'depth_of_trans', 'heading', 'pitch',
                             'roll', 'salinity', 'temperature', 'mpt_min', 'mpt_sec', 'mpt_hsec', 'hdg_stdev',
                             'pitch_stdev', 'roll_stdev', 'adc0', 'adc1', 'adc2', 'adc3', 'adc4', 'adc5', 'adc6', 'adc7',
                             'error_status', 'spare1', 'pressure', 'press_variance', 'spare2', 'rtc_cen',
                             'rtc_year', 'rtc_month', 'rtc_day', 'rtc_hour', 'rtc_min', 'rtc_sec', 'rtc_hsec'],
                    'unpack': "<H7BB4H4hBBBBBB8BIHiiB8B"}

inst_system_decoder = {}
inst_system_decoder[0] = ['75kHz', 76.8]
inst_system_decoder[1] = ['150kHz', 153.6]
inst_system_decoder[2] = ['300kHz', 307.2]
inst_system_decoder[3] = ['600kHz', 614.4]
inst_system_decoder[4] = ['1200kHz', 1228.8]
inst_system_decoder[5] = ['2400kHz', 2457.6]

inst_coords_decoder = {}
inst_coords_decoder[0] = 'Beam'
inst_coords_decoder[1] = 'Instrument'
inst_coords_decoder[2] = 'Ship'
inst_coords_decoder[3] = 'Earth'

volt_scale_system = {}
volt_scale_system[0] = [2092719, 43838]
volt_scale_system[1] = [592157, 11451]
volt_scale_system[2] = [592157, 11451]
volt_scale_system[3] = [380667, 11451]
volt_scale_system[4] = [253765, 11451]
volt_scale_system[5] = [253765, 11451]


def rdi_parse(files):
    filepath = files[0]
    ts_start = None

    speed_of_sound = []

    corr_mag = []
    echo_int = []
    pct_good = []
    status = []

    ad1 = []

    number_ensambles_read = 0

    # create the netCDF file
    outputName = (os.path.basename(filepath) + ".nc")

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    # add time variable

    #     TIME:axis = "T";
    #     TIME:calendar = "gregorian";
    #     TIME:long_name = "time";
    #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

    tDim = ncOut.createDimension("TIME")
    ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
    ncTimesOut.long_name = "time"
    ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
    ncTimesOut.calendar = "gregorian"
    ncTimesOut.axis = "T"

    var_temp = ncOut.createVariable("INST_TEMP", "f4", ("TIME",), zlib=True, chunksizes=[1024])
    var_temp.units = 'degrees_Celsius'
    var_temp.instrument_uncertainty = float(0.5)

    var_press = ncOut.createVariable("PRES", "f4", ("TIME",), zlib=True, chunksizes=[1024])
    var_press.units = 'dbar'
    var_press.applied_offset = float(-10.1353)
    var_press_v = ncOut.createVariable("PRES_VAR", "f4", ("TIME",), zlib=True, chunksizes=[1024])
    var_press_v.units = 'dbar'

    var_head = ncOut.createVariable("HEADING_MAG", "f4", ("TIME",), zlib=True, chunksizes=[1024])
    var_head.units = 'degrees'
    var_pitch = ncOut.createVariable("PITCH", "f4", ("TIME",), zlib=True, chunksizes=[1024])
    var_pitch.units = 'degrees'
    var_roll = ncOut.createVariable("ROLL", "f4", ("TIME",), zlib=True, chunksizes=[1024])
    var_roll.units = 'degrees'

    var_txv = ncOut.createVariable("TX_VOLT", "f4", ("TIME",), zlib=True, chunksizes=[1024])
    var_txv.units = 'V'
    var_txi = ncOut.createVariable("TX_CURRENT", "f4", ("TIME",), zlib=True, chunksizes=[1024])
    var_txi.units = 'A'

    var_sspeed= ncOut.createVariable("SOUND_SPEED", "f4", ("TIME",), zlib=True, chunksizes=[1024])
    var_sspeed.units = 'm/s'

    cellDim = None

    # loop over file, adding data to netCDF file for each ensemble

    with open(filepath, "rb") as binary_file:
        data = binary_file.read(2)
        while data:
            print()
            # print("hdr ", data)
            if data == b'\x7f\x7f':
                sum = 0
                for hdr_ens_n in data:
                    sum += hdr_ens_n

                data = binary_file.read(2)
                (ensemble_len,) = struct.unpack("<H", data)

                for hdr_ens_n in data:
                    sum += hdr_ens_n

                #print("ensemble pos", binary_file.tell(), "length", ensemble_len)
                ensemble = binary_file.read(ensemble_len-4)
                for hdr_ens_n in ensemble:
                    sum += hdr_ens_n

                sum = sum % 65536
                cksum_data = binary_file.read(2)
                if len(cksum_data) != 2:
                    print("checksum error")
                    continue
                (cksum,) = struct.unpack("<H", cksum_data)
                #print("checksum ", cksum, sum)

                if cksum == sum:
                    header = struct.unpack(header_decoder["unpack"], ensemble[0:2])
                    header_decoded = dict(zip(header_decoder['keys'], header))
                    #print("header ", header_decoded)

                    hdr_addr = 2
                    addrs = [0 for x in range(0, header_decoded["dataTypes"])]
                    for hdr_ens_n in range(0, header_decoded["dataTypes"]):
                        addr_data = ensemble[hdr_addr:hdr_addr + 2]
                        addrs[hdr_ens_n] = struct.unpack("<H", addr_data)[0]
                        #print("data type", hdr_ens_n, "addr", addrs[hdr_ens_n])
                        hdr_addr += 2

                        ens_pos = addrs[hdr_ens_n] - 4
                        ens_type = ensemble[ens_pos:ens_pos+2]
                        #print(hdr_addr, ens_pos, "ens type ", ens_type, 'total len', ensemble_len - 6)

                        try:
                            if ens_type == b'\x00\x00':  # fixed header
                                data = ensemble[ens_pos+2:ens_pos+59]
                                fixed = struct.unpack(fixed_decoder["unpack"], data)
                                fixed_decoded = dict(zip(fixed_decoder['keys'], fixed))
                                #print("fixed ", fixed_decoded)
                                
                                coord_sys = (fixed_decoded['coord_trans'] >> 3) & 0x03
                                ncOut.data_coordinates = inst_coords_decoder[coord_sys]

                                num_cells = fixed_decoded['num_cells']
                                num_beams = fixed_decoded['num_beam']

                                print('fixed header, num_cells', num_cells)

                                # know how big a cell is now, create the cell based variables
                                if 'CELL' not in ncOut.dimensions:
                                    #cellDim = ncOut.createDimension("CELL")
                                    cellDim = ncOut.createDimension("CELL", num_cells)
                                    # create cell variables, one for each beam, generic names until we know the coordinates
                                    var_vel1 = ncOut.createVariable("V1", "f4", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_vel1.units = 'm/s'
                                    var_vel1.valid_max = 20
                                    var_vel1.valid_min = -20
                                    var_vel2 = ncOut.createVariable("V2", "f4", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_vel2.units = 'm/s'
                                    var_vel2.valid_max = 20
                                    var_vel2.valid_min = -20
                                    var_vel3 = ncOut.createVariable("V3", "f4", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_vel3.units = 'm/s'
                                    var_vel3.valid_max = 20
                                    var_vel3.valid_min = -20
                                    var_vel4 = ncOut.createVariable("V4", "f4", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_vel4.units = 'm/s'
                                    var_vel4.valid_max = 20
                                    var_vel4.valid_min = -20

                                    # beam_dim = ncOut.createDimension("BEAM", 4)
                                    # field_dim = ncOut.createDimension("FIELD", 4)
                                    var_corr1 = ncOut.createVariable("CORR_MAG1", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_corr2 = ncOut.createVariable("CORR_MAG2", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_corr3 = ncOut.createVariable("CORR_MAG3", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_corr4 = ncOut.createVariable("CORR_MAG4", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])

                                    var_echo_int1 = ncOut.createVariable("ECHO_INT1", "f4", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_echo_int2 = ncOut.createVariable("ECHO_INT2", "f4", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_echo_int3 = ncOut.createVariable("ECHO_INT3", "f4", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_echo_int4 = ncOut.createVariable("ECHO_INT4", "f4", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])

                                    var_per_good1 = ncOut.createVariable("PCT_GOOD1", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_per_good2 = ncOut.createVariable("PCT_GOOD2", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_per_good3 = ncOut.createVariable("PCT_GOOD3", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_per_good4 = ncOut.createVariable("PCT_GOOD4", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])

                                    var_status1 = ncOut.createVariable("STATUS1", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_status2 = ncOut.createVariable("STATUS2", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_status3 = ncOut.createVariable("STATUS3", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])
                                    var_status4 = ncOut.createVariable("STATUS4", "u1", ("TIME", "CELL"), zlib=True, chunksizes=[1024, num_cells])

                                inst_system = fixed_decoded['sysConfig'] & 0x7
                                inst_system_text = inst_system_decoder[inst_system][0]
                                #print("system ", inst_system_text)

                            elif ens_type == b'\x80\x00':  # variable header
                                data = ensemble[ens_pos+2:ens_pos+65]
                                variable = struct.unpack(variable_decoder["unpack"], data)
                                variable_decoded = dict(zip(variable_decoder['keys'], variable))
                                #print("variable header ", variable_decoded)

                                # ts = datetime.datetime(year=variable_decoded['rtc_cen']*100 + variable_decoded['rtc_year'],
                                #                        month=variable_decoded['rtc_month'], day=variable_decoded['rtc_day'],
                                #                        hour=variable_decoded['rtc_hour'], minute=variable_decoded['rtc_min'],
                                #                        second=variable_decoded['rtc_sec'],
                                #                        microsecond=variable_decoded['rtc_hsec']*1000*10)

                                ts = datetime.datetime(year=2000 + variable_decoded['year'],
                                                       month=variable_decoded['month'], day=variable_decoded['day'],
                                                       hour=variable_decoded['hour'], minute=variable_decoded['minute'],
                                                       second=variable_decoded['second'],
                                                       microsecond=variable_decoded['hsec']*1000*10)

                                print("ts = ", ts)
                                if not ts_start:
                                    ts_start = ts

                                ncTimesOut[number_ensambles_read] = date2num(ts, calendar=ncTimesOut.calendar, units=ncTimesOut.units)

                                var_head[number_ensambles_read] = variable_decoded['heading']*0.01
                                var_pitch[number_ensambles_read] = variable_decoded['pitch']*0.01
                                var_roll[number_ensambles_read] = variable_decoded['roll']*0.01
                                var_temp[number_ensambles_read] = variable_decoded['temperature']*0.01

                                var_press[number_ensambles_read] = variable_decoded['pressure']/1000
                                var_press_v[number_ensambles_read] = variable_decoded['press_variance']/1000
                                var_txv[number_ensambles_read] = variable_decoded['adc1']*volt_scale_system[inst_system][0]/1000000
                                var_txi[number_ensambles_read] = variable_decoded['adc0']*volt_scale_system[inst_system][1]/1000000
                                var_sspeed[number_ensambles_read] = variable_decoded['speed_of_sound']

                            elif ens_type == b'\x00\x01':  # velocity data
                                data = ensemble[ens_pos+2:ens_pos + 2 + (num_beams * 2) * num_cells]
                                velocity = np.array(struct.unpack("<%dh" % (num_beams*num_cells), data))
                                #print("velocity shape ", velocity.shape)
                                v = velocity.reshape([num_cells, num_beams])
                                #print(var_vel1, number_ensambles_read, v.shape)
                                var_vel1[number_ensambles_read, :] = v[:, 0] / 1000
                                var_vel2[number_ensambles_read, :] = v[:, 1] / 1000
                                var_vel3[number_ensambles_read, :] = v[:, 2] / 1000
                                var_vel4[number_ensambles_read, :] = v[:, 3] / 1000

                                #print("var vel shape ", var_vel1.shape)
                            elif ens_type == b'\x00\x02':  # correlation mag
                                data = ensemble[ens_pos+2:ens_pos + 2 + num_beams * num_cells]
                                np_corr = np.array(struct.unpack("<%dB" % (num_beams*num_cells), data)).reshape([num_cells, num_beams])
                                #print('size corr', len(np_corr))
                                if len(np_corr) > 0:
                                    var_corr1[number_ensambles_read, :] = np_corr[:, 0]
                                    var_corr2[number_ensambles_read, :] = np_corr[:, 1]
                                    var_corr3[number_ensambles_read, :] = np_corr[:, 2]
                                    var_corr4[number_ensambles_read, :] = np_corr[:, 3]
                            elif ens_type == b'\x00\x03':  # echo intensity
                                data = ensemble[ens_pos+2:ens_pos + 2 + num_beams * num_cells]
                                np_echo_int = np.array(struct.unpack("<%dB" % (num_beams*num_cells), data)).reshape([num_cells, num_beams])
                                var_echo_int1[number_ensambles_read, :] = np_echo_int[:, 0] * 0.45
                                var_echo_int2[number_ensambles_read, :] = np_echo_int[:, 1] * 0.45
                                var_echo_int3[number_ensambles_read, :] = np_echo_int[:, 2] * 0.45
                                var_echo_int4[number_ensambles_read, :] = np_echo_int[:, 3] * 0.45
                            elif ens_type == b'\x00\x04':  # percent good
                                data = ensemble[ens_pos+2:ens_pos + 2 + num_beams * num_cells]
                                np_pg = np.array(struct.unpack("<%dB" % (4*num_cells), data)).reshape([num_cells, 4])
                                var_per_good1[number_ensambles_read, :] = np_pg[:, 0]
                                var_per_good2[number_ensambles_read, :] = np_pg[:, 1]
                                var_per_good3[number_ensambles_read, :] = np_pg[:, 2]
                                var_per_good4[number_ensambles_read, :] = np_pg[:, 3]
                            elif ens_type == b'\x00\x05':  # status data
                                data = ensemble[ens_pos+2:ens_pos + 2 + num_beams * num_cells]
                                print(len(ensemble), num_beams*num_cells)
                                np_status = np.array(struct.unpack("<%db" % (num_beams*num_cells), data)).reshape([num_cells, num_beams])
                                var_status1[number_ensambles_read, :] = np_status[:, 0]
                                var_status2[number_ensambles_read, :] = np_status[:, 1]
                                var_status3[number_ensambles_read, :] = np_status[:, 2]
                                var_status4[number_ensambles_read, :] = np_status[:, 3]
                            else:
                                print('unknown ens_type', ens_type[0], ens_type[1], 'hdr ens n', hdr_ens_n)
                        except struct.error as e:
                            print(num_cells, len(ensemble), num_beams * num_cells)
                            print(e)
                            traceback.print_exc(limit=2, file=sys.stdout)
                            print('file parse error, maybe truncated, building file anyway')
                            pass

                    if number_ensambles_read % 1000 == 0:
                        print("number ensambles read ", number_ensambles_read)
                    number_ensambles_read += 1

            data = binary_file.read(2)

    print("file start time ", ts_start)
    print("file end time   ", ts)

    # add global attributes
    instrument_model = 'WorkHorse ' + inst_system_text
    instrument_serialnumber = fixed_decoded['inst_serial']

    ncOut.instrument = 'RDI ; ' + instrument_model
    ncOut.instrument_model = instrument_model
    ncOut.instrument_serial_number = str(instrument_serialnumber)
    ncOut.frequency = float(inst_system_decoder[inst_system][1])

    beam_names = {}
    beam_names[0] = ('BEAM1_VEL', 'BEAM2_VEL', 'BEAM3_VEL', 'BEAM4_VEL')
    beam_names[1] = ('BM1_BM2_VEL', 'BM4_BM3_VEL', 'XDUCER_VEL', 'ERROR_VEL')
    beam_names[2] = ('PORT_STDB_VEL', 'AFT_FWD_VEL', 'SURFACE_VEL', 'ERROR_VEL')
    beam_names[3] = ('EAST_VEL', 'NORTH_VEL', 'SURFACE_VEL', 'ERROR_VEL')

    # rename variables to coordinate system variables
    ncOut.renameVariable("V1", beam_names[coord_sys][0])
    ncOut.renameVariable("V2", beam_names[coord_sys][1])
    ncOut.renameVariable("V3", beam_names[coord_sys][2])
    ncOut.renameVariable("V4", beam_names[coord_sys][3])

    if coord_sys > 0:  # when not in beam coords is this true
        var_per_good1.comment = 'Percentage of good 3-beam solutions'
        var_per_good2.comment = 'Percentage of transformations rejected'
        var_per_good3.comment = 'Percentage of more than one beam bad in bin'
        var_per_good4.comment = 'Percentage of good 4-beam solutions'

    ncOut.number_beams = np.int32(num_beams)
    ncOut.number_cells = np.int32(num_cells)
    ncOut.cell_size_cm = np.int32(fixed_decoded['cell_length'])
    ncOut.blank_after_tx_cm = np.int32(fixed_decoded['blank_after_tx'])
    ncOut.dist_centre_bin1_cm = np.int32(fixed_decoded['bin1_dist'])
    ncOut.xmit_pulse_length_cm = np.int32(fixed_decoded['xmit_pulse_len'])

    ncOut.time_between_pings_sec = float(fixed_decoded['tpp_min'] * 60 + fixed_decoded['tpp_sec'] + fixed_decoded['tpp_hun_sec']/100)

    # add fixed header data as attributes
    for x in fixed_decoded:
        #print("fixed value ", x)
        #print(x, " type ", type(fixed_decoded[x]))
        if fixed_decoded[x] < np.iinfo(np.int32).max:
            ncOut.setncattr("instrument_setup_fixed_" + x, np.int32(fixed_decoded[x]))

    #for x in variable_decoded:
    #    print("variable value ", x)
    #    ncOut.setncattr("variable_" + x, np.int32(variable_decoded[x]))

    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    ncOut.setncattr("time_coverage_start", ts_start.strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", ts.strftime(ncTimeFormat))

    # add creating and history entry
    ncOut.setncattr("date_created", datetime.datetime.now(UTC).strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + filepath)

    return outputName


if __name__ == "__main__":
    rdi_parse(sys.argv[1:])
