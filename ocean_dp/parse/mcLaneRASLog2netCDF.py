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
import re
import os

from datetime import datetime, timedelta, UTC
from cftime import num2date, date2num
from glob2 import glob
from netCDF4 import Dataset
import numpy as np
from dateutil import parser

# source file must have 'timek' column for time
#  flag column is excluded
#
# parsers need to output
#  instrument
#  instrument_serial_number
#  time_coverage_start
#  time_coverage_end
# optional
#  date_created
#  history
#
# convert time to netCDF cf-timeformat (double days since 1950-01-01 00:00:00 UTC)

# search expressions within file

   # 1  03/31/19 15:00:02  32.7 Vbat  10.6 °C  PORT = 00
   #    Pre-sample acid flush         0 ml        0 sec  LB  0.0 V  . . .
   #    Flush port  = 49
   #    Intake flush                251 ml      201 sec  LB 32.0 V  Average I 74.0 mA Highest I 82.0 mA  Volume reached
   #    Flush port  = 00
   #    Sample                      501 ml      402 sec  LB 31.8 V  Average I 78.0 mA Highest I 87.0 mA  Volume reached
   #    Sample port = 01
   #    03/31/19 15:10:15  32.3 Vbat  14.3 °C  PORT = 01
   #    Post-sample acid flush       11 ml       10 sec  LB 31.8 V  Volume reached
   #    Flush port  = 49


#
# parse the file
#

event_ts = r'.*?Event *(?P<event>\d{1,2}).*of.*(?P<time>\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2,4})'
event_data = r'.*?(?P<event>\d{1,2}) *(?P<time>\d{2}\/\d{2}\/\d{2,4} \d{2}:\d{2}:\d{2}) *(?P<vbat>[0-9\.]*).*Vbat.*PORT = (?P<port>\d+)'
sample_time_bv = r'.*Sample.*\d* ml\s*(?P<pump_time>\d+).*sec.*LB(?P<vbat>.*).*V.*Volume reached'
sample_time_bv_current = r'.*?Sample.*\d* ml\s*(?P<pump_time>\d+).*sec.*LB(?P<vbat>.*).*V.*Average I (?P<current>.*) mA Highest I (?P<maxcurrent>.*) mA.*Volume reached'
pump_data = r'.*?(?P<event>[0-9.]+) +(?P<mlpmin>[0-9.]+) +(?P<ml>[0-9.]+) +(?P<vbat>[0-9.]+) +(?P<cur>[0-9.]+) +(?P<maxcur>[0-9.]+)'
pump_data7 = r'.*?(?P<event>[0-9.]+) +(?P<mlpmin>[0-9.]+) +(?P<ml>[0-9.]+) +(?P<vbat>[0-9.]+)'
event_data_13 = r'.*?(?P<event>\d+)\|Sample.*?\|.*?(?P<time>\d{2}\/\d{2}\/\d{2,4} \d{2}:\d{2}:\d{2})\|.*?(?P<temp>[0-9.]+)\|.*?(?P<vbat>[0-9.]+)\|.*?(?P<vol>[0-9.]+)\|.*?(?P<current>[0-9.]+)\| Volume reached'
sample_port = r'.*?Sample port = (?P<sample_port>\d+)'
pump_sample_interval = r'.*?Sample interval = (?P<sample_interval>\d+) \[(\S*)\]'
pump_sample_interval13 = r'.*?Pump data period = (?P<sample_interval>\d+) (\S*)'

evt = r'.*?Event:.*?(\d+)'
start_time = r'.*?Start time:.*?(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2,4})'
vol_pumped = r'.*?Volume pumped:.*?(\d+).*?ml'
lowest_bat = r'.*?Lowest battery:.*?(\d+).*?V'
elapsed_time = r'.*?Elapsed time:.*?(\d+).*?sec'
av_current = r'.*?Average current:.*?(\d+).*?mA'
max_current = r'.*?Highest current:.*?(\d+).*?mA'

sn_expr = r'.*?(ML\d+-\d+)'

def parse(files):

    output_names = []

    for filepath in files:

        instrument_model = 'RAS-125P500'
        instrument_serialnumber = 'Unknown'
        number_samples_read = 0

        print('file', filepath)
        events = {}
        pump_data_values = []
        interval = None
        interval_unit = None

        with open(filepath, 'r', errors='ignore') as fp:
            line = fp.readline()

            while line:
                line = line.strip()
                #print("Line ", line)
                
                ## chatGPT version

                # # Regular expression to match the event string format
                # event_pattern = r"Event\s+(\d+)\s+of\s+(\d+)\s+@ (\d{2}/\d{2}/\d{4}) (\d{2}:\d{2}:\d{2})"
                #
                # # List to hold parsed event data
                # parsed_events = []
                #
                # # Parse each event using regex
                # for event in events:
                #     match = re.match(event_pattern, event)
                #     if match:
                #         event_num = int(match.group(1))
                #         total_events = int(match.group(2))
                #         event_date_str = match.group(3)
                #         event_time_str = match.group(4)
                #
                #         # Combine the date and time strings and convert to datetime object
                #         event_datetime_str = f"{event_date_str} {event_time_str}"
                #         event_datetime = datetime.strptime(event_datetime_str, "%m/%d/%Y %H:%M:%S")
                #
                #         # Store the parsed data in a dictionary
                #         parsed_events.append({
                #             "event_number": event_num,
                #             "total_events": total_events,
                #             "event_datetime": event_datetime
                #         })

                # import re
                # from pprint import pprint

                # def parse_log(log_text):
                # entries = []
                # blocks = re.split(r'\n\s*(?=\d{1,3}\s+\d{2}/\d{2}/\d{4})', log_text.strip())

                # for block in blocks:
                # lines = block.strip().splitlines()
                # entry = {}
                # operations = []

                # # First line: Entry number, timestamp, Vbat, temp, port
                # first_line = lines[0]
                # m = re.match(r'(\d+)\s+(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\s+([\d.]+) Vbat\s+([\d.]+) øC\s+PORT = (\d+)', first_line)
                # if m:
                # entry['entry_number'] = int(m.group(1))
                # entry['start_time'] = m.group(2)
                # entry['start_vbat'] = float(m.group(3))
                # entry['start_temp'] = float(m.group(4))
                # entry['start_port'] = m.group(5)

                # # Parse operations in the middle
                # i = 1
                # while i < len(lines):
                # line = lines[i].strip()

                # # Match operation line
                # op_match = re.match(r'([A-Za-z\- ]+)\s+(\d+) ml\s+(\d+) sec\s+LB ([\d.]+) V\s+Volume reached\.', line)
                # if op_match:
                # operation = {
                # 'type': op_match.group(1).strip(),
                # 'volume_ml': int(op_match.group(2)),
                # 'duration_sec': int(op_match.group(3)),
                # 'voltage': float(op_match.group(4))
                # }

                # # Next line should be port info
                # if i + 1 < len(lines):
                # port_line = lines[i + 1].strip()
                # port_match = re.search(r'(Flush|Sample) port = (\d+)', port_line)
                # if port_match:
                # operation['port'] = port_match.group(2)
                # i += 1  # Skip next line
                # operations.append(operation)

                # # Match end timestamp line (usually second last line)
                # elif re.match(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}', line):
                # m2 = re.match(r'(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2})\s+([\d.]+) Vbat\s+([\d.]+) øC\s+PORT = (\d+)', line)
                # if m2:
                # entry['end_time'] = m2.group(1)
                # entry['end_vbat'] = float(m2.group(2))
                # entry['end_temp'] = float(m2.group(3))
                # entry['end_port'] = m2.group(4)

                # i += 1

                # entry['operations'] = operations
                # entries.append(entry)

                # return entries

                # # Example usage
                # log_data = """
                # 1  08/06/2011 02:00:00  33.4 Vbat  10.8 øC  PORT = 00
                # Pre-sample acid flush         4 ml        4 sec  LB 32.5 V   Volume reached.
                # Flush port = 49
                # Intake flush     100 ml       80 sec  LB 32.3 V   Volume reached.
                # Flush port = 00
                # Sample           500 ml      447 sec  LB 32.2 V   Volume reached.
                # Sample port = 01
                # 08/06/2011 02:10:20  32.8 Vbat  14.5 øC  PORT = 01
                # Post-sample acid flush         4 ml        3 sec  LB 32.3 V   Volume reached.
                # Flush port = 49

                # 2  08/06/2011 03:00:00  33.3 Vbat  10.9 øC  PORT = 00
                # Pre-sample acid flush         4 ml        4 sec  LB 32.6 V   Volume reached.
                # Flush port = 49
                # Intake flush     100 ml       80 sec  LB 32.4 V   Volume reached.
                # Flush port = 00
                # Sample           500 ml      401 sec  LB 32.2 V   Volume reached.
                # Sample port = 02
                # 08/06/2011 03:09:35  32.7 Vbat  14.3 øC  PORT = 02
                # Post-sample acid flush         4 ml        3 sec  LB 32.2 V   Volume reached.
                # Flush port = 49
                # """

                # parsed = parse_log(log_data)
                # pprint(parsed, width=120)

                matchObj = re.match(event_ts, line)
                if matchObj:
                    print('match event_ts', matchObj, matchObj.groups())
                    event = int(matchObj.group('event'))
                    if len(matchObj.group('time')) == 17:
                        ts = datetime.strptime(matchObj.group('time'), '%m/%d/%y %H:%M:%S')
                    else:
                        ts = datetime.strptime(matchObj.group('time'), '%m/%d/%Y %H:%M:%S')
                    print('event', event, 'time', ts)
                    if event not in events:
                        events[event] = {}
                    events[event].update({'ts': ts})

                matchObj = re.match(event_data, line)
                if matchObj:
                    print('match event_data', matchObj, matchObj.groups())
                    event = int(matchObj.group('event'))
                    if len(matchObj.group('time')) == 17:
                        ts = datetime.strptime(matchObj.group('time'), '%m/%d/%y %H:%M:%S')
                    else:
                        ts = datetime.strptime(matchObj.group('time'), '%m/%d/%Y %H:%M:%S')
                    vbat = float(matchObj.group('vbat'))
                    port = float(matchObj.group('port'))
                    print('event', event, 'time', ts, 'vbat', vbat, 'port', port)
                    if event not in events:
                        events[event] = {}
                    events[event].update({'ts-start': ts, 'vbat': vbat, 'start-port': port})

                matchObj = re.match(event_data_13, line)
                if matchObj:
                    print('match event_data_13', matchObj, matchObj.groups())
                    event = int(matchObj.group('event'))
                    ts = datetime.strptime(matchObj.group('time'), '%m/%d/%Y %H:%M:%S')
                    vbat = float(matchObj.group('vbat'))
                    vol = float(matchObj.group('vol'))
                    current = float(matchObj.group('current'))
                    print('event', event, ts, vbat, vol)
                    if event not in events:
                        events[event] = {}
                    events[event].update({'ts-start': ts, 'vbat': vbat, 'current': current})

                matchObj = re.match(sample_time_bv_current, line)
                if matchObj:
                    print('match sample_time_bv_current', matchObj, matchObj.groups())
                    vbat = float(matchObj.group('vbat'))
                    pump_time = float(matchObj.group('pump_time'))
                    current = float(matchObj.group('current'))
                    maxcurrent = float(matchObj.group('maxcurrent'))
                    print('sample_time', vbat, pump_time)
                    events[event].update({'sample-vbat': vbat, 'pump-time': pump_time, 'current': current, 'maxcurrent': maxcurrent})

                matchObj = re.match(sample_time_bv, line)
                if matchObj:
                    print('match sample_time_bv', matchObj, matchObj.groups())
                    vbat = float(matchObj.group('vbat'))
                    pump_time = float(matchObj.group('pump_time'))
                    print('sample_time', vbat, pump_time)
                    events[event].update({'sample-vbat': vbat, 'pump-time': pump_time})

                matchObj = re.match(sample_port, line)
                if matchObj:
                    print('match sample_port', matchObj, matchObj.groups())
                    s_port = int(matchObj.group('sample_port'))
                    events[event].update({'sample-port': s_port})

                matchObj = re.match(pump_data, line)
                if matchObj:
                    print('match pump_data', matchObj, matchObj.groups())
                    event = int(matchObj.group('event'))
                    mlpmin = float(matchObj.group('mlpmin'))
                    ml = float(matchObj.group('ml'))
                    vbat = float(matchObj.group('vbat'))
                    cur = float(matchObj.group('cur'))
                    maxcur = float(matchObj.group('maxcur'))
                    pump_data_values.append({'event': event, 'mlpmin': mlpmin, 'ml': ml, 'vbat': vbat, 'current': cur, 'max-current': maxcur})
                else:
                    matchObj = re.match(pump_data7, line)
                    if matchObj:
                        print('match pump_data7', matchObj, matchObj.groups())
                        event = int(matchObj.group('event'))
                        mlpmin = float(matchObj.group('mlpmin'))
                        ml = float(matchObj.group('ml'))
                        vbat = float(matchObj.group('vbat'))
                        pump_data_values.append({'event': event, 'mlpmin': mlpmin, 'ml': ml, 'vbat': vbat})

                matchObj = re.match(pump_sample_interval, line)
                if matchObj:
                    print('match pump_sample_interval', matchObj, matchObj.groups())
                    interval = int(matchObj.group(1))
                    interval_unit = matchObj.group(2)
                matchObj = re.match(pump_sample_interval13, line)
                if matchObj:
                    print('match pump_sample_interval', matchObj, matchObj.groups())
                    interval = int(matchObj.group(1))
                    interval_unit = matchObj.group(2)

                matchObj = re.match(evt, line)
                if matchObj:
                    print('match evt', matchObj, matchObj.groups())
                    event = int(matchObj.group(1))
                    if event not in events:
                        events[event] = {}
                matchObj = re.match(vol_pumped, line)
                if matchObj:
                    print('match vol_pumped', matchObj, matchObj.groups())
                    vol = float(matchObj.group(1))
                    events[event].update({'vol': vol})
                matchObj = re.match(lowest_bat, line)
                if matchObj:
                    print('match lowest_bat', matchObj, matchObj.groups())
                    vbat = float(matchObj.group(1))
                    events[event].update({'vbat': vbat})
                matchObj = re.match(elapsed_time, line)
                if matchObj:
                    print('match elapsed_time', matchObj, matchObj.groups())
                    et = float(matchObj.group(1))
                    events[event].update({'pump-time': et})
                matchObj = re.match(av_current, line)
                if matchObj:
                    print('match av_current', matchObj, matchObj.groups())
                    et = float(matchObj.group(1))
                    events[event].update({'current': et})
                matchObj = re.match(max_current, line)
                if matchObj:
                    print('match max_current', matchObj, matchObj.groups())
                    et = float(matchObj.group(1))
                    events[event].update({'max-current': et})
                matchObj = re.match(start_time, line)
                if matchObj:
                    print('match start_time', matchObj, matchObj.groups())
                    ts = datetime.strptime(matchObj.group(1), '%m/%d/%y %H:%M:%S')

                    events[event].update({'ts': ts})

                matchObj = re.match(sn_expr, line)
                if matchObj:
                    print('match sn_expr', matchObj, matchObj.groups())
                    instrument_serialnumber = matchObj.group(1)


                line = fp.readline()

        print()
        for event in events:
            print('{:2d}'.format(event), events[event])

        print()
        for pump in pump_data_values:
            print('pump', pump)

        print()
        print('instrument_serialnumber', instrument_serialnumber)
        if interval_unit == 'minutes':
            interval = interval * 60
        print('punp sample interval', interval, interval_unit)
        #
        # build the netCDF file
        #

        ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

        outputName = (os.path.basename(filepath) + ".nc")

        print("output file : %s" % outputName)

        ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

        ncOut.instrument = 'McLane ; ' + instrument_model
        ncOut.instrument_model = instrument_model
        ncOut.instrument_serial_number = instrument_serialnumber

        #     TIME:axis = "T";
        #     TIME:calendar = "gregorian";
        #     TIME:long_name = "time";
        #     TIME:units = "days since 1950-01-01 00:00:00 UTC";

        tDim = ncOut.createDimension("TIME", len(events))
        ncTimesOut = ncOut.createVariable("TIME", "d", ("TIME",), zlib=True)
        ncTimesOut.long_name = "time"
        ncTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
        ncTimesOut.calendar = "gregorian"
        ncTimesOut.axis = "T"

        nc_vbat = ncOut.createVariable("VBAT", "f4", ("TIME",), fill_value = np.nan, zlib=True)
        nc_sample_vbat = ncOut.createVariable("VBAT_SAMPLE", "f4", ("TIME",), fill_value = np.nan, zlib=True)
        nc_pump_time = ncOut.createVariable("PUMP_TIME", "f4", ("TIME",), fill_value = np.nan, zlib=True)
        nc_current = ncOut.createVariable("CURRENT", "f4", ("TIME",), fill_value = np.nan, zlib=True)
        nc_maxcurrent = ncOut.createVariable("CURRENT_MAX", "f4", ("TIME",), fill_value = np.nan, zlib=True)

        for event in events:
            print('{:2d}'.format(event), events[event])
            if 'ts' in events[event]:
                ncTimesOut[event-1] = date2num(events[event]['ts'], units=ncTimesOut.units, calendar=ncTimesOut.calendar)
            elif 'ts-start' in events[event]:
                ncTimesOut[event-1] = date2num(events[event]['ts-start'], units=ncTimesOut.units, calendar=ncTimesOut.calendar)
            if 'vbat' in events[event]:
                nc_vbat[event-1] = events[event]['vbat']
            if 'sample-vbat' in events[event]:
                nc_sample_vbat[event-1] = events[event]['sample-vbat']
            if 'pump-time' in events[event]:
                nc_pump_time[event-1] = events[event]['pump-time']
            if 'current' in events[event]:
                nc_current[event-1] = events[event]['current']
            if 'maxcurrent' in events[event]:
                nc_maxcurrent[event-1] = events[event]['maxcurrent']

        ncOut.createDimension("TIME_PUMP", len(pump_data_values))
        ncPTimesOut = ncOut.createVariable("TIME_PUMP", "d", ("TIME_PUMP",), fill_value = np.nan, zlib=True)
        ncPTimesOut.long_name = "time"
        ncPTimesOut.units = "days since 1950-01-01 00:00:00 UTC"
        ncPTimesOut.calendar = "gregorian"

        nc_pump_vbat = ncOut.createVariable("VBAT_PUMP", "f4", ("TIME_PUMP",), fill_value = np.nan, zlib=True)
        nc_pump_current = ncOut.createVariable("CURRENT_PUMP", "f4", ("TIME_PUMP",), fill_value = np.nan, zlib=True)
        nc_pump_event = ncOut.createVariable("EVENT_PUMP", "f4", ("TIME_PUMP",), fill_value = np.nan, zlib=True)
        i = 0
        pump_sample = 0
        pump_t0 = None
        pump_e = -1
        for pump in pump_data_values:
            if pump['event'] != pump_e:
                pump_e = pump['event']
                print('new pump event', pump_e)
                pump_sample = 0
            if pump_sample == 0:
                print('new pump sample event', pump['event'])
                pump_t0 = events[pump['event']]['ts-start']
            print('pump', pump, pump_t0)
            ncPTimesOut[i] = date2num(pump_t0, units=ncTimesOut.units, calendar=ncTimesOut.calendar)

            nc_pump_vbat[i] = pump['vbat']
            nc_pump_event[i] = pump['event']
            if 'current' in pump:
                nc_pump_current[i] = pump['current']
                
            pump_t0 += timedelta(seconds=interval)
            i += 1
            pump_sample += 1

        # add timespan attributes
        ncOut.setncattr("time_coverage_start", num2date(ncTimesOut[0], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))
        ncOut.setncattr("time_coverage_end", num2date(ncTimesOut[-1], units=ncTimesOut.units, calendar=ncTimesOut.calendar).strftime(ncTimeFormat))

        # add creating and history entry
        ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
        ncOut.setncattr("history", datetime.now(UTC).strftime("%Y-%m-%d") + " created from file " + os.path.basename(filepath))

        ncOut.close()

        output_names.append(outputName)

    return output_names


if __name__ == "__main__":
    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))
    parse(files)
