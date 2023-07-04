# /usr/bin/python3

import email
import email.policy
import os
import struct
import sys

from glob2 import glob
import numpy as np
from datetime import datetime
import re
import json
import sqlite3
import re

import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import time

bucket = "rtdp"
org = "users"
token = "jlR97gAz0CUAg7VbJLhl0Ry7ahStQc5QR2snWBaa3A2RHura-2NLtS6zvkd-V8U_weCUrnMtLZab5oBvs7E7-A=="
# Store the URL of your InfluxDB instance
url="http://localhost:8086"


sbd_file = re.compile(r"SBD Msg From Unit: (3\d*)")


class proc_mail:
    msg_mosn = re.compile(r"MOMSN: (\d*)")
    msg_size = re.compile(r"Message Size \(bytes\): (\d*)")
    msg_time = re.compile(r"Time of Session \((.*)\): (.*)")
    msg_loc = re.compile(r"Unit Location: Lat = (.*) Long = (.*)")

    data = None
    session_time = None
    
    def header(self, email):
        attachments = msg.get_payload()
        sbd_data = None
        subject = msg.get('Subject')
        msg_time = msg.get('date').datetime
        self.data['fields']['email_time'] = time.mktime(msg_time.timetuple())

        # match iridium short-burst-data subject, and extract IMEI from subject
        match_sbd = sbd_file.match(subject)
        if match_sbd:
            #print('SBD number:', match_sbd.group(1))
            self.data['tags']['IMEI'] = match_sbd.group(1)

        # process through the mail attachments
        for attachment in attachments:
            fnam = attachment.get_filename()
            #print('attachment file', fnam, attachment.get_content_type())
            payload = attachment.get_payload(decode=True,)
            if attachment.get_content_type() == "text/plain":
                payload = payload.decode('utf-8')

            # if the attachment has no file name, then assume its the body text
            if fnam is None:
                match_loc = self.msg_loc.search(payload)
                if match_loc:
                    #print('location', match_loc.group(1), match_loc.group(2))
                    self.data['fields']['lat_iridium'] = float(match_loc.group(1))
                    self.data['fields']['lon_iridium'] = float(match_loc.group(2))
                match_size = self.msg_size.search(payload)
                if match_size:
                    #print('size ', int(match_size.group(1)))
                    pass
                match_time = self.msg_time.search(payload)
                if match_time:
                    self.session_time = datetime.strptime(match_time.group(2), "%a %b %d %H:%M:%S %Y")
                    #print('time ', match_time.group(2), self.session_time)
                    self.data['time'] = self.session_time.strftime("%Y-%m-%d %H:%M:%S") # will get overwritten should the packet have a time
                    self.data['fields']['session_time'] = time.mktime(self.session_time.timetuple())

            if attachment.get_content_type() == "application/x-zip-compressed": # or name ends with .sbd
                sbd_data = payload
                #save_file = open(fnam, 'wb')
                #save_file.write(attachment.get_payload(decode=True,))
                #save_file.close()

        return sbd_data

    def ASIMET(self, email):
        #print('ASIMET Processor')
        self.data['tags']['processor'] = 'ASIMET'
        asimet_data = self.header(email)
        if not asimet_data is None:

            sbd_data = struct.unpack(">BBBBBHhhhHhHhhhHHHbBB", asimet_data)
            #print(sbd_data)
            sbd_data_dict = dict(zip(['hour', 'min', 'day', 'mon', 'year', 'record', 'we', 'wn', 'compass', 'bp', 'rh', 'th', 'sr', 'lwflux', 'prlev', 'sst', 'scc', 'wsavg', 'flag', 'spare2', 'spare3'], sbd_data))
            self.data['fields']['we'] = sbd_data_dict['we'] / 100.0
            self.data['fields']['wn'] = sbd_data_dict['wn'] / 100.0
            self.data['fields']['ws-avg'] = sbd_data_dict['wsavg'] / 100.0
            self.data['fields']['compass'] = sbd_data_dict['compass'] / 10.0
            self.data['fields']['th'] = sbd_data_dict['th'] / 1000.0 - 20.0
            self.data['fields']['rh'] = sbd_data_dict['rh'] / 100.0
            self.data['fields']['sst'] = sbd_data_dict['sst'] / 1000.0 - 5.0
            self.data['fields']['scc'] = sbd_data_dict['scc'] / 10000.0
            self.data['fields']['sr'] = sbd_data_dict['sr'] / 10.0
            self.data['fields']['lwflux'] = sbd_data_dict['lwflux'] / 10.0
            self.data['fields']['rain'] = sbd_data_dict['prlev'] / 100.0
            self.data['fields']['pres'] = sbd_data_dict['bp'] / 100.0 + 900.0

            self.data['tags']['flag'] = sbd_data_dict['flag']

            dt = datetime(sbd_data_dict['year'] + 2000, sbd_data_dict['mon'], sbd_data_dict['day'], sbd_data_dict['hour'], sbd_data_dict['min'], 0)
            self.data['time'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            self.data['fields']['data_time'] = time.mktime(dt.timetuple())
            self.data['fields']['data_session_offset'] = self.data['fields']['session_time'] - self.data['fields']['data_time']

            #print(dt, sbd_data_dict)

    def unknown(self, email):
        #print('unknown Processor')
        self.data['tags']['processor'] = 'unknown'
        self.header(email)

    def TriAXYS(self, email):
        #print('TriAXYS Processor')
        self.data['tags']['processor'] = 'TriAXYS'
        tri_data = self.header(email)
        if tri_data is None or len(tri_data) == 0:
            return
        if tri_data[0:6] == b'$CSWAS':
            tri_str = tri_data.decode('utf-8');
            tri_split = re.split(',|\\*', tri_str)
            #print("CSWAS", tri_split)
            dt = datetime.strptime(tri_split[1] + " " + tri_split[2], "%y%m%d %H%M%S")
            self.data['time'] = dt.strftime("%Y-%m-%d %H:%M:%S")
            self.data['fields']['data_time'] = time.mktime(dt.timetuple())

       #String[] CSWAS = {"ZCN", "Havg", "Tz", "Hmax", "Tmax", "Hsig", "Tsig", 
       #                   "H1-10", "T1-10", "Tavg", "TP", "Tp5", "HW0", "Te", "WaveSteepness", "MeanDir", "MeanSpread", "WaveReturn", 
       #                   "Lat", "Lon", "PosStatus", "Volt", "Resets"};



            self.data['fields']['ZCN'] = float(tri_split[4])
            self.data['fields']['Havg'] = float(tri_split[5])
            self.data['fields']['Tz'] = float(tri_split[6])
            self.data['fields']['Hmax'] = float(tri_split[7])
            self.data['fields']['Tmax'] = float(tri_split[8])
            self.data['fields']['Hsig'] = float(tri_split[9])
            self.data['fields']['Tsig'] = float(tri_split[10])
            self.data['fields']['H1-10'] = float(tri_split[11])
            self.data['fields']['T1-10'] = float(tri_split[12])
            self.data['fields']['Tavg'] = float(tri_split[13])
            self.data['fields']['Tp'] = float(tri_split[14])
            self.data['fields']['Tp5'] = float(tri_split[15])
            self.data['fields']['HW0'] = float(tri_split[16])
            self.data['fields']['Te'] = float(tri_split[17])
            self.data['fields']['WaveSteepness'] = float(tri_split[18])
            self.data['fields']['MeanDir'] = float(tri_split[19])
            self.data['fields']['MeanSpread'] = float(tri_split[20])
            self.data['fields']['WaveReturn'] = float(tri_split[21])

        #print(tri_data)

    def XEOS(self, email):
        #print('XEOS Processor')
        self.data['tags']['processor'] = 'XEOS'
        sbd_data = self.header(email)
        if sbd_data is None or len(sbd_data) == 0:
            return

        sbd_data_str = re.split(",|\\*| |\\r|\\n", sbd_data.decode('utf-8').strip('\r\n '))
        print('xeos data', sbd_data_str)

        if sbd_data_str[1] == 'P':  # position report
            try:
                dt = datetime.strptime(str(self.session_time.year) + '-' + sbd_data_str[0], "%Y-%m%d%H%M")
                self.data['time'] = dt.strftime("%Y-%m-%d %H:%M:%S")
                self.data['data_time'] = time.mktime(dt.timetuple())

                self.data['fields']['lat'] = float(sbd_data_str[3])
                self.data['fields']['lon'] = float(sbd_data_str[4])
                self.data['fields']['SNR'] = float(sbd_data_str[5])
                self.data['fields']['batt-volt'] = float(sbd_data_str[6])/100
            except ValueError:
                print('Value Error', str(self.session_time.year) + '-' + sbd_data_str[0], dt)

        print(self.data)

    def OSD(self, payload):
        #print('OSD Processor')
        self.data['tags']['processor'] = 'OSD'
        
        nrs_data = self.header(email)
        if nrs_data is None or len(nrs_data) == 0:
            return

        data_types = {}
        data_types[0x00] = 'Mixed'
        data_types[0x10] = 'GPS'
        data_types[0x11] = 'GPS'
        data_types[0x12] = 'GPS'
        data_types[0x20] = 'Modem'
        data_types[0x40] = 'MRU'
        data_types[0x41] = 'MRU'
        data_types[0x50] = 'ADCP'
        data_types[0x8F] = 'Platform'
        data_types[0x90] = 'BGC'
        data_types[0xFF] = 'WQM'

        hdr = nrs_data[0:12]
        if len(hdr) < 12:
            return

        [siteNumber, dataType, retryCount, CSQ, date, time] = struct.unpack(">BBBBII", hdr)
        #print('datatype :', dataType, data_types[dataType], 'date', date, 'time', time)

        month = np.floor(date / 10000)
        day = np.floor(date % 10000 / 100)
        year = np.floor(date % 100) + 2000
        hour = np.floor(time / 10000)
        min = np.floor(time % 10000 / 100)
        sec = np.floor(time % 100)
        #print('date time: ', year, month, day, hour, min, sec)
        #timestamp = datetime(int(year), int(month), int(day), int(hour), int(min), int(sec))
        timestamp = datetime(int(year), int(month), int(day), int(hour), 0, 0)
        #print('timestamp ', timestamp)
        self.data['time'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        self.data['fields']['data_time'] = time.mktime(timestamp.timetuple())

        self.data['tags']['site_number'] = siteNumber
        self.data['fields']['CSQ'] = CSQ

        hdr_data = nrs_data[13:13+4]
        [filesToTx, numModems, filePointer] = struct.unpack(">BBH", hdr_data)
        #print('file pointer', filePointer)

        data_type_str = data_types[dataType]
        self.data['tags']['data_type'] = data_type_str
        pos = 16

        if data_type_str == 'GPS':
            gps_data = nrs_data[16:16 + 10]
            [latddmm, latmmmm, londdmm, lonmmmm, bx, hdop] = struct.unpack(">HHHHBB", gps_data)
            ##print('latddmm', latddmm, latmmmm, 'longddmm', londdmm, lonmmmm)
            ##print('lat dd', int(latddmm/100), 'lat min', ((latddmm - int(latddmm/100)*100) + latmmmm/10000))
            lat = int(latddmm/100) + ((latddmm - int(latddmm/100)*100) + latmmmm/10000)/60
            if (latddmm & 0x8000): # N/S bit
                latddmm = latddmm & 0x7fff
            else:
                lat = -lat
            if (londdmm & 0x8000): # E/W bit
                londdmm = londdmm & 0x7fff

            lon = int(londdmm/100) + ((londdmm - int(londdmm/100)*100) + lonmmmm/10000)/60
            #print('lat', lat, 'lon', lon)
            self.data['fields']['lat'] = lat
            self.data['fields']['lon'] = lon

        if data_type_str == 'MRU':
            mru_data = nrs_data[pos:pos + 2 * 4 + 256]
            pos += 2 * 4 + 256
            #print('len mru_data', len(mru_data))
            mru_data_array = struct.unpack(">2f256B", mru_data[0:264])
            #print('offset', mru_data_array[0], 'scale', mru_data_array[1])
            offset = float(mru_data_array[0])
            scale = float(mru_data_array[1])

            # self.data['fields']['wave-accel-spectra'] = [x / scale + offset for x in mru_data[2:256+2]]

            # double df = 2.5/256;
            # add("df", df);
            # add("log[0]", (b[0] / scale) + offset);
            # for (int j = 1; j < 256; j++)
            # {
            # 	d = (b[j] / scale) + offset;
            # 	double f = j * 2.5 / 256.0;
            # 	double wds = Math.pow(10, d) / Math.pow(2 * Math.PI * f , 4) / df;
            # 	add("wds["+j+"]", wds);
            # }

            df = 2.5/256

            #f, wave_acceleration_spectra = signal.welch(a, fs=5, nfft=512, scaling='density', window='hamming', detrend='linear', nperseg=512)

            # create an array for wave_displacement spectra
            wave_displacement_spectra = np.zeros(256) * np.nan

            wave_acceleration_spectra = [np.power(10, offset + x / scale) for x in mru_data_array[2:256+2]]
            f_wave_disp = np.arange(0, 256) * df

            # compute displacement spectra from wave acceleration spectra
            # by divinding by (2*pi*f) ^ 4, first point is nan as f[0] = 0
            wave_displacement_spectra[0] = np.nan
            wave_displacement_spectra[1:] = wave_acceleration_spectra[1:] / (2*np.pi*f_wave_disp[1:])**4

            # calculate wave height, NOAA use frequency band 0.0325 to 0.485 https://www.ndbc.noaa.gov/wavecalc.shtml
            # 0.05 = 20 sec wave period, MRU overestimates the acceleration at this low frequency,
            # almost 1m at 7m SWH, ~ 10% because of noise
            # use f[1] as the delta frequency (the width of a frequency bin width)
            msk = (f_wave_disp > 0.05) & (f_wave_disp < 0.485)
            m0 = sum(wave_displacement_spectra[msk])
            swh = 4 * np.sqrt(m0)

            m2 = sum(wave_displacement_spectra[msk] * (f_wave_disp[msk] ** 2))

            apd = np.sqrt(m0/m2)

            print('swh, apd', swh, apd)

            self.data['fields']['Hsig'] = swh
            self.data['fields']['Tavg'] = apd

            pkt_len = len(nrs_data)
            #print('len', pkt_len)
            if pkt_len > 282:
                (volts,) = struct.unpack(">f", nrs_data[pos:pos+4])
                pos += 4
                self.data['fields']['battery-volts'] = volts

            if pkt_len > 284:
                (z_accel, z_std) = struct.unpack(">ff", nrs_data[pos:pos + 8])
                pos += 8
                self.data['fields']['zAccelAverage'] = z_accel
                self.data['fields']['zAccelStd'] = z_std

            if pkt_len > 292:
                (load_temp,) = struct.unpack(">f", nrs_data[pos:pos + 4])
                pos += 4
                if dataType == 0x41:
                    self.data['fields']['load'] = load_temp
                else:
                    self.data['fields']['temperature'] = load_temp

        if data_type_str == 'BGC':
            bgc_data = nrs_data[16:16 + 8 * 4]
            [bv, itemp, obphase, otemp, chl, ntu, par, load] = struct.unpack(">8f", bgc_data)
            #print('bv', bv, 'itemp', itemp)

            self.data['fields']['battery-volts'] = bv
            self.data['fields']['itemp'] = itemp
            self.data['fields']['obpahse'] = obphase
            self.data['fields']['otemp'] = otemp
            self.data['fields']['chl'] = chl
            self.data['fields']['ntu'] = ntu
            self.data['fields']['par'] = par
            self.data['fields']['load'] = load

    def process(self, name, email, deployment_code):
        self.data = {}
        self.data['fields'] = {}
        self.data['tags'] = {}
        self.data['tags']['deployment_code'] = deployment_code
        #print('processing', name)

        func = None
        try:
            func = getattr(self, name)
        except AttributeError as ae:
            print("unknown processor", ae)
            pass

        if not func is None:
            if hasattr(self, name) and callable(func):
                func(email)

        return self.data


# insert data into influx database
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

def influx_insert(data):

    p = influxdb_client.Point("rtdp")
    p.time(data["time"])
    for key, value in data["tags"].items():
        p.tag(key, value)
    for key, value in data["fields"].items():
        if isinstance(value, float) | isinstance(value, int):
            p.field(key, value)

    write_api.write(bucket=bucket, org=org, record=p)


# main, parse command files
if __name__ == "__main__":

    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    con = sqlite3.connect("imei-deployments.sqlite")  # change to 'sqlite:///your_filename.db'
    # files.sort()
    for fle in files:
        print('file :', fle)

        if str.lower(fle[-3:]) == "eml":
            msg = email.message_from_file(open(fle), policy=email.policy.default)

            # extract from the mail header
            subject = msg.get('Subject')
            dt = msg.get('date').datetime
            #print('Date:', dt, 'FROM:', msg.get('From'), 'Subject:', subject)

            # match iridium short-burst-data subject, and extract IMEI from subject
            match_sbd = sbd_file.match(subject)
            if match_sbd:

                p = proc_mail()
                #print('SBD number:', match_sbd.group(1))
                imei = match_sbd.group(1)
                cur = con.cursor()

                cur.execute("SELECT * FROM imei WHERE imei=? AND ? BETWEEN deployment_date AND recovery_date", (imei, dt))

                rows = cur.fetchall()

                deployment_code = 'unknown'
                processor = 'unknown'
                for row in rows:
                    #print(row)
                    deployment_code = row[3]
                    processor = row[4]

                cur.close()
                print('deployment, processor', deployment_code, processor)

                data = p.process(processor, msg, deployment_code)
                json_data = json.dumps(data)
                #print('json-data', json_data)

                if 'time' in data:
                    influx_insert(data)

    con.close()
