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

sbd_file = re.compile(r"SBD Msg From Unit: (3\d*)")


class proc_mail:
    msg_mosn = re.compile(r"MOMSN: (\d*)")
    msg_size = re.compile(r"Message Size \(bytes\): (\d*)")
    msg_time = re.compile(r"Time of Session \(UTC\): (.*)$")
    msg_loc = re.compile(r"Unit Location: Lat = (.*) Long = (.*)")

    data = None
    
    def header(self, email):
        attachments=msg.get_payload()
        sbd_data = None
        for attachment in attachments:
            fnam = attachment.get_filename()
            print('attachment file', fnam, attachment.get_content_type())
            payload = attachment.get_payload(decode=True,)
            if attachment.get_content_type() == "text/plain":
                payload = payload.decode('utf-8')

            if fnam is None:
                match_loc = self.msg_loc.search(payload)
                if match_loc:
                    print('location', match_loc.group(1), match_loc.group(2))
                    self.data['fields']['lat_iridium'] = float(match_loc.group(1))
                    self.data['fields']['lon_iridium'] = float(match_loc.group(2))
                match_size = self.msg_size.search(payload)
                if match_size:
                    print('size ', int(match_size.group(1)))

            if attachment.get_content_type() == "application/x-zip-compressed": # or name ends with .sbd
                sbd_data = payload
                #save_file = open(fnam, 'wb')
                #save_file.write(attachment.get_payload(decode=True,))
                #save_file.close()

        return sbd_data

    def ASIMET(self, email):
        print('ASIMET Processor')
        self.data['tags']['processor'] = 'ASIMET'
        asimet_data = self.header(email)

        sbd_data = struct.unpack(">BBBBBHhhhHhHhhhHHHBBB", asimet_data)
        print(sbd_data)
        sbd_data_dict = dict(zip(['hour', 'min', 'day', 'mon', 'year', 'record', 'we', 'wn', 'compass', 'bp', 'rh', 'th', 'sr', 'lwflux', 'prlev', 'sct', 'scc', 'swavg', 'flag', 'spare2', 'spare3'], sbd_data))
        self.data['fields']['we'] = sbd_data_dict['we']
        self.data['fields']['wn'] = sbd_data_dict['wn']
        self.data['fields']['th'] = sbd_data_dict['th']
        self.data['fields']['rh'] = sbd_data_dict['rh']
        self.data['fields']['sct'] = sbd_data_dict['sct']
        self.data['fields']['scc'] = sbd_data_dict['scc']

        dt = datetime(sbd_data_dict['year'] + 2000, sbd_data_dict['mon'], sbd_data_dict['day'], sbd_data_dict['hour'], sbd_data_dict['min'], 0)
        self.data['time'] = dt.strftime("%Y-%m-%d %H:%M:%S")
        print(dt, sbd_data_dict)

    def unknown(self, email):
        print('unknown Processor')
        self.data['tags']['processor'] = 'unknown'
        self.header(email)

    def NRS(self, payload):
        print('NRS Processor')
        self.data['tags']['processor'] = 'NRS'
        
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
        [siteNumber, dataType, retryCount, CSQ, date, time] = struct.unpack(">BBBBII", hdr)
        print('datatype :', dataType, data_types[dataType], 'date', date, 'time', time)

        month = np.floor(date / 10000)
        day = np.floor(date % 10000 / 100)
        year = np.floor(date % 100) + 2000
        hour = np.floor(time / 10000)
        min = np.floor(time % 10000 / 100)
        sec = np.floor(time % 100)
        print('date time: ', year, month, day, hour, min, sec)
        timestamp = datetime(int(year), int(month), int(day), int(hour), int(min), int(sec))
        print('timestamp ', timestamp)
        self.data['time'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        self.data['tags']['site_number'] = siteNumber
        self.data['fields']['CSQ'] = CSQ

        hdr_data = nrs_data[13:13+4]
        [filesToTx, numModems, filePointer] = struct.unpack(">BBH", hdr_data)
        print('file pointer', filePointer)

        data_type_str = data_types[dataType]
        self.data['tags']['data_type'] = data_type_str
        pos = 16

        if data_type_str == 'GPS':
            gps_data = nrs_data[16:16 + 10]
            [latddmm, latmmmm, londdmm, lonmmmm, bx, hdop] = struct.unpack(">HHHHBB", gps_data)
            #print('latddmm', latddmm, latmmmm, 'longddmm', londdmm, lonmmmm)
            #print('lat dd', int(latddmm/100), 'lat min', ((latddmm - int(latddmm/100)*100) + latmmmm/10000))
            lat = int(latddmm/100) + ((latddmm - int(latddmm/100)*100) + latmmmm/10000)/60
            if (latddmm & 0x8000): # N/S bit
                latddmm = latddmm & 0x7fff
            else:
                lat = -lat
            if (londdmm & 0x8000): # E/W bit
                londdmm = londdmm & 0x7fff

            lon = int(londdmm/100) + ((londdmm - int(londdmm/100)*100) + lonmmmm/10000)/60
            print('lat', lat, 'lon', lon)
            self.data['fields']['lat'] = lat
            self.data['fields']['lon'] = lon


        if data_type_str == 'MRU':
            mru_data = nrs_data[pos:pos + 2 * 4 + 256]
            pos += 2 * 4 + 256
            print('len mru_data', len(mru_data))
            mru_data_array = struct.unpack(">2f256B", mru_data[0:264])
            print('offset', mru_data_array[0], 'scale', mru_data_array[1])
            offset = float(mru_data_array[0])
            scale = float(mru_data_array[1])
            self.data['fields']['wave-accel-spectra'] = [x / scale + offset for x in mru_data[2:256+2]]

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

            pkt_len = len(nrs_data)
            print('len', pkt_len)
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
            print('bv', bv, 'itemp', itemp)

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

        try:
            func = getattr(self, name)
            if hasattr(self, name) and callable(func):
                func(email)

            json_data = json.dumps(self.data)
            print(json_data)

        except AttributeError:
            print("unknown processor")


if __name__ == "__main__":

    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    con = sqlite3.connect("imei-deployments.db")  # change to 'sqlite:///your_filename.db'
    # files.sort()
    for fle in files:
        print('file :', fle)

        if str.lower(fle[-3:]) == "eml":
            msg = email.message_from_file(open(fle), policy=email.policy.default)

            # extract from the mail header
            subject = msg.get('Subject')
            dt = msg.get('date').datetime
            print('Date:', dt, 'FROM:', msg.get('From'), 'Subject:', subject)

            # match iridium short-burst-data subject, and extract IMEI from subject
            match_sbd = sbd_file.match(subject)
            if match_sbd:

                p = proc_mail()
                print('SBD number:', match_sbd.group(1))
                imei = match_sbd.group(1)
                cur = con.cursor()

                cur.execute("SELECT * FROM imei WHERE imei=? AND ? BETWEEN deployment_date AND recovery_date", (imei, dt))

                rows = cur.fetchall()

                deployment_code = 'unknown'
                processor = 'unknown'
                for row in rows:
                    print(row)
                    deployment_code = row[3]
                    processor = row[4]

                cur.close()

                p.process(processor, msg, deployment_code)

    con.close()
