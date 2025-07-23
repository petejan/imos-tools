import struct
import sys
from datetime import datetime, timezone

from glob2 import glob


def read(files):

    for file in files:
        #print(file)

        with open(file, "rb") as f:
            l_pkt = f.read(4)
            while l_pkt:
                l = struct.unpack("<I", l_pkt)[0]

                pkt = f.read(l)
                # read the packet timestamp
                ts = ''
                pkt_i = 0
                while  pkt[pkt_i] != 0:
                    ts += chr(pkt[pkt_i])
                    pkt_i += 1
                pkt_i += 1
                # read the packet name
                name = ''
                while  pkt[pkt_i] != 0:
                    name += chr(pkt[pkt_i])
                    pkt_i += 1
                pkt_i += 1

                print(file, l, ts, name, len(pkt) - pkt_i)
                if name == 'GPS Fix':
                    # struct fix_information{
                    #   long posix_timestamp;
                    #   long latitude;
                    #   long longitude;
                    # };

                    #print('GPS Fix')
                    #print(pkt_i, len(pkt[pkt_i:]))
                    gps_fix_packet = struct.unpack("<lll", pkt[pkt_i:])
                    gps_ts = datetime.fromtimestamp(gps_fix_packet[0], timezone.utc)
                    print(file, ',', gps_ts.strftime("%Y-%m-%d %H:%M:%S"), ',', gps_fix_packet[1]/1e7, ',', gps_fix_packet[2]/1e7)
                elif name == 'Working wave packet':
                    #print(pkt_i, len(pkt[pkt_i:]))
                    working_packet = struct.unpack("<lIffff56H", pkt[pkt_i:])
                    #print(working_packet)
                elif name == 'Board Data Manager':
                    # a vector of floats or vertical acceleration values
                    samples_to_read = (12288 + 99) / 4 - 1
                    samples = struct.unpack("<{:.0f}f".format(samples_to_read), pkt[pkt_i:-3])
                    print(samples)
                    pass
                elif name == 'working_welch_spectrum':
                    # a vector of floats or wave spectra
                    #         float32_t working_welch_spectrum[welch_bin_max-welch_bin_min];
                    pass
    #
                l_pkt = f.read(4)



if __name__ == "__main__":

    files = []
    for f in sys.argv[1:]:
        files.extend(glob(f))

    read(files)
