import struct

import numpy

# generate a RDI ADCP file, with set pitch/roll/heading

header_pack = '<HHBB'
hdr_offsets = '<HHHHHH'

fixed_pack = '<HBBHBBBBHHHBBBBHBBBBHHBBHHHBBHQHBBLB'

variable_pack = '<HH7BB4H4hBBBBBB8BIHiiB8B'

with open('rdi-gen-file.000', "wb") as binary_file:
    h_data = struct.pack(header_pack, 0x7f7f, 952, 0, 6)
    o_data = struct.pack(hdr_offsets, 18, 77, 142, 464, 626, 788)

    beams = 4
    cells = 40
    coord_transform = 7 # bean coordinates, bin mapping (1), 3 beam solution (2), tilts (3), these are all unused in beam coordinates

    # create the fixed header
    f_data = struct.pack(fixed_pack, 0x0, 50, 41, 16840, 0, 13, beams, cells, 1, 1600, 704, 1, 0, 9, 0, 0, 3, 45, 0,
                         coord_transform, 0, 0, 125, 61, 2463, 1723, 1281, 255, 0, 195, 703057451779358815, 1, 255, 0, 14489, 20)

    ping_pitch = [0, 0, 0, 10, 10, 10, -10, -10, -10, 0, 0, 0, 10, 10, 10, -10, -10, -10]
    ping_roll  = [0, 10, -10, 0, 10, -10, 0, 10, -10, 0, 10, -10, 0, 10, -10, 0, 10, -10]
    ping_head  = [0,  0,   0,  0,  0,  0,  0,  0,  0, 10,  10,   10,  10,  10,  10,  10,  10,  10]

    # create an ensemble for each pitch/roll/heading
    for i in range(len(ping_pitch)):
        head = int(ping_head[i] * 100)
        pitch = int(ping_pitch[i] * 100)
        roll = int(ping_roll[i] * 100)
        depth = 100*1000
        sec = i

        # create a varaible header, need to expand, see RDI manual WorkHorse Commands and Output Data Format, June 2018, page 130
        v_data = struct.pack(variable_pack, 0x0080, 15, 18, 10, 15, 0, 52, 30, 0, 0, 0, 1525, 9, head, pitch, roll,
                             35, 2131, 0, 0, 10, 0, 0, 0, 104, 171, 88, 82, 82, 80, 131, 159, 2281701376, 20635, depth,
                             0, 0,
                             20, 18, 10, 15, 0, 52, sec, 0)

        # velocity data
        velocity_header = struct.pack('<H', 0x0100)
        vel = numpy.zeros((cells, beams), dtype=int)
        vel[:, 0] = range(cells)
        vel[:, 1] = range(cells)
        vel[:, 2] = range(cells)
        vel[:, 3] = range(cells)
        vel_data = struct.pack('<160h', *vel.reshape((beams*cells)))

        correlation_header = struct.pack('<H', 0x0200)
        corr = numpy.zeros((cells, beams), dtype=int) + 200
        corr_data = struct.pack('<160B', *corr.reshape((beams*cells)))

        echo_intensity_header = struct.pack('<H', 0x0300)
        eint = numpy.zeros((cells, beams), dtype=int) + 100
        eint_data = struct.pack('<160B', *eint.reshape((beams*cells)))

        percent_good_header = struct.pack('<H', 0x0400)
        pg = numpy.zeros((cells, 4), dtype=int)
        pg[:, 0] = 100 # Percentage of good 3-beam solutions
        pg[:, 1] = 100 # Percentage of transformations rejected
        pg[:, 2] = 100 # Percentage of more than one beam bad in bin
        pg[:, 3] = 100 # Percentage of good 4-beam solutions
        pg_data = struct.pack('<160B', *pg.reshape((4*cells)))

        # combine data into a byte array
        data = h_data + o_data + f_data + v_data + velocity_header + vel_data + correlation_header + corr_data + echo_intensity_header + eint_data + percent_good_header + pg_data

        # add the check sum to the end
        sum = 0
        for i in range(len(data)):
            sum += data[i]
        sum = sum & 0xffff

        data = data + struct.pack('<HH', 0, sum)

        print(i, 'ensemble length', len(data))

        # save ensemble
        binary_file.write(data)

    binary_file.close()
