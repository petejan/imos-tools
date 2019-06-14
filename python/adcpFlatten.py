from netCDF4 import Dataset
import numpy as np
import sys

path_file = sys.argv[1]
# path_file = '/Users/pete/Downloads/IMOS_ABOS-DA_AETVZ_20150515T000000Z_EAC4200_FV01_EAC4200-2016-WORKHORSE-ADCP-726_END-20161104T205740Z_C-20170703T055611Z.nc'

outputName = path_file + ".flat.nc"

ncOut = Dataset(outputName, 'w', format='NETCDF4')

dsIn = Dataset(path_file, mode='r')

# copy global attributes
for a in dsIn.ncattrs():
    print("Attribute %s value %s" % (a, dsIn.getncattr(a)))
    ncOut.setncattr(a, dsIn.getncattr(a))

ht = dsIn.variables['HEIGHT_ABOVE_SENSOR']
depth = dsIn.variables['DEPTH']
time = dsIn.variables['TIME']

ncOut.createDimension('HEIGHT_ABOVE_SENSOR', dsIn.dimensions['HEIGHT_ABOVE_SENSOR'].size)

# create the time
tDim = ncOut.createDimension("OBS", len(ht) * len(depth))
time_var = ncOut.createVariable('TIME', time.dtype, ('OBS', ), zlib=True)
for a in time.ncattrs():
    print("Attribute %s = %s" % (a, time.getncattr(a)))
    attValue = time.getncattr(a)

    time_var.setncattr(a, attValue)

time_var[:] = np.repeat(time[:], len(ht[:]))

# create a 'cell' variable, the ADCP cell which the data is from
cell_var = ncOut.createVariable('CELL', np.byte, ('OBS', ), zlib=True)
cell_var.setncattr("long_name", "which cell this OBS is from")
cell_var.setncattr("instance_dimension", "HEIGHT_ABOVE_SENSOR")
cell_var.setncattr("comment", "WARNING: is this the correct cell?")

# TODO: generate the correct cell
cell_var[:] = np.repeat(np.arange(len(ht[:])), len(time[:]))

# create the new depth
invert = 1
if depth.positive != ht.positive:
    invert = -1

h = invert * ht[:]
d = depth[:]

h1 = h.reshape(1, len(h))
d1 = d.reshape(len(d), 1)

hd = d1 + h1

ht = hd.reshape(len(h) * len(d), 1)

hd_var = ncOut.createVariable('DEPTH', depth.dtype, ('OBS', ), zlib=True)

# copy depth attributes to new variable
for a in depth.ncattrs():
    print("Attribute %s = %s" % (a, depth.getncattr(a)))
    attValue = depth.getncattr(a)

    hd_var.setncattr(a, attValue)

hd_var[:] = ht

# vars to include
varInclude = ['LATITUDE', 'LONGITUDE', 'NOMINAL_DEPTH', 'HEIGHT_ABOVE_SENSOR']

# copy the currents over
for v in ('UCUR', 'VCUR', 'WCUR'):

    cur = dsIn.variables[v]
    cur_out = ncOut.createVariable(v, cur.dtype, ('OBS', ), zlib=True)
    for a in cur.ncattrs():
        print("%s Attribute %s = %s" % (v, a, cur.getncattr(a)))
        attValue = cur.getncattr(a)

        cur_out.setncattr(a, attValue)

    if hasattr(cur, 'ancillary_variables'):
        varInclude += [cur.ancillary_variables]

    cur_out[:] = cur[:].flatten()

# copy other variables
for v in varInclude:
    print("adding %s" % v)

    cur = dsIn.variables[v]
    print("dimensions %d %s" % (len(cur.dimensions), cur.shape))

    if len(cur.dimensions) <= 1:
        cur_out = ncOut.createVariable(v, cur.dtype, cur.dimensions, zlib=True)
    else:
        cur_out = ncOut.createVariable(v, cur.dtype, ('OBS', ), zlib=True)

    # copy attributes
    for a in cur.ncattrs():
        print("%s : Attribute %s = %s" % (v, a, cur.getncattr(a)))
        attValue = cur.getncattr(a)

        cur_out.setncattr(a, attValue)

    cur_out[:] = cur[:].flatten()

# close files

dsIn.close()

ncOut.close()

