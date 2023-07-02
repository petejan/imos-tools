from netCDF4 import Dataset
import numpy as np
import sys

# flatten a ADCP file

# from this structure
# double TIME(TIME);
#     TIME:axis = "T";
#     TIME:calendar = "gregorian";
#     TIME:long_name = "time";
#     TIME:standard_name = "time";
#     TIME:units = "days since 1950-01-01 00:00:00 UTC";
#     TIME:valid_max = 90000.;
#     TIME:valid_min = 0.;
# float HEIGHT_ABOVE_SENSOR(HEIGHT_ABOVE_SENSOR);
#     HEIGHT_ABOVE_SENSOR:axis = "Z";
#     HEIGHT_ABOVE_SENSOR:long_name = "height_above_sensor";
#     HEIGHT_ABOVE_SENSOR:positive = "up";
#     HEIGHT_ABOVE_SENSOR:reference_datum = "sensor";
#     HEIGHT_ABOVE_SENSOR:standard_name = "height";
#     HEIGHT_ABOVE_SENSOR:units = "m";
#     HEIGHT_ABOVE_SENSOR:valid_max = 12000.f;
#     HEIGHT_ABOVE_SENSOR:valid_min = -12000.f;
# float UCUR(TIME, HEIGHT_ABOVE_SENSOR);
#
# to :
#
# double TIME(OBS);
#     TIME:axis = "T";
#     TIME:calendar = "gregorian";
#     TIME:long_name = "time";
#     TIME:standard_name = "time";
#     TIME:units = "days since 1950-01-01 00:00:00 UTC";
#     TIME:valid_max = 90000.;
#     TIME:valid_min = 0.;
# byte CELL(OBS);
#     CELL:long_name = "which cell this OBS is from";
#     CELL:instance_dimension = "HEIGHT_ABOVE_SENSOR";
#     CELL:comment = "WARNING: is this the correct cell?";
# float DEPTH(OBS);
#     DEPTH:ancillary_variables = "DEPTH_quality_control";
#     DEPTH:coordinates = "TIME LATITUDE LONGITUDE NOMINAL_DEPTH";
#     DEPTH:_FillValue = 999999.f;
#     DEPTH:long_name = "actual depth";
#     DEPTH:positive = "down";
#     DEPTH:reference_datum = "sea surface";
#     DEPTH:standard_name = "depth";
#     DEPTH:units = "m";
#     DEPTH:valid_max = 12000.f;
#     DEPTH:valid_min = -5.f;
# float UCUR(OBS);
#     ......

# example
# 12/11/2018  03:07 PM       468,765,047 IMOS_DWM-DA_AETVZ_20160825T000059Z_EAC4200_FV01_EAC4200-2018-WORKHORSE-ADCP-119_END-20180426T055529Z_C-20181024T032202Z.nc
# 14/06/2019  10:15 AM       199,585,003 IMOS_DWM-DA_AETVZ_20160825T000059Z_EAC4200_FV01_EAC4200-2018-WORKHORSE-ADCP-119_END-20180426T055529Z_C-20181024T032202Z.nc.flat.nc


def flatten(path_file):

    dsIn = Dataset(path_file, mode='r')

    outputName = path_file.replace('.nc', '-flat.nc')

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    # copy global attributes
    for a in dsIn.ncattrs():
        print("Attribute %s value %s" % (a, dsIn.getncattr(a)))
        ncOut.setncattr(a, dsIn.getncattr(a))

    # create new dimensions
    ht = dsIn.variables['HEIGHT_ABOVE_SENSOR']
    depth = dsIn.variables['DEPTH']
    time = dsIn.variables['TIME']

    # keep this dimension,so we can keep the HEIGHT_ABOVE_SENSOR variable
    ncOut.createDimension('HEIGHT_ABOVE_SENSOR', dsIn.dimensions['HEIGHT_ABOVE_SENSOR'].size)

    # create the time
    tDim = ncOut.createDimension("OBS", len(ht) * len(depth))
    timeOutVar = ncOut.createVariable('TIME', time.dtype, ('OBS',), zlib=True)
    for a in time.ncattrs():
        print("Attribute %s = %s" % (a, time.getncattr(a)))
        attValue = time.getncattr(a)

        timeOutVar.setncattr(a, attValue)

    timeOutVar[:] = np.repeat(time[:], len(ht[:]))

    # create a 'cell' variable, the ADCP cell which the data is from
    cell_var = ncOut.createVariable('CELL', np.byte, ('OBS', ), zlib=True)
    cell_var.setncattr("long_name", "which cell this OBS is from")
    cell_var.setncattr("instance_dimension", "HEIGHT_ABOVE_SENSOR")

    # get the same of variables
    invert = 1
    if depth.positive != ht.positive:
        invert = -1

    h = invert * ht[:]
    d = depth[:]

    # write the cell
    cell = np.arange(len(h)).reshape(1, len(h))
    cell_zero = np.zeros([len(time[:]), 1])

    cd = cell_zero + cell
    cell_var[:] = cd.reshape(len(h) * len(d), 1)

    # generate the new depth
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

    # Variables to include
    varInclude = ['LATITUDE', 'LONGITUDE', 'NOMINAL_DEPTH', 'HEIGHT_ABOVE_SENSOR']

    # copy the currents to the new file, flattening the variable
    for v in ('UCUR', 'VCUR', 'WCUR'):

        cur = dsIn.variables[v]
        cur_out = ncOut.createVariable(v, cur.dtype, ('OBS', ), zlib=True)
        for a in cur.ncattrs():
            print("%s Attribute %s = %s" % (v, a, cur.getncattr(a)))
            attValue = cur.getncattr(a)

            cur_out.setncattr(a, attValue)

        # keep a list of 'ancillary_variables' to copy also
        if hasattr(cur, 'ancillary_variables'):
            varInclude += [cur.ancillary_variables]

        cur_out[:] = cur[:].flatten()

    # copy other variables, this picks up the QC variables as ancillary variables
    for v in varInclude:
        print("adding %s" % v)

        cur = dsIn.variables[v]
        print("%s dimensions %d %s" % (v, len(cur.dimensions), cur.shape))

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

    return outputName


if __name__ == "__main__":
    flatten(sys.argv[1])

