from netCDF4 import Dataset
import numpy as np
import sys


def add_depth(path_file):

    outputName = path_file.replace(".nc", "-depth.nc")

    ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

    dsIn = Dataset(path_file, mode='r')

    # copy global attributes
    for a in dsIn.ncattrs():
        print("Attribute %s value %s" % (a, dsIn.getncattr(a)))
        ncOut.setncattr(a, dsIn.getncattr(a))

    # copy dimensions
    for d in dsIn.dimensions:
        ncOut.createDimension(d, dsIn.dimensions[d].size)

    # copy variables
    varList = dsIn.variables

    for v in varList:
        print("%s file %s" % (v, path_file))

        maVariable = dsIn.variables[v][:]  # get the data
        varDims = varList[v].dimensions

        ncVariableOut = ncOut.createVariable(v, varList[v].dtype, varDims, zlib=True)

        for a in varList[v].ncattrs():
            print("%s Attribute %s = %s" % (v, a, varList[v].getncattr(a)))
            attValue = varList[v].getncattr(a)

            ncVariableOut.setncattr(a, attValue)

        ncVariableOut[:] = maVariable

    # create the new depth

    ht = dsIn.variables['HEIGHT_ABOVE_SENSOR']
    depth = dsIn.variables['DEPTH']

    # deal with opposite positive up/down attributes

    invert = 1
    if depth.positive != ht.positive:
        invert = -1

    h = invert * ht[:]
    d = depth[:]

    h1 = h.reshape(1, len(h))
    d1 = d.reshape(len(d), 1)

    hd = d1 + h1

    hd_var = ncOut.createVariable('DEPTH_HEIGHT', np.float32, ('TIME', 'HEIGHT_ABOVE_SENSOR'), zlib=True)

    # copy depth attributes to new variable
    for a in depth.ncattrs():
        print("Attribute %s = %s" % (a, depth.getncattr(a)))
        attValue = depth.getncattr(a)

        hd_var.setncattr(a, attValue)

    hd_var[:] = hd

    # close files

    dsIn.close()

    ncOut.close()

    return outputName


if __name__ == "__main__":
    add_depth(sys.argv[1])
