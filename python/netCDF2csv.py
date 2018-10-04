from netCDF4 import Dataset
from netCDF4 import num2date
import datetime as dt
import numpy as np
import sys
import os

def ncdump(nc_fid, verb=True):
    '''
    ncdump outputs dimensions, variables and their attribute information.
    The information is similar to that of NCAR's ncdump utility.
    ncdump requires a valid instance of Dataset.

    Parameters
    ----------
    nc_fid : netCDF4.Dataset
        A netCDF4 dateset object
    verb : Boolean
        whether or not nc_attrs, nc_dims, and nc_vars are printed

    Returns
    -------
    nc_attrs : list
        A Python list of the NetCDF file global attributes
    nc_dims : list
        A Python list of the NetCDF file dimensions
    nc_vars : list
        A Python list of the NetCDF file variables
    '''

    def print_ncattr(key):
        """
        Prints the NetCDF file attributes for a given key

        Parameters
        ----------
        key : unicode
            a valid netCDF4.Dataset.variables key
        """
        try:
            print ("\t\ttype:", repr(nc_fid.variables[key].dtype))
            for ncattr in nc_fid.variables[key].ncattrs():
                print('\t\t%s:' % ncattr, repr(nc_fid.variables[key].getncattr(ncattr)))
        except KeyError:
            print("\t\tWARNING: %s does not contain variable attributes" % key)

    # NetCDF global attributes
    nc_attrs = nc_fid.ncattrs()
    if verb:
        print("NetCDF Global Attributes:")
        for nc_attr in nc_attrs:
            print('\t%s:' % nc_attr, repr(nc_fid.getncattr(nc_attr)))

    # Dimension shape information.
    nc_dims = [dim for dim in nc_fid.dimensions]  # list of nc dimensions
    if verb:
        print("NetCDF dimension information:")
        for dim in nc_dims:
            print("\tName:", dim)
            print("\t\tsize:", len(nc_fid.dimensions[dim]))
            print_ncattr(dim)

    # Variable information.
    nc_vars = [var for var in nc_fid.variables]  # list of nc variables
    if verb:
        print("NetCDF variable information:")
        for var in nc_vars:
            if var not in nc_dims:
                print('\tName:', var)
                print("\t\tdimensions:", nc_fid.variables[var].dimensions)
                print("\t\tsize:", nc_fid.variables[var].size)
                print_ncattr(var)

    return nc_attrs, nc_dims, nc_vars


for path_file in sys.argv[1:len(sys.argv)]:

    nc = Dataset(path_file)

    f = open(path_file + '.csv', 'w')

    print(path_file)

    # put some of the metedata into the header
    f.write('; ' + path_file + '\n')
    f.write('; time_deployment_start ' + nc.getncattr('time_deployment_start') + '\n')
    f.write('; time_deployment_end   ' + nc.getncattr('time_deployment_end') + '\n')
    f.write('; latitude,longitude   ' + str(nc.getncattr('geospatial_lat_min')) + ',' + str(nc.getncattr('geospatial_lon_min')) + '\n')
    f.write('; instrument   ' + nc.getncattr('instrument') + ' ' + nc.getncattr('instrument_serial_number') + '\n')
    f.write('\n')

    nc_attrs, nc_dims, nc_vars = ncdump(nc, False)

    nctime = nc.variables['TIME'][:]
    t_unit = nc.variables['TIME'].units  # get unit  "days since 1950-01-01T00:00:00Z"

    try:
        t_cal = nc.variables['TIME'].calendar

    except AttributeError:  # Attribute doesn't exist
        t_cal = u"gregorian"  # or standard

    dt_time = num2date(nctime, units=t_unit, calendar=t_cal)
    print('time [0]', dt_time[0])

    # remove any dimensions from the list to process
    nc_vars_to_process = nc_vars
    for i in nc_dims:
        try:
            nc_vars_to_process.remove(i)
        except ValueError:
            print('did not remove ', i)

    # remove an auxiliary variables from the list to process
    aux_vars = list()
    for var in nc.variables:
        try:
            aux_vars.append(nc.variables[var].getncattr('ancillary_variables'))
        except AttributeError:
            pass

    # remove any variables without a TIME dimension from the list to process
    to_process = list()

    for var in nc.variables:
        # print var
        if var in nc_dims:
            continue
        if var in aux_vars:
            continue
        if 'TIME' in nc.variables[var].dimensions:
            # print 'to process ', var
            to_process.append(var)

    # output a header line with variable names
    line = 'TIME'
    for process in to_process:

        process_var = nc.variables[process]

        line += ',' + process_var.name

        print('variable :', process_var.name)

    f.write(line + '\n')

    print('points :', len(dt_time))

    # output the data from each variable
    for i in range(1, len(dt_time)):
        line = dt_time[i].strftime("%Y-%m-%d %H:%M:%S")
        print("ts %s" % line)

        for process in to_process:

            process_var = nc.variables[process]

            var = process_var[:]
            shape_len = len(var.shape)

            if process_var.dimensions[0] != 'TIME':
                var = np.transpose(var)

            var = np.squeeze(var)

            line += ',' + str(var[i])

        f.write(line + '\n')

    nc.close()
    f.close()
