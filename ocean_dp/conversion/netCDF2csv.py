from netCDF4 import Dataset
from netCDF4 import num2date
import datetime as dt
import numpy as np
import sys
import os


for path_file in sys.argv[1:len(sys.argv)]:

    nc = Dataset(path_file)

    f = open(path_file + '.csv', 'w')

    print(path_file)

    # put some of the metedata into the header
    f.write('; ' + path_file + '\n')
    #f.write('; time_deployment_start ' + nc.getncattr('time_deployment_start') + '\n')
    #f.write('; time_deployment_end   ' + nc.getncattr('time_deployment_end') + '\n')
    #f.write('; latitude,longitude   ' + str(nc.getncattr('geospatial_lat_min')) + ',' + str(nc.getncattr('geospatial_lon_min')) + '\n')
    f.write('; instrument   ' + nc.getncattr('instrument') + ' ' + nc.getncattr('instrument_serial_number') + '\n')
    f.write('\n')

    nc_dims = [dim for dim in nc.dimensions]  # list of nc dimensions
    nc_vars = [var for var in nc.variables]
	
    nctime = nc.variables['TIME'][:]
    t_unit = nc.variables['TIME'].units  # get unit  "days since 1950-01-01T00:00:00Z"

    try:
        t_cal = nc.variables['TIME'].calendar

    except AttributeError:  # Attribute doesn't exist
        t_cal = u"gregorian"  # or standard

    dt_time = num2date(nctime, units=t_unit, calendar=t_cal)
    print('time [0]', dt_time[0])

    nc_vars_to_process = [var for var in nc.variables]

    # remove any dimensions from the list to process
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

        for process in to_process:
            process_var = nc.variables[process]
            #qc_var = nc.variables[process_var.ancillary_variables]  # get the QC variable

            var = process_var[:]
            #qc_values = qc_var[:]
            #var.mask = qc_values > 1  # mask out all values not GOOD, or unknown
            if var.dtype == 'float32':
                var.fill_value = float('nan')
            shape_len = len(var.shape)

            #if process_var.dimensions[0] != 'TIME':
            #    var = np.transpose(var)
            #    qc_values = np.transpose(qc_values)

            var = np.squeeze(var)

            #line += ',' + str(var[i])
            #print (var[i].shape)
            #if (not var.mask[i].all()) or (len(var[i].shape) > 0):
            s = np.array2string(var[i], separator=',', prefix='', max_line_width=8192, formatter={'float_kind': lambda x: "%.3f" % x})
            #else:
            #    s = 'nan'
            s = s.replace('nan', '')
            s = s.replace('[', '')
            s = s.replace(']', '')
            line += ',' + s

        print (line)
        f.write(line + '\n')

    nc.close()
    f.close()
