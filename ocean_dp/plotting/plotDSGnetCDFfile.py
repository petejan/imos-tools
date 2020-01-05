from netCDF4 import Dataset
from netCDF4 import num2date
import datetime as dt
import numpy as np
import matplotlib

#matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import sys
import os
from matplotlib import rc

# rc('text', usetex=True)

for path_file in sys.argv[1:len(sys.argv)]:

    nc = Dataset(path_file)
    nc_dims = [dim for dim in nc.dimensions]  # list of nc dimensions

    nctime = nc.variables['TIME'][:]
    t_unit = nc.variables['TIME'].units  # get unit  "days since 1950-01-01T00:00:00Z"

    try:
        t_cal = nc.variables['TIME'].calendar

    except AttributeError:  # Attribute doesn't exist
        t_cal = u"gregorian"  # or standard

    dt_time = num2date(nctime, units=t_unit, calendar=t_cal)

    ndepth = nc.variables["NOMINAL_DEPTH"][:]
    inst = nc.variables["instrument_index"][:]
    stationStr = nc.variables["instrument_type"][:]

    temp = nc.variables["PRES"]
    #temp_qc = nc.variables["TEMP_quality_control"][:]

    # print(plot_var.name, " shape ", var.shape, " len ", shape_len)

    for i in set(inst):
        print ('instrument index ', i)
        time_masked = np.ma.masked_where((inst != i), dt_time)
        temp_masked = np.ma.masked_where((inst != i), temp[:])

        pl = plt.plot(time_masked.compressed(), temp_masked.compressed())

    plt.grid(True)

    # should we invert the axis
    try:
        if temp.units == 'dbar':
            plt.gca().invert_yaxis()
    except AttributeError:
        pass
    try:
        if temp.positive == 'down':
            plt.gca().invert_yaxis()
    except AttributeError:
        pass

    plt.show()


