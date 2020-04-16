#!/usr/bin/python3

# raw2netCDF
# Copyright (C) 2019 Peter Jansen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

from netCDF4 import Dataset, chartostring
from netCDF4 import num2date
import datetime as dt
import numpy as np
import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import sys
import os
from matplotlib import rc

# rc('text', usetex=True)

for path_file in sys.argv[1:len(sys.argv)]:

    nc = Dataset(path_file)

    # get time variable
    vs = nc.get_variables_by_attributes(standard_name='time')
    nctime = vs[0]
    t_unit = nctime.units  # get unit  "days since 1950-01-01T00:00:00Z"

    try:
        t_cal = nctime.calendar
    except AttributeError:  # Attribute doesn't exist
        t_cal = u"gregorian"  # or standard

    dt_time = [num2date(t, units=t_unit, calendar=t_cal) for t in nctime]

    # work out variables to plot
    nc_vars_to_plot = [var for var in nc.variables]

    # remove any dimensions from the list to plot
    nc_dims = [dim for dim in nc.dimensions]  # list of nc dimensions

    for i in nc_dims:
        try:
            nc_vars_to_plot.remove(i)
        except ValueError:
            print('did not remove ', i)

    # remove an auxiliary variables from the list to plot
    aux_vars = list()
    for var in nc.variables:
        try:
            aux_vars.extend(nc.variables[var].getncattr('ancillary_variables').split(' '))
        except AttributeError:
            pass

    # remove any variables without a TIME dimension from the list to plot
    to_plot = list()

    for var in nc.variables:
        print('var dims ', nc.variables[var].dimensions, " time dims ", nctime.get_dims()[0].name)
        # print var
        if var in nc_dims:
            continue
        if var in aux_vars:
            continue
        if var in nctime.name:
            continue
        if nctime.get_dims()[0].name in nc.variables[var].dimensions:
            print('to plot ', var)
            to_plot.append(var)

    # pdffile = path_file[path_file.rfind('/')+1:len(path_file)] + '-' + nc.getncattr('deployment_code') + '-plot.pdf'

    pdffile = path_file + '.pdf'

    pp = PdfPages(pdffile)

    txt = ""
    lines = 0
    plt.figure(figsize=(11.69, 8.27))

    txt = 'file name : ' + os.path.basename(path_file) + '\n\n'

    txt += 'Dimensions:\n'
    for x in nc.dimensions:
        txt += '    ' + x + ' (' + str(nc.dimensions[x].size) + ')\n'

    txt += '\nVariables:\n'
    for x in nc.variables:
        v_atts = nc.variables[x]
        var_line = '    ' + x + ' ' + str(v_atts.dimensions)

        try:
            var_line += ' : long_name = ' + v_atts.long_name
        except AttributeError:
            pass
        try:
            var_line += ' (' + v_atts.units + ')'
        except AttributeError:
            pass
        var_line += ' : type ' + str(v_atts.datatype)

        print(var_line)

        lines = txt.count('\n') + var_line.count('\n')
        # print("lines ", lines)
        if lines > 57:
            #print(txt)
            print('new page')
            plt.text(-0.1, -0.1, txt, fontsize=8, family='monospace')
            plt.axis('off')
            pp.savefig()
            plt.close()
            plt.figure(figsize=(11.69, 8.27))

            txt = ""

            lines = 0

        txt += var_line + '\n'

    plt.figure(figsize=(11.69, 8.27))

    plt.text(-0.1, -0.1, txt, fontsize=8, family='monospace')
    plt.axis('off')
    pp.savefig()
    plt.close()

    txt = ""
    plt.figure(figsize=(11.69, 8.27))

    lines = 0
    # print "NetCDF Global Attributes:"
    for nc_attr in sorted(nc.ncattrs(), key=lambda s: s.lower()):
        #print('\t%s:' % nc_attr, repr(nc.getncattr(nc_attr)))
        attrib_txt = nc_attr + ' : ' + str(nc.getncattr(nc_attr)).replace('\n', '\n   ') + '\n'
        lines = txt.count('\n') + attrib_txt.count('\n')
        # print("lines ", lines)
        if lines > 57:
            #print(txt)
            print('new page')
            plt.text(-0.1, -0.1, txt, fontsize=8, family='monospace')
            plt.axis('off')
            pp.savefig()
            plt.close()
            plt.figure(figsize=(11.69, 8.27))

            txt = ""

            lines = 0

        txt += attrib_txt

        lines += 1

    #print(txt)
    plt.text(-0.1, -0.1, txt, fontsize=8, family='monospace')
    plt.axis('off')
    pp.savefig()
    plt.close()

    instrument_index_var = nc.variables['instrument_index']
    instrument_index = instrument_index_var[:]
    instrument_index_unique = np.unique(instrument_index)
    print("instrument index values ", instrument_index_unique)

    # plot each variable in the to_plot list
    for plot in to_plot:

        plot_var = nc.variables[plot]
        print("plotting ", plot_var.name)

        var = plot_var[:]
        shape_len = len(var.shape)

        # create a page with information about the variable, and any aux variables
        fig = plt.figure(figsize=(11.69, 8.27))

        text = "Variable : " + plot_var.name + str(plot_var.dimensions) + "\n"
        nc_attrs = plot_var.ncattrs()
        # print "NetCDF Variable Attributes:"
        for nc_attr in nc_attrs:
            attrVal = plot_var.getncattr(nc_attr)
            #print('\t%s:' % nc_attr, repr(plot_var.getncattr(nc_attr)), type(attrVal))
            text += nc_attr + ' : ' + str(attrVal) + '\n'

        qc = np.zeros_like(plot_var[:])
        if hasattr(plot_var, 'ancillary_variables'):
            qc_var_name = plot_var.getncattr('ancillary_variables')
            for qc_var_n in qc_var_name.split(' '):
                qc_var = nc.variables[qc_var_n]

                text += "\nAUX : " + qc_var.name + str(qc_var.dimensions) + "\n"

                nc_attrs = qc_var.ncattrs()
                # print "NetCDF AUX Variable Attributes:"
                for nc_attr in nc_attrs:
                    # print '\t%s:' % nc_attr, repr(nc.getncattr(nc_attr))
                    text += nc_attr + ' : ' + str(qc_var.getncattr(nc_attr)) + '\n'

                if qc_var_n.endswith("_quality_control"):

                    qc = qc_var[:]

                    if plot_var.dimensions[0] != nctime.get_dims()[0].name:
                        qc = np.transpose(qc)

                    qc = np.squeeze(qc)

        plt.text(-0.1, 0.0, text, fontsize=8, family='monospace')
        plt.axis('off')
        pp.savefig(fig)
        plt.close(fig)

        # now create a page with the plot

        fig = plt.figure(figsize=(11.69, 8.27))
        ax = plt.subplot(111)
        pl = []

        if plot_var.dimensions[0] != nctime.get_dims()[0].name:
            var = np.transpose(var)
        var = np.squeeze(var)
        var.mask = False

        for idx in instrument_index_unique:
            print(" plotting : ", plot_var.name, " shape ", var.shape, " var_shape ", shape_len, " instrument ", idx)

            var_idx = np.ma.masked_where(instrument_index != idx, var).compressed()
            qc_idx = np.ma.masked_where(instrument_index != idx, qc).compressed()
            dt_time_msk = np.ma.masked_where(instrument_index != idx, dt_time).compressed()

            print("var_idx", var_idx)
            print("qc_idx", qc_idx)

            # create range from only good data
            qc_m = np.ma.masked_where((qc_idx == 9) | (qc_idx == 4) | (qc_idx == 6), var_idx)
            mx = qc_m.max()
            mi = qc_m.min()

            marg = (mx - mi) * 0.1
            print("  max ", mx, " min ", mi)

            #plt.ylim([mi - marg, mx + marg])

            # create a legend entry made from serial_number and depth
            if hasattr(plot_var, 'sensor_serial_number'):
                sn = plot_var.getncattr('sensor_serial_number').split('; ')
            elif hasattr(plot_var, 'sensor_serial_number'):
                sn = nc.getncattr('instrument_serial_number').split('; ')
            else:
                sn = 'not found'
                sn = chartostring(nc.variables["instrument_type"][:])

            print(" sn ", sn)

            if hasattr(plot_var, 'sensor_depth'):
                dpth = plot_var.getncattr('sensor_depth').split('; ')
            elif hasattr(plot_var, 'sensor_height'):
                dpth = plot_var.getncattr('sensor_height').split('; ')
            elif hasattr(nc, 'instrument_nominal_depth'):
                dpth = str(nc.getncattr('instrument_nominal_depth')).split('; ')
            else:
                dpth = 'unknown'
                dpth = nc.variables["NOMINAL_DEPTH"][:]

            print("  depth ", dpth)

            leg = [x.strip() + ' (' + str(y).strip() + ' m)' for x, y in zip(sn, dpth)]

            # if less than 200 points plot with a dot and line
            plot_marks = '-'
            if len(dt_time) < 200:
                plot_marks = '.-'

            #print(dt_time_msk)
            #print(qc_m)

            pl.extend(ax.plot(dt_time_msk, qc_m, plot_marks, label=leg[idx-1]))
            #print(" plot list ", pl)

            # mark qc>2 with yellow dot, qc>3 with red dot
            qc_m = np.ma.masked_where((qc_idx <= 2) | (qc_idx == 8), var_idx)
            ax.plot(dt_time_msk, qc_m, 'yo')
            qc_m = np.ma.masked_where((qc_idx <= 3) | (qc_idx == 8), var_idx)
            ax.plot(dt_time_msk, qc_m, 'ro')

            # add deployment/instrument/standard name as title

            # plt.title(nc.getncattr('deployment_code') + ' : ' + plot_var.sensor_name + ' ' + \
            #          plot_var.sensor_serial_number + ' : ' + plot_var.name, fontsize=10)

            # plt.title(nc.getncattr('deployment_code') + ' : ' + plot_var.getncattr('name'), fontsize=10)
            try:
                plt.title(nc.getncattr('deployment_code'), fontsize=10)
            except AttributeError:
                pass

            # add units to Y axis
            try:
                plt.ylabel(plot + ' (' + plot_var.units + ')')
            except AttributeError:
                pass

        date_time_start = None
        date_time_end = None

        # plot only the time of deployment
        try:
            date_time_start = dt.datetime.strptime(nc.getncattr('time_coverage_start'), '%Y-%m-%dT%H:%M:%SZ')
            date_time_end = dt.datetime.strptime(nc.getncattr('time_coverage_end'), '%Y-%m-%dT%H:%M:%SZ')
        except AttributeError:
            pass
        try:
            date_time_start = dt.datetime.strptime(nc.getncattr('time_deployment_start'), '%Y-%m-%dT%H:%M:%SZ')
            date_time_end = dt.datetime.strptime(nc.getncattr('time_deployment_end'), '%Y-%m-%dT%H:%M:%SZ')
        except AttributeError:
            pass

        if date_time_start:
            plt.xlim(date_time_start, date_time_end)

        # invert the yaxis if the units are dbar
        try:
            print("  units ", plot_var.units)
            if plot_var.units.startswith('dbar'):
                plt.gca().invert_yaxis()
        except AttributeError:
            pass
        # invert the yaxis if the it has the attribute positive down
        try:
            print("  positive ", plot_var.positive)
            if plot_var.positive == 'down':
                plt.gca().invert_yaxis()
        except AttributeError:
            pass

        #fig.autofmt_xdate()
        plt.grid()

        # shrink the plot some
        box = ax.get_position()
        ax.set_position([box.x0, box.y0 + box.height * 0.1, box.width, box.height * 0.9])

        # add legend below plot
        #plt.legend(iter(pl), leg, loc='lower center', bbox_to_anchor=(0.5, -0.05), ncol=5)

        #plt.legend(iter(pl), leg, bbox_to_anchor=(0.0, -0.2, 1.0, -0.15), loc=3, ncol=6, mode="expand", borderaxespad=0.0, fontsize='x-small')
        plt.legend(handles=pl, bbox_to_anchor=(0.0, -0.2, 1.0, -0.15), loc=3, ncol=1, mode="expand", borderaxespad=0.0, fontsize='x-small')

        # plt.savefig(plot + '.pdf')
        pp.savefig(fig, papertype='a4')
        plt.close(fig)

    # plt.show()

    pp.close()

    nc.close()
