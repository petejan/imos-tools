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

from netCDF4 import Dataset
import sys
from datetime import datetime
import os
import numpy as np

nameCode = {}
nameCode['PAR'] = "F"
nameCode['TEMP'] = "T"
nameCode['AIRT'] = "M"
nameCode['PSAL'] = "S"
nameCode['CNDC'] = "C"
nameCode['PRES'] = "P"
nameCode['DOX2'] = "O"
nameCode['UCUR_MAG'] = "V"
nameCode['UCUR'] = "V"
nameCode['BATT'] = "E"
nameCode['HEADING'] = "E"
nameCode['ABSIC1'] = "A"
nameCode['EAST_VEL'] = "V"
nameCode['ECHO_INT1'] = "A"
nameCode['CPHL'] = "B"
nameCode['BB'] = "U"

# IMOS_<Facility-Code>_<Data-Code>_<Start-date>_<Platform-Code>_FV<File-Version>_ <Product-Type>_END-<End-date>_C-<Creation_date>_<PARTX>.nc

ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

def rename(netCDFfile):

    ds = Dataset(netCDFfile, 'a')

    ds_variables = ds.variables

    facility = ds.institution
    time_start = datetime.strptime(ds.time_coverage_start, ncTimeFormat)
    time_end = datetime.strptime(ds.time_coverage_end, ncTimeFormat)
    file_version = ds.file_version
    creation_date = datetime.strptime(ds.date_created, ncTimeFormat)
    platform_code = ds.platform_code
    deployment = ds.deployment_code
    instrument = ds.instrument_model
    instrument_sn = ds.instrument_serial_number

    nominal_depth = ds.variables["NOMINAL_DEPTH"][:]

    fv = 'FV00'
    if file_version == 'Level 1 - Quality Controlled data':
        fv = 'FV01'

    product_code = deployment + "-" + instrument + "-" + instrument_sn + "-" + "{0:.0f}".format(np.abs(nominal_depth)) + "m"
    product_code = product_code.replace(" ", "-")

    codes = []
    for v in ds_variables:
        try:
            codes.append(nameCode[v])
        except KeyError:
            pass

    codes = set(codes)
    codes = sorted(codes)
    codesCat = ""
    for c in codes:
        codesCat += c

    if len(codesCat) == 0:
        codesCat = "R"

    ds.close()

    folder = os.path.dirname(netCDFfile)

    imosName = os.path.join(folder, \
               'IMOS_' + facility + \
               "_" + codesCat + \
               "_" + time_start.strftime("%Y%m%d") + \
               "_" + platform_code + \
               "_" + fv + \
               "_" + product_code + \
               "_END-" + time_end.strftime("%Y%m%d") + \
               "_C-" + creation_date.strftime("%Y%m%d") + \
               ".nc")

    print(imosName)

    # rename the file, maybe should be copy

    os.rename(netCDFfile, imosName)

    return imosName


if __name__ == "__main__":
    rename(sys.argv[1])


