import csv
import sys
from datetime import datetime

from glob2 import glob
from netCDF4 import Dataset

if __name__ == "__main__":

    # process command line arguments
    files = []
    metadata_file = None
    for f in sys.argv[1:]:
        if f.startswith('-metadata='):
            metadata_file = f.replace('-metadata=', '')
        files.extend(glob(f))
    files.sort()

    # read the metadata file
    dep_codes = []
    if metadata_file:
        with open(metadata_file, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
            headers = next(csv_reader)
            for row in csv_reader:
                dict1 = {key: value for key, value in zip(headers, row)}
                try:
                    if dict1['attribute_name'] == 'deployment_code':
                        dep_codes.append(dict1)
                except KeyError:
                    pass

    # list each files model/serialnumber, and list matching deployment_codes
    for f in files:
        ds = Dataset(f, 'r')
        time_start = datetime.strptime(ds.time_coverage_start, '%Y-%m-%dT%H:%M:%SZ')
        time_end = datetime.strptime(ds.time_coverage_end, '%Y-%m-%dT%H:%M:%SZ')

        dep_list = []
        if metadata_file:
            for d in dep_codes:
                no_match = False
                td = datetime.strptime(d['time_deployment'][0:10], '%Y-%m-%d')
                tr = datetime.strptime(d['time_recovery'][0:10], '%Y-%m-%d')
                if time_end < td:
                    no_match = True
                if time_start > tr:
                    no_match = True
                if d['model'] != ds.instrument_model:
                    no_match = True
                if d['serial_number'] != ds.instrument_serial_number:
                    no_match = True

                if not no_match:
                    dep_list.append(d['value'])

        print_list = ""
        if metadata_file:
            print_list = "** no matching deployment code **"
            if len(dep_list) > 0:
                print_list = str(dep_list)
        # match deployment_code in metadata file
        print("{:s},{:s},{:s},{:s}".format(f, ds.instrument_model, ds.instrument_serial_number, print_list))
