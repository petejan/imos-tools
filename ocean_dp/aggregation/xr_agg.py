
import sys
from netCDF4 import Dataset
import numpy
import argparse
import glob
import xarray as xr


def collect_vars_to_agg(files):
    return ["TEMP"]


nc_global_atts = []
nc_vars = []


def get_meta_data(nc_files):

    for path_file in nc_files:
        nc = Dataset(path_file, mode="r")

        file_globals = {}
        for file_attrs in nc.ncattrs():
            file_globals = {"name": file_attrs, "value": nc.getncattr(file_attrs)}

        nc_global_atts.append(file_globals)

        nc.close()

    return


def aggregate(files_list, vars_list):
    get_meta_data(files_list)

    def read_netcdfs(files, dim, transform_func=None):
        def process_one_path(path):
            # use a context manager, to ensure the file gets closed after use
            with xr.open_dataset(path) as ds:
                # transform_func should do some sort of selection or
                # aggregation
                if transform_func is not None:
                    ds = transform_func(ds)
                # load all data from the transformed dataset, to ensure we can
                # use it after closing each original file
                ds.load()

                print("file", ds.info())
                return ds

        paths = sorted(files)
        datasets = [process_one_path(p) for p in paths]
        combined = xr.concat(datasets, dim)
        return combined

    combined = read_netcdfs(files_list, dim='OBS')

    print(combined.info())

    combined.to_netcdf("file-name.nc")

    return ["file-name.nc"]


if __name__ == "__main__":

    files = []
    varToAgg = None # defaults to all in first file

    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument('-v', action='append', dest='var', help='variable to include in output file (defaults to all)')
        parser.add_argument('-f', dest='filelist', help='read file names from file')
        parser.add_argument('file', nargs='*', help='input file name(s)')
        args = parser.parse_args()

        if not isinstance(args.filelist, type(None)):
            with open(args.filelist, "r") as ins:
                for line in ins:
                    print(line)
                    files.append(line.strip())

        if len(args.file):
            # files = args.file
            for fn in args.file:
                files.extend(glob.glob(fn))

        varToAgg = args.var

    if isinstance(varToAgg, type(None)):
        varToAgg = collect_vars_to_agg(files)

    print("Aggregating variables ", varToAgg)

    outputName = aggregate(files, varToAgg)

    print("Output file :  %s" % outputName)
