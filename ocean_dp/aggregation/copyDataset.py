import re
from datetime import datetime, timedelta
from netCDF4 import num2date, date2num
from netCDF4 import stringtochar
import numpy.ma as ma
import sys
from netCDF4 import Dataset
import numpy
import argparse
import glob

# netcdf file aggregator for the IMOS mooring specific data case
# Pete Jansen 2019-10-02
# Pete 2020-01-21 make into function

# similar more general tool project https://ncagg.readthedocs.io/en/latest/ (does not work on python3 2019-10-01)
# has configurable way of dealing with attributes
# file sets to test against
# http://thredds.aodn.org.au/thredds/catalog/IMOS/ANMN/NRS/NRSKAI/Temperature/catalog.html
# http://thredds.aodn.org.au/thredds/catalog/IMOS/ANMN/NRS/NRSKAI/Biogeochem_profiles/catalog.html
# http://thredds.aodn.org.au/thredds/catalog/IMOS/ABOS/DA/EAC2000/catalog.html

from dateutil.parser import parse


def aggregate(files, varNames):
    # split this into   createCatalog - copy needed information into structure
    #                   createTimeArray (1D, OBS) - from list of structures
    #                   createNewFile
    #                   copyAttributes
    #                   updateAttributes
    #                   copyData

    #
    # createCatalog - copy needed information into structure
    #

    # look over all files, create a time array from all files
    # TODO: maybe delete files here without variables we're not interested in
    # TODO: Create set of variables in all files

    filen = 0
    for path_file in files:

        print("input file %s" % path_file)

        nc = Dataset(path_file, mode="r")

        ncTime = nc.get_variables_by_attributes(standard_name='time')

        time_deployment_start = nc.time_deployment_start
        time_deployment_end = nc.time_deployment_end

        tStart = parse(time_deployment_start)
        tEnd = parse(time_deployment_end)

        tStartnum = date2num(tStart.replace(tzinfo=None), units=ncTime[0].units)
        tEndnum = date2num(tEnd.replace(tzinfo=None), units=ncTime[0].units)

        maTime = ma.array(ncTime[0][:])
        #msk = (maTime < tStartnum) | (maTime > tEndnum)
        #maTime.mask = msk
        maTime.mask = 0

        timeLen = 1
        if len(ncTime[0].shape) > 0:
            timeLen = ncTime[0].shape[0]

        if filen == 0:
            maTimeAll = maTime
            instrumentIndex = ma.ones(timeLen) * filen
        else:
            maTimeAll = ma.append(maTimeAll, maTime)
            instrumentIndex = ma.append(instrumentIndex, ma.ones(timeLen) * filen)

        nc.close()
        filen += 1

    instrumentIndex.mask = maTimeAll.mask  # same mask for instrument index

    idx = maTimeAll.argsort(0)  # sort by time dimension

    #
    # createTimeArray (1D, OBS) - from list of structures
    #

    dsTime = Dataset(files[0], mode="r")

    ncTime = dsTime.get_variables_by_attributes(standard_name='time')

    dates = num2date(maTimeAll[idx].compressed(), units=ncTime[0].units, calendar=ncTime[0].calendar)

    #
    # createNewFile
    #

    # create a new filename
    # IMOS_<Facility-Code>_<Data-Code>_<Start-date>_<Platform-Code>_FV<File-Version>_ <Product-Type>_END-<End-date>_C-<Creation_date>_<PARTX>.nc

    # TODO: what to do with <Data-Code> with a reduced number of variables

    splitPath = files[0].split("/")
    splitParts = splitPath[-1].split("_") # get the last path item (the file nanme), split by _

    tStartMaksed = num2date(maTimeAll[idx].compressed()[0], units=ncTime[0].units, calendar=ncTime[0].calendar)
    tEndMaksed = num2date(maTimeAll[idx].compressed()[-1], units=ncTime[0].units, calendar=ncTime[0].calendar)

    fileProductTypeSplit = splitParts[6].split("-")
    fileProductType = fileProductTypeSplit[0]

    # could use the global attribute site_code for the product type

    fileTimeFormat = "%Y%m%d"
    ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"

    outputName = splitParts[0] + "_" + splitParts[1] + "_" + splitParts[2] \
                 + "_" + tStartMaksed.strftime(fileTimeFormat) \
                 + "_" + splitParts[4] \
                 + "_" + "FV02" \
                 + "_" + fileProductType + "-Aggregate-" + varNames[0] \
                 + "_END-" + tEndMaksed.strftime(fileTimeFormat) \
                 + "_C-" + datetime.utcnow().strftime(fileTimeFormat) \
                 + ".nc"

    print("output file : %s" % outputName)

    ncOut = Dataset(outputName, 'w', format='NETCDF4')

    #
    # create additional dimensions needed
    #

    # for d in nc.dimensions:
    #     print("Dimension %s " % d)
    #     ncOut.createDimension(nc.dimensions[d].name, size=nc.dimensions[d].size)
    #

    tDim = ncOut.createDimension("OBS", len(maTimeAll.compressed()))
    iDim = ncOut.createDimension("instrument", len(files))
    strDim = ncOut.createDimension("strlen", 256) # netcdf4 allow variable length strings, should we use them, probably not

    #
    # copyAttributes
    #

    # some of these need re-creating from the combined source data
    globalAttributeBlackList = ['time_coverage_end', 'time_coverage_start',
                                'time_deployment_end', 'time_deployment_start',
                                'compliance_checks_passed', 'compliance_checker_version', 'compliance_checker_imos_version',
                                'date_created',
                                'deployment_code',
                                'geospatial_lat_max',
                                'geospatial_lat_min',
                                'geospatial_lon_max',
                                'geospatial_lon_min',
                                'geospatial_vertical_max',
                                'geospatial_vertical_min',
                                'instrument',
                                'instrument_nominal_depth',
                                'instrument_sample_interval',
                                'instrument_serial_number',
                                'quality_control_log',
                                'history', 'netcdf_version']


    # global attributes
    # TODO: get list of variables, global attributes and dimensions from first pass above
    dsIn = Dataset(files[0], mode='r')
    for a in dsIn.ncattrs():
        if not (a in globalAttributeBlackList):
            #print("Attribute %s value %s" % (a, dsIn.getncattr(a)))
            ncOut.setncattr(a, dsIn.getncattr(a))

    for d in dsIn.dimensions:
        if not(d in 'TIME'):
            ncOut.createDimension(d, dsIn.dimensions[d].size)

    varList = dsIn.variables

    # add the ancillary variables for the ones requested
    for v in dsIn.variables:
        if v in varNames:
            if hasattr(dsIn.variables[v], 'ancillary_variables'):
                varNames.extend(dsIn.variables[v].ancillary_variables.split(" "))

    ncOut.setncattr("data_mode", "A")  # something to indicate its an aggregate

    # TIME variable
    # TODO: get TIME attributes from first pass above
    ncTimesOut = ncOut.createVariable("TIME", ncTime[0].dtype, ("OBS",))

    #  copy TIME variable attributes
    for a in ncTime[0].ncattrs():
        if a not in ('comment',):
            print("TIME Attribute %s value %s" % (a, ncTime[0].getncattr(a)))
            ncTimesOut.setncattr(a, ncTime[0].getncattr(a))

    ncTimesOut[:] = maTimeAll[idx].compressed()

    ncOut.setncattr("time_coverage_start", dates[0].strftime(ncTimeFormat))
    ncOut.setncattr("time_coverage_end", dates[-1].strftime(ncTimeFormat))
    ncOut.setncattr("date_created", datetime.utcnow().strftime(ncTimeFormat))
    ncOut.setncattr("history", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC : Create Aggregate"))

    # instrument index
    indexVarType = "i1"
    if len(files) > 128:
        indexVarType = "i2"
        if len(files) > 32767: # your really keen then
            indexVarType = "i4"

    #
    # create new variables needed
    #

    ncInstrumentIndexVar = ncOut.createVariable("instrument_index", indexVarType, ("OBS",))
    ncInstrumentIndexVar.setncattr("long_name", "which instrument this obs is for")
    ncInstrumentIndexVar.setncattr("instance_dimension", "instrument")
    ncInstrumentIndexVar[:] = instrumentIndex[idx].compressed()

    # create a variable with the source file name
    ncFileNameVar = ncOut.createVariable("source_file", "S1", ("instrument", "strlen"))
    ncFileNameVar.setncattr("long_name", "source file for this instrument")

    ncInstrumentTypeVar = ncOut.createVariable("instrument_type", "S1", ("instrument", "strlen"))
    ncInstrumentTypeVar.setncattr("long_name", "source instrument make, model, serial_number")

    filen = 0
    data = numpy.empty(len(files), dtype="S256")
    instrument = numpy.empty(len(files), dtype="S256")
    for path_file in files:
        data[filen] = path_file
        ncType = Dataset(path_file, mode='r')
        instrument[filen] = ncType.instrument + '-' + ncType.instrument_serial_number
        filen += 1

    ncFileNameVar[:] = stringtochar(data)
    ncInstrumentTypeVar[:] = stringtochar(instrument)

    #
    # create a list of variables needed
    #

    filen = 0

    # variables we want regardless 
    varNames = [varNames]+['LATITUDE', 'LONGITUDE', 'NOMINAL_DEPTH']

    # remove any duplicates
    varNamesOut = set(varNames)

    #
    # copyData
    #

    # copy variable data from all files into output file

    # should we add uncertainty to variables here if they don't have one from a default set

    for v in varNamesOut:
        varOrder = -1
        filen = 0

        if (v != 'TIME') & (v in varList):

            # TODO: need to deal with files that don't have v variable in it
            for path_file in files:
                print("%d : %s file %s" % (filen, v, path_file))

                nc1 = Dataset(path_file, mode="r")

                maVariable = nc1.variables[v][:]
                varDims = varList[v].dimensions
                varOrder = len(varDims)

                if len(varDims) > 0:
                    # need to replace the TIME dimension with the now extended OBS dimension
                    # should we extend this to the CTD case where the variables have a DEPTH dimension and no TIME
                    if varList[v].dimensions[0] == 'TIME':
                        if filen == 0:
                            maVariableAll = maVariable

                            dim = ('OBS',) + varDims[1:len(varDims)]
                            ncVariableOut = ncOut.createVariable(v, varList[v].dtype, dim)
                        else:
                            maVariableAll = ma.append(maVariableAll, maVariable, axis=0) # add new data to end along OBS axis
                    else:
                        if filen == 0:
                            maVariableAll = maVariable
                            maVariableAll.shape = (1,) + maVariable.shape

                            dim = ('instrument',) + varDims[0:len(varDims)]
                            varOrder += 1
                            ncVariableOut = ncOut.createVariable(v, varList[v].dtype, dim)
                        else:
                            vdata = maVariable
                            vdata.shape = (1,) + maVariable.shape
                            maVariableAll = ma.append(maVariableAll, vdata, axis=0)

                else:
                    if filen == 0:
                        maVariableAll = maVariable

                        dim = ('instrument',) + varDims[0:len(varDims)]
                        ncVariableOut = ncOut.createVariable(v, varList[v].dtype, dim)
                    else:
                        maVariableAll = ma.append(maVariableAll, maVariable)

                # copy the variable attributes
                # this is ends up as the super set of all files
                for a in varList[v].ncattrs():
                    if a not in ('comment', '_FillValue') and not re.match(r"calibration.*", a):
                        #print("%s Attribute %s value %s" % (v, a, varList[v].getncattr(a)))
                        ncVariableOut.setncattr(a, varList[v].getncattr(a))

                nc1.close()
                filen += 1

            # write the aggregated data to the output file
            if varOrder == 2:
                maVariableAll.mask = maTimeAll.mask  # apply the time mask
                ncVariableOut[:] = maVariableAll[idx][:].compressed()
            elif varOrder == 1:
                maVariableAll.mask = maTimeAll.mask  # apply the time mask
                ncVariableOut[:] = maVariableAll[idx].compressed()
            elif varOrder == 0:
                ncVariableOut[:] = maVariableAll

                # create the output global attributes
                if hasattr(ncVariableOut, 'standard_name'):
                    if ncVariableOut.standard_name == 'latitude':
                        laMax = maVariableAll.max(0)
                        laMin = maVariableAll.max(0)
                        ncOut.setncattr("geospatial_lat_max", laMax)
                        ncOut.setncattr("geospatial_lat_min", laMin)
                    if ncVariableOut.standard_name == 'longitude':
                        loMax = maVariableAll.max(0)
                        loMin = maVariableAll.max(0)
                        ncOut.setncattr("geospatial_lon_max", loMax)
                        ncOut.setncattr("geospatial_lon_min", loMin)
                    if ncVariableOut.standard_name == 'depth':
                        dMax = maVariableAll.max(0)
                        dMin = maVariableAll.max(0)
                        ncOut.setncattr("geospatial_vertical_max", dMax)
                        ncOut.setncattr("geospatial_vertical_min", dMin)
                        
    dsIn.close()  # we're done with the varList now

    ncOut.close()

    return outputName


def collect_vars_to_agg(files):

    var_list = []

    nc = Dataset(files[0])
    varList = nc.variables

    # default to all variables in first file should no variable be specified
    var_list.extend(varList.keys())
    var_list.remove("TIME")

    nc.close()

    print("collect_vars_to_agg::", var_list)

    return var_list


if __name__ == "__main__":

    files = []
    varToAgg = None # defaults to all in first file

    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument('-v', action='append', dest='var', help='variable to include in output file (defaults to all)')
        parser.add_argument('-f', dest='filelist', help='read file names from file')
        parser.add_argument('file', nargs='*', help='input file name')
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


