from datetime import datetime, timedelta, UTC
from netCDF4 import num2date, date2num
import numpy.ma as ma
from netCDF4 import Dataset
import argparse
import re
import numpy

# IMOS file format conversion to OceanSITES format
# Pete Jansen 2018-10-09

from dateutil.parser import parse


# sort the attributes alphabetcially
def sort_attributes(outputName):

    ds = Dataset(outputName, 'a')

    attrs = ds.ncattrs()

    # get all attributes into a list
    list_of_values = []
    for item in attrs:
        list_of_values.append(ds.getncattr(item))

    di = dict(zip(attrs, list_of_values))
    # delete all attributes, this allows for them to be added again in sorted order

    for item in attrs:
        ds.delncattr(item)

    # print (di)

    # not re-add the attributes in sort order
    for att in sorted(attrs, key=str.lower):  # or  key=lambda s: s.lower()
        value = di[att]
        if type(value) is str:
            value = value.format(**di)
        ds.setncattr(att, value)

        # print("attr : ", att, " = " , value)

    ds.close()


parser = argparse.ArgumentParser()
parser.add_argument('file', help='input file name')
args = parser.parse_args()

path_file = args.file

# split this into   createCatalog - copy needed information into structure
#                   createTimeArray (1D, OBS) - from list of structures
#                   createNewFile
#                   copyAttributes
#                   updateAttributes
#                   copyData

#
# createCatalog - copy needed information into structure
#

print("input file %s" % path_file)

ds_in = Dataset(path_file, mode="r")

ncTime = ds_in.get_variables_by_attributes(standard_name='time')

if not ncTime:
    print("time variable not found")
    # exit(-1)
    ncTime = {}
    ncTime[0] = ds_in.variables["TIME"]

time_deployment_start = ds_in.time_deployment_start
time_deployment_end = ds_in.time_deployment_end

tStart = parse(time_deployment_start, ignoretz=True)
tEnd = parse(time_deployment_end, ignoretz=True)

tStartnum = date2num(tStart, units=ncTime[0].units, calendar=ncTime[0].calendar)
tEndnum = date2num(tEnd, units=ncTime[0].units, calendar=ncTime[0].calendar)

maTime = ma.array(ncTime[0][:])
msk = (maTime < tStartnum) | (maTime > tEndnum)
maTime.mask = msk

dates = num2date(maTime.compressed(), units=ncTime[0].units, calendar=ncTime[0].calendar)

# create a new filename
# from:
# IMOS_<Facility-Code>_<Data-Code>_<Start-date>_<Platform-Code>_FV<File-Version>_ <Product-Type>_END-<End-date>_C-<Creation_date>_<PARTX>.nc
# to:
# OS_[PlatformCode]_[DeploymentCode]_[DataMode]_[PARTX].nc

# TODO: what to do with <Data-Code> with a reduced number of variables

splitPath = path_file.split("/")
fileName = splitPath[-1]
splitParts = fileName.split("_") # get the last path item (the file nanme), split by _

tStartMaksed = dates[0]
tEndMaksed = dates[-1]

fileProductTypeSplit = splitParts[6].split("-")
fileProductType = fileProductTypeSplit[0]

# could use the global attribute site_code for the product type

fileTimeFormat = "%Y%m%d"
ncTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
sensor = fileProductTypeSplit[3]
nominal_depth = fileProductTypeSplit[7]
original_file_creation_date = splitParts[-1].split(".")

# Generate OceanSITES file name
outputName = "OS" \
             + "_" + "SOTS" \
             + "_" + fileProductType + "-" + fileProductTypeSplit[1] \
             + "_D" \
             + "_" + sensor + "-" + nominal_depth + "_" + original_file_creation_date[0]  \
             + ".nc"

print("output file : %s" % outputName)

ncOut = Dataset(outputName, 'w', format='NETCDF4_CLASSIC')

#
# copyAttributes
#

# some of these need re-creating from the combined source data
globalAttributeBlackList = ['time_coverage_end', 'time_coverage_start',
                            'time_deployment_end', 'time_deployment_start',
                            'compliance_checks_passed', 'compliance_checker_version', 'compliance_checker_imos_version',
                            'date_created', 'Mooring', 'project',
                            'deployment_code', 'Metadata_Conventions',
                            'instrument', 'disclaimer', 'distribution_statement',
                            'instrument_nominal_depth', 'standard_name_vocabulary',
                            'instrument_sample_interval',
                            'instrument_serial_number', 'institution', 'institution_address',
                            'quality_control_log', 'citation', 'contributor_role', 'data_centre', 'data_centre_email',
                            'history', 'acknowledgement', 'abstract', 'author', 'author_email', 'file_version'
                            'references', 'voyage_deployment', 'voyage_recovery']

# these attributes are actually part of the OceanSites list, so I took them off the BlackList
#, 'netcdf_version','geospatial_lat_max', 'geospatial_lat_min', 'geospatial_lon_max', 'geospatial_lon_min',
# 'geospatial_vertical_max', 'geospatial_vertical_min',]

# global attributes

for a in ds_in.ncattrs():
    if not (a in globalAttributeBlackList):
        print("Attribute %s value %s" % (a, ds_in.getncattr(a)))
        ncOut.setncattr(a, ds_in.getncattr(a))

# copy dimensions

for d in ds_in.dimensions:
    size = ds_in.dimensions[d].size
    if d == 'TIME':
        size = dates.shape[0]
    print("dimension", d, " shape ", size)
    ncOut.createDimension(d, size)

ncOut.createDimension('DEPTH', 1)
ncOut.createDimension('LATITUDE', 1)
ncOut.createDimension('LONGITUDE', 1)

history = ds_in.getncattr("history")
instrumentName = ds_in.getncattr("instrument")
serialNumber = ds_in.getncattr("instrument_serial_number")
deployment_start = ds_in.getncattr("time_deployment_start")
deployment_end = ds_in.getncattr("time_deployment_end")
deployment_voyage = ds_in.getncattr("voyage_deployment")
recovery_voyage = ds_in.getncattr("voyage_recovery")

# work out the cruise/voyage info from the voyage attributes
deployment_splitparts = deployment_voyage.split("=")
recovery_splitparts = recovery_voyage.split("=")

# cruise name
if len(deployment_splitparts) < 1:
    platform_deployment_cruise_name = deployment_voyage
else:
    platform_deployment_cruise_name = deployment_splitparts[-1]
    
if len(recovery_splitparts) < 1:
    platform_recovery_cruise_name = recovery_voyage
else:
    platform_recovery_cruise_name = recovery_splitparts[-1]

# ship name
if platform_deployment_cruise_name[:2]  == "IN":
    deployment_ship = "RV Investigator"
elif platform_deployment_cruise_name[:2]  == "SS":
    deployment_ship = "RV Southern Surveyor"
else:
    deployment_ship = "RV Aurora Australis"

if platform_recovery_cruise_name[:2] == "IN":
    recovery_ship = "RV Investigator"
elif platform_recovery_cruise_name[:2] == "SS":
    recovery_ship = "RV Southern Surveyor"
else:
    recovery_ship = "RV Aurora Australis"

# ICES names
if deployment_ship == "RV Aurora Australis":
    ICES_deployment = "09AR"
elif deployment_ship == "RV Southern Surveyor":
    ICES_deployment = "09SS"
elif deployment_ship == "RV Investigator":
    ICES_deployment = "096U"

if recovery_ship == "RV Aurora Australis":
    ICES_recovery = "09AR"
elif recovery_ship == "RV Southern Surveyor":
    ICES_recovery = "09SS"
elif recovery_ship == "RV Investigator":
    ICES_recovery = "096U"

# create the oceanSITES global attributes
ncOut.setncattr("time_coverage_start", dates[0].strftime(ncTimeFormat))
ncOut.setncattr("time_coverage_end", dates[-1].strftime(ncTimeFormat))
ncOut.setncattr("date_created", datetime.now(UTC).strftime(ncTimeFormat))
ncOut.setncattr("history", history + '\n' + datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC : Convert from IMOS file : ") + fileName)
ncOut.setncattr("acknowledgement", "We acknowledge support from the following agencies: the Australian Antarctic Program Partnership (AAPP), the Antarctic Climate and Ecosystems Cooperative Research Centre (ACE CRC), the Integrated Marine Observing System (www.imos.org.au), University of Tasmania (UTAS), Bureau of Meteorology (BoM), the Marine National Facility (MNF) and the Australian Antarctic Division (AAD). We also acknowledge the support of the CSIRO Moored Sensor Systems team.")
ncOut.setncattr("data_type", "OceanSITES time-series data")
ncOut.setncattr("format_version", "1.3")
ncOut.setncattr("network", "IMOS")
ncOut.setncattr("theme", "Global Ocean Watch")
ncOut.setncattr("summary", "Particle flux data from the Southern Ocean Time Series observatory in the Southern Ocean southwest of Tasmania.")
ncOut.setncattr("id", outputName)
ncOut.setncattr("sea_area", "Pacific Ocean")
ncOut.setncattr("array", "SOTS")
ncOut.setncattr("update_interval", "void")
ncOut.setncattr("creator_institution", "Commonwealth Scientific and Industrial Research Organisation (CSIRO)")
ncOut.setncattr("source", "subsurface mooring")
ncOut.setncattr("naming_authority", "OceanSITES")
ncOut.setncattr("data_assembly_center", "Australian Ocean Data Network (AODN)")
ncOut.setncattr("citation", "Any users of IMOS data are required to clearly acknowledge the source of the material derived from IMOS in the format: "
                            "\"Data was sourced from the Australian Integrated Marine Observing System (IMOS). IMOS is enabled by the National Collaborative Research Infrastructure Strategy (NCRIS). "
                            "It is operated by a consortium of institutions as an unincorporated joint venture, with the University of Tasmania as Lead Agent.\" "
                            "as well as \"PI Elizabeth Shadwick SOTS - SAZ. These data were collected and made freely "
                            "available by the OceanSITES program and the national programs that contribute to it. "
                            "[year-of-data-download], [Title], [Data access URL], accessed [date- of-access].\"")
ncOut.setncattr("Conventions", "CF-1.6 OceanSITES-1.3 NCADD-1.2.1")
ncOut.setncattr("license", "Follows CLIVAR (Climate Variability and Predictability) standards,cf. http://www.clivar.org/data/data_policy.php Data available free of charge. User assumes all risk for use of  data. User must display citation in any publication or product using data. User must contact PI prior to any commercial use of data.")
ncOut.setncattr("contributor_name", "CSIRO; IMOS; ACE-CRC; MNF; AAPP")
ncOut.setncattr("processing_level", "data manually reviewed")
# TODO: can we say "excellent" for the SAZ data since it is QCed?
ncOut.setncattr("QC_indicator", "excellent")
ncOut.setncattr("instrument", instrumentName + "-" + serialNumber)
ncOut.setncattr("cdm_data_type", "Station")
ncOut.setncattr("platform_deployment_date", deployment_start)
ncOut.setncattr("platform_recovery_date", deployment_end)
ncOut.setncattr("creator_name", "Cathryn Wynn-Edwards")
ncOut.setncattr("creator_email", "cathryn.wynn-edwards@csiro.au")
ncOut.setncattr("creator_type", "person")
ncOut.setncattr("publisher_name", "Peter Jansen")
ncOut.setncattr("publisher_email", "peter.jansen@csiro.au")
ncOut.setncattr("netcdf_version", "netcdf-4 classic")
ncOut.setncattr("keywords_vocabulary", "GCMD Science Keywords")
ncOut.setncattr("keywords", "PARTICLE FLUX, CARBON, SEDIMENT COMPOSITION, INORGANIC CARBON,"
                            "MARINE GEOCHEMISTRY, ORGANIC CARBON, ORGANIC MATTER, SILICATE, CARBONATE")
ncOut.setncattr("platform_deployment_ship_name", deployment_ship)
ncOut.setncattr("platform_deployment_cruise_name", platform_deployment_cruise_name)
ncOut.setncattr("platform_recovery_ship_name", recovery_ship)
ncOut.setncattr("platform_recovery_cruise_name", platform_recovery_cruise_name)
ncOut.setncattr("references", "http://www.imos.org.au, data QC procedure document: http://dx.doi.org/10.26198/5dfad21358a8d, http://www.oceansites.org/")
ncOut.setncattr("platform_deployment_ship_ICES", ICES_deployment)
ncOut.setncattr("platform_recovery_ship_ICES", ICES_recovery)
ncOut.setncattr("platform_recovery_voyage_url", recovery_voyage)
ncOut.setncattr("platform_deployment_voyage_url", deployment_voyage)


# copyData
#

# copy variable data from input files into output file

varList = list(ds_in.variables)

varList.remove("NOMINAL_DEPTH")
varList.remove("LATITUDE")
varList.remove("LONGITUDE")

var_depth = ncOut.createVariable("DEPTH", 'f4', 'DEPTH')
var_depth.standard_name = 'depth'
var_depth.long_name = 'Depth of each measurement'
var_depth.comment = 'nominal depth'
var_depth.units = 'meters'
var_depth.axis = 'Z'
var_depth.valid_min = -5
var_depth.valid_max = 12000
var_depth.positive = "down"
var_depth.reference_datum = "sea surface"
var_depth[:] = ds_in.variables['NOMINAL_DEPTH'][:]

var_lat = ncOut.createVariable("LATITUDE", 'f4', 'LATITUDE')
var_lat.standard_name = 'latitude'
var_lat.long_name = 'Latitude of each location'
var_lat.units = 'degrees_north'
var_lat.axis = 'Y'
var_lat.valid_min = -90
var_lat.valid_max = -90
var_lat_reference_datum = "WGS84 coordinate reference system"
var_lat.coordinate_reference_frame = 'urn:ogc:crs:EPSG::4326'
var_lat[:] = ds_in.variables['LATITUDE'][:]

var_lon = ncOut.createVariable("LONGITUDE", 'f4', 'LONGITUDE')
var_lon.standard_name = 'longitude'
var_lon.long_name = 'Longitude of each location'
var_lon.units = 'degrees_east'
var_lon.axis = 'X'
var_lon.valid_min = -90
var_lon.valid_max = -90
var_lon_reference_datum = "WGS84 coordinate reference system"
var_lon.coordinate_reference_frame = 'urn:ogc:crs:EPSG::4326'
var_lon[:] = ds_in.variables['LONGITUDE'][:]

for v in varList:
    print("processing variable : %s from file %s" % (v, path_file))

    var_in = ds_in.variables[v]

    maVariable = var_in[:]  # get the data
    varDims = var_in.dimensions
    var_out = maVariable
    if 'TIME' in varDims:
        print("its a time variable shape ", var_out.shape, "dims", varDims, "len shape", len(var_out.shape))
        if varDims[0] != 'TIME':
            var_out = maVariable[:, ~msk]
            print("mask time dimension")
        elif len(var_out.shape) > 2:
            var_out = maVariable[~msk, :]
        else:
            var_out = maVariable[~msk]
    else:
        var_out = maVariable

    print("var out shape ", var_out.shape)

    # rename the _quality_control variables _QC
    varnameOut = re.sub("_quality_control", "_QC", v)

    fill = None
    try:
        if v.endswith("_quality_control"):
            fill = numpy.int8(-128)
        else:
            fill = var_in._FillValue
    except:
        pass

    print('variable fill value', var_in.dtype, fill)

    ncVariableOut = ncOut.createVariable(varnameOut, var_in.dtype, varDims, fill_value=fill)
    print("netCDF variable out shape", ncVariableOut.shape, "dims", varDims)
    # copy the variable attributes
    for a in var_in.ncattrs():
        if a != '_FillValue':
            print("%s Attribute %s = %s" % (varnameOut, a, var_in.getncattr(a)))
            attValue = var_in.getncattr(a)

            # rename ancilliary variable names
            if (a == 'ancillary_variables') & isinstance(attValue, str):
               attValue = re.sub("_quality_control", "_QC", var_in.getncattr(a))

            # make flag_meanings oceanSITES compliant
            if (a == 'flag_meanings') & isinstance(attValue, str):
               attValue = re.sub("unknown good_data probably_good_data probably_bad_data bad_data missing_value",
                                  "unknown good_data probably_good_data potentially_correctable_bad_data bad_data nominal_value interpolated_value missing_value",
                                  var_in.getncattr(a))
            if a == 'flag_values':
                attValue = numpy.int8([0, 1, 2, 3, 4, 7, 8, 9])
            ncVariableOut.setncattr(a, attValue)

    # oceanSITES insist on time units timezone as Z not UTC
    if (v == 'TIME') | (v == 'TIME_bnds'):
        ncVariableOut.setncattr('units', var_in.getncattr('units').replace(" UTC", "Z"))

    # oceanSITES stores instrument (called sensor in oceanSITES) info in each variable
    if ~(v.endswith("_QC") | v.endswith("_uncertainty")):
        ncVariableOut.sensor_model = instrumentName
        ncVariableOut.sensor_serial_number = serialNumber

    ncVariableOut[:] = var_out

    # update the history attribute
    try:
        hist = ds_in.history + "\n"
    except AttributeError:
        hist = ""

    ncOut.setncattr('history', hist + datetime.now(UTC).strftime("%Y-%m-%d") + " : converted to oceanSITES format from file " + path_file)


ds_in.close()

ncOut.close()

sort_attributes(outputName)

#return ncOut
