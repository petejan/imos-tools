#! /bin/bash
#
# convert netCDF file from IMOS to OceanSITES convention
# (using nco tools: ncks, ncatted and ncrename)
#
# usage: convert.sh infile
#
# 2017-06-28 PJ changed for EAC-2015 data

# set -x

ATTED='ncatted -Oh'
RENAME='ncrename -Oh'
GETATT=./getattr.sh
FN=$1

oldIFS="$IFS"
IFS='_' read -r -a fnarray <<< "$FN"
IFS="$oldIFS"

# create output filename
#PLATFORM=IMOS-$($GETATT $1 platform_code | sed 's/[1-9]$//' )
DEPLOYMENT=$($GETATT $1 deployment_code)
PART=$($GETATT $1 instrument |  sed -n -e 's/^.*,//p' | sed 's/[ ,]/-/g' )-$($GETATT $1 instrument_nominal_depth | sed 's/[ ,]/-/g' )-m

PLATFORM=IMOS-EAC
#DEPLOYMENT=${fnarray[4]}
#PART=${fnarray[6]}

DATAMODE=D
SENSOR=$($GETATT $1 instrument |  sed -n -e 's/^.*,//p')-$($GETATT $1 instrument_serial_number)
ID=OS_${PLATFORM}_${DEPLOYMENT}_${DATAMODE}_${PART}
NCOUT=OS/$ID.nc
HISTORY="$(date -u +'%F %T') Created From $1"

echo Output file: $NCOUT Sensor: ${SENSOR}

# copy input file
cp -v $1 $NCOUT

howmany() { echo $#; }

## attributes to add/overwrite
echo Setting attributes ...
# global
$ATTED  -a data_type,global,o,c,"OceanSITES time-series data" \
        -a format_version,global,o,c,"1.3" \
        -a data_mode,global,o,c,$DATAMODE  \
        -a network,global,o,c,"IMOS"  \
        -a institution_references,global,o,c,"http://www.oceansites.org, http://imos.org.au"  \
        -a id,global,o,c,$ID  \
        -a area,global,o,c,"Pacific Ocean"  \
        -a array,global,o,c,$PLATFORM  \
        -a update_interval,global,o,c,"void"  \
        -a institution,global,o,c,"Commonwealth Scientific and Industrial Research Organisation (CSIRO)" \
        -a source,global,o,c,"subsurface mooring" \
        -a naming_authority,global,o,c,"OceanSITES" \
        -a data_assembly_center,global,o,c,"IMOS" \
        -a citation,global,o,c,"Ocean Sites. [year-of-data-download], [Title], [Data access URL], accessed [date- of-access]" \
        -a Conventions,global,o,c,"CF-1.6 OceanSITES-1.3 NCADD-1.2.1" \
        -a license,global,o,c,"Follows CLIVAR (Climate Variability and Predictability) standards,cf. http://www.clivar.org/data/data_policy.php Data available free of charge. User assumes all risk for use of  data. User must display citation in any publication or product using data. User must contact PI prior to any commercial use of data." \
        -a contributor_name,global,o,c,"CSIRO; IMOS; ACE-CRC; MNF" \
        -a processing_level,global,o,c,"data manually reviewed" \
        -a QC_indicator,global,o,c,"mixed" \
        -a sensor_name,global,o,c,"$SENSOR" \
        -a history,global,o,c,"$HISTORY" \
        -a cdm_data_type,global,o,c,"Station" \
        -a contributor_name,global,o,c,"Peter Jansen" \
        -a contributor_role,global,o,c,"conversion to OceanSITES format" \
        -a contributor_email,global,o,c,"peter.jansen@csiro.au" \
        $NCOUT
        
## attributes to delete

echo Deleting attributes ...
# global
ARG=''
for att in Latitude \
           Longitude \
           quality_control_set \
           product_type \
           field_trip_id \
           field_trip_description \
           level \
           instrument \
           instrument_serial_number \
           file_version ; do
    ARG=$ARG'  -a '$att',global,d,,'
done
$ATTED  $ARG  $NCOUT

# variable
$ATTED  -a quality_control_set,,d,, \
        $NCOUT

## attribute values to change
echo Converting global attributes to string ...
# geospatial_*_min/max convert to string
ARG=''
for att in geospatial_lat_min \
	   geospatial_lon_min \
	   geospatial_lat_max \
	   geospatial_lon_max \
	   geospatial_vertical_min \
	   geospatial_vertical_max; do
    ARG=$ARG'  -a '$att',global,m,c,'$($GETATT $NCOUT $att)
done
$ATTED $ARG $NCOUT

$ATTED -a long_name,LATITUDE,m,c,"Latitude of each location" $NCOUT
$ATTED -a long_name,LONGITUDE,m,c,"Longitude of each location" $NCOUT

## attributes to rename
echo Renaming attributes ...
$RENAME -a .abstract,summary \
        -a .data_centre_email,contact \
        -a .quality_control_conventions,conventions \
        -a .author_email,publisher_email \
        -a .author,publisher \
        $NCOUT

## rename quality variables
echo Rename *_quality_control to _QC

# move quality_control_global_conventions and quality_control_global from the QC variable to the main variable
QC=$(ncks -m $1 | grep _quality_control: | sed 's/:.*//g')
ARG=''
DEL=''
for Q in $QC; do
    new=$(echo $Q | sed 's/_quality_control/_QC/g')
    ARG=$ARG' -v '$Q,$new
    DEL=$DEL' -a quality_control_global_conventions,'$new',d,,'
    DEL=$DEL' -a quality_control_global,'$new',d,,'
done
$RENAME $ARG $NCOUT
$ATTED $DEL $NCOUT

for Q in $QC; do
    VAR=$(echo $Q | sed 's/_quality_control//')
    OLD=$(ncks -m $1 | grep $Q |  grep quality_control_global_conventions, | sed 's/.*value = //g')
    ADD1='QC_indicator_conventions,'$VAR',c,c,'$OLD
    ADD2='QC_indicator,'$VAR',c,c,'$(ncks -m $1 | grep $Q |  grep quality_control_global, | sed 's/.*value = //g')''
    $ATTED -a "$ADD1" -a $ADD2 $NCOUT
done

# rename any ancillary_variable attributes
QC=$(ncks -m $1 | grep ancillary_variables | sed 's/ .*//g')
echo QC : $QC
ARG=''
for Q in $QC; do
    new=$(ncks -m $1 | grep $Q | grep ancillary_variables | sed 's/.* value = //' | sed 's/_quality_control/_QC/g')
    ARG=$ARG' -a ancillary_variables,'$Q',m,c,'$new
done
$ATTED $ARG $NCOUT

#remove the TIME_QC, LONGITUDE_QC, LATITUDE_QC, HEIGHT_ABOVE_SENSOR_QC variables
#echo remove the TIME_QC, LONGITUDE_QC, LATITUDE_QC, HEIGHT_ABOVE_SENSOR_QC variables

#mv $NCOUT tmp.nc
#ncks -Oh -x -v TIME_QC,LONGITUDE_QC,LATITUDE_QC,HEIGHT_ABOVE_SENSOR_QC,HEIGHT_BELOW_SENSOR_QC tmp.nc $NCOUT
#rm tmp.nc
#$ATTED -a ancillary_variables,TIME,d,, $NCOUT
#$ATTED -a ancillary_variables,LONGITUDE,d,, $NCOUT 
#$ATTED -a ancillary_variables,LATITUDE,d,, $NCOUT
#V=$(ncks -m $NCOUT | grep "^HEIGHT_ABOVE_SENSOR. ")
#if [ $(howmany $V) -ge 1 ] ; then 
#    $ATTED -a ancillary_variables,HEIGHT_ABOVE_SENSOR,d,, $NCOUT
#fi

#echo "Fixup some units, Decibel, add count to CMAG*, time units"

# change the Decibels to dBel

#DBVAR=$(ncks -m $NCOUT | grep Decibel | sed 's/ .*//g')
#if [ $(howmany $DBVAR) -ge 1 ] ; then 
#    ARG=''
#    for v in $DBVAR; do
#        ARG=$ARG' -a units,'$v',m,c,dB'
#    done
#    $ATTED $ARG $NCOUT
#fi

# fix CMAG units -> counts, HEADING units -> degrees_true, TIME -> days since 1950-01-01T00:00:00Z, LONGITUDE:axis -> Z
#V=$(ncks -m $NCOUT | grep "^CMAG. ")
#if [ $(howmany $V) -ge 1 ] ; then 
#    $ATTED -a units,CMAG1,a,c,counts $NCOUT
#    $ATTED -a units,CMAG2,a,c,counts $NCOUT
#    $ATTED -a units,CMAG3,a,c,counts $NCOUT
#    $ATTED -a units,CMAG4,a,c,counts $NCOUT
#fi
V=$(ncks -m $NCOUT | grep "^HEADING ")
if [ $(howmany $V) -ge 1 ] ; then 
    $ATTED -a units,HEADING,m,c,degrees_true $NCOUT
fi
$ATTED -a units,TIME,m,c,"days since 1950-01-01T00:00:00Z" $NCOUT
$ATTED -a axis,LONGITUDE,m,c,"X" $NCOUT
$ATTED -a units,PSAL,m,c,"1" $NCOUT

## create a DEPTH dimension
#echo "Creating DEPTH DIMENSION"

# first rename DEPTH as ZPOS

#$RENAME -v DEPTH,ZPOS $NCOUT
#$ATTED -a ancillary_variables,ZPOS,m,c,ZPOS_QC $NCOUT
#$RENAME -v DEPTH_QC,ZPOS_QC $NCOUT

#./fixDEPTH.sh $NCOUT

#./checkNCfile.sh $NCOUT
