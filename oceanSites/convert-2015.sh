#! /bin/bash
#
# convert netCDF file from IMOS to OceanSITES convention
# (using nco tools: ncks, ncatted and ncrename)
#
# usage: convert.sh infile
#
# 2017-06-28 PJ changed for EAC-2015 data
# 2017-08-22 PJ re-write for new OceanSITES checker

# set -x

ATTED='ncatted -Oh'
RENAME='ncrename -Oh'
GETATT=./getattr.sh

FN=$1

oldIFS="$IFS"
IFS='_' read -r -a fnarray <<< "$FN"
IFS="$oldIFS"

# create output filename
DEPLOYMENT=$($GETATT $1 deployment_code)
PART=$($GETATT $1 instrument |  sed -n -e 's/^.*,//p' | sed 's/[ ,]/-/g' )-$($GETATT $1 instrument_nominal_depth | sed 's/[ ,]/-/g' )-m

PLATFORM=IMOS-EAC

DATAMODE=D
SENSOR=$($GETATT $1 instrument |  sed -n -e 's/^.*,//p')-$($GETATT $1 instrument_serial_number)
ID=OS_${PLATFORM}_${DEPLOYMENT}_${DATAMODE}_${PART}
NCOUT=OS/$ID.nc
HISTORY="$(date -u +'%F %T') Created From $1"
MOD_DATE="$(date -u +'%FT%TZ')"

echo Output file: $NCOUT Sensor: ${SENSOR} Part: ${PART} Deployment: ${DEPLOYMENT}

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
        -a date_modified,global,o,c,"$MOD_DATE" \
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

# TIME units
$ATTED -a units,TIME,m,c,"days since 1950-01-01T00:00:00Z" $NCOUT

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

# rename any ancillary_variable attributes
QC=$(ncks -m $1 | grep ancillary_variables | sed 's/ .*//g')
echo QC : $QC
ARG=''
for Q in $QC; do
    new=$(ncks -m $1 | grep $Q | grep ancillary_variables | sed 's/.* value = //' | sed 's/_quality_control/_QC/g')
    ARG=$ARG' -a ancillary_variables,'$Q',m,c,'$new
done
$ATTED $ARG $NCOUT

## create a DEPTH dimension
echo "Creating DEPTH DIMENSION"

# first rename DEPTH as ZPOS

$RENAME -v DEPTH,ZPOS $NCOUT
$ATTED -a ancillary_variables,ZPOS,m,c,ZPOS_QC $NCOUT
$RENAME -v DEPTH_QC,ZPOS_QC $NCOUT

# rename NOMINAL_DEPTH as DEPTH

echo " rename NOMINAL_DEPTH as DEPTH"
$RENAME -v NOMINAL_DEPTH,DEPTH $NCOUT

echo " create new DIMENSIONS"
$ATTED -a units,DEPTH,m,c,meters $NCOUT
$ATTED -a long_name,DEPTH,m,c,"Depth of each measurement" $NCOUT
$ATTED -a long_name,LONGITUDE,m,c,"Longitude of each location" $NCOUT
$ATTED -a long_name,LATITUDE,m,c,"Latitude of each location" $NCOUT

ncap2 -Oh -s "defdim(\"DEPTH\",1);defdim(\"LATITUDE\",1);defdim(\"LONGITUDE\",1);DEPTH[\$DEPTH]=DEPTH;LATITUDE[\$LATITUDE]=LATITUDE;LONGITUDE[\$LONGITUDE]=LONGITUDE" $NCOUT $NCOUT

echo "Add DEPTH as a dimension to variables"

# probably a way of doing this with ncap2 scripts directly
HASd=$(ncks -m $NCOUT | grep "dimension 1: HEIGHT_ABOVE_SENSOR" | sed 's/ .*//g' | sort -r )
DABd=$(ncks -m $NCOUT | grep "dimension 1: DIST_ALONG_BEAMS" | sed 's/ .*//g' | sort -r )

echo "HEIGHT_ABOVE_SENSOR : " $HASd
for Q in $HASd; do
    echo "  processing : " $Q
    ncap2 -Oh -s "*new[\$TIME,\$DEPTH,\$HEIGHT_ABOVE_SENSOR]=$Q; *new=$Q; new.ram_write(); $Q=new" $NCOUT $NCOUT
    ncks -Oh -x -v new $NCOUT $NCOUT
done
echo "DIST_ALONG_BEAMS : " $DABd
for Q in $DABd; do
    echo "  processing : " $Q
    ncap2 -Oh -s "*new[\$TIME,\$DEPTH,\$DIST_ALONG_BEAMS]=$Q; *new=$Q; new.ram_write(); $Q=new" $NCOUT $NCOUT
    ncks -Oh -x -v new $NCOUT $NCOUT
done

# with a script it might be possiable to do all this at once
TIMEd=$(ncks -m $NCOUT | grep "dimension 0: TIME" | sed 's/ .*//g' | sort -r | sed 's/TIME//g')
echo TIMEd : $TIMEd
for Q in $TIMEd; do
    found=0;
    for D in $HASd ; do if [ $D == $Q ]; then found=1; fi; done
    for D in $DABd ; do if [ $D == $Q ]; then found=1; fi; done
    echo "  processing : " $Q " already done " $found
    if [ $found == 0 ]; then
      ncap2 -Oh -s "*new[\$TIME,\$DEPTH]=$Q; *new=$Q; new.ram_write(); $Q=new" $NCOUT $NCOUT
      ncks -Oh -x -v new $NCOUT $NCOUT
    fi
done

echo "set QC flag meanings"

QC=$(ncks -m $NCOUT | grep _QC: | sed 's/: .*//g')
echo QC : $QC
for Q in $QC; do
#    $ATTED -a _FillValue,$Q,c,b,99 $NCOUT
    $ATTED -a flag_meanings,$Q,m,c,"unknown good_data probably_good_data potentially_correctable_bad_data bad_data nominal_value interpolated_value missing_value" $NCOUT
done

#ncap2 -Oh -s "TEMP[\$TIME,\$DEPTH]=TEMP" $NCOUT $NCOUT
#$ATTED -a _FillValue,TEMP,o,f,999999 $NCOUT


