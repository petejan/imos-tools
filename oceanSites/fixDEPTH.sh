#!/bin/sh

# set -x

ATTED='ncatted -Oh'
RENAME='ncrename -Oh'
GETATT=./getattr.sh

NCOUT=$1

DEPTH=$($GETATT $1 instrument_nominal_depth)
DEPTHF=$(printf "%4.1ff" $DEPTH)

echo DEPTH = $DEPTHF

ncap2 -Oh -s "FOO=$DEPTHF" $NCOUT $NCOUT 
ncecat -Oh -u DEPTH $NCOUT tmp.nc
ncks -Oh --fix_rec_dmn DEPTH tmp.nc $NCOUT 
rm tmp.nc
ncap2 -Oh -s "DEPTH=FOO" $NCOUT $NCOUT 

$ATTED -a standard_name,DEPTH,a,c,depth $NCOUT
$ATTED -a long_name,DEPTH,a,c,"Depth of each measurement" $NCOUT
$ATTED -a positive,DEPTH,a,c,down $NCOUT
$ATTED -a reference_datum,DEPTH,a,c,"sea surface" $NCOUT
$ATTED -a units,DEPTH,a,c,meters $NCOUT
$ATTED -a axis,DEPTH,a,c,Z $NCOUT
$ATTED -a QC_indicator,DEPTH,a,c,A $NCOUT
$ATTED -a QC_indicator_conventions,DEPTH,a,c,"Argo reference table 2a (see http://www.cmar.csiro.au/argo/dmqc/user_doc/QC_flags.html), applied on data in position only (between global attributes time_deployment_start and time_deployment_end)" $NCOUT

mv $NCOUT tmp.nc
ncpdq -Oh -a TIME,HEIGHT_ABOVE_SENSOR,DEPTH tmp.nc $NCOUT
rm tmp.nc

mv $NCOUT tmp.nc
ncks -Oh -x -v FOO tmp.nc $NCOUT
rm tmp.nc
