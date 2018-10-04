#! /bin/bash
#
# return the value of a netCDF attribute 
# (using ncks, grep and sed)
#
# getattr.sh file attname

ncks -M $1 | grep ": $2," | sed 's/.*value = //'
