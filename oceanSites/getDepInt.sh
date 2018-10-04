#!/bin/sh

echo $1:
ncdump -h $1 | grep "vertical_max\|:time_deployment"
