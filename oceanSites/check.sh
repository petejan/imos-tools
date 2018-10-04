#!/bin/sh

java -cp NetcdfFormatChecker/RULES:NetcdfFormatChecker/CLASSES:NetcdfFormatChecker/PROPERTIES:NetcdfFormatChecker/LIB/netcdfUI-4.2.jar:NetcdfFormatChecker/LIB/nlog4j-1.2.25.jar:NetcdfFormatChecker/LIB/xercesImpl.jar -Dapplication.properties=application.properties oco.FormatControl $1
