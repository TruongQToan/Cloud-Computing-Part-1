#!/bin/sh
joinfrom=`grep joined dbg.log | cut -d" " -f2 | sort -u`
for i in $joinfrom
do
    echo $joinfrom
    jointo=`grep joined dbg.log | grep '^ '$i`
    echo $jointo
done
