#!/bin/bash
cd $1
for DIR in `ls` ; do
	if [ -d $DIR ] ; then
		echo $DIR
		rm  -f $DIR/SDK.py
		cp $1/SDK.py $DIR
	fi
done