#!/bin/bash
cd $1
for DIR in `ls` ; do
	if [ -d $DIR ] ; then
		echo $DIR
		rm  $DIR/SDK.sh
		cp $1/SDK.sh $DIR
	fi
done