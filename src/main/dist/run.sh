#!/usr/bin/env bash
#
# load external database ids for RatGtex
#
. /etc/profile
APPNAME="ratgtex-pipeline"
APPDIR=/home/rgddata/pipelines/$APPNAME
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST="akundurthi@mcw.edu mtutaj@mcw.edu"

cd $APPDIR
java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -jar lib/$APPNAME.jar "$@" > run.log 2>&1

mailx -s "[$SERVER] RatGTex Pipeline Run" $EMAIL_LIST < $APPDIR/logs/summary.log
