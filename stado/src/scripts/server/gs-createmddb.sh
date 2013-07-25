#!/bin/sh
##########################################################################
#
# Copyright (C) 2010 EnterpriseDB Corporation.
# Copyright (C) 2011 Stado Global Development Group.
#
# gs-createmddb.sh
#
#
# Creates the Stado metadata database
#
##########################################################################

EXECCLASS=org.postgresql.stado.util.CreateMdDb

DIRNAME=`dirname $0`

GSCONFIG=../config/stado.config

java -classpath ../lib/stado.jar:../lib/log4j.jar:../lib/postgresql.jar:${CLASSPATH} -Dconfig.file.path=${GSCONFIG} $EXECCLASS $* 

