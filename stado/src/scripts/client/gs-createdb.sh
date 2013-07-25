#!/bin/sh
##########################################################################
#
# Copyright (C) 2010 EnterpriseDB Corporation.
# Copyright (C) 2011 Stado Global Development Group.
#
# gs-createdb.sh
#
#
# Creates an Stado database
#
##########################################################################

EXECCLASS=org.postgresql.stado.util.CreateDb

GSCONFIG=../config/stado.config

java -classpath ../lib/stado.jar:../lib/log4j.jar:../lib/postgresql.jar -Dconfig.file.path=${GSCONFIG} $EXECCLASS "$@"

