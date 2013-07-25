#!/bin/sh
##########################################################################
#
# Copyright (C) 2010 EnterpriseDB Corporation.
# Copyright (C) 2011 Stado Global Development Group.
# 
# gs-impex.sh
#
#
# Used for importing and exporting with Stado. 
# If populating with a large amount of data, use XDBLoader instead.
#
##########################################################################

EXECCLASS=org.postgresql.stado.util.XdbImpEx

java -classpath ../lib/stado.jar:../lib/log4j.jar:../lib/postgresql.jar $EXECCLASS "$@"

