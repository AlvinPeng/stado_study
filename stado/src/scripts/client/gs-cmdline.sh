#!/bin/sh
##########################################################################
#
# Copyright (C) 2010 EnterpriseDB Corporation.
# Copyright (C) 2011 Stado Global Development Group.
#
# gs-cmdline.sh
#
#
# Used for getting a SQL command prompt
#
##########################################################################

EXECCLASS=org.postgresql.stado.util.CmdLine

java -classpath ../lib/stado.jar:../lib/jline-0_9_5.jar:../lib/log4j.jar:../lib/postgresql.jar:${CLASSPATH} $EXECCLASS $* 
