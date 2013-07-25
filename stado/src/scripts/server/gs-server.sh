#!/bin/sh
##########################################################################
#
# Copyright (C) 2010 EnterpriseDB Corporation.
# Copyright (C) 2011 Stado Global Development Group.
#
# gs-server.sh
#
#
# Starts the main Stado server process
#
##########################################################################


EXECCLASS=org.postgresql.stado.util.XdbServer

DIRNAME=`dirname $0`

GSCONFIG=../config/stado.config

# Adjust these if more memory is required
MINMEMORY=512M
MAXMEMORY=512M
STACKSIZE=256K
XDEBUG="-agentlib:jdwp=transport=dt_socket,address=localhost:18000,server=y,suspend=n"

echo "Starting...."

nohup java -classpath ../lib/stado.jar:../lib/log4j.jar:../lib/postgresql.jar:${CLASSPATH} -Xms${MINMEMORY} -Xmx${MAXMEMORY} -Xss${STACKSIZE} -Dconfig.file.path=${GSCONFIG} $EXECCLASS $* > ../log/server.log 2>&1 &

PROCID=$!

sleep 8

ps $PROCID >/dev/null 2>/dev/null
CHECK=$?

if [ "$CHECK" -ne "0" ]
then
	echo "Error starting XDBServer"
	echo " server.log output:"
	cat ../log/server.log
	echo ""
	echo " tail of console.log output:"
	tail -10 ../log/console.log
fi

