@echo off
rem ###########################################################################
rem Copyright (C) 2010 EnterpriseDB Corporation.
rem Copyright (C) 2011 Stado Global Development Group.
rem 
rem
rem gs-server.bat 
rem
rem Starts the main Stado server process
rem
rem ###########################################################################

set GSCONFIG=..\config\stado.config

set EXECCLASS=org.postgresql.stado.util.XdbServer

rem  Adjust these if more memory is required

set MINMEMORY=512M
set MAXMEMORY=512M

java -classpath ..\lib\stado.jar;..\lib\log4j.jar;..\lib\postgresql.jar;%CLASSPATH% -Xms%MINMEMORY% -Xmx%MAXMEMORY% -Dconfig.file.path=%GSCONFIG% %EXECCLASS% %*%

