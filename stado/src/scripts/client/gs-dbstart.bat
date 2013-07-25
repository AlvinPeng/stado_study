@echo off
rem ###########################################################################
rem Copyright (C) 2010 EnterpriseDB Corporation.
rem Copyright (C) 2011 Stado Global Development Group.
rem 
rem
rem gs-dbstart.bat
rem
rem
rem Brings a Stado database online
rem
rem ###########################################################################

set EXECCLASS=org.postgresql.stado.util.XdbDbStart

set GSCONFIG=..\config\stado.config

java -classpath ..\lib\stado.jar;..\lib\log4j.jar -Dconfig.file.path=%GSCONFIG% %EXECCLASS% %*%

