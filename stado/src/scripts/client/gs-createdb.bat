@echo off
rem ###########################################################################
rem Copyright (C) 2010 EnterpriseDB Corporation.
rem Copyright (C) 2011 Stado Global Development Group.
rem 
rem
rem gs-createdb.bat
rem
rem Creates an Stado database
rem
rem ###########################################################################

set EXECCLASS=org.postgresql.stado.util.CreateDb

set GSCONFIG=..\config\stado.config

java -classpath ..\lib\stado.jar;..\lib\log4j.jar;%CLASSPATH% -Dconfig.file.path=%GSCONFIG% %EXECCLASS% %*%

