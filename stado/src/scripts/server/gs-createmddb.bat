@echo off
rem ###########################################################################
rem Copyright (C) 2010 EnterpriseDB Corporation.
rem Copyright (C) 2011 Stado Global Development Group.
rem 
rem
rem gs-createmddb.bat
rem
rem Creates the Stado metadata database
rem
rem ###########################################################################

set EXECCLASS=org.postgresql.stado.util.CreateMdDb

set GSCONFIG=..\config\stado.config

java -classpath ..\lib\stado.jar;..\lib\log4j.jar;..\lib\postgresql.jar;%CLASSPATH% -Dconfig.file.path=%GSCONFIG% %EXECCLASS% %*%

