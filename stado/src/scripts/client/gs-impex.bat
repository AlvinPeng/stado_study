@echo off
rem ###########################################################################
rem Copyright (C) 2010 EnterpriseDB Corporation.
rem Copyright (C) 2011 Stado Global Development Group.
rem 
rem
rem gs-impex.bat
rem
rem
rem Used for importing and exporting with Stado. 
rem If populating with a large amount of data, use XDBLoader instead.
rem
rem ###########################################################################

set EXECCLASS=org.postgresql.stado.util.XdbImpEx

java -classpath ..\lib\stado.jar;..\lib\log4j.jar;%CLASSPATH% %EXECCLASS% %*%
