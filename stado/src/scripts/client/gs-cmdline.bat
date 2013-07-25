@echo off
rem ###########################################################################
rem Copyright (C) 2010 EnterpriseDB Corporation.
rem Copyright (C) 2011 Stado Global Development Group.
rem 
rem
rem gs-cmdline.bat
rem
rem
rem Used for getting a SQL command prompt
rem
rem ###########################################################################

set EXECCLASS=org.postgresql.stado.util.CmdLine

java -classpath ..\lib\stado.jar;..\lib\jline-0.9.5.jar;..\lib\log4j.jar %EXECCLASS% %*%
