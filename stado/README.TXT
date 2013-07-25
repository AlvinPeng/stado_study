Stado README

Compiling and executing Stado
-------------------------------
In working with the code, in many IDEs you can simply indicate
the root source directory (apps/stado/src) and add the 
libraries in the lib directory to the project, and everything 
will compile without any problems.

When executing the programs, you can either use the shell script
(or .bat) wrappers. When executing the server in an IDE, it is 
important that it finds the stado.config file. The simplest 
way to do this is to copy apps/stado/src/config/server/stado.config
to another directory like the root project directory, modify the 
properties of the file appropriately depending on your environment,
and set the project properties in your IDE to use the directory
where this file is as the execution directory.

The Makefile found in apps/stado can be used Linux systems, 
and is primarily intended to be used when creating distributables.

There is a separate README in apps/stado/src/org that provides a 
brief overview of the packages. More information about the architecture
can be found at http://www.stado.us