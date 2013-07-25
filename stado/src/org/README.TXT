Stado README.TXT

Stado allows for clustering together multiple nodes to parallelize 
data warehousing type of queries. 

Stado is written in Java, requiring versionj 5 to run. It uses JDBC to 
communicate with the other database nodes, as well as ties into the 
underlying database's high speed load utility.

You can build using your favorite Java IDE with the files in src and lib,
or use the Makefile found in src/org

There are some scripts included in the tools directory that are used
by the Makefile to strip out logging. For activity like ETL, removing
all of the logging helps improve performance.

The packages under org.postgresql.stado are:

common - common shared code, misc classes

communication - for communication between the individual NodeAgents and 
  Coordinator, whether a LocalConnector or SocketConnector

constraintchecker - a special package for validating constraints to ensure 
  consistency across the cluster

engine - handles the main execution of statements, includes MultinodeExecutor,
  NodeAgent, NodeThread, NodeProducerThread, and loader package.

exception - Stado exceptions

metadata - Stado maintains its own metadata, information about the schema 
  for SQL validation and to help the optimizer parallelize SQL query plans.

misc - contains CombinedResultSet, for combining results from the nodes

optimizer - includes objects like QueryTree, which is built up to form the 
  basis of a query plan

parser - handles parsing of SQL statements, and includes one object for each 
  type of SQL statement

planner - converts a QueryTree into an ExecutionPlan with ExecutionSteps.

protocol - handles communication with clients. Compatible with the PostgreSQL 
  protocol.

queryproc - used with engine for query processing.

server - for the main server listener

util - contains various utilities for creating databases, starting/stopping the server, etc.

