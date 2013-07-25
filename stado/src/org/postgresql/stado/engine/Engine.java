/*****************************************************************************
 * Copyright (C) 2008 EnterpriseDB Corporation.
 * Copyright (C) 2011 Stado Global Development Group.
 *
 * This file is part of Stado.
 *
 * Stado is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stado is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stado.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can find Stado at http://www.stado.us
 *
 ****************************************************************************/
/*
 *
 */
package org.postgresql.stado.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.IMetaDataUpdate;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.planner.QueryPlan;


// This is the start of a class that will enclose the
// various parts of the system and glue them together

public class Engine {
    private static final XLogger logger = XLogger.getLogger(Engine.class);

    private static final Engine engine = new Engine();

    /**
     *
     * @return
     */
    public static final Engine getInstance() {
        return engine;
    }

    // ----------------------------------------------------------------
    private Engine() {
        // logger.throwing(new XDBServerException("Engine is created"));
    }

    /**
     * Begins a user-defined transaction
     *
     * @param client
     * @param nodeList
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void beginTransaction(XDBSessionContext client,
            Collection<DBNode> nodeList) throws XDBServerException {

        final String method = "beginTransaction";
        logger.entering(method, new Object[] { client });
        try {

            client.setInTransaction(true);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Begins a user-defined transaction
     *
     * @param client
     * @param nodeList
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void commitTransaction(XDBSessionContext client,
            Collection<DBNode> nodeList) throws XDBServerException {

        final String method = "commitTransaction";
        logger.entering(method, new Object[] { client });
        try {

            client.getSysDatabase();
            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);
            aMultinodeExecutor.commit();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Begins a user-defined transaction
     *
     * @param client
     * @param nodeList
     * @throws java.sql.SQLException
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void rollbackTransaction(XDBSessionContext client,
            Collection<DBNode> nodeList) throws SQLException,
            XDBServerException {

        final String method = "rollbackTransaction";
        logger.entering(method, new Object[] { client });
        try {

            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);
            aMultinodeExecutor.rollback();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param dropList
     * @param nodeList
     * @param client
     * @throws java.lang.Exception
     */
    public void dropNodeTempTables(Collection<String> dropList,
            Collection<DBNode> nodeList, XDBSessionContext client)
    throws Exception {
        final String method = "dropNodeTempTables";
        logger.entering(method, new Object[] { dropList, nodeList, client });
        try {
            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);
            aMultinodeExecutor.dropNodeTempTables(dropList);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param tableName
     * @param address
     * @param nodeList
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void startLoaders(String tableName, String address,
            Collection<DBNode> nodeList, XDBSessionContext client)
    throws XDBServerException {
        final String method = "startLoaders";
        logger.entering(method, new Object[] { client });
        try {

            SysDatabase database = client.getSysDatabase();
            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList == null ? new ArrayList<DBNode>(
                    database.getDBNodeList())
                    : nodeList);
            aMultinodeExecutor.startLoaders(tableName, address);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param sqlCommand
     * @param nodeList
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public Map<Integer, ResultSet> executeQueryOnMultipleNodes(
            String sqlCommand, Collection<DBNode> nodeList,
            XDBSessionContext client) throws XDBServerException {

        final String method = "executeSingleRowQueryOnMultipleNodes";
        logger.entering(method, new Object[] { sqlCommand, nodeList, client });
        try {

            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);
            QueryPlan.CATEGORY_QUERYFLOW.log(Level.DEBUG,
                    "Executing query %0% on Nodes %1%", new Object[] {
                    sqlCommand, nodeList });
            Map<Integer, ResultSet> results = new HashMap<Integer, ResultSet>();
            for (NodeMessage aMessage : aMultinodeExecutor.execute(sqlCommand,
                    nodeList, false)) {
                results.put(aMessage.getSourceNodeID(), aMessage.getResultSet());
            }
            return results;

        } finally {
            logger.exiting(method);
        }
    }

    // ----------------------------------------------------------------
    // This is a generic method to execute a command on multiple nodes.
    // It assumes that there is a single SQL statement as part of the
    // "transaction.".
    //
    // If/when we write something to allow transactions
    // over multiple commands, this can be readily adopted.
    // ----------------------------------------------------------------
    /**
     *
     * @param sqlCommand
     * @param nodeList
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public int executeOnMultipleNodes(String sqlCommand,
            Collection<DBNode> nodeList, XDBSessionContext client)
    throws XDBServerException {

        final String method = "executeOnMultipleNodes";
        logger.entering(method, new Object[] { sqlCommand, nodeList, client });
        int numRowsAffected = 0;
        try {

            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);

            QueryPlan.CATEGORY_QUERYFLOW.log(Level.DEBUG,
                    "Executing command %0% on Nodes %1%", new Object[] {
                    sqlCommand, nodeList });
            // amart:
            // if we are running the command on single node in autocommit mode
            // do autocommit immediately
            numRowsAffected = aMultinodeExecutor.executeCommand(sqlCommand,
                    nodeList, nodeList.size() == 1 && !client.isInTransaction()
                    && !client.isInSubTransaction());
            return numRowsAffected;

        } finally {
            logger.exiting(method, new Integer(numRowsAffected));
        }
    }

    // ----------------------------------------------------------------
    // This is a generic method to execute a command on multiple nodes.
    // It assumes that there is a single SQL statement as part of the
    // "transaction.".
    //
    // If/when we write something to allow transactions
    // over multiple commands, this can be readily adopted.
    // ----------------------------------------------------------------
    /**
     *
     * @param commands
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public int executeOnMultipleNodes(Map<DBNode, String> commands,
            XDBSessionContext client) throws XDBServerException {
        final String method = "executeOnMultipleNodes";
        logger.entering(method, new Object[] { commands, client });
        int numRowsAffected = 0;
        try {

            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(commands.keySet());
            QueryPlan.CATEGORY_QUERYFLOW.log(Level.DEBUG,
                    "Executing commands %0%", new Object[] { commands });
            // amart:
            // if we are running the command on single node in autocommit mode
            // do autocommit immediately
            numRowsAffected = aMultinodeExecutor.executeCommand(commands,
                    commands.size() == 1 && !client.isInTransaction()
                    && !client.isInSubTransaction());
            return numRowsAffected;

        } finally {
            logger.exiting(method, new Integer(numRowsAffected));
        }
    }

    /**
     * executeDDLOnMultipleNodes()
     *
     * This method executes the DDL on multiple nodes and also updates the
     * MetaData DB. It also refreshes the MetaData cache.
     *
     * NOTES - The following plan is used to update the databases Phase 1 Begin
     * Transaction on user db Execute statements on user db
     *
     * Phase 2 Begin Transaction for MetaData db Execute statements for MetaData
     * db update ( by calling updater.execute() )
     *
     * Phase 3 If all is well, Commit on user db
     *
     * Phase 4 Commit on MetaData db ( will also refresh the cache by calling
     * updater.refresh())
     *
     * @param sqlCommand
     * @param nodeList
     * @param updater
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void executeDDLOnMultipleNodes(String sqlCommand,
            Collection<DBNode> nodeList, IMetaDataUpdate updater,
            XDBSessionContext client) throws XDBServerException {

        final String method = "executeDDLOnMultipleNodes";
        logger.entering(method, new Object[] { sqlCommand, nodeList, updater,
                client });
        try {

            String commands[] = new String[1];
            commands[0] = sqlCommand;
            executeDDLOnMultipleNodes(commands, nodeList, updater, client);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param sqlCommands
     * @param nodeList
     * @param updater
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void executeDDLOnMultipleNodes(String sqlCommands[],
            Collection<DBNode> nodeList, IMetaDataUpdate updater,
            XDBSessionContext client) throws XDBServerException {

        final String method = "executeDDLOnMultipleNodes";
        logger.entering(method, new Object[] { sqlCommands, nodeList, updater,
                client });
        try {
            String sqlCommand = "";
            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);
            beginTransaction(client, nodeList);
            try {
                for (String element : sqlCommands) {
                    if (element != null) {
                        sqlCommand = element;
                        QueryPlan.CATEGORY_QUERYFLOW.log(Level.DEBUG,
                                "Executing DDL command %0% on Nodes %1%",
                                new Object[] { sqlCommand, nodeList });
                        aMultinodeExecutor.executeCommand(sqlCommand, nodeList);
                    }
                }
            } catch (XDBServerException e) {
                aMultinodeExecutor.rollback();
                throw e;
            }
            doMetadataUpdate(aMultinodeExecutor, updater, client);

        } finally {
            logger.exiting(method);
        }
    } // end executeDDLOnMultipleNodes()

    /**
    *
    * @param sqlCommands
    * @param nodeList
    * @param updater
    * @param client
    * @throws com.edb.gridsql.exception.XDBServerException
    */
   public Map<Integer, ResultSet> executeDDLFunctionOnMultipleNodes(String sqlCommand,
           Collection<DBNode> nodeList, IMetaDataUpdate updater,
           XDBSessionContext client) throws XDBServerException {

       final String method = "executeDDLFunctionOnMultipleNodes";
       logger.entering(method, new Object[] { sqlCommand, nodeList, updater,
               client });
       try {
           MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);
           Map<Integer, ResultSet> results = new HashMap<Integer, ResultSet>();
           beginTransaction(client, nodeList);
           try {
               QueryPlan.CATEGORY_QUERYFLOW.log(Level.DEBUG,
                       "Executing query %0% on Nodes %1%", new Object[] {
                       sqlCommand, nodeList });
               for (NodeMessage aMessage : aMultinodeExecutor.execute(sqlCommand,
                       nodeList, false)) {
                   results.put(aMessage.getSourceNodeID(), aMessage.getResultSet());
               }        	   
               
           } catch (XDBServerException e) {
               aMultinodeExecutor.rollback();
               throw e;
           }
           doMetadataUpdate(aMultinodeExecutor, updater, client);

           return results;
       } finally {
           logger.exiting(method);
       }
   } // end executeDDLOnMultipleNodes()

    /**
     * A kind of two-phase commit to ensure consistency.
     * Update the metadata database, if succeeded commit on nodes, then commit metadata changes.
     * This method is synchronized, to avoid concurrency issues
     *  
     * @param aMultinodeExecutor
     * @param updater
     * @param client
     */
    synchronized private void doMetadataUpdate(MultinodeExecutor aMultinodeExecutor,
            IMetaDataUpdate updater, XDBSessionContext client) {
        // phase 2
        try {
            MetaData.getMetaData().beginTransaction();
            updater.execute(client);
        } catch (Exception e) {
            try {
                aMultinodeExecutor.rollback();
            } catch (Exception e2) {
            }
            try {
                MetaData.getMetaData().rollbackTransaction();
            } catch (Exception e4) {
            }
            if (e instanceof XDBServerException) {
                throw (XDBServerException) e;
            } else {
                throw new XDBServerException(e.getMessage(), e);
            }
        }

        // phase 3
        try {
            aMultinodeExecutor.commit();
        } catch (XDBServerException e) {
            // rollback MetaData DB
            try {
                MetaData.getMetaData().rollbackTransaction();
            } catch (Exception e4) {
            }
            throw e;
        }

        // phase 4
        try {
            MetaData.getMetaData().commitTransaction(updater);
        } catch (XDBServerException e) {
            try {
                MetaData.getMetaData().rollbackTransaction();
            } catch (Exception e4) {
            }
            throw e;
        }
    }

    /**
     * @param statements
     * @param updater
     * @param client
     */
    public void executeDDLOnMultipleNodes(Map<DBNode, String> statements,
            IMetaDataUpdate updater, XDBSessionContext client) {
        final String method = "executeOnMultipleNodes";
        logger.entering(method, new Object[] { statements, updater, client });
        int numRowsAffected = 0;
        try {

            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(statements.keySet());
            beginTransaction(client, statements.keySet());
            try {
                QueryPlan.CATEGORY_QUERYFLOW.log(Level.DEBUG,
                        "Executing commands %0%", new Object[] { statements });
                aMultinodeExecutor.executeCommand(statements);
            } catch (XDBServerException e) {
                aMultinodeExecutor.rollback();
                throw e;
            }
            doMetadataUpdate(aMultinodeExecutor, updater, client);

        } finally {
            logger.exiting(method, new Integer(numRowsAffected));
        }
    }

    /**
     *
     * @param statements
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public boolean addToBatchOnNodes(Map<DBNode, String> statements,
            XDBSessionContext client) throws XDBServerException {
        // This line makes client persistent
        client.setInBatch(true);
        MultinodeExecutor aExecutor = client.getMultinodeExecutor(statements.keySet());
        return aExecutor.addBatchOnNodeList(statements);
    }

    /**
     *
     * @param statement
     * @param nodeList
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public boolean addToBatchOnNodes(String statement,
            Collection<DBNode> nodeList, XDBSessionContext client)
    throws XDBServerException {
        // This line makes client persistent
        client.setInBatch(true);
        MultinodeExecutor aExecutor = client.getMultinodeExecutor(nodeList);
        return aExecutor.addBatchOnNodeList(statement, nodeList);
    }

    /**
     *
     * @param statement
     * @param node
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public boolean addToBatchOnNode(String statement, DBNode node,
            XDBSessionContext client) throws XDBServerException {
        // This line makes client persistent
        client.setInBatch(true);
        MultinodeExecutor aExecutor = client.getMultinodeExecutor(Collections.singleton(node));
        return aExecutor.addBatchOnNode(statement, node.getNodeId());
    }

    /**
     *
     * @param client
     * @param strict
     * @return
     */
    public int[] executeBatchOnNodes(XDBSessionContext client, boolean strict) {
        // If addToBatchOnNodes() was called client is persistent and does not
        // release
        // NodeThreads, just returns latest ME
        List<DBNode> emptyNodeList = Collections.emptyList();
        MultinodeExecutor aExecutor = client.getMultinodeExecutor(emptyNodeList);
        int[] result = aExecutor.executeBatch(strict);
        client.setInBatch(false);
        return result;
    }

    /**
     *
     * @param client
     */
    public void clearBatchOnNodes(XDBSessionContext client) {
        // If addToBatchOnNodes() was called client is persistent and does not
        // release
        // NodeThreads, just returns latest ME
        List<DBNode> emptyNodeList = Collections.emptyList();
        MultinodeExecutor aExecutor = client.getMultinodeExecutor(emptyNodeList);
        aExecutor.clearBatch();
        client.setInBatch(false);
    }

    /**
     *
     * @param commandID
     * @param sqlCommand
     * @param nodeList
     * @param client
     * @return
     * @throws XDBServerException
     */
    public String prepareStatement(String commandID, String sqlCommand,
            int[] paramTypes, Collection<DBNode> nodeList,
            XDBSessionContext client) throws XDBServerException {
        final String method = "prepareStatement";
        logger.entering(method, new Object[] { commandID, sqlCommand, nodeList,
                client });
        int numRowsAffected = 0;
        try {

            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);
            commandID = aMultinodeExecutor.prepareStatement(commandID,
                    sqlCommand, paramTypes, nodeList);
            QueryPlan.CATEGORY_QUERYFLOW.log(Level.DEBUG,
                    "Creating prepared statement %0%",
                    new Object[] { commandID });
            return commandID;

        } finally {
            logger.exiting(method, new Integer(numRowsAffected));
        }
    }

    /**
     *
     * @param commandID
     * @param parameters
     * @param client
     * @return
     * @throws XDBServerException
     */
    public int executePreparedCommand(String commandID, String[] parameters,
            Collection<DBNode> nodeList, XDBSessionContext client)
    throws XDBServerException {
        final String method = "executePreparedStatemnt";
        logger.entering(method, new Object[] { commandID, parameters, client });
        int numRowsAffected = 0;
        try {

            Collection<DBNode> empty = Collections.emptySet();
            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(empty);
            QueryPlan.CATEGORY_QUERYFLOW.log(Level.DEBUG,
                    "Executing prepared statement %0%",
                    new Object[] { commandID });
            numRowsAffected = aMultinodeExecutor.executePreparedCommand(
                    commandID, parameters, nodeList);
            return numRowsAffected;

        } finally {
            logger.exiting(method, new Integer(numRowsAffected));
        }
    }

    public void closePreparedStatement(String commandID,
            XDBSessionContext client) throws XDBServerException {
        final String method = "closePreparedStatement";
        logger.entering(method, new Object[] { commandID, client });
        int numRowsAffected = 0;
        try {

            Collection<DBNode> empty = Collections.emptySet();
            MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(empty);
            QueryPlan.CATEGORY_QUERYFLOW.log(Level.DEBUG,
                    "Closing prepared statement %0%",
                    new Object[] { commandID });
            aMultinodeExecutor.closePrepared(commandID);

        } finally {
            logger.exiting(method, new Integer(numRowsAffected));
        }
    }

    /**
     * Evaluates SqlExpression using Coordinator connection.
     * Returns result as a constant SqlExpression
     * A query like SELECT <the_expression> is build and executed on Coordinator
     * connection
     * Only works if backend supports SELECT without FROM clause.
     * Execution will fail if the expression contain column reference
     * Currently used to evaluate and normalize partitioning expression
     * May be improved in future to leave off coordinator connection. Tasks &
     * issues to target:
     * 1. Federated database. Evaluation result may depend on backend type and
     * may cause partitioning errors. Complete internal evaluator will target
     * this issue.
     * 2. Do better job analyzing the expression. This function may partly
     * calculate (reduce) the expression to speed up backend query and return
     * reduced Expression (or original if not reduceable) instead of throwing.
     * 3. Loading. Current approach is too slow to use when loading data.
     * @param client Session object (to get coordinator connection)
     * @param expr SqlExpression to evaluate
     * @return Constant SqlExpression
     * @throws SQLException if evaluation failed
     */
    public SqlExpression evaluate(XDBSessionContext client, SqlExpression expr)
    throws SQLException {
        if (expr == null || expr.getExprType() == SqlExpression.SQLEX_CONSTANT) {
            return expr;
        }
        Connection oConn = client.getAndSetCoordinatorConnection();
        Statement stmt = oConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT " + expr.rebuildString());
        try {
            rs.next();
            String value = rs.getString(1);
            SqlExpression constExpr = new SqlExpression(expr.getBelongsToTree());
            constExpr.setExprType(SqlExpression.SQLEX_CONSTANT);
            constExpr.setExprDataType(expr.getExprDataType());
            constExpr.setConstantValue(value);
            return constExpr;
        } finally {
            rs.close();
        }
    }

    /**
     * Evaluates the SQL expressions using Coordinator connection.
     * Only works if backend supports SELECT without FROM clause.
     * Execution will fail if any the expression contains column reference
     * @see #evaluate(XDBSessionContext, SqlExpression)
     * @param client Session object (to get coordinator connection)
     * @param expr SQL expression to evaluate
     * @return SQL literal
     * @throws SQLException if evaluation failed
     */
    public String evaluate(XDBSessionContext client, String expr)
            throws SQLException {
        Connection oConn = client.getAndSetCoordinatorConnection();
        Statement stmt = oConn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT " + expr);
        try {
            rs.next();
            return rs.getString(1);
        } finally {
            rs.close();
        }
    }
}
