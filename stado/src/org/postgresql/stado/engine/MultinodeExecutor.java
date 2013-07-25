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
package org.postgresql.stado.engine;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import org.postgresql.stado.common.ActivityLog;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.CoordinatorAgent;
import org.postgresql.stado.communication.IMessageListener;
import org.postgresql.stado.communication.SendMessageHelper;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.communication.message.SendRowsMessage;
import org.postgresql.stado.exception.XDBMessageMonitorException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.Node;
import org.postgresql.stado.metadata.scheduler.LockManager;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.planner.NodeUsage;
import org.postgresql.stado.planner.StepDetail;
import org.postgresql.stado.queryproc.QueryCombiner;


// Runs a command on multiple nodes, by using multiple
// NodeThread objects.
//

// We should improve exception handling here..
// If things go wrong, it is up to the code that uses the
// MultinodeExecutor to check for the exception in
// XBarrier and then tell MultinodeExecutor to rollback
// and/or end its nodes cleanly
//----------------------------------------------
public class MultinodeExecutor implements IMessageListener {

    private static final XLogger logger = XLogger
            .getLogger(MultinodeExecutor.class);

    // This is a list of ALL the nodes involved in all of the steps
    // of execution. If unsure, use all for database
    private Collection<DBNode> nodeList;

    /**
     * the client connection to the xdb server making use of this multinode
     * executor
     */
    private XDBSessionContext client;

    /**
     * Instance of CoordinatorAgent used for passing around messages
     */
    private SendMessageHelper sendHelper;

    /**
     * Helps manage batches
     */
    private BatchHandler aBatchHandler;

    /**
     * Tracks the data message sequence number
     */

    // Track received messages
    private MessageMonitor monitor;

    /**
     * Messages to ProducerSender
     */
    private LinkedBlockingQueue<NodeMessage> coordinatorQueue = new LinkedBlockingQueue<NodeMessage>();

    private ProducerSender insertProducerSender;

    private Collection<Integer> currentStepProcesses;

    /**
     * MSG_SEND_DATA messages are arriving while we are waiting for
     * MSG_EXECUTE_STEP_SENT messages. We put it here.
     */
    private LinkedBlockingQueue<NodeMessage> sendDataMessages = new LinkedBlockingQueue<NodeMessage>();

    /**
     * for streaming results from nodes
     */
    private HashMap<String, HashMap<Integer,NodeResultSetImpl>> finalRSs = new HashMap<String, HashMap<Integer,NodeResultSetImpl>>();

    /**
     * To avoid mixing new RSs with those from previous queries
     */
    private String currentResultSetID = null;

    private static final long SHORT_TIMEOUT = Property.getLong(
            "xdb.messagemonitor.timeout.short.millis", 600000L);

    private static final long CONNECTION_TIMEOUT = Property.getLong(
            "xdb.messagemonitor.timeout.connection.millis",
            Props.XDB_DEFAULT_THREADS_POOL_TIMEOUT);


    /**
     *
     * @param cmdNodeList
     * @param client
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    MultinodeExecutor(Collection<DBNode> cmdNodeList, XDBSessionContext client)
            throws XDBServerException {
        final String method = "MultinodeExecutor";

        logger.entering(method, new Object[] { cmdNodeList, client });
        try {
            nodeList = new HashSet<DBNode>();
            nodeList.addAll(cmdNodeList);
            this.client = client;
            monitor = new MessageMonitor(client);

            // Executor should be registered before registration new one with
            // Agent
            // client.registerExecutor(this);
            Integer sessionID = new Integer(client.getSessionID());
            CoordinatorAgent agent = CoordinatorAgent.getInstance();
            agent.addProcess(sessionID, this);
            sendHelper = new SendMessageHelper(0, sessionID, agent);
            initNodeThreads(nodeList);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * If we are reusing this object, it double checks to see if we have all the
     * needed nodes and adds them, if necessary.
     *
     * @param newNodeList
     */
    void addNeededNodes(Collection<DBNode> newNodeList) {
        final String method = "addNeededNodes";
        logger.entering(method, new Object[] { newNodeList });
        try {

            // Do synchronization here because we have methods, that are called
            // from
            // different sesssion's thread (kill() and terminate()), they send
            // messages to nodeList and want to have it immutable.
            synchronized (nodeList) {
                ArrayList<DBNode> addList = new ArrayList<DBNode>();

                HashSet<DBNode> nodeSet = new HashSet<DBNode>(nodeList);
                Iterator<DBNode> newIt = newNodeList.iterator();

                while (newIt.hasNext()) {
                    DBNode newNode = newIt.next();
                    if (!nodeSet.remove(newNode)) {
                        addList.add(newNode);
                    }
                }

                // if we added new nodes, initialize them
                if (addList.size() > 0) {
                    initNodeThreads(addList);
                }
                // amart: I just added removing not needed nodes
                // it should be OK if client was not persistent.
                // to rollback the change next two code lines should be replaced
                // with
                //
                // nodeList.addAll(addList);
                //
                // and releaseNodeThreads(nodeSet); line should be removed.

                // if client persistent keep old entries
                if (!client.isPersistent()) {
                    nodeList.removeAll(nodeSet);
                }
                nodeList.addAll(addList);
                // After while loop nodeSet contains not needed nodes,
                // appropriate NodeThreads should be removed
                // We can do it outside synchronized block: nodeList does not
                // contain
                // these Nodes
                if (!nodeSet.isEmpty() && !client.isPersistent()) {
                    releaseNodeThreads(nodeSet);
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @param nodeList
     */
    private void releaseNodeThreads(Collection<DBNode> nodeList) {
        final String method = "releaseNodeThreads";
        logger.entering(method, new Object[] { nodeList });
        try {

            int requestId = client.getRequestId();
            monitor.setMonitor(requestId, nodeList);
            try {
                sendHelper.sendMessageToList(nodeList,
                        NodeMessage.MSG_CONNECTION_END, requestId);
            } finally {
                monitor.waitForMessages(SHORT_TIMEOUT);
            }

        } catch (Throwable t) {
            // Excetion must not be thrown from this method
            // It is pretty safe here to ignore non-responding nodes
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param initList
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void initNodeThreads(Collection<DBNode> initList)
            throws XDBServerException {
        final String method = "initNodeThreads";
        logger.entering(method, new Object[] { initList });
        try {

            int requestId = client.getRequestId();
            monitor.setMonitor(requestId, initList);

            try {
                NodeMessage msg = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_CONNECTION_BEGIN);
                msg.setDatabase(client.getDBName());
                msg.setRequestId(requestId);
                sendHelper.sendMessageToList(initList, msg);
            } finally {
                monitor.waitForMessages(CONNECTION_TIMEOUT);
            }

        } finally {
            logger.exiting(method);
        }
    }

    // make sure that connections are still alive
    // ------------------------------------------------------------------
    /**
     *
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void checkConnections() throws XDBServerException {
        int requestId = client.getRequestId();
        monitor.setMonitor(requestId, nodeList);
        try {
            sendHelper.sendMessageToList(nodeList, NodeMessage.MSG_PING,
                    requestId);
        } finally {
            monitor.waitForMessages(SHORT_TIMEOUT);
        }
    }

    /**
     * Begins a savepoint for a "subtransaction"
     *
     * @param savepointName
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    void beginSavepoint(String savepointName) throws XDBServerException {
        final String method = "beginSavepoint";
        logger.entering(method, new Object[] { savepointName });
        try {

            /** make these connections persistent */
            client.setSavepoint(savepointName);

            int requestId = client.getRequestId();

            monitor.setMonitor(requestId, nodeList);

            try {
                NodeMessage aNodeMessage = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_TRAN_BEGIN_SAVEPOINT);
                aNodeMessage.setSavepoint(savepointName);
                aNodeMessage.setRequestId(requestId);
                sendHelper.sendMessageToList(nodeList, aNodeMessage);
            } finally {
                monitor.waitForMessages(SHORT_TIMEOUT);
            }

        }

        finally {
            logger.exiting(method);
        }
    }

    /**
     * Ends a savepoint for a "subtransaction"
     *
     * @param savepointName
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    void endSavepoint(String savepointName) throws XDBServerException {
        final String method = "endSavepoint";
        logger.entering(method, new Object[] { savepointName });
        try {

            checkConnections();

            int requestId = client.getRequestId();

            monitor.setMonitor(requestId, nodeList);

            try {
                NodeMessage aNodeMessage = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_TRAN_END_SAVEPOINT);
                aNodeMessage.setSavepoint(savepointName);
                aNodeMessage.setRequestId(requestId);
                sendHelper.sendMessageToList(nodeList, aNodeMessage);
            } finally {
                monitor.waitForMessages(SHORT_TIMEOUT);
            }

            client.clearSavepoint(savepointName);

        } finally {
            logger.exiting(method);
        }
    }

    // We pass in the nodes that we want to execute this command on.
    // returns the number of rows affected.
    // -----------------------------------------------------------------------
    /**
     *
     * @param sqlCommand
     * @param execNodeList
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public int executeCommand(String sqlCommand, Collection<DBNode> execNodeList)
            throws XDBServerException {
        return executeCommand(sqlCommand, execNodeList, false);
    }

    /**
     *
     * @param sqlCommand
     * @param execNodeList
     * @param autocommit
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public int executeCommand(String sqlCommand,
            Collection<DBNode> execNodeList, boolean autocommit)
            throws XDBServerException {
        final String method = "executeCommand";
        logger.entering(method, new Object[] { sqlCommand, execNodeList,
                autocommit ? Boolean.TRUE : Boolean.FALSE });

        int numRows = 0;
        try {

            int requestId = client.getRequestId();
            monitor.setMonitor(requestId, execNodeList);

            try {
                NodeMessage aNodeMessage = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_EXEC_COMMAND);
                aNodeMessage.setSqlCommand(sqlCommand);
                aNodeMessage.setRequestId(requestId);
                aNodeMessage.setAutocommit(autocommit);
                sendHelper.sendMessageToList(execNodeList, aNodeMessage);
            } finally {
                NodeMessage[] results = monitor.waitForMessages();

                for (NodeMessage element : results) {
                    numRows += element.getNumRowsResult();
                }
            }

            return numRows;

        } finally {
            logger.exiting(method, new Integer(numRows));
        }
    }

    /**
     *
     * @return
     * @param sqlCommand
     * @param execNodeList
     * @param autocommit
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public Collection<NodeMessage> execute(String sqlCommand,
            Collection<DBNode> execNodeList, boolean autocommit)
            throws XDBServerException {
        final String method = "executeSingleRowQuery";
        logger.entering(method, new Object[] { sqlCommand, execNodeList });

        Collection<NodeMessage> results;
        try {

            int requestId = client.getRequestId();
            monitor.setMonitor(requestId, execNodeList);

            try {
                NodeMessage aNodeMessage = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_EXEC_COMMAND);
                aNodeMessage.setSqlCommand(sqlCommand);
                aNodeMessage.setRequestId(requestId);
                aNodeMessage.setAutocommit(autocommit);
                sendHelper.sendMessageToList(execNodeList, aNodeMessage);
            } finally {
                try {
                    results = Arrays.asList(monitor.waitForMessages());
                } catch (XDBMessageMonitorException ex) {
                    HashSet<Integer> nodesToAbort = new HashSet<Integer>();
                    if (ex.getRemainingNodes() != null) {
                        nodesToAbort.addAll(ex.getRemainingNodes());
                    }
                    if (nodesToAbort.size() == 0) {
                        for (DBNode aDBNode : execNodeList) {
                            nodesToAbort.add(aDBNode.getNodeId());
                        }
                    }
                    abortSQLCommand(nodesToAbort, monitor.getRequestId());
                    throw ex;
                } catch (XDBServerException ex) {
                    abortSQLCommand(execNodeList, monitor.getRequestId());
                    throw ex;
                }
            }
            // Register final result sets to be able to fetch rows
            for (NodeMessage message : results) {
                // Enable fetching rows
                if (message.getMessageType() == NodeMessage.MSG_EXEC_QUERY_RESULT) {
                    addFinalResultSet(message);
                }
            }
            return results;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @param message
     */
    private void addFinalResultSet(NodeMessage message) {
        synchronized (finalRSs) {
            HashMap<Integer,NodeResultSetImpl> nodeRSs = finalRSs.get(message.getResultSetID());
            if (nodeRSs == null) {
                nodeRSs = new HashMap<Integer,NodeResultSetImpl>();
                finalRSs.put(message.getResultSetID(), nodeRSs);
            }
            nodeRSs.put(message.getSourceNodeID(), message.getResultSet());
        }
    }

    /**
     *
     * @param commands
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public int executeCommand(Map<DBNode,String> commands) throws XDBServerException {
        return executeCommand(commands, false);
    }

    /**
     *
     * @param commands
     * @param autocommit
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return the number of rows affected
     */
    public int executeCommand(Map<DBNode,String> commands, boolean autocommit)
            throws XDBServerException {
        final String method = "executeCommand";
        logger.entering(method, new Object[] { commands });

        int numRows = 0;
        try {

            int requestId = client.getRequestId();
            monitor.setMonitor(requestId, commands.keySet());

            try {
                for (Map.Entry<DBNode,String> entry : commands.entrySet()) {
                    NodeMessage aNodeMessage = NodeMessage
                            .getNodeMessage(NodeMessage.MSG_EXEC_COMMAND);
                    aNodeMessage.setSqlCommand(entry.getValue());
                    aNodeMessage.setRequestId(requestId);
                    aNodeMessage.setAutocommit(autocommit);
                    sendHelper.sendMessage(entry.getKey().getNodeId(),
                            aNodeMessage);
                }
            } finally {
                NodeMessage[] results = monitor.waitForMessages();

                for (NodeMessage element : results) {
                    numRows += element.getNumRowsResult();
                }
            }

            return numRows;

        } finally {
            logger.exiting(method, new Integer(numRows));
        }
    }

    /**
     * The caller of this function will have to take care of calling back all
     * the the threads it once started.
     *
     * @throws XDBServerException
     */
    void commit() throws XDBServerException {
        final String method = "commit";
        logger.entering(method);
        try {

            try {
                if (nodeList.size() > 1) {
                    checkConnections();
                }

                LockManager lm = client.getSysDatabase().getLm();
                lm.beforeCommit(client);
                int requestId = client.getRequestId();
                monitor.setMonitor(requestId, nodeList);

                try {
                    sendHelper.sendMessageToList(nodeList,
                            NodeMessage.MSG_TRAN_COMMIT, requestId);
                } finally {
                    monitor.waitForMessages();
                }
            } finally {
                client.setInTransaction(false);
            }

        } finally {
            logger.exiting(method);
        }
    }

    // ------------------------------------------------------------------

    /**
     * The Caller of this function will have to take care of calling back all
     * the threads.
     *
     */
    void rollback() {
        final String method = "rollback";
        logger.entering(method);
        try {
            int requestId = client.getRequestId();
            monitor.setMonitor(requestId, nodeList);

            // If single-node statement is executed in autocommit mode
            // backend connection is switched to autocommit mode too to
            // avoid transaction handling overhead in the driver.
            // So we do not have to send explicit rollback to the node
            if (client.isInTransaction() || nodeList.size() > 1) {
                try {
                    sendHelper.sendMessageToList(nodeList,
                            NodeMessage.MSG_TRAN_ROLLBACK, requestId);

                    client.setInTransaction(false);
                } finally {
                    monitor.waitForMessages();
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * The Caller of this function will have to take care of calling back all
     * the threads.
     *
     * @param savepointName
     * @throws Exception
     */
    void rollbackSavepoint(String savepointName) throws XDBServerException {
        final String method = "rollbackSavepoint";
        logger.entering(method, new Object[] { savepointName });
        try {
            int requestId = client.getRequestId();

            monitor.setMonitor(requestId, nodeList);

            try {
                NodeMessage aNodeMessage = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_TRAN_ROLLBACK_SAVEPOINT);
                aNodeMessage.setSavepoint(savepointName);
                aNodeMessage.setRequestId(requestId);
                sendHelper.sendMessageToList(nodeList, aNodeMessage);
            } finally {
                monitor.waitForMessages();
                client.clearSavepoint(savepointName);
            }
        } finally {
            logger.exiting(method);
        }

    }

    // -----------------------------------------------------------------------

    void releaseNodeThreads() {
        final String method = "releaseNodeThreads";
        logger.entering(method);
        try {

            releaseNodeThreads(nodeList);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param dropList
     */
    void dropNodeTempTables(Collection<String> dropList) {
        final String method = "dropNodeTempTables";
        logger.entering(method, new Object[] { dropList });
        try {
            int requestId = client.getRequestId();
            monitor.setMonitor(requestId, nodeList);
            try {

                NodeMessage aNodeMessage = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_DROP_TEMP_TABLES);
                aNodeMessage.setTempTables(dropList);
                aNodeMessage.setRequestId(requestId);
                aNodeMessage.setAutocommit(!client.isInTransaction()
                        && !client.isInSubTransaction());
                sendHelper.sendMessageToList(nodeList, aNodeMessage);
            } finally {
                monitor.waitForMessages(SHORT_TIMEOUT);
            }

        } finally {
            logger.exiting(method);
        }
    }

    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------

    /*
     * New methods for supporting passing down tables and combining results.
     */

    // -----------------------------------------------------------------------
    /**
     *
     * @param tableName
     * @param targetNodeList
     */
    void initInserts(String tableName, Collection<Integer> targetNodeList) {
        final String method = "initInserts";
        logger.entering(method, new Object[] { tableName, targetNodeList });
        try {

            currentStepProcesses = targetNodeList;

            int requestId = client.getRequestId();

            NodeMessage aNodeMessage = NodeMessage
                    .getNodeMessage(NodeMessage.MSG_SEND_DATA_INIT);
            aNodeMessage.setTargetTable(tableName);
            aNodeMessage.setRequestId(requestId);
            sendHelper.sendMessageToList(currentStepProcesses, aNodeMessage);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * For xdbimport
     *
     * @param isList
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    void finishProducerSender(boolean isList) throws XDBServerException {
        final String method = "finishProducerSender";
        logger.entering(method, new Object[] { new Boolean(isList) });
        try {

            if (currentStepProcesses != null) {
                // if first one, initialize ProducerSender
                if (insertProducerSender != null) {
                    if (isList) {
                        insertProducerSender
                                .finishInserts(StepDetail.DEST_TYPE_BROADCAST);
                    } else {
                        insertProducerSender
                                .finishInserts(StepDetail.DEST_TYPE_ONE);
                    }
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @throws org.postgresql.stado.exception.XDBServerException
     */

    // -----------------------------------------------------------------------
    private void finishInserts() throws XDBServerException {
        final String method = "finishInserts";
        logger.entering(method);
        try {

            if (currentStepProcesses != null) {
                int requestId = client.getRequestId();
                monitor.setMonitor(requestId, currentStepProcesses);

                try {
                    sendHelper.sendMessageToList(currentStepProcesses,
                            NodeMessage.MSG_EXECUTE_STEP_END, requestId);
                } finally {
                    monitor.waitForMessages();
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * Execute a step of our query.
     *
     */

    // -----------------------------------------------------------------------
    /**
     *
     * @param aQueryCombiner
     * @param aStepDetail
     * @param coordStepDetail
     * @param nodeUsageTable
     * @param producerCount
     * @param consumerCount
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void executeStep(QueryCombiner aQueryCombiner,
            StepDetail aStepDetail, StepDetail coordStepDetail,
            Map<Integer,NodeUsage> nodeUsageTable, int producerCount, // number of nodes
            // that are
            // producers
            int consumerCount // number of nodes that are consumers
    ) throws XDBServerException {

        final String method = "executeStep";
        logger.entering(method, new Object[] { aQueryCombiner, aStepDetail,
                coordStepDetail, nodeUsageTable, new Integer(producerCount),
                new Integer(consumerCount) });
        try {
            StepDetail currentStepDetail;

            // We use this to tell producer thread where to broadcast to, if
            // necessary
            NodeMessage aNodeMessage;

            // Initialize QueryCombiner settings.
            aQueryCombiner.sCreateStatement = aStepDetail.targetSchema;

            // Also, go ahead and create this target table, if we need to
            // combine
            // the results on the target node
            if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD
                    || aStepDetail.getDestType() == StepDetail.DEST_TYPE_BROADCAST_AND_COORD
                    || aStepDetail.combineOnCoordFirst
                    || coordStepDetail != null
                    && coordStepDetail.getDestType() == StepDetail.DEST_TYPE_BROADCAST_AND_COORD) {
                aQueryCombiner.createTempTable();
            }

            // Now, loop through usage list again and initilize all threads
            currentStepProcesses = new HashSet<Integer>();

            ArrayList<Integer> producers = new ArrayList<Integer>(producerCount);
            HashMap<Integer, NodeMessage> initMessages = new HashMap<Integer, NodeMessage>();

            int requestId = client.getRequestId();

            // Determine list of consumer nodes, so that we know where to
            // broadcast,
            // if necessary.
            for (NodeUsage aNodeUsageElement : nodeUsageTable.values()) {
                Integer nodeID = new Integer(aNodeUsageElement.nodeId);
                currentStepDetail = aStepDetail.copy();

                // Add to consumer node thread list for finishing up later
                if (aNodeUsageElement.isConsumer) {
                    currentStepProcesses.add(nodeID);
                } else {
                    // Make sure that the step gets a copy of one that it is not
                    // a consumer, in case that is the general case.
                    currentStepDetail.isConsumer = false;
                }

                // Make sure that the step gets a copy of one that it is not
                // a consumer, in case that is the general case.
                if (aNodeUsageElement.isProducer) {
                    currentStepProcesses.add(nodeID);
                    producers.add(nodeID);
                    if (Props.XDB_ENABLE_ACTIVITY_LOG) {
                        ActivityLog.startStep(client.getStatementId(),
                                aNodeUsageElement.nodeId);
                    }
                } else {
                    currentStepDetail.isProducer = false;
                }
                // Tell nodes to prepare to execute step
                aNodeMessage = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_EXECUTE_STEP_INIT);

                // For correlated queries, we do some substitution
                if (currentStepDetail.queryString != null) {
                    // look for last XNODEID (the deepest one if nested
                    // correlated)
                    int xpos = currentStepDetail.queryString
                            .lastIndexOf("XNODEID");

                    if (xpos > 0) {
                        StepDetail newStepDetail = currentStepDetail.copy();

                        // get XNODEID string
                        String sXNodeId = newStepDetail.queryString.substring(
                                xpos, newStepDetail.queryString.indexOf(" ",
                                        xpos));

                        // replace XNODEID with nodeid constant
                        newStepDetail.queryString = newStepDetail.queryString
                                .replaceFirst(sXNodeId, nodeID + " AS "
                                        + sXNodeId);

                        currentStepDetail = newStepDetail;
                    } else {
                        String outerNodeIdColumn = currentStepDetail
                                .getOuterNodeIdColumn();

                        if (outerNodeIdColumn != null) {
                            StepDetail newStepDetail = currentStepDetail.copy();

                            // replace XNODEID with nodeid constant
                            newStepDetail.queryString = newStepDetail.queryString
                                    .replaceAll(outerNodeIdColumn, nodeID
                                            + " AS " + IdentifierHandler.quote(outerNodeIdColumn));

                            currentStepDetail = newStepDetail;
                        }
                    }
                    logger.debug ("ME to execute on nodes: "
                            + currentStepDetail.queryString);
                }

                // If CREATE TABLE AS, use tablespace if specified
                if (currentStepDetail.usesTablespace()) {
                    currentStepDetail.targetSchema += " TABLESPACE "
                            + currentStepDetail.getTablespaceClause(nodeID);
                }
                aNodeMessage.setStepDetail(currentStepDetail);
                aNodeMessage.setRequestId(requestId);

                initMessages.put(nodeID, aNodeMessage);

            }
            // Only bother doing the INIT step if we are a
            // consumer and need to create a temp table
            if (aStepDetail.isConsumer) {
                monitor.setMonitor(requestId, initMessages.keySet());

                try {
                    for (Entry<Integer, NodeMessage> entry : initMessages
                            .entrySet()) {
                        sendHelper
                                .sendMessage(entry.getKey(), entry.getValue());
                    }
                } finally {
                    monitor.waitForMessages(SHORT_TIMEOUT);
                }

                sendDataMessages.clear();
            }

            // Set up execution monitor
            monitor.setMonitor(requestId, producers);

            for (Entry<Integer, NodeMessage> entry : initMessages.entrySet()) {
                NodeMessage nodeMessage = entry.getValue();
                nodeMessage.setMessageType(NodeMessage.MSG_EXECUTE_STEP_RUN);
                sendHelper.sendMessage(entry.getKey(), nodeMessage);
            }

            // If this step runs on the coorinator and sends to the nodes,
            // go ahead and produce the data on the coordinator now.
            if (coordStepDetail != null && coordStepDetail.isProducer
                    && aStepDetail.isConsumer) {
                try {
                    coordinatorQueue.clear();
                    aQueryCombiner.queryOnCoordAndSendToNodes(
                            coordStepDetail.queryString, coordStepDetail,
                            new ProducerSender(sendHelper, coordinatorQueue));
                } catch (Exception e) {
                    logger.catching(e);
                    this.abortSQLCommand(nodeList, requestId);
                    XDBServerException ex = new XDBServerException(
                            "Step on coordinator failed " + e.getMessage(), e);
                    logger.throwing(ex);
                    throw ex;
                }
            }

            NodeMessage[] messages = null;
            do {
                NodeMessage msg;
                try {
                    // Due to a missed signal, we added poll here to avoid
                    // waiting on take() forever.
                    msg = sendDataMessages.poll(20,
                           java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    break;
                }
                if (msg == null) {
                    // May be monitor is aborted
                    messages = monitor.checkMessages();
                    continue;
                }
                if (msg.getMessageType() == NodeMessage.MSG_SEND_DATA) {
                    String[] rowData = msg.getRowData();
                    for (String element : rowData) {
                        aQueryCombiner.insertFromStatementOnCombiner(element);
                    }
                    NodeMessage reply = NodeMessage
                            .getNodeMessage(NodeMessage.MSG_SEND_DATA_ACK);
                    reply.setDataSeqNo(msg.getDataSeqNo());
                    reply.setRequestId(msg.getRequestId());
                    sendHelper.sendReplyMessage(msg, reply);
                } else {
                    // Check to see if it an informational message about sending
                    // rows
                    if (msg.getMessageType() == NodeMessage.MSG_BEGIN_SEND_ROWS) {
                        ActivityLog.startShipRows(client.getStatementId(), msg
                                .getSourceNodeID(), ((SendRowsMessage) msg)
                                .getDestNodeForRows());
                    } else if (msg.getMessageType() == NodeMessage.MSG_END_SEND_ROWS) {
                        ActivityLog.endShipRows(client.getStatementId(), msg
                                .getSourceNodeID(), ((SendRowsMessage) msg)
                                .getDestNodeForRows(), ((SendRowsMessage) msg)
                                .getNumRowsSent());
                    } else if (msg.getMessageType() != NodeMessage.MSG_ABORT) {
                        // If this is our error message monitor is already
                        // aborted
                        // and we quit by an exception in next line
                        // otherwise it is pretty safe to ignore it
                        monitor.register(msg);
                    }
                    messages = monitor.checkMessages();
                }
            } while (messages == null);

            if (coordStepDetail != null && coordStepDetail.isConsumer
                    && Props.XDB_DUMPSTEPPATH != null) {
                NodeThread.dumpStepResults(client
                        .getAndSetCoordinatorConnection(),
                        coordStepDetail.targetTable, 0);
            }

            if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD_FINAL) {
                // Put results to be unioned in finalResultSets,
                // they will be handled in QueryProcessor.

                for (NodeMessage message : messages) {
                    addFinalResultSet(message);
                    currentResultSetID = message.getResultSetID();
                }
            }

            if (coordStepDetail != null && coordStepDetail.isConsumer) {
                aQueryCombiner.finishInserts();
            }

            // Tell all NodeThreads that we are done producing, so they can
            // finish consuming
            if (aStepDetail != null && aStepDetail.isConsumer) {
                this.finishInserts();
            }
        }

        catch (XDBMessageMonitorException ex) {
            // If some node has done its step and error is occured on other,
            // first node won't be aborted, but will stay in DATADOWN mode
            // This will cause illegal state error when doing Rollback
            // So, Abort also nodes that are consumers
            HashSet<Integer> nodesToAbort = new HashSet<Integer>(ex
                    .getRemainingNodes());
            nodesToAbort.addAll(currentStepProcesses);
            abortSQLCommand(nodesToAbort, monitor.getRequestId());
            throw ex;
        } finally {
            if (Props.XDB_ENABLE_ACTIVITY_LOG) {

                for (NodeUsage aNodeUsageElement : nodeUsageTable.values()) {
                    // Add to consumer node thread list for finishing up later
                    if (aNodeUsageElement.isProducer) {
                        ActivityLog.endStep(client.getStatementId(),
                                aNodeUsageElement.nodeId);
                    }
                }
            }
            logger.exiting(method);
        }
    }

    // -----------------------------------------------------------------
    // This is based on the other (old) sendTableDown.
    // It is called when we combined results on the combiner node,
    // and we need to send the results back down to the nodes.
    // -----------------------------------------------------------------
    /**
     *
     * @param aStepDetail the node's step (not from coord)
     * @param targetNodeList destination node Ids
     * @param aQueryCombiner
     * @throws java.lang.Exception
     */
    public void sendTableDownDistinct(
            StepDetail aStepDetail,
            Collection<Integer> targetNodeList,
            QueryCombiner aQueryCombiner) throws Exception {
        final String method = "sendTableDownDistinct";
        logger.entering(method, new Object[] { aStepDetail, targetNodeList,
                aQueryCombiner });
        try {

            String queryString;

            // If the final destination is the coordinator itself,
            // it is already here, so don't bother sending!
            if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD) {
                return;
            }

            initInserts(aStepDetail.targetTable, targetNodeList);

            queryString = "SELECT DISTINCT * FROM " + aStepDetail.targetTable;
            coordinatorQueue.clear();
            aQueryCombiner.queryOnCoordAndSendToNodes(queryString, aStepDetail,
                    new ProducerSender(sendHelper, coordinatorQueue));

            finishInserts();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Returns node list
     *
     * @return
     */
    Collection<DBNode> getNodeList() {
        return Collections.unmodifiableCollection(nodeList);
    }

    /**
     *
     * @return
     */
    public Collection<? extends ResultSet> getFinalResultSets() {
        synchronized (finalRSs) {
            HashMap<Integer,NodeResultSetImpl> currentResults = finalRSs.get(currentResultSetID);
            currentResultSetID = null;
            return currentResults == null ? null : new ArrayList<NodeResultSetImpl>(currentResults.values());
        }
    }

    // ------------------------------------------------------------------------

    /**
     * This is to called as soon as we receive an abort message from one of the
     * nodes.
     *
     * We try and tell the other nodes to abort.
     *
     * @param nodeList
     * @param requestId
     *                The request we are trying to abort.
     */
    private void abortSQLCommand(Collection nodeList, int requestId) {
        sendHelper.sendMessageToList(nodeList, NodeMessage.MSG_ABORT, requestId);
    }

    /**
     * This method is normally called by thread listening for messages from
     * other nodes. Therefore we should synchronize access to all class members
     * and should not block execution here because it is preventing other
     * messages from receiving
     *
     * @see org.postgresql.stado.communication.IMessageListener#processMessage(org.postgresql.stado.communication.message.NodeMessage)
     * @param message
     * @return
     */
    public boolean processMessage(NodeMessage message) {
        final String method = "processMessage";
        logger.entering(method, new Object[] { message });
        try {

            switch (message.getMessageType()) {
            case NodeMessage.MSG_EXECUTE_BATCH_ACK:
            case NodeMessage.MSG_SEND_DATA_ACK:
                // Redirect data acknowlegements to ProducerSender
                // through coordinatorQueue
                coordinatorQueue.offer(message);
                break;
            case NodeMessage.MSG_ABORT:
                coordinatorQueue.offer(message);
                boolean consumed = monitor.abort(message);
                synchronized (finalRSs) {
                    for (Iterator<HashMap<Integer,NodeResultSetImpl>> it = finalRSs.values().iterator(); it.hasNext();) {
                        HashMap<Integer,NodeResultSetImpl> nodeResults = it.next();
                        for (Iterator<NodeResultSetImpl> it1 = nodeResults.values().iterator(); it1.hasNext(); ) {
                            if (it1.next().processMessage(message)) {
                                it1.remove();
                                consumed = true;
                            }
                        }
                        if (nodeResults.isEmpty()) {
                            it.remove();
                        }
                    }
                }
                // ensure the SendData loop is broken
                sendDataMessages.offer(message);
                return consumed;
            case NodeMessage.MSG_PING:
                sendHelper.sendReplyMessage(message, NodeMessage.MSG_PING_ACK);
                break;
            case NodeMessage.MSG_SEND_DATA:
            case NodeMessage.MSG_EXECUTE_STEP_SENT:
                sendDataMessages.offer(message);
                break;
            case NodeMessage.MSG_RESULT_ROWS:
                NodeResultSetImpl rs = null;
                synchronized (finalRSs) {
                    HashMap<Integer,NodeResultSetImpl> nodeResults = finalRSs.get(message.getResultSetID());
                    if (nodeResults != null) {
                        if (message.isResultSetHasMoreRows()) {
                            rs = nodeResults.get(message.getSourceNodeID());
                        } else {
                            rs = nodeResults.remove(message.getSourceNodeID());
                        }
                        if (nodeResults.isEmpty()) {
                            finalRSs.remove(message.getResultSetID());
                        }
                    }
                }

                if (rs != null) {
                    return rs.processMessage(message);
                }
                return false;
            case NodeMessage.MSG_RESULT_CLOSE:
                synchronized (finalRSs) {
                    HashMap<Integer,NodeResultSetImpl> nodeResults = finalRSs.get(message.getResultSetID());
                    if (nodeResults != null) {
                        nodeResults.remove(message.getSourceNodeID());
                        if (nodeResults.isEmpty()) {
                            finalRSs.remove(message.getResultSetID());
                        }
                    }
                }
                return true;
            default:
                monitor.register(message);
            }

            return true;

        } finally {
            logger.exiting(method);
        }
    }

    // -----------------------------------------------------------------------

    /**
     * Adds the command to the current batch
     *
     * @param sqlCommand
     * @param execNodeList
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    boolean addBatchOnNodeList(String sqlCommand,
            Collection<DBNode> execNodeList) throws XDBServerException {
        final String method = "addBatchOnNodeList";
        logger.entering(method, new Object[] { sqlCommand, execNodeList });
        try {

            if (aBatchHandler == null) {
                coordinatorQueue.clear();
                aBatchHandler = new BatchHandler(sendHelper, coordinatorQueue,
                        client);
            }

            return aBatchHandler.addToBatchOnNodeList(sqlCommand, execNodeList);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Adds the command to the current batch
     *
     * @param statements
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    boolean addBatchOnNodeList(Map<DBNode, String> statements)
            throws XDBServerException {
        final String method = "addBatchOnNodeList";
        logger.entering(method, new Object[] { statements });
        try {

            if (aBatchHandler == null) {
                coordinatorQueue.clear();
                aBatchHandler = new BatchHandler(sendHelper, coordinatorQueue,
                        client);
            }

            return aBatchHandler.addToBatchOnNodes(statements);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Adds the command to the current batch
     *
     * @param sqlCommand
     * @param nodeId
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    boolean addBatchOnNode(String sqlCommand, int nodeId)
            throws XDBServerException {
        final String method = "addBatchOnNode";
        logger.entering(method,
                new Object[] { sqlCommand, new Integer(nodeId) });
        try {

            if (aBatchHandler == null) {
                coordinatorQueue.clear();
                aBatchHandler = new BatchHandler(sendHelper, coordinatorQueue,
                        client);
            }

            return aBatchHandler.addToBatchOnNode(sqlCommand, nodeId);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Executes the batch, which executes batches on the appropriate underlying
     * nodes.
     *
     * @param strict
     * @return
     */
    int[] executeBatch(boolean strict) {
        final String method = "executeBatch";
        logger.entering(method);
        int[] batchResults = null;
        try {

            // No row were added
            if (aBatchHandler == null) {
                return new int[0];
            }

            try {
                batchResults = aBatchHandler.executeBatch(strict);
            } catch (XDBServerException xe) {
                abortSQLCommand(nodeList, aBatchHandler.getRequestId());
                throw xe;
            } finally {
                aBatchHandler.clearBatch();
            }

            // notifyPostExecutionOb();
            return batchResults;

        } finally {
            logger.exiting(method, batchResults);
        }
    }

    /**
     *
     */
    void clearBatch() {
        final String method = "clearBatch";
        logger.entering(method);
        try {

            if (aBatchHandler != null) {
                aBatchHandler.clearBatch();
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Try and kill current operation by sending "kill" signal to NodeTreads,
     * triggering exceptions there. Operation executor should handle the
     * exception (e.g. pass up an exception, rollback transaction, etc.)
     */
    void kill() {
        final String method = "kill";
        logger.entering(method, new Object[] {});
        try {

            // Normally this method is executed not in the session's thread,
            // and nodeList could be changed in meantime.
            // So, access is synchronized
            // Also, we do not need any response.
            synchronized (nodeList) {
                sendHelper.sendMessageToList(nodeList, NodeMessage.MSG_KILL, 0);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param tableName
     * @param address
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    void startLoaders(String tableName, String address)
            throws XDBServerException {
        final String method = "startLoaders";
        logger.entering(method);
        try {

            int requestId = client.getRequestId();
            NodeMessage startLoadersMsg = NodeMessage
                    .getNodeMessage(NodeMessage.MSG_START_LOADERS);
            startLoadersMsg.setTargetTable(tableName);
            startLoadersMsg.setSqlCommand(address);
            startLoadersMsg.setRequestId(requestId);
            monitor.setMonitor(requestId, getNodeList());
            try {
                for (Object element : nodeList) {
                    DBNode dbNode = (DBNode) element;
                    Node aNode = dbNode.getNode();
                    NodeMessage aCopy = (NodeMessage) startLoadersMsg.clone();
                    aCopy.setJdbcString(aNode.getSHost());
                    aCopy.setDatabase(aNode.getNodeDatabaseString(client
                            .getDBName()));
                    aCopy.setJdbcUser(aNode.getJdbcUser());
                    aCopy.setJdbcPassword(aNode.getJdbcPassword());
                    sendHelper.sendMessage(new Integer(dbNode.getNodeId()),
                            aCopy);
                }
            } finally {
                monitor.waitForMessages(SHORT_TIMEOUT);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @return
     * @param sqlCommand
     * @param execNodeList
     * @param autocommit
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public String prepareStatement(String commandID, String sqlCommand,
            int[] paramTypes, Collection<DBNode> execNodeList)
            throws XDBServerException {
        final String method = "prepareStatement";
        logger.entering(method, new Object[] { commandID, sqlCommand,
                paramTypes, execNodeList });

        try {
            String id = client.registerPreparedStatement(commandID,
                    execNodeList);
            try {

                int requestId = client.getRequestId();
                monitor.setMonitor(requestId, execNodeList);
                try {
                    NodeMessage aNodeMessage = NodeMessage
                            .getNodeMessage(NodeMessage.MSG_PREPARE_COMMAND);
                    aNodeMessage.addRowData(id);
                    aNodeMessage.addRowData(sqlCommand);
                    for (int type : paramTypes) {
                        aNodeMessage.addRowData("" + type);
                    }
                    aNodeMessage.setRequestId(requestId);
                    sendHelper.sendMessageToList(execNodeList, aNodeMessage);
                } finally {
                    monitor.waitForMessages();
                }
                return id;

            } catch (XDBServerException ex) {
                logger.catching(ex);
                // TODO close on nodes where command succeeded
                client.unregisterPreparedStatement(id);
                logger.throwing(ex);
                throw ex;
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param commandID
     * @param values
     * @return
     * @throws XDBServerException
     */
    public Collection<NodeMessage> executePreparedStatement(String commandID,
            String[] values, Collection<DBNode> execNodeList)
            throws XDBServerException {
        final String method = "executePrepared";
        logger.entering(method, new Object[] { commandID, values });

        Collection<NodeMessage> results;
        try {

            Collection<DBNode> psNodeList = client
                    .getPreparedStatementNodes(commandID);
            if (psNodeList == null) {
                throw new XDBServerException(
                        "Prepared statement does not exist");
            }
            if (!psNodeList.containsAll(execNodeList)) {
                throw new XDBServerException(
                        "Prepared statement does not exist on requested node(s)");
            }
            int requestId = client.getRequestId();
            monitor.setMonitor(requestId, execNodeList);

            try {
                NodeMessage aNodeMessage = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_PREPARE_COMMAND_EXEC);
                aNodeMessage.setRequestId(requestId);
                // RowData[0] - statement id, following are statement params
                aNodeMessage.addRowData(commandID);
                for (String value : values) {
                    aNodeMessage.addRowData(value);
                }
                sendHelper.sendMessageToList(execNodeList, aNodeMessage);
            } finally {
                results = Arrays.asList(monitor.waitForMessages());
            }
            // Register final result sets to be able to fetch rows
            for (NodeMessage message : results) {
                // Enable fetching rows
                if (message.getMessageType() == NodeMessage.MSG_EXEC_QUERY_RESULT) {
                    addFinalResultSet(message);
                }
            }
            return results;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param commandID
     * @param values
     * @return
     * @throws XDBServerException
     */
    public int executePreparedCommand(String commandID, String[] values,
            Collection<DBNode> execNodeList) throws XDBServerException {
        Collection<NodeMessage> responses = executePreparedStatement(commandID,
                values, execNodeList);
        int rowcount = 0;
        for (NodeMessage msg : responses) {
            rowcount += msg.getNumRowsResult();
        }
        return rowcount;
    }

    /**
     *
     * @return
     * @param sqlCommand
     * @param execNodeList
     * @param autocommit
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void closePrepared(String commandID) throws XDBServerException {
        final String method = "closePrepared";
        logger.entering(method, new Object[] { commandID });

        Collection<DBNode> execNodeList = null;
        try {

            execNodeList = client.getPreparedStatementNodes(commandID);
            if (execNodeList == null) {
                throw new XDBServerException("Prepared statement is not found");
            }

            int requestId = client.getRequestId();
            monitor.setMonitor(requestId, execNodeList);

            try {
                NodeMessage aNodeMessage = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_PREPARE_COMMAND_CLOSE);
                aNodeMessage.setSqlCommand(commandID);
                aNodeMessage.setRequestId(requestId);
                sendHelper.sendMessageToList(execNodeList, aNodeMessage);
            } finally {
                monitor.waitForMessages();
            }

        } finally {
            logger.exiting(method);
        }
    }

}
