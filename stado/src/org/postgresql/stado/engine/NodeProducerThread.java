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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLevel;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.SendMessageHelper;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.engine.copy.CopyManager;
import org.postgresql.stado.engine.copy.CopyOut;
import org.postgresql.stado.engine.io.DataTypes;
import org.postgresql.stado.engine.io.ResponseMessage;
import org.postgresql.stado.engine.io.ResultSetResponse;
import org.postgresql.stado.engine.io.XMessage;
import org.postgresql.stado.exception.XDBBaseException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.exception.XDBUnexpectedMessageException;
import org.postgresql.stado.exception.XDBUnexpectedStateException;
import org.postgresql.stado.exception.XDBWrappedException;
import org.postgresql.stado.exception.XDBWrappedSQLException;
import org.postgresql.stado.misc.Timer;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.planner.StepDetail;

/**
 * This thread is paired with a NodeThread, and handles producing the results
 * from a query step.
 *
 *
 */
public class NodeProducerThread implements Runnable {
    private static final XLogger logger = XLogger
    .getLogger(NodeProducerThread.class);

    public static final XLogger CATEGORY_NODEQUERYTIME = XLogger
    .getLogger("nodequerytime");

    /*
     * For supporting multiple ResultSets
     */
    class QueryResult {
        ResultSetResponse rsResponse;

        ResultSet aResultSet;

        NodeMessage nextMessage = null;

        /**
         *
         * @param rsResponse
         */
        public QueryResult(ResultSetResponse rsResponse) {
            this.rsResponse = rsResponse;
        }

        /**
         *
         * @param msgType
         * @throws java.sql.SQLException
         * @return
         */
        private NodeMessage packNextResultRows(int msgType) throws SQLException {
            final String method = "packNextResultRows";
            logger.entering(method, new Object[] { msgType });
            try {

                if (rsResponse == null) {
                    return null;
                }

                NodeMessage.CATEGORY_MESSAGE.debug("Node " + nodeId
                        + ": Packing results");
                ResponseMessage rm = rsResponse.nextResults(rsResponse
                        .getFetchSize());
                byte[] resultSetData = new byte[rm.getPacketLength()];
                System.arraycopy(rm.getHeaderBytes(), 0, resultSetData, 0,
                        XMessage.HEADER_SIZE);
                System.arraycopy(rm.getMessage(), 0, resultSetData,
                        XMessage.HEADER_SIZE, rm.getPacketLength()
                        - XMessage.HEADER_SIZE);
                nextMessage = NodeMessage.getNodeMessage(msgType);
                nextMessage.setResultSetData(resultSetData);
                boolean lastPacket = rm.getMessage()[0] == ResultSetResponse.LAST_PACKET_TRUE;
                logger
                .log(
                        Level.INFO,
                        "Node %0% sends new ResultSet packet, size: %1%, last: %2%",
                        new Object[] { new Integer(nodeId),
                                new Integer(resultSetData.length),
                                new Boolean(lastPacket) });
                if (lastPacket) {
                    closeResultSet(aResultSet);
                    rsResponse = null;
                }
                nextMessage.setResultSetHasMoreRows(!lastPacket);
                return nextMessage;
            } finally {
                logger.exiting(method);
            }
        }
    }

    protected static final int STATEDISCONNECTED = -1; // don't do anything

    protected static final int STATEWAIT = 0; // don't do anything

    protected static final int STATEDONE = 5;

    protected static final int STATESTEP = 7;

    protected static final int STATEDATADOWN = 8; // sending data down

    private int currentState;

    private volatile int requestIDtoAbort = -1;

    private Connection oConn; // = null;

    // These are for preparing inserts
    int iColumnCount;

    private NodeThread parent;

    int nodeId;

    private SendMessageHelper sendHelper;

    int procCount = 0;

    private LinkedBlockingQueue<NodeMessage> producerQueue;

    private HashMap<String, QueryResult> resultTable = new HashMap<String, QueryResult>();

    /** For tracking prepared statements */
    private HashMap<String, PreparedStatementEx> preparedStatementTable = new HashMap<String, PreparedStatementEx>();

    private ProducerSender aProducerSender = null;
    
    // to help with canceling statements, use for the last statement
    private Statement aStatement = null;
    
    private CopyOut copyOut = null;

    /**
     * Creates a new instance of NodeProducerThread
     *
     * @param parent
     * @param producerQueue
     */

    // ----------------------------------------------------------------------
    public NodeProducerThread(NodeThread parent,
            LinkedBlockingQueue<NodeMessage> producerQueue) {
        final String method = "NodeProducerThread";
        logger.entering(method, new Object[] { parent, producerQueue });
        try {

            this.parent = parent;
            nodeId = parent.getNodeId();
            this.producerQueue = producerQueue;

            currentState = STATEDISCONNECTED;
            resultTable = new HashMap<String, QueryResult>();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    public synchronized void reset() throws XDBBaseException {
        final String method = "reset";
        logger.entering(method);
        try {

            if (aProducerSender != null) {
                aProducerSender.setAbort(true);
            }                     
            // Block reset procedure until NPT finish current job
            for (int i = 0; currentState != STATEDISCONNECTED
            && currentState != STATEWAIT && currentState != STATEDONE
            && i <= 20; i++) {
                try {
                    wait(100);
                } catch (InterruptedException ie) {
                    logger.catching(ie);
                }
            }

            for (int i = 0; currentState != STATEDISCONNECTED
            && currentState != STATEWAIT && currentState != STATEDONE
            && i <= 20; i++) {
                kill();
                try {
                    wait(500);
                } catch (InterruptedException ie) {
                    logger.catching(ie);
                }
            }
   
            if (currentState == STATEDONE) {
                // Restart instead of throwing ???
                XDBUnexpectedStateException ex = new XDBUnexpectedStateException(
                        nodeId, STATEDONE, new int[] { STATEWAIT,
                                STATEDISCONNECTED /* , STATEABORT */});
                logger.throwing(ex);
                throw ex;
            }

            oConn = parent.getConnection();
            sendHelper = parent.getSendHelper();

            if (oConn == null || sendHelper == null) {
                setState(STATEDISCONNECTED);
                closeAllResultSets();
            } else {
                setState(STATEWAIT);
            }

            requestIDtoAbort = -1;

        } finally {
            aStatement = null;
            copyOut = null;
            logger.exiting(method);
        }
    }

    // This is used to set the new state
    // ----------------------------------------------------------------------
    /**
     *
     * @param newState
     */
    private synchronized void setState(int newState) {
        final String method = "setState";
        logger.entering(method, new Object[] { new Integer(newState) });
        try {

            currentState = newState;
            notify();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param allowed
     * @param newState
     * @throws org.postgresql.stado.exception.XDBUnexpectedStateException
     * @return
     */
    private synchronized int checkCurrentState(int[] allowed, int newState)
    throws XDBUnexpectedStateException {
        for (int element : allowed) {
            if (currentState == element) {
                setState(newState);
                return element;
            }
        }
        XDBUnexpectedStateException ex = new XDBUnexpectedStateException(
                nodeId, currentState, allowed);
        logger.throwing(ex);
        throw ex;
    }

    // ------------------------------------------------------------------------
    public void run() {
        final String method = "run";
        logger.entering(method);
        try {

            NodeMessage aNodeMessage = null;
            while (true) {
                try {
                    // We wait on our message queue for instructions on what to
                    // do
                    aNodeMessage = producerQueue.poll(5,
                            java.util.concurrent.TimeUnit.SECONDS);
                    if (aNodeMessage == null) {
                        continue;
                    }

                    NodeMessage.CATEGORY_MESSAGE
                    .debug("NPT.run(): received message: "
                            + aNodeMessage);

                    switch (aNodeMessage.getMessageType()) {
                    case NodeMessage.MSG_EXECUTE_STEP_RUN:
                        checkCurrentState(new int[] { STATEWAIT }, STATESTEP);
                        try {
                            ResultSet aResultSet = processStep(aNodeMessage);
                            QueryResult aQueryResult = null;

                            CATEGORY_NODEQUERYTIME
                            .debug("Node " + nodeId
                                    + ": Step is done, ResulSet: "
                                    + aResultSet);
                            if (aNodeMessage.getStepDetail().getDestType() == StepDetail.DEST_TYPE_COORD_FINAL) {
                                String queryID = String.valueOf(aNodeMessage
                                        .getRequestId());
                                aQueryResult = new QueryResult(
                                        new ResultSetResponse(0, null,
                                                aResultSet));
                                aQueryResult
                                .packNextResultRows(NodeMessage.MSG_EXECUTE_STEP_SENT);
                                aQueryResult.nextMessage
                                .setResultSetID(queryID);

                                if (!aQueryResult.nextMessage
                                        .isResultSetHasMoreRows()) {
                                    closeResultSet(aQueryResult.aResultSet);
                                }
                                // send first group of results
                                sendHelper.sendReplyMessage(aNodeMessage,
                                        aQueryResult.nextMessage);
                                // If this is not the last message,
                                // we are going to have to track it
                                if (aQueryResult.nextMessage
                                        .isResultSetHasMoreRows()) {
                                    resultTable.put(queryID, aQueryResult);
                                    // for more parallelism, go ahead and
                                    // prepare
                                    // next message, too.
                                    aQueryResult
                                    .packNextResultRows(NodeMessage.MSG_RESULT_ROWS);
                                }
                            } else {
                                // Be sure to close the statement, otherwise
                                // it may still be locking the table.
                                NodeMessage reply = NodeMessage
                                .getNodeMessage(NodeMessage.MSG_EXECUTE_STEP_SENT);

                                sendHelper
                                .sendReplyMessage(aNodeMessage, reply);
                            }
                        } finally {
                            setState(STATEWAIT);
                        }

                        break;

                    case NodeMessage.MSG_EXEC_COMMAND:
                    case NodeMessage.MSG_EXEC_QUERY:
                        checkCurrentState(new int[] { STATEWAIT }, STATESTEP);
                        try {
                            try {
                                executeCommand(aNodeMessage);
                            } catch (SQLException se) {
                                parent.handleSqlException(se, aNodeMessage
                                        .getSqlCommand());
                                try {
                                    executeCommand(aNodeMessage);
                                } catch (SQLException se1) {
                                    logger.catching(se1);
                                    XDBWrappedSQLException ex = new XDBWrappedSQLException(
                                            nodeId, se1);
                                    logger.throwing(ex);
                                    throw ex;
                                }
                            }
                        } finally {
                            setState(STATEWAIT);
                        }

                        break;

                    case NodeMessage.MSG_DROP_TEMP_TABLES:
                        checkCurrentState(new int[] { STATEWAIT }, STATESTEP);
                        try {
                            dropTempTables(aNodeMessage.getTempTables(),
                                    aNodeMessage.getAutocommit());
                            sendHelper.sendReplyMessage(aNodeMessage,
                                    NodeMessage.MSG_DROP_TEMP_TABLES_ACK);
                        } finally {
                            setState(STATEWAIT);
                        }
                        break;

                    case NodeMessage.MSG_STOP_THREAD:

                        setState(STATEDONE);
                        return;

                    case NodeMessage.MSG_ABORT:

                        if (aProducerSender != null) {
                            aProducerSender.setAbort(true);
                        }
                        requestIDtoAbort = aNodeMessage.getRequestId();
                        // setState(STATEABORT);
                        break;

                    case NodeMessage.MSG_RESULT_ROWS_REQUEST:
                        checkCurrentState(new int[] { STATEWAIT },
                                STATEDATADOWN);
                        try {
                            String resultSetId = aNodeMessage.getResultSetID();
                            QueryResult aQueryResult = resultTable
                            .get(resultSetId);
                            if (aQueryResult == null
                                    || aQueryResult.nextMessage == null) {
                                XDBBaseException ex = new XDBUnexpectedMessageException(
                                        nodeId, "No more rows available",
                                        aNodeMessage);

                                // Switch to STATEABORT if we are still
                                // connected and notify sender
                                if (sendHelper != null) {
                                    requestIDtoAbort = aNodeMessage
                                    .getRequestId();
                                    NodeMessage msg = NodeMessage
                                    .getNodeMessage(NodeMessage.MSG_ABORT);
                                    msg.setCause(ex);
                                    msg.setRequestId(requestIDtoAbort);
                                    sendHelper.sendReplyMessage(aNodeMessage,
                                            msg);
                                    break;
                                }
                                resultTable.remove(resultSetId);
                            }
                            if (!aQueryResult.nextMessage
                                    .isResultSetHasMoreRows()) {
                                // Be sure to close the statement, otherwise
                                // it may still be locking the table.
                                closeResultSet(aQueryResult.aResultSet);
                            }
                            aQueryResult.nextMessage
                            .setResultSetID(resultSetId);
                            // send results that we previously prepared
                            sendHelper.sendReplyMessage(aNodeMessage,
                                    aQueryResult.nextMessage);

                            // try and prepare next set of data
                            if (aQueryResult.nextMessage
                                    .isResultSetHasMoreRows()) {
                                aQueryResult
                                .packNextResultRows(NodeMessage.MSG_RESULT_ROWS);
                            } else {
                                resultTable.remove(resultSetId);
                            }
                        } finally {
                            setState(STATEWAIT);
                        }
                        break;
                    case NodeMessage.MSG_RESULT_CLOSE:
                        checkCurrentState(new int[] { STATEWAIT },
                                STATEDATADOWN);
                        try {
                            String resultSetId = aNodeMessage.getResultSetID();
                            QueryResult query = resultTable.remove(resultSetId);
                            if (query != null) {
                                closeResultSet(query.aResultSet);
                            }
                        } finally {
                            setState(STATEWAIT);
                        }
                        break;
                    case NodeMessage.MSG_PREPARE_COMMAND:
                        String[] data = aNodeMessage.getRowData();
                        String key = data[0];
                        String command = data[1];
                        int[] types = new int[data.length - 2];
                        for (int i = 0; i < types.length; i++) {
                            types[i] = Integer.parseInt(data[i + 2]);
                        }
                        preparedStatementTable.put(key,
                                new PreparedStatementEx(oConn
                                        .prepareStatement(command), types));
                        sendHelper.sendReplyMessage(aNodeMessage,
                                NodeMessage.MSG_PREPARE_COMMAND_ACK);
                        break;
                    case NodeMessage.MSG_PREPARE_COMMAND_EXEC:
                        data = aNodeMessage.getRowData();
                        PreparedStatementEx pse = preparedStatementTable
                        .get(data[0]);
                        if (pse == null) {
                            throw new XDBServerException(
                                    "Prepared statement does not exist");
                        }
                        for (int i = 1; i < data.length; i++) {
                            DataTypes.setParameter(pse.ps, i, data[i],
                                    pse.paramTypes[i - 1]);
                        }
                        handleExecutionResult(pse.ps.execute(), pse.ps,
                                aNodeMessage);
                        break;
                    case NodeMessage.MSG_PREPARE_COMMAND_CLOSE:
                        key = aNodeMessage.getSqlCommand();
                        pse = preparedStatementTable.remove(key);
                        pse.ps.close();
                        sendHelper.sendReplyMessage(aNodeMessage,
                                NodeMessage.MSG_PREPARE_COMMAND_CLOSE_ACK);
                        break;
                    }
                }
                /*
                 * catch (XDBUnexpectedStateException ex) { // Most probably NT
                 * has unexpected state synchronized (this) {
                 * logger.catching(ex); // Switch to STATEABORT if we are still
                 * connected and notify sender if (sendHelper != null) {
                 * currentState = STATEABORT; NodeMessage msg = new
                 * NodeMessage(NodeMessage.MSG_ABORT); msg.setCause(new
                 * XDBUnexpectedMessageException( nodeId, "Node Thread has
                 * illegal state, could not process message", aNodeMessage));
                 * sendHelper.sendReplyMessage(aNodeMessage, msg); } } }
                 */
                catch (XDBBaseException ex) {
                    synchronized (this) {
                        logger.catching(ex);
                        // Switch to STATEABORT if we are still connected and
                        // notify sender
                        if (sendHelper != null) {
                            requestIDtoAbort = aNodeMessage.getRequestId();
                            NodeMessage msg = NodeMessage
                            .getNodeMessage(NodeMessage.MSG_ABORT);
                            msg.setCause(ex);
                            sendHelper.sendReplyMessage(aNodeMessage, msg);
                        }
                    }
                } catch (Throwable t) {
                    synchronized (this) {
                        logger.catching(t);
                        // Switch to STATEABORT if we are still connected and
                        // notify sender
                        if (sendHelper != null) {
                            requestIDtoAbort = aNodeMessage.getRequestId();
                            NodeMessage msg = NodeMessage
                            .getNodeMessage(NodeMessage.MSG_ABORT);
                            msg.setCause(new XDBWrappedException(nodeId, t));
                            sendHelper.sendReplyMessage(aNodeMessage, msg);
                        }
                    }
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * Initializes array of types and the base insert string that we use
     */

    /**
     *
     */
    private synchronized void closeAllResultSets() {
        if (resultTable != null) {
            for (QueryResult aQueryResult : resultTable.values()) {
                closeResultSet(aQueryResult.aResultSet);
                resultTable.remove(aQueryResult);
            }
        }
    }

    /**
     *
     * @param aResultSet
     */
    private synchronized void closeResultSet(ResultSet aResultSet) {
        final String method = "closeResultSet";
        logger.entering(method);
        try {
            if (currentState == STATEDISCONNECTED) {
                return;
            }

            synchronized (oConn) {
                logger.log(XLevel.TRACE,
                        "Entered critical section for connection: %0%",
                        new Object[] { oConn });
                if (aResultSet != null) {
                    aResultSet.getStatement().close();
                } else {
                    logger
                    .log(
                            XLevel.TRACE,
                            "For connection: %0% current result set is Null, nothing to do",
                            new Object[] { oConn });
                }
                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { oConn });
                oConn.notify();
            }
        } catch (Exception e) {
            logger.catching(e);
        } finally {
            logger.exiting(method);
        }
    }


    /**
     * processStep here is for an individual step in a SELECT
     * processStatement handles non SELECT statements.

     * @param aNodeMessage
     * @throws org.postgresql.stado.exception.XDBBaseException
     * @return
     */
    private ResultSet processStep(NodeMessage aNodeMessage)
    throws XDBBaseException {
        final String method = "processStep";
        logger.entering(method);
        
        // Make sure NodeThread does not try and rollback while we are 
        // processing, put in synchronized block
        synchronized(parent.nptBusy) {
            try {

                StepDetail aStepDetail = aNodeMessage.getStepDetail();
                ResultSet aResultSet = null;
                copyOut = null;

                try {

                    if (Props.XDB_USE_COPY_OUT_FOR_STEP && aStepDetail.getDestType() != StepDetail.DEST_TYPE_COORD_FINAL) {
                            CopyManager copyManager = CopyManager.getCopyManager(oConn);
                            copyOut = copyManager.copyOut("copy (" + aStepDetail.queryString + ") to stdout"); 
                            aProducerSender = new ProducerSender(sendHelper, producerQueue);

                        try {
                            aProducerSender.sendToNodes(copyOut, aStepDetail, oConn,
                                    nodeId, aNodeMessage.getRequestId());

                        } catch (Exception e) {
                            // Try and clean things up if COPY failed.
                            // If it was manually cancelled, we cleanup
                            if (oConn != null) {
                                synchronized (oConn) {
                                    try {
                                        copyOut.cancelCopyFinish();
                                    } catch (IOException ioe) {
                                        // if we get here we may want to 
                                        // consider forcibly closing the connection
                                        // if there are still problems
                                        oConn.notify();
                                        throw new XDBServerException(e.getLocalizedMessage());
                                    }
                                }
                            }
                        } finally {        
                            copyOut = null;
                        }
                    } else {
                            try {
                                aResultSet = executeQuery(aStepDetail.queryString);
                            } catch (SQLException se) {
                                parent.handleSqlException(se, aStepDetail.queryString);
                                try {
                                    aResultSet = executeQuery(aStepDetail.queryString);
                                } catch (SQLException se1) {
                                    logger.catching(se1);
                                    XDBWrappedSQLException ex = new XDBWrappedSQLException(
                                            nodeId, se1);
                                    logger.throwing(ex);
                                    throw ex;
                                }
                            }

                            // Don't send data if we are on the final step
                            if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD_FINAL) {
                                return aResultSet;
                            }

                            logger.trace(" Ready to iterate results at node");

                            aProducerSender = new ProducerSender(sendHelper, producerQueue);
                            aProducerSender.sendToNodes(aResultSet, aStepDetail, oConn,
                                    nodeId, aNodeMessage.getRequestId());
                    }

                    dropTempTables(aStepDetail.dropList, true);
                } catch (Exception e) {
                    logger.catching(e);
                    try {
                        dropTempTables(aStepDetail.dropList, true);
                    } catch (Exception e1) {
                        logger.catching(e1);
                    }
                    XDBBaseException ex;
                    if (e instanceof XDBBaseException) {
                        ex = (XDBBaseException) e;
                    } else if (e instanceof SQLException) {
                        ex = new XDBWrappedSQLException(nodeId, (SQLException) e);
                    } else {
                        ex = new XDBWrappedException(nodeId, e);
                    }
                    // Interrupt all the consumers
                    if (aStepDetail.consumerNodeList != null) {
                        requestIDtoAbort = aNodeMessage.getRequestId();
                        NodeMessage abort = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_ABORT);
                        abort.setRequestId(requestIDtoAbort);
                        abort.setCause(ex);
                        sendHelper.sendMessageToList(aStepDetail.consumerNodeList,
                                abort);
                    }

                    logger.throwing(ex);
                    throw ex;
                }
                return aResultSet;
            } finally {
                aProducerSender = null;
                parent.nptBusy.notify();
                logger.exiting(method);
            }
        }
    }

    // -------------------------------------------------------------------------

    /**
     *
     * @return
     * @param queryString
     * @throws SQLException
     */
    private ResultSet executeQuery(String queryString) throws SQLException {
        ResultSet aResultSet = null;
        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });
            aStatement = oConn.createStatement();
            aStatement.setFetchSize(Props.XDB_NODEFETCHSIZE);
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
            oConn.notify();
        }

        Timer qTimer = new Timer();

        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });
            qTimer.startTimer();
            aResultSet = aStatement.executeQuery(queryString);
            qTimer.stopTimer();
            oConn.notify();
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
        }

        NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Node: " + nodeId
                + " Duration: " + qTimer + " : " + queryString);
        return aResultSet;
    }

    /**
     *
     * @param aNodeMessage
     * @throws java.sql.SQLException
     */
    private void executeCommand(NodeMessage aNodeMessage) throws SQLException {
        Statement aStatement = null;
        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });
            aStatement = oConn.createStatement();
            aStatement.setFetchSize(Props.XDB_NODEFETCHSIZE);
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
            oConn.notify();
        }

        Timer qTimer = new Timer();
        boolean resultset;
        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });
            qTimer.startTimer();
            if (aNodeMessage.getAutocommit()) {
                oConn.setAutoCommit(true);
            }
            try {
                resultset = aStatement.execute(aNodeMessage.getSqlCommand());
            } finally {
                if (aNodeMessage.getAutocommit()) {
                    oConn.setAutoCommit(false);
                }
            }
            qTimer.stopTimer();
            oConn.notify();
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
        }

        NodeProducerThread.CATEGORY_NODEQUERYTIME
        .debug("Node: " + nodeId + " Duration: " + qTimer + " : "
                + aNodeMessage.getSqlCommand());
        handleExecutionResult(resultset, aStatement, aNodeMessage);
    }

    /**
     *
     * @param resultset
     * @param aStatement
     * @param aNodeMessage
     * @throws SQLException
     */
    private void handleExecutionResult(boolean resultset, Statement aStatement,
            NodeMessage aNodeMessage) throws SQLException {
        if (resultset) {
            ResultSet aResultSet = aStatement.getResultSet();
            String queryID = String.valueOf(aNodeMessage.getRequestId());
            // Try and get metadata from backend, providing not null ResultSetID
            QueryResult aQueryResult = new QueryResult(new ResultSetResponse(0,
                    "", aResultSet));
            aQueryResult.packNextResultRows(NodeMessage.MSG_EXEC_QUERY_RESULT);
            aQueryResult.nextMessage.setResultSetID(queryID);
            // If this is not the last message,
            // we are going to have to track it
            if (aQueryResult.nextMessage.isResultSetHasMoreRows()) {
                resultTable.put(queryID, aQueryResult);
            } else {
                // Be sure to close the statement, otherwise
                // it may block other statements
                aResultSet.close();
            }
            // send first group of results
            sendHelper.sendReplyMessage(aNodeMessage, aQueryResult.nextMessage);
            // for more parallelism, go ahead and prepare
            // next message, too.
            if (aQueryResult.nextMessage.isResultSetHasMoreRows()) {
                aQueryResult.packNextResultRows(NodeMessage.MSG_RESULT_ROWS);
            }
        } else {
            NodeMessage responseMsg = NodeMessage
            .getNodeMessage(NodeMessage.MSG_EXEC_COMMAND_RESULT);
            responseMsg.setNumRowsResult(aStatement.getUpdateCount());
            sendHelper.sendReplyMessage(aNodeMessage, responseMsg);
        }
    }

    /**
     * This is for dropping tables no longer needed after a step.
     *
     * @param dropList
     */
    private void dropTempTables(Collection dropList, boolean autocommit) {
        final String method = "dropTempTables";
        logger.entering(method);
        try {

            for (Iterator it = dropList.iterator(); it.hasNext();) {
                String dropName = (String) it.next();

                // ignore error -- We are purposefully ignoring the error
                try {
                    processSqlCommand("DROP TABLE " + IdentifierHandler.quote(dropName), false);
                    synchronized (parent.tempTableNames) {
                        parent.tempTableNames.remove(dropName);
                    }
                } catch (Exception e) {
                    if (oConn != null) {
                        synchronized (oConn) {
                            try {
                                oConn.rollback();
                            } catch (SQLException se) {
                            } finally {
                                oConn.notify();
                            }
                        }
                    }
                    logger.catching(e);
                }
            }
            if (autocommit) {
                try {
                    oConn.commit();
                } catch (SQLException se) {
                    try {
                        oConn.rollback();
                    } catch (SQLException se2) {
                    }
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    // -------------------------------------------------------------------------
    /**
     *
     * @param sqlCommandStr
     * @param commitCommand
     * @throws org.postgresql.stado.exception.XDBBaseException
     * @return
     */
    private int processSqlCommand(String sqlCommandStr, boolean commitCommand)
    throws XDBBaseException {
        final String method = "processSqlCommand";
        logger.entering(method, new Object[] { sqlCommandStr });
        Statement aStatement = null;
        int rowsAffected = 0;
        try {

            synchronized (oConn) {
                logger.log(XLevel.TRACE,
                        "Entered critical section for connection: %0%",
                        new Object[] { oConn });
                aStatement = oConn.createStatement();
                try {
                    rowsAffected = aStatement.executeUpdate(sqlCommandStr);
                    if (commitCommand) {
                        oConn.commit();
                    }
                } finally {
                    aStatement.close();
                    aStatement = null;
                }
                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { oConn });
                oConn.notify();
            }
            return rowsAffected;

        } catch (SQLException se) {
            logger.catching(se);
            XDBWrappedSQLException ex = new XDBWrappedSQLException(nodeId, se);
            logger.throwing(ex);
            throw ex;
        } finally {
            logger.exiting(method, new Integer(rowsAffected));
        }
    }

    /**
     *
     */
    public void kill() {
        final String method = "kill";
        logger.entering(method);
        try {

            if (aProducerSender != null) {
                aProducerSender.setAbort(true);
            }

            try {
                logger.log(XLevel.TRACE, "Canceling connection %0%",
                        new Object[] { oConn });

                for (QueryResult aQueryResult : resultTable.values()) {
                    if (aQueryResult.aResultSet != null) {
                        Statement queryStatement = aQueryResult.aResultSet
                        .getStatement();
                        if (queryStatement == null) {
                            aQueryResult.aResultSet.close();
                        } else {
                            queryStatement.cancel();
                        }
                    }
                }
                resultTable.clear();
                
                // The above may not have found the currently executing statement
                if (aStatement != null) {
                    aStatement.cancel();
                } 
                
                // Original thread We may also be in the middle of using COPY
                // It is up to the original NPT thread to cleanup afterward,
                // after we cancel its COPY
                if (copyOut != null) {
                    try {
                        copyOut.cancelCopy();
                    } catch (Exception e) { 
                        // ok to ignore, but lets log it
                        logger.log(XLevel.DEBUG, "CopyOut Exception " + e.getLocalizedMessage() + "%0%", 
                                new Object[] { copyOut });                   
                    }
                }

                logger.log(XLevel.TRACE, "Canceled connection %0%",
                        new Object[] { oConn });
            } catch (Throwable e) {
                logger.catching(e);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Get current state of thread
     *
     * @return
     */
    protected int getState() {
        return currentState;
    }

    private class PreparedStatementEx {
        PreparedStatement ps;

        int[] paramTypes;

        PreparedStatementEx(PreparedStatement ps, int[] paramTypes) {
            this.ps = ps;
            this.paramTypes = paramTypes;
        }
    }
}
