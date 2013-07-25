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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLevel;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.IMessageListener;
import org.postgresql.stado.communication.NodeAgent;
import org.postgresql.stado.communication.SendMessageHelper;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBBaseException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.exception.XDBUnexpectedMessageException;
import org.postgresql.stado.exception.XDBUnexpectedStateException;
import org.postgresql.stado.exception.XDBWrappedException;
import org.postgresql.stado.exception.XDBWrappedSQLException;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.planner.StepDetail;



//----------------------------------------------------------------
// Class handles being a thread that can execute multiple SQL
// commands for a thread that is associated with a single node,
// commiting transactions at the end.
//----------------------------------------------------------------
public class NodeThread implements Runnable, IMessageListener {
    private static final XLogger logger = XLogger.getLogger(NodeThread.class);

    private static final boolean USEBATCHES = true;

    private static final boolean AUTOCOMMIT = false;

    private static final int BATCHSIZE = 1000;

    private static final int STATEDONE = -3; // Thread destroyed

    // private static final int STATEABORT = -2; //accept nothing, only reset

    private static final int STATEDISCONNECTED = -1; // don't do anything

    private static final int STATEWAIT = 0; // don't do anything

    private static final int STATECOMMAND = 1;

    private static final int STATEBEGINTRAN = 2;

    private static final int STATECOMMIT = 3;

    private static final int STATEROLLBACK = 4;

    private static final int STATEENDTRAN = 5;

    private static final int STATESTEP = 8;

    private static final int STATEDATADOWN = 9; // sending data down

    private static final int STATESTEPEND = 10; // sending data down

    private static final int freeInterval = Property.getInt(
            "xdb.memory.freeinterval", 0);

    private int currentState;

    private int nodeId;

    private volatile int requestIDtoAbort = -1;

    /**
     * Temporarily used here until we add networking to contain all of the other
     * nodeThreads' message Queues
     */
    private StepDetail aStepDetail;

    private Connection oConn; // = null;

    private LinkedBlockingQueue<NodeMessage> msgQueue = new LinkedBlockingQueue<NodeMessage>();

    private LinkedBlockingQueue<NodeMessage> producerQueue = new LinkedBlockingQueue<NodeMessage>();

    private Statement batchStatement = null;

    private int iCount;

    private String downTableName;

    // Avoid some string concatenations
    private String baseInsert;

    private JDBCPool currentPool;

    private SendMessageHelper sendHelper;

    /** For tracking savepoints */
    private HashMap<String, Savepoint> savepointTable;

    /* For tracking temp tables */
    HashSet<String> tempTableNames = new HashSet<String>();

    private volatile Statement aStatement = null;

    /** whether or not we are waiting to take from queue */
    private boolean isTaking;

    /** helps debug */
    private int lastMessageType = -1;

    final Object nptBusy;
    
    /**
     * create a NodeThread object to be used in NodeThreadPool.
     *
     * @see org.postgresql.stado.engine.NodeThreadPool
     * @param nodeID
     */
    public NodeThread(int nodeID) {
        final String method = "NodeThread";
        logger.entering(method, new Object[] { new Integer(nodeID) });
        try {
            savepointTable = new HashMap<String, Savepoint>();
            this.nodeId = nodeID;
            currentState = STATEDISCONNECTED;
            nptBusy = new Object();
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * resets the thread state to statewait
     *
     * @param sessionID
     * @param pool
     * @throws XDBBaseException
     */
    public synchronized void reset(Integer sessionID, JDBCPool pool)
            throws XDBBaseException {
        final String method = "reset";
        logger.entering(method, new Object[] { sessionID, pool });
        logger.trace(" State = " + currentState + " lastMessageType = "
                + NodeMessage.getMessageTypeString(lastMessageType)
                + " isTaking = " + isTaking);
        try {
            for (int i = 0; currentState != STATEDISCONNECTED
                    && currentState != STATEWAIT && currentState != STATEDONE
                    // && currentState != STATEABORT
                    && i <= 10; i++) {
                try {
                    wait(100);
                } catch (InterruptedException ie) {
                    logger.catching(ie);
                }
            }

            // Block reset procedure until NPT finish current job
            for (int i = 0; currentState != STATEDISCONNECTED
                    && currentState != STATEWAIT
                    // && currentState != STATEABORT
                    && currentState != STATEDONE; i++) {
                // try to cancel locked statement
                XLogger.getLogger("Server").warn(
                        " NodeThread in inconsistent state during reset. "
                                + " State = " + currentState);

                kill();

                try {
                    wait(500);
                } catch (InterruptedException ie) {
                    logger.catching(ie);
                }
                if (i >= 6) {
                    XDBUnexpectedStateException ex = new XDBUnexpectedStateException(
                            nodeId, currentState, new int[] { STATEWAIT,
                                    STATEDISCONNECTED /*
                                                         * , STATEABORT
                                                         */});
                    logger.throwing(ex);
                    throw ex;
                }
            }

            if (currentState == STATEDONE) {
                // Restart instead of throwing ???
                XDBUnexpectedStateException ex = new XDBUnexpectedStateException(
                        nodeId, STATEDONE, new int[] { STATEWAIT,
                                STATEDISCONNECTED /*
                                                     * , STATEABORT
                                                     */});
                logger.throwing(ex);
                throw ex;
            }

            if (sessionID != null) {
                sendHelper = new SendMessageHelper(nodeId, sessionID, NodeAgent
                        .getNodeAgent(nodeId));
            }

            boolean destroyConnection = false;

            if (oConn != null) {
                try {
                    if (batchStatement != null) {
                        batchStatement.close();
                        batchStatement = null;
                    }
                    // Ensure connection is clean
                    oConn.rollback();
                } catch (SQLException se) {
                    destroyConnection = true;
                    logger.catching(se);
                }

                synchronized (tempTableNames) {
                    for (String string : tempTableNames) {
                        try {
                            doProcessSqlCommand("DROP TABLE " +
                                    IdentifierHandler.quote(string), true);
                        } catch (Throwable ignore) {
                        }
                    }
                    tempTableNames.clear();
                }
            }

            savepointTable.clear();

            if (oConn != null) {
                logger.log(XLevel.TRACE, "Releasing connection: %0%",
                        new Object[] { oConn });
                if (destroyConnection) {
                    currentPool.destroyConnection(oConn);
                } else {
                    currentPool.releaseConnection(oConn);
                }
                logger.log(XLevel.TRACE, "Released connection: %0%",
                        new Object[] { oConn });
                oConn = null;
            }
            currentPool = pool;
            if (currentPool != null) {
                oConn = currentPool.getConnection();
                // Moved to JDBCPool.createEntry()
                // oConn.setAutoCommit(false);
                logger.log(XLevel.TRACE, "Got connection: %0%",
                        new Object[] { oConn });
                setState(STATEWAIT);
            } else {
                setState(STATEDISCONNECTED);
            }
            if (aNodeProducerThread != null) {
                aNodeProducerThread.reset();
            }
            requestIDtoAbort = -1;

        } catch (Exception e) {
            logger.catching(e);

            // Remove bad connection from pool, if exists
            if (currentPool != null && oConn != null) {
                currentPool.destroyConnection(oConn);
            }

            XDBBaseException ex;
            if (e instanceof XDBBaseException) {
                ex = (XDBBaseException) e;
            } else if (e instanceof SQLException) {
                ex = new XDBWrappedSQLException(nodeId, (SQLException) e);
            } else {
                ex = new XDBWrappedException(nodeId, e);
            }
            logger.throwing(ex);
            throw ex;
        } finally {
            logger.exiting(method);
        }
    }

    private NodeProducerThread aNodeProducerThread = null;

    /**
     *
     * @param oConn
     * @param sendHelper
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private synchronized void initNodeProducerThread(Connection oConn,
            SendMessageHelper sendHelper) throws XDBBaseException {
        final String method = "initNodeProducerThread";
        logger.entering(method, new Object[] { oConn, sendHelper });
        try {

            if (aNodeProducerThread == null) {
                NodeMessage.CATEGORY_MESSAGE.debug("NTP create");
                producerQueue = new LinkedBlockingQueue<NodeMessage>();
                aNodeProducerThread = new NodeProducerThread(this,
                        producerQueue);
                new Thread(aNodeProducerThread).start();
            }
            logger.log(XLevel.TRACE, "Passing down connection to NPT: %0%",
                    new Object[] { oConn });
            aNodeProducerThread.reset();
            logger.log(XLevel.TRACE, "Passed down connection to NPT: %0%",
                    new Object[] { oConn });

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param savepointName
     * @throws java.sql.SQLException
     */
    private void doBeginSavepoint(String savepointName) throws SQLException {
        /*
         * There seems to be a problem with some db implementations of
         * SAVEPOINT. Instead, we work-around and use SUBTRANS
         * BEGIN. So, savepointName is not important here anymore Savepoint
         * aSavepoint = oConn.setSavepoint(savepointName); savepointTable.put
         * (savepointName, aSavepoint);
         */

        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });

            if (Props.XDB_SAVEPOINTTYPE.equals("S")) {
                Savepoint aSavepoint = oConn.setSavepoint(savepointName);
                savepointTable.put(savepointName, aSavepoint);
            } else {
                aStatement = oConn.createStatement();
                logger.log(XLevel.TRACE,
                        "From connection: %0% created Statement: %1%",
                        new Object[] { oConn, aStatement });
                try {
                    aStatement.executeUpdate("SUBTRANS BEGIN");
                    logger.log(XLevel.TRACE,
                            "From connection: %0% executed Statement: %1%",
                            new Object[] { oConn, aStatement });
                } finally {
                    aStatement.close();
                    aStatement = null;
                    logger.log(XLevel.TRACE,
                            "From connection: %0% closed Statement: %1%",
                            new Object[] { oConn, aStatement });
                }
            }
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
            oConn.notify();
        }
    }

    /**
     * Set savepoint for rolling back "subtransaction" later, if necessary.
     *
     * @param savepointName
     * @throws XDBBaseException
     */
    private void beginSavepoint(String savepointName) throws XDBBaseException {
        final String method = "beginSavepoint";
        logger.entering(method, new Object[] { savepointName });
        try {

            try {
                doBeginSavepoint(savepointName);
            } catch (SQLException se) {
                if (handleSqlException(se, null)) {
                    // If problem has been solved re-run the command
                    try {
                        doBeginSavepoint(savepointName);
                    } catch (SQLException se1) {
                        logger.catching(se1);
                        XDBBaseException ex = new XDBWrappedSQLException(
                                nodeId, se1);
                        logger.throwing(ex);
                        throw ex;
                    }
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param savepointName
     * @throws java.sql.SQLException
     */
    private void doRollbackSavepoint(String savepointName) throws SQLException {
        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });

            if (Props.XDB_SAVEPOINTTYPE.equals("S")) {
                Savepoint aSavepoint = savepointTable.get(savepointName);

                if (aSavepoint == null) {
                    throw new XDBServerException(
                            ErrorMessageRepository.SQL_ROLLBACK_SAVEPOINT_ERROR
                                    + " nodeId = " + this.nodeId,
                            XDBServerException.SEVERITY_HIGH,
                            ErrorMessageRepository.SQL_ROLLBACK_SAVEPOINT_ERROR_CODE);
                }
                oConn.rollback(aSavepoint);
            } else {
                aStatement = oConn.createStatement();
                logger.log(XLevel.TRACE,
                        "From connection: %0% created Statement: %1%",
                        new Object[] { oConn, aStatement });
                try {
                    aStatement.executeUpdate("SUBTRANS ROLLBACK");
                    logger.log(XLevel.TRACE,
                            "From connection: %0% executed Statement: %1%",
                            new Object[] { oConn, aStatement });
                } finally {
                    aStatement.close();
                    aStatement = null;
                    logger.log(XLevel.TRACE,
                            "From connection: %0% closed Statement: %1%",
                            new Object[] { oConn, aStatement });
                }
            }
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
        }
    }

    /**
     * Rolls back savepoint
     *
     * @param savepointName
     * @throws XDBBaseException
     */
    private void rollbackSavepoint(String savepointName)
            throws XDBBaseException {
        final String method = "rollbackSavepoint";
        logger.entering(method, new Object[] { savepointName });
        try {

            doRollbackSavepoint(savepointName);
        } catch (SQLException se) {
            if (handleSqlException(se, null)) {
                // If problem has been solved re-run the command
                try {
                    doRollbackSavepoint(savepointName);
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            }
        } finally {
            savepointTable.remove(savepointName);
            logger.exiting(method);
        }
    }

    /**
     *
     * @param savepointName
     * @throws java.sql.SQLException
     */
    private void doEndSavepoint(String savepointName) throws SQLException {
        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });

            if (Props.XDB_SAVEPOINTTYPE.equals("S")) {
                Savepoint aSavepoint = savepointTable.get(savepointName);
                oConn.releaseSavepoint(aSavepoint);
            } else {
                aStatement = oConn.createStatement();
                logger.log(XLevel.TRACE,
                        "From connection: %0% created Statement: %1%",
                        new Object[] { oConn, aStatement });
                try {
                    aStatement.executeUpdate("SUBTRANS END");
                    logger.log(XLevel.TRACE,
                            "From connection: %0% executed Statement: %1%",
                            new Object[] { oConn, aStatement });
                } finally {
                    aStatement.close();
                    logger.log(XLevel.TRACE,
                            "From connection: %0% closed Statement: %1%",
                            new Object[] { oConn, aStatement });
                    aStatement = null;
                }
            }
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
            oConn.notify();
        }
    }

    /**
     * "commit" subtransaction up to
     * now, to release any locks on the table
     *
     * @param savepointName
     * @throws XDBBaseException
     */
    private void endSavepoint(String savepointName) throws XDBBaseException {
        final String method = "endSavepoint";
        logger.entering(method, new Object[] { savepointName });
        try {
            doEndSavepoint(savepointName);
        } catch (SQLException se) {
            if (handleSqlException(se, null)) {
                try {
                    doEndSavepoint(savepointName);
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            }
        } finally {
            // remove here instead of doEndSavepoint, in case this is retried
            savepointTable.remove(savepointName);
            logger.exiting(method);
        }
    }

    /**
     *
     * @param sqlCommandStr
     * @param autocommit
     * @throws java.sql.SQLException
     * @return
     */
    private int doProcessSqlCommand(String sqlCommandStr, boolean autocommit)
            throws SQLException {
        int rowsAffected = 0;
        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });
            aStatement = oConn.createStatement();
            logger.log(XLevel.TRACE,
                    "From connection: %0% created Statement: %1%",
                    new Object[] { oConn, aStatement });
            if (autocommit) {
                oConn.setAutoCommit(true);
            }
            try {
                try {
                    logger.log(Level.DEBUG, "SQL: %0%",
                            new Object[] { sqlCommandStr });
                    rowsAffected = aStatement.executeUpdate(sqlCommandStr);
                    logger.log(XLevel.TRACE,
                            "From connection: %0% executed Statement: %1%",
                            new Object[] { oConn, aStatement });
                } finally {
                    aStatement.close();
                    aStatement = null;
                    logger.log(XLevel.TRACE,
                            "From connection: %0% closed Statement: %1%",
                            new Object[] { oConn, aStatement });
                }
            } finally {
                if (autocommit) {
                    oConn.setAutoCommit(false);
                }
            }
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
            oConn.notify();
        }
        return rowsAffected;
    }

    // ---------------------------------------------------------
    /**
     *
     * @param sqlCommandStr
     * @param autocommit
     * @throws org.postgresql.stado.exception.XDBBaseException
     * @return
     */
    private int processSqlCommand(String sqlCommandStr, boolean autocommit)
            throws XDBBaseException {
        final String method = "processSqlCommand";
        logger.entering(method, new Object[] { sqlCommandStr,
                autocommit ? Boolean.TRUE : Boolean.FALSE });
        try {
            return doProcessSqlCommand(sqlCommandStr, autocommit);
        } catch (SQLException se) {
            if (handleSqlException(se, sqlCommandStr)) {
                try {
                    return doProcessSqlCommand(sqlCommandStr, autocommit);
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            } else {
                return 0;
            }
        } finally {
            logger.exiting(method);
        }
    }

    // -----------------------------------------------------------------
    // New methods added here for integrating with existing QueryProcessor
    // Initializes step
    // ---------------------------------------------------------
    /**
     *
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void initStep() throws XDBBaseException {
        final String method = "initStep";
        logger.entering(method);
        try {

            // Create target schema, if applicable
            if (aStepDetail.isConsumer && aStepDetail.getDestType() != StepDetail.DEST_TYPE_COORD_FINAL) {
                boolean doSavepoint = !Props.XDB_COMMIT_AFTER_CREATE_TEMP_TABLE
                        && !Props.XDB_USE_LOAD_FOR_STEP;
                // Only create the target table if specified.
                // For INSERT SELECT, target schema may be null
                if (aStepDetail.targetSchema != null && aStepDetail.targetSchema.length() > 0) {
                    try {
                        if (doSavepoint) {
                            beginSavepoint("stepSavepoint");
                        }
                        processSqlCommand(aStepDetail.targetSchema, !doSavepoint);
                    } finally {
                        if (doSavepoint) {
                            endSavepoint("stepSavepoint");
                        }
                    }
                    if (aStepDetail.targetTable != null) {
                        synchronized (tempTableNames) {
                            tempTableNames.add(aStepDetail.targetTable);
                        }
                    }
                }

                if (!Props.XDB_USE_LOAD_FOR_STEP) {
                    // If we are a consumer, init target, change state to be be
                    // ready
                    // to accept data sent from combiner.
                    initDataDown(aStepDetail.targetTable);
                }
            }

            // Use NodeProducerThread if we need to create data
            if (aStepDetail.isProducer) {
                initNodeProducerThread(oConn, sendHelper);
            }

        } finally {
            logger.exiting(method);
        }
    }

    // processStatement handles non SELECT statements.
    // processStep here is for an individual step in a SELECT
    // ---------------------------------------------------------
    /**
     *
     * @param stepMessage
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void processStep(NodeMessage stepMessage) throws XDBBaseException {
        final String method = "processStep";
        logger.entering(method, new Object[] { stepMessage });
        try {

            // Use NodeProducerThread if we need to create data
            if (aStepDetail.isProducer) {
                initNodeProducerThread(oConn, sendHelper);

                stepMessage.setStepDetail(aStepDetail);
                // pass on to producer
                if (producerQueue != null) {
                    producerQueuePut(stepMessage);
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param aMessage
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void processQuery(NodeMessage aMessage) throws XDBBaseException {
        final String method = "processQuery";
        logger.entering(method, new Object[] { aMessage });
        try {

            initNodeProducerThread(oConn, sendHelper);
            // pass on to producer
            if (producerQueue != null) {
                producerQueuePut(aMessage);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param downTableName
     * @throws java.sql.SQLException
     */
    private void doInitDataDown(String downTableName) throws SQLException {
        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });
            batchStatement = oConn.createStatement();
            aStatement = batchStatement;
            logger.log(XLevel.TRACE,
                    "From connection: %0% created Statement: %1%",
                    new Object[] { oConn, batchStatement });
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
            oConn.notify();
        }
    }

    // Some methods for sending temp results down to the nodes
    // Made this one synchronized, since I did not want other threads
    // who send a small amount of rows to change the state
    // ---------------------------------------------------------
    // ---------------------------------------------------------
    /**
     *
     * @param downTableName
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void initDataDown(String downTableName) throws XDBBaseException {

        this.downTableName = downTableName;
        baseInsert = "INSERT INTO " + IdentifierHandler.quote(downTableName)
                + " VALUES ";

        final String method = "initDataDown";
        logger.entering(method, new Object[] { downTableName });
        try {
            // set target table name
            this.downTableName = downTableName;
            baseInsert = "INSERT INTO " + IdentifierHandler.quote(downTableName)
                    + " VALUES ";
            doInitDataDown(downTableName);
        } catch (SQLException se) {
            if (handleSqlException(se, null)) {
                try {
                    doInitDataDown(downTableName);
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param obj
     * @throws java.sql.SQLException
     */
    private void doPopulateDownData(String obj) throws SQLException {
        String sInsertStatement;

        if (obj == null) {
            return;
        }
        if (Props.XDB_JUST_DATA_VALUES) {
            sInsertStatement = baseInsert + obj;
        } else {
            sInsertStatement = obj;
        }

        if (USEBATCHES) {
            synchronized (oConn) {
                logger.log(XLevel.TRACE,
                        "Entered critical section for connection: %0%",
                        new Object[] { oConn });
                batchStatement.addBatch(sInsertStatement);
                logger
                        .log(
                                XLevel.TRACE,
                                "From connection: %0% added batch command to Statement: ",
                                new Object[] { oConn, batchStatement });

                if (++iCount % BATCHSIZE == 0) {
                    logger
                            .log(
                                    XLevel.TRACE,
                                    "From connection: %0% executing batch Statement: %1%",
                                    new Object[] { oConn, batchStatement });
                    batchStatement.executeBatch();
                    logger
                            .log(
                                    XLevel.TRACE,
                                    "From connection: %0% executed batch Statement: %1%",
                                    new Object[] { oConn, batchStatement });
                    batchStatement.clearBatch();
                    logger
                            .log(
                                    XLevel.TRACE,
                                    "From connection: %0% cleared batch Statement: %1%",
                                    new Object[] { oConn, batchStatement });
                }

                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { oConn });
                oConn.notify();
            }
        } else {
            synchronized (oConn) {
                logger.log(XLevel.TRACE,
                        "Entered critical section for connection: %0%",
                        new Object[] { oConn });
                batchStatement.executeUpdate(sInsertStatement);
                ++iCount;
                logger.log(XLevel.TRACE,
                        "From connection: %0% executed Statement: %1%",
                        new Object[] { oConn, batchStatement });
                if (!AUTOCOMMIT && iCount % BATCHSIZE == 0) {
                    logger.log(XLevel.TRACE,
                            "Committing work for connection: %0%",
                            new Object[] { oConn });
                    oConn.commit();
                    logger.log(XLevel.TRACE,
                            "Committed work for connection: %0%",
                            new Object[] { oConn });
                }

                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { oConn });
                oConn.notify();
            }
        }
    }

    // ---------------------------------------------------------
    /**
     *
     * @param obj
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void populateDownData(String obj) throws XDBBaseException {
        final String method = "populateDownData";
        logger.entering(method, new Object[] { obj });
        try {
            doPopulateDownData(obj);
        } catch (SQLException se) {
            if (handleSqlException(se, null)) {
                try {
                    doPopulateDownData(obj);
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @throws java.sql.SQLException
     */
    private void doFinishInserts() throws SQLException {
        // now look for data to insert into the temp table
        // insert any remaining ones
        if (USEBATCHES) {
            if (batchStatement != null) {
                synchronized (oConn) {
                    logger.log(XLevel.TRACE,
                            "Entered critical section for connection: %0%",
                            new Object[] { oConn });
                    logger.log(
                            XLevel.TRACE,
                            "From connection: %0% executing batch Statement: %1%",
                            new Object[] { oConn, batchStatement });
                    batchStatement.executeBatch();
                    logger.log(
                            XLevel.TRACE,
                            "From connection: %0% executed batch Statement: %1%",
                            new Object[] { oConn, batchStatement });
                    batchStatement.close();
                    batchStatement = null;
                    logger.log(XLevel.TRACE,
                            "From connection: %0% closed batch Statement: %1%",
                            new Object[] { oConn, batchStatement });
                    logger.log(XLevel.TRACE,
                            "Leaving critical section for connection: %0%",
                            new Object[] { oConn });
                    oConn.notify();
                }
            }
        } else {
            synchronized (oConn) {
                logger.log(XLevel.TRACE,
                        "Entered critical section for connection: %0%",
                        new Object[] { oConn });

                if (!AUTOCOMMIT) {
                    logger.log(XLevel.TRACE,
                            "Committing work for connection: %0%",
                            new Object[] { oConn });
                    oConn.commit();
                    logger.log(XLevel.TRACE,
                            "Committed work for connection: %0%",
                            new Object[] { oConn });
                }
                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { oConn });
                oConn.notify();
            }
        }
    }

    // -----------------------------------------------------------------------
    /**
     *
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void finishInserts() throws XDBBaseException {
        final String method = "finishInserts";
        logger.entering(method);
        try {

            doFinishInserts();
        } catch (SQLException se) {
            if (handleSqlException(se, null)) {
                try {
                    doFinishInserts();
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            }
            logger.trace("NT.finishInserts: Inserted " + iCount + " rows in "
                    + this.downTableName);
            iCount = 0;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Create any indexes or execute analyze, as appropriate
     *
     * @throws java.sql.SQLException
     */
    private void doFinalizeStep() throws SQLException {
        // now look for data to insert into the temp table
        // insert any remaining ones

        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });
            if (aStepDetail.isConsumer && aStepDetail.getDestType() != StepDetail.DEST_TYPE_COORD_FINAL) {
                if (Props.XDB_STEP_INDEX_CORRELATED) {
                    // See if we need to create an index for a correlated join
                    String idxDef = aStepDetail.getCorrelatedIndex();
                    logger.debug("Creating index: " + idxDef);
                    if (idxDef != null) {
                        Statement aStatement = oConn.createStatement();
                        aStatement.execute(idxDef);
                        aStatement.close();
                    }
                }
                if (Props.XDB_STEP_RUNANALYZE) {
                    // Analyze / Update Stats
                    HashMap<String, String> params = new HashMap<String, String>();
                    params.put("table", IdentifierHandler.quote(aStepDetail.targetTable));
                    String templateUpdate = Props.XDB_SQLCOMMAND_ANALYZE_TEMPLATE_TABLE
                            .trim();
                    String analyzeCmd = ParseCmdLine.substitute(templateUpdate,
                            params);
                    Statement anIdxStatement = oConn.createStatement();
                    logger.debug("ANALYZE: " + analyzeCmd);
                    anIdxStatement.execute(analyzeCmd);
                    anIdxStatement.close();
                }
            }
            if (!AUTOCOMMIT) {
                logger.log(XLevel.TRACE, "Committing work for connection: %0%",
                        new Object[] { oConn });
                oConn.commit();
                logger.log(XLevel.TRACE, "Committed work for connection: %0%",
                        new Object[] { oConn });
            }
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
            oConn.notify();
        }
    }

    // -----------------------------------------------------------------------
    /**
     *
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void finalizeStep() throws XDBBaseException {
        final String method = "finalizeStep";
        logger.entering(method);
        try {

            doFinalizeStep();
        } catch (SQLException se) {
            if (handleSqlException(se, null)) {
                try {
                    doFinalizeStep();
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            }
            iCount = 0;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Executes all of the commands sent over in the batch
     *
     * @param aNodeMessage
     * @throws XDBBaseException
     * @return
     */
    private int[] processExecuteBatch(NodeMessage aNodeMessage)
            throws XDBBaseException {
        final String method = "processExecuteBatch";
        logger.entering(method, new Object[] { aNodeMessage });
        try {

            initBatch();

            String[] rowData = aNodeMessage.getRowData();

            for (String element : rowData) {
                addBatch(element);
            }

            return executeBatch();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @throws java.sql.SQLException
     */
    private void doInitBatch() throws SQLException {
        if (batchStatement == null) {
            synchronized (oConn) {
                logger.log(XLevel.TRACE,
                        "Entered critical section for connection: %0%",
                        new Object[] { oConn });
                batchStatement = oConn.createStatement();
                logger.log(XLevel.TRACE,
                        "From connection: %0% created Statement: %1%",
                        new Object[] { oConn, batchStatement });
                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { oConn });
                oConn.notify();
            }
        }
    }

    // ---------------------------------------------------------
    /**
     *
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void initBatch() throws XDBBaseException {
        final String method = "initBatch";
        logger.entering(method);
        try {
            doInitBatch();
        } catch (SQLException se) {
            if (handleSqlException(se, null)) {
                try {
                    doInitBatch();
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param sqlStmt
     * @throws java.sql.SQLException
     */
    private void doAddBatch(String sqlStmt) throws SQLException {
        // It won't work without batches - results are not returned
        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });
            batchStatement.addBatch(sqlStmt);
            logger
                    .log(
                            XLevel.TRACE,
                            "From connection: %0% added batch command to Statement: %1%",
                            new Object[] { oConn, batchStatement });
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
            oConn.notify();
        }
    }

    // ---------------------------------------------------------
    /**
     *
     * @param sqlStmt
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void addBatch(String sqlStmt) throws XDBBaseException {
        final String method = "addBatch";
        logger.entering(method, new Object[] { sqlStmt });
        try {
            doAddBatch(sqlStmt);
        } catch (SQLException se) {
            if (handleSqlException(se, null)) {
                try {
                    doAddBatch(sqlStmt);
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @throws java.sql.SQLException
     * @return
     */
    private int[] doExecuteBatch() throws SQLException {
        int[] updateCounts = null;
        // now look for data to insert into the temp table
        try {
            synchronized (oConn) {
                logger.log(XLevel.TRACE,
                        "Entered critical section for connection: %0%",
                        new Object[] { oConn });
                // Special handling for batches.
                // Handle the cases:
                // user-defined transaction w/ sub-trans (replicated) -
                // autocommit = false
                // user-defined transaction w/ no sub-trans - autocommit =
                // false
                // no trans w/ sub-trans (replicated) - autocommit = false
                // no trans w/ no sub-trans - autocommit = true

                // boolean oldCommit = false;
                // if (!isInUserTransaction && !isInSubTransaction)
                // {
                // oldCommit = oConn.getAutoCommit();
                // logger.log(XLevel.TRACE,
                // "Switching autocommit to true for connection: %0%",
                // new Object[] {oConn});
                // oConn.setAutoCommit(true);
                // }
                // try
                // {
                updateCounts = batchStatement.executeBatch();
                logger.log(XLevel.TRACE,
                        "From connection: %0% executed batch Statement: %1%",
                        new Object[] { oConn, batchStatement });
                // }
                // finally
                // {
                // // flip back to default
                // if (!isInUserTransaction && !isInSubTransaction)
                // {
                // oConn.setAutoCommit(oldCommit);
                // logger.log(XLevel.TRACE,
                // "Switching autocommit to %0% for connection: %1%",
                // new Object[] {new Boolean(oldCommit), oConn});
                // }
                // }
                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { oConn });
                oConn.notify();
            }
        } catch (BatchUpdateException be) {
            logger.catching(be);
            updateCounts = be.getUpdateCounts();
        }
        iCount = 0;
        return updateCounts;
    }

    // -----------------------------------------------------------------------
    /**
     * Executes the batch.
     *
     * @throws XDBBaseException
     */
    private int[] executeBatch() throws XDBBaseException {
        final String method = "executeBatch";
        logger.entering(method);
        try {

            return doExecuteBatch();

        } catch (SQLException se) {
            handleSqlException(se, null);
            try {
                return doExecuteBatch();
            } catch (SQLException se1) {
                logger.catching(se1);
                XDBBaseException ex = new XDBWrappedSQLException(nodeId, se1);
                logger.throwing(ex);
                throw ex;
            }

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * Do not do anything on MSG_ABORT, letting server to take care of this
     */
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

    /**
     *
     * @return
     */
    public synchronized boolean isAlive() {
        return currentState != STATEDONE;
    }

    // This is the main loop that controls the execution of the
    // thread.
    // ---------------------------------------------------------
    public void run() {
        final String method = "run";
        logger.entering(method);
        try {

            NodeMessage aNodeMessage = null;
            // we do what main thread tells us to do, then we wait for the other
            // threads to finish, too
            while (true) {
                try {
                    // We wait on our message queue for instructions on what to
                    // do
                    // NodeMessage.CATEGORY_MESSAGE.debug("NT.run(): node: "
                    // + nodeId + " getting ready to take()");

                    isTaking = true;
                    aNodeMessage = msgQueue.poll(5,
                            java.util.concurrent.TimeUnit.SECONDS);

                    if (aNodeMessage == null) {
                        continue;
                    }
                    NodeMessage.CATEGORY_MESSAGE.debug("NT.run(): node: "
                            + nodeId + " received message: " + aNodeMessage);
                    isTaking = false;
                    lastMessageType = aNodeMessage.getMessageType();

                    if (aNodeMessage.getMessageType() != NodeMessage.MSG_ABORT
                            && aNodeMessage.getRequestId() == requestIDtoAbort) {
                        /*
                         * This outputs a lot of extra messages
                         * when an error occurred. Just ignore any messages tied
                         * to old, aborted request. XDBBaseException ex = new
                         * XDBUnexpectedMessageException( nodeId, "The request
                         * is aborting", aNodeMessage); logger.throwing(ex);
                         * throw ex;
                         */
                        continue;
                    } else {
                        requestIDtoAbort = -1;
                    }

                    switch (aNodeMessage.getMessageType()) {
                    case NodeMessage.MSG_TRAN_COMMIT:
                        tranCommit(aNodeMessage);
                        break;

                    case NodeMessage.MSG_TRAN_ROLLBACK:
                        tranRollback(aNodeMessage);
                        break;

                    case NodeMessage.MSG_TRAN_BEGIN_SAVEPOINT:
                        checkCurrentState(new int[] { STATEWAIT },
                                STATEBEGINTRAN);
                        try {
                            beginSavepoint(aNodeMessage.getSavepoint());

                            sendHelper.sendReplyMessage(aNodeMessage,
                                    NodeMessage.MSG_TRAN_BEGIN_SAVEPOINT_ACK);
                        } finally {
                            setState(STATEWAIT);
                        }
                        break;

                    case NodeMessage.MSG_TRAN_ROLLBACK_SAVEPOINT:

                        checkCurrentState(
                                new int[] { STATEWAIT /* , STATEABORT */},
                                STATEROLLBACK);
                        try {
                            rollbackSavepoint(aNodeMessage.getSavepoint());

                            sendHelper
                                    .sendReplyMessage(
                                            aNodeMessage,
                                            NodeMessage.MSG_TRAN_ROLLBACK_SAVEPOINT_ACK);
                        } finally {
                            setState(STATEWAIT);
                        }
                        break;

                    case NodeMessage.MSG_TRAN_END_SAVEPOINT:

                        checkCurrentState(new int[] { STATEWAIT }, STATEENDTRAN);
                        try {
                            endSavepoint(aNodeMessage.getSavepoint());

                            sendHelper.sendReplyMessage(aNodeMessage,
                                    NodeMessage.MSG_TRAN_END_SAVEPOINT_ACK);
                        } finally {
                            setState(STATEWAIT);
                        }
                        break;

                    case NodeMessage.MSG_EXEC_COMMAND:
                    case NodeMessage.MSG_EXEC_QUERY:
                        checkCurrentState(new int[] { STATEWAIT }, STATECOMMAND);
                        try {
                            processQuery(aNodeMessage);
                        } finally {
                            setState(STATEWAIT);
                        }

                        break;

                    // This is for initializing a step in a query
                    case NodeMessage.MSG_EXECUTE_STEP_INIT:
                        checkCurrentState(new int[] { STATEWAIT }, STATESTEP);
                        try {
                            aStepDetail = aNodeMessage.getStepDetail();
                            initStep();
                            sendHelper.sendReplyMessage(aNodeMessage,
                                    NodeMessage.MSG_EXECUTE_STEP_INIT_ACK);
                        } finally {
                            setState(aStepDetail.isConsumer ? STATEDATADOWN
                                    : STATEWAIT);
                        }

                        break;

                    // This is for executing a step in a query
                    case NodeMessage.MSG_EXECUTE_STEP_RUN:

                        int aState = checkCurrentState(new int[] { STATEWAIT,
                                STATEDATADOWN }, STATESTEP);

                        aStepDetail = aNodeMessage.getStepDetail();

                        try {
                            processStep(aNodeMessage);
                        } finally {
                            setState(aState);
                        }
                        break;

                    case NodeMessage.MSG_EXECUTE_STEP_END:

                        // At this point, we know that all other nodes have
                        // also completed getting data from this node,
                        // so it is safe to go ahead and drop any temp
                        // tables
                        // that we can
                        checkCurrentState(
                                new int[] { STATEWAIT, STATEDATADOWN },
                                STATESTEPEND);

                        try {
                            if (!Props.XDB_USE_LOAD_FOR_STEP) {
                                // In case we are inserting in batches,
                                // go ahead and finish them up
                                finishInserts();
                            }

                            // Take any final actions on step
                            finalizeStep();

                            // If debugging, dump results
                            if (Props.XDB_DUMPSTEPPATH != null) {
                                dumpStepResults(oConn, aStepDetail.targetTable,
                                        nodeId);
                            }

                            sendHelper.sendReplyMessage(aNodeMessage,
                                    NodeMessage.MSG_EXECUTE_STEP_END_ACK);

                        } finally {
                            setState(STATEWAIT);
                        }

                        break;

                    case NodeMessage.MSG_SEND_DATA_INIT:
                        checkCurrentState(new int[] { STATEWAIT },
                                STATEDATADOWN);
                        initDataDown(aNodeMessage.getTargetTable());
                        break;

                    case NodeMessage.MSG_SEND_DATA:
                        if (aStepDetail == null
                                || aStepDetail.consumerNodeList
                                        .contains(new Integer(nodeId))) {
                            checkCurrentState(new int[] { STATEDATADOWN },
                                    STATEDATADOWN);
                            String[] rowData = aNodeMessage.getRowData();

                            for (String element : rowData) {
                                populateDownData(element);
                            }

                            NodeMessage msg = NodeMessage
                                    .getNodeMessage(NodeMessage.MSG_SEND_DATA_ACK);
                            msg.setDataSeqNo(aNodeMessage.getDataSeqNo());
                            sendHelper.sendReplyMessage(aNodeMessage, msg);

                            // Try to help with Garbage Collection
                            rowData = null;
                            msg = null;

                            // Just in case we want to try and force
                            // garbase collection
                            if (freeInterval > 0) {
                                if (aNodeMessage.getDataSeqNo() % freeInterval == 0) {
                                    aNodeMessage = null;
                                    System.gc();
                                }
                            }
                        }
                        break;

                    case NodeMessage.MSG_SEND_DATA_ACK:
                        producerQueuePut(aNodeMessage);
                        break;

                    case NodeMessage.MSG_EXECUTE_BATCH:
                        checkCurrentState(new int[] { STATEWAIT }, STATECOMMAND);
                        try {
                            NodeMessage outNodeMessage = NodeMessage
                                    .getNodeMessage(NodeMessage.MSG_EXECUTE_BATCH_ACK);
                            outNodeMessage.setRequestId(aNodeMessage
                                    .getRequestId());
                            int[] response;
                            int[] result = processExecuteBatch(aNodeMessage);
                            if (result.length < aNodeMessage.getRowCount()) {
                                // there was an error
                                response = new int[aNodeMessage.getRowCount()];
                                System.arraycopy(result, 0, response, 0,
                                        result.length);
                                Arrays.fill(response, result.length,
                                        response.length,
                                        Statement.EXECUTE_FAILED);
                            } else {
                                response = result;
                            }
                            outNodeMessage.setBatchResult(response);
                            sendHelper.sendReplyMessage(aNodeMessage,
                                    outNodeMessage);
                        } finally {
                            setState(STATEWAIT);
                        }
                        break;

                    case NodeMessage.MSG_DROP_TEMP_TABLES:
                        producerQueuePut(aNodeMessage);
                        break;

                    case NodeMessage.MSG_STOP_THREAD:
                        // tell producer to stop, too.
                        setState(STATEDONE);

                        try {
                            if (producerQueue != null) {
                                producerQueuePut(aNodeMessage);
                            }
                            producerQueue = null;
                        } catch (Exception e) {
                            logger.catching(e);
                        }

                        return;

                    case NodeMessage.MSG_ABORT:

                        // try and clean things up.
                        // TODO: track and delete intermediate temp tables.
                        try {
                            if (producerQueue != null) {
                                producerQueuePut(aNodeMessage);
                            }
                        } finally {
                            requestIDtoAbort = aNodeMessage.getRequestId();
                            setState(STATEWAIT);
                        }
                        break;

                    case NodeMessage.MSG_PING:
                        checkCurrentState(new int[] { STATEWAIT }, STATECOMMAND);
                        try {
                            checkConnection();
                        }

                        finally {
                            setState(STATEWAIT);
                        }
                        sendHelper.sendReplyMessage(aNodeMessage,
                                NodeMessage.MSG_PING_ACK);
                        break;

                    case NodeMessage.MSG_RESULT_ROWS_REQUEST:
                        producerQueuePut(aNodeMessage);
                        break;

                    case NodeMessage.MSG_RESULT_CLOSE:
                        producerQueuePut(aNodeMessage);
                        break;

                    case NodeMessage.MSG_PING_ACK:
                        // Do nothing
                        break;
                    case NodeMessage.MSG_PREPARE_COMMAND:
                    case NodeMessage.MSG_PREPARE_COMMAND_EXEC:
                    case NodeMessage.MSG_PREPARE_COMMAND_CLOSE:
                        producerQueuePut(aNodeMessage);
                        break;
                    default:
                        XDBBaseException ex = new XDBUnexpectedMessageException(
                                nodeId, "NodeThread can not handle message",
                                aNodeMessage);
                        logger.throwing(ex);
                        throw ex;
                    }
                } catch (XDBBaseException ex) {
                    synchronized (this) {
                        logger.catching(ex);
                        // Switch to STATEABORT if we are still connected and
                        // notify sender
                        if (sendHelper != null) {
                            requestIDtoAbort = aNodeMessage.getRequestId();
                            // currentState = STATEABORT;
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
                            // currentState = STATEABORT;
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

    /**
     *
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void checkConnection() throws XDBBaseException {
        final String method = "checkConnection";
        logger.entering(method, new Object[] {});
        try {

            if ("".equals(Props.XDB_BACKEND_PING_STATEMENT)) {
                return;
            }
            Savepoint aSavepoint = null;

            synchronized (oConn) {
                try {
                    logger.log(XLevel.TRACE,
                            "Entered critical section for connection: %0%",
                            new Object[] { oConn });

                    aStatement = oConn.createStatement();
                    try {
                        logger.log(XLevel.TRACE,
                                "From connection: %0% created Statement: %1%",
                                new Object[] { oConn, aStatement });
                        logger
                                .debug("SQL: "
                                        + Props.XDB_BACKEND_PING_STATEMENT);
                        boolean doSavepoint = Props.XDB_BACKEND_PING_SETUP != null
                                && Props.XDB_SAVEPOINTTYPE.equals("S");
                        if (doSavepoint) {
                            aSavepoint = oConn.setSavepoint("checkConn");
                        }
                        try {
                            aStatement
                                    .execute(Props.XDB_BACKEND_PING_STATEMENT);
                            logger
                                    .log(
                                            XLevel.TRACE,
                                            "From connection: %0% executed Statement: %1%",
                                            new Object[] { oConn, aStatement });
                            if (doSavepoint) {
                                oConn.releaseSavepoint(aSavepoint);
                            }
                        } catch (SQLException se) {
                            if (doSavepoint) {
                                oConn.rollback(aSavepoint);
                            }

                            if (Props.XDB_BACKEND_PING_SETUP != null) {
                                logger.debug("SQL: "
                                        + Props.XDB_BACKEND_PING_SETUP);
                                aStatement
                                        .execute(Props.XDB_BACKEND_PING_SETUP);
                                logger
                                        .log(
                                                XLevel.TRACE,
                                                "From connection: %0% executed Statement: %1%",
                                                new Object[] { oConn,
                                                        aStatement });
                            } else {
                                throw se;
                            }
                        }

                    } finally {
                        aStatement.close();
                        logger.log(XLevel.TRACE,
                                "From connection: %0% closed Statement: %1%",
                                new Object[] { oConn, aStatement });
                        aStatement = null;
                    }
                } catch (SQLException se) {
                    throw new XDBBaseException(getNodeId(), "Ping failed: "
                            + se.getMessage());
                }
                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { oConn });
                oConn.notify();
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param aNodeMessage
     * @throws org.postgresql.stado.exception.XDBBaseException
     * @throws java.sql.SQLException
     */
    private void doTranRollback(NodeMessage aNodeMessage)
            throws XDBBaseException, SQLException {
        
        checkCurrentState(new int[] { STATEWAIT /* , STATEABORT */},
                STATEROLLBACK);
        try {
            synchronized (oConn) {
                logger.log(XLevel.TRACE,
                        "Entered critical section for connection: %0%",
                        new Object[] { oConn });
                logger.log(XLevel.TRACE,
                        "Rolling back work for connection: %0%",
                        new Object[] { oConn });
                oConn.rollback();
                logger.log(XLevel.TRACE,
                        "Rolled back work for connection: %0%",
                        new Object[] { oConn });
                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { oConn });
                oConn.notify();
            }
        } finally {
            setState(STATEWAIT);
        }
        sendHelper.sendReplyMessage(aNodeMessage,
                NodeMessage.MSG_TRAN_ROLLBACK_ACK);
    }

    /**
     *
     *
     * @param aNodeMessage
     * @throws XDBBaseException
     */
    private void tranRollback(NodeMessage aNodeMessage) throws XDBBaseException {
        // do not try and rollback if the producer is busy with COPY
        synchronized(nptBusy) {
            try {
                if (oConn != null) {
                    doTranRollback(aNodeMessage);
                }
            } catch (SQLException se) {
                if (handleSqlException(se, null)) {
                    try {
                        doTranRollback(aNodeMessage);
                    } catch (SQLException se1) {
                        logger.catching(se1);
                        XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                                se1);
                        logger.throwing(ex);
                        throw ex;
                    }
                }
            } finally {
                savepointTable.clear();
                setState(STATEWAIT);
                nptBusy.notify();
            }
        }
    }

    /**
     *
     * @param aNodeMessage
     * @throws org.postgresql.stado.exception.XDBBaseException
     * @throws java.sql.SQLException
     */
    private void doTranCommit(NodeMessage aNodeMessage)
            throws XDBBaseException, SQLException {
        checkCurrentState(new int[] { STATEWAIT }, STATECOMMIT);
        synchronized (oConn) {
            logger.log(XLevel.TRACE,
                    "Entered critical section for connection: %0%",
                    new Object[] { oConn });
            logger.log(XLevel.TRACE, "Committing work for connection: %0%",
                    new Object[] { oConn });
            oConn.commit(); // commit transaction
            logger.log(XLevel.TRACE, "Committed work for connection: %0%",
                    new Object[] { oConn });
            logger.log(XLevel.TRACE,
                    "Leaving critical section for connection: %0%",
                    new Object[] { oConn });
            oConn.notify();
        }
        if (aNodeMessage != null) {
            sendHelper.sendReplyMessage(aNodeMessage,
                    NodeMessage.MSG_TRAN_COMMIT_ACK);
        }
    }

    /**
     *
     *
     * @param aNodeMessage
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    public void tranCommit(NodeMessage aNodeMessage) throws XDBBaseException {
        try {
            doTranCommit(aNodeMessage);
        } catch (SQLException se) {
            if (handleSqlException(se, null)) {
                try {
                    doTranCommit(aNodeMessage);
                } catch (SQLException se1) {
                    logger.catching(se1);
                    XDBBaseException ex = new XDBWrappedSQLException(nodeId,
                            se1);
                    logger.throwing(ex);
                    throw ex;
                }
            }
        } finally {
            synchronized (this) {
                savepointTable.clear();
                setState(STATEWAIT);
            }
        }
    }

    /**
     * We will only pass received messages to main thread for processing and
     * won't block Connector's receiving thread for ages.
     *
     *
     * @see org.postgresql.stado.communication.IMessageListener#processMessage(org.postgresql.stado.communication.message.NodeMessage)
     * @param message
     * @return
     */
    public boolean processMessage(NodeMessage message) {

        NodeMessage.CATEGORY_MESSAGE.debug("NT.processMessage(): node: "
                + nodeId + " received message: " + message);
        switch (message.getMessageType()) {
        case NodeMessage.MSG_KILL:
            synchronized (this) {
                if (oConn != null) {
                    kill();
                }
            }
            break;
        case NodeMessage.MSG_ABORT:
            synchronized (this) {
                if (oConn != null) {
                    kill();
                }
            }
            // Do not break, put MSG_ABORT to the queue
        default:
            msgQueue.offer(message);
        }

        return true;
    }

    /**
     *
     */
    private void kill() {
        final String method = "kill";
        logger.entering(method);
        try {
            // Kill producer first
            if (aNodeProducerThread != null) {
                aNodeProducerThread.kill();
            }

            logger.log(XLevel.TRACE, "Canceling connection %0%",
                    new Object[] { oConn });
            if (aStatement != null) {
                aStatement.cancel();
            }
            logger.log(XLevel.TRACE, "Canceled connection %0%",
                    new Object[] { oConn });
        } catch (SQLException e) {
            logger.catching(e);
        } catch (Throwable t) {
            logger.catching(t);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * To make producerQueue safer, use synchronized
     *
     * @param aNodeMessage
     * @throws org.postgresql.stado.exception.XDBBaseException
     */
    private void producerQueuePut(NodeMessage aNodeMessage)
            throws XDBBaseException {
        // Need to check if we alread created.
        // This fixes a bug after startup and we abort a client session
        // that has temp tables
        if (aNodeProducerThread == null
                || aNodeProducerThread.getState() == NodeProducerThread.STATEDISCONNECTED) {
            initNodeProducerThread(oConn, sendHelper);
        }
        NodeMessage.CATEGORY_MESSAGE.debug("NTP offer: " + aNodeMessage);
        producerQueue.offer(aNodeMessage);
    }

    /**
     * Try and handle the SQLException. Compatibility note: Implementation
     * depends on underlying database
     *
     *
     * @param arg
     * @param se
     * @throws org.postgresql.stado.exception.XDBBaseException
     * @return
     */
    boolean handleSqlException(SQLException se, Object arg)
            throws XDBBaseException {
        // 1
        logger.catching(se);
        switch (se.getErrorCode()) {
        case -708:
            int saveState;
            synchronized (this) {
                saveState = currentState;
                setState(STATEWAIT);
            }
            // Passing null as a sessionID keeps old SendHelper
            reset(null, currentPool);
            checkCurrentState(new int[] { STATEWAIT }, saveState);
            return true;
            // Unknown table name
        case -4004:
            // If it is DROP TABLE ..., ignore the error
            if (arg instanceof String) {
                String sql = ((String) arg).trim();
                if (sql.length() > 5) {
                    String firstWord = sql.substring(0, 4).toUpperCase();
                    if (firstWord.equals("DROP")) {
                        sql = sql.substring(5).trim();
                        if (sql.length() > 6) {
                            String secondWord = sql.substring(0, 5)
                                    .toUpperCase();
                            if (secondWord.equals("TABLE")) {
                                return false;
                            }

                        }
                    }
                }
            }
            break;
        }
        // End 1

        // PostgreSQL section
        if ("57P01".equals(se.getSQLState())
                || "08006".equals(se.getSQLState())) {
            // Broken connection
            try { 
                Connection newConn = currentPool.getConnection();
                currentPool.destroyConnection(oConn);
                oConn = newConn;
                if (aNodeProducerThread != null) {
                    try {
                        aNodeProducerThread.reset();
                    } catch (Throwable t0) {
                        NodeMessage.CATEGORY_MESSAGE.debug("NTP stopping");
                        producerQueue.offer(NodeMessage
                                .getNodeMessage(NodeMessage.MSG_STOP_THREAD));
                        aNodeProducerThread = null;
                    }
                }
                return true;
            } catch (Throwable t) {
                // Ignore, original exception will be thrown
            }
        }
        if ("42P01".equals(se.getSQLState())) {
            // Table not found
            if (arg instanceof String) {
                String sql = ((String) arg).trim();
                if (sql.length() > 5) {
                    String firstWord = sql.substring(0, 4).toUpperCase();
                    if (firstWord.equals("DROP")) {
                        sql = sql.substring(5).trim();
                        if (sql.length() > 6) {
                            String secondWord = sql.substring(0, 5)
                                    .toUpperCase();
                            if (secondWord.equals("TABLE")) {
                                // also rollback, due to PostgreSQL

                                try {
                                    oConn.rollback();
                                } catch (SQLException e) {
                                }
                                return false;
                            }
                        }
                    }
                }
            }
        }
        // End of PostgreSQL section

        //
        if ("08003".equals(se.getSQLState())) {
            // Closed connecton
            try {
                Connection newConn = currentPool.getConnection();
                currentPool.destroyConnection(oConn);
                oConn = newConn;
                if (aNodeProducerThread != null) {
                    try {
                        aNodeProducerThread.reset();
                    } catch (Throwable t0) {
                        NodeMessage.CATEGORY_MESSAGE.debug("NTP stopping");
                        producerQueue.offer(NodeMessage
                                .getNodeMessage(NodeMessage.MSG_STOP_THREAD));
                        aNodeProducerThread = null;
                    }
                }
                return true;
            } catch (Throwable t) {
                // Ignore, original exception will be thrown
            }
        }
        //

        // If error was not handled wrap it and throw up
        XDBBaseException ex = new XDBWrappedSQLException(nodeId, se);
        logger.throwing(ex);
        throw ex;
    }

    /**
     *
     * @return
     */
    int getNodeId() {
        return nodeId;
    }

    /**
     *
     * @return
     */
    Connection getConnection() {
        return oConn;
    }

    /**
     *
     * @return
     */
    SendMessageHelper getSendHelper() {
        return sendHelper;
    }

    /**
     * Save the results of this step for this node in a file
     *
     * @param oConn
     * @param targetTable
     * @param nodeId
     */
    public static void dumpStepResults(Connection oConn, String targetTable,
            int nodeId) {
        HashMap<String, String> propMap = new HashMap<String, String>();
        propMap.put("path", Props.XDB_DUMPSTEPPATH);
        propMap.put("table", targetTable);
        propMap.put("node", "" + nodeId);

        String copyCommand = ParseCmdLine.substitute(Props.XDB_DUMPCOMMAND,
                propMap);

        synchronized (oConn) {
            try {
                Statement stmt = oConn.createStatement();

                stmt.execute(copyCommand);
            } catch (Exception e) {
                // ignore, this is just for debugging
                logger.debug("dumpStepResults Exception");
                logger.catching(e);
            }
        }
    }
}
