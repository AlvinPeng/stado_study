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
 * ExecutableRequest.java
 *
 *
 */
package org.postgresql.stado.engine;

import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.postgresql.stado.common.CommandLog;
import org.postgresql.stado.common.util.XLevel;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.scheduler.BatchCost;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.RequestCost;
import org.postgresql.stado.misc.Timer;
import org.postgresql.stado.parser.IXDBSql;
import org.postgresql.stado.parser.SqlBeginTransaction;
import org.postgresql.stado.parser.SqlBulkInsert;
import org.postgresql.stado.parser.SqlCommitTransaction;
import org.postgresql.stado.parser.SqlModifyTable;
import org.postgresql.stado.parser.SqlRollbackTransaction;
import org.postgresql.stado.parser.SqlSelect;


/**
 *
 */
public class ExecutableRequest {
    private static final XLogger logger = XLogger
            .getLogger(ExecutableRequest.class);

    public static final char STATUS_QUEUED = 'Q';

    public static final char STATUS_PREPARED = 'P';

    public static final char STATUS_EXECUTING = 'E';

    public static final char STATUS_DYING = 'D';

    // BUILD_CUT_START
    public static Timer requestTimer = new Timer();

    public static Timer batchTimer = new Timer();

    // BUILD_CUT_END
    private static int SYS_REQUEST_ID = 0;

    private int requestID;

    private long submitTime;

    private int fetchSize;

    private RequestCost cost;

    private ILockCost[] subRequests;

    private String originalCommand;

    private volatile char status = STATUS_QUEUED;

    private static long nextStatementId = 0;

    private synchronized static long getNextStatementId() {
        if (nextStatementId >= Long.MAX_VALUE) {
            nextStatementId = 0;
        }
        return ++nextStatementId;
    }

    private long statementId = getNextStatementId();

    /**
     *
     * @param command
     */
    public ExecutableRequest(String command) {
        requestID = SYS_REQUEST_ID == Integer.MAX_VALUE ? SYS_REQUEST_ID = 0
                : ++SYS_REQUEST_ID;
        submitTime = System.currentTimeMillis();
        subRequests = null;
        originalCommand = command;
    }

    /**
     *
     * @return
     */
    public int getRequestID() {
        return requestID;
    }

    /**
     * @return Returns the cost. Value returned is never null.
     */
    public RequestCost getCost() {
        if (cost == null) {
            cost = new RequestCost(null);
        }
        return cost;
    }

    /**
     * @return Returns the submitTime.
     */
    public long getSubmitTime() {
        return submitTime;
    }

    /**
     * @return Returns the statement.
     */
    public String getStatement() {
        return originalCommand;
    }

    /**
     * @return Returns the fetchSize.
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     *
     * @return
     */
    public char getStatus() {
        return status;
    }

    /**
     * @param fetchSize
     *                the fetchSize.
     */
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     *
     * @param sqlObject
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void setSQLObject(ILockCost sqlObject) throws XDBServerException {
        if (sqlObject instanceof IPreparable) {
            try {
                ((IPreparable) sqlObject).prepare();
            } catch (Exception e) {
                throw new XDBServerException("Can not prepare request: "
                        + e.getMessage(), e);
            }
        }
        cost = new RequestCost(sqlObject);
        status = STATUS_PREPARED;
    }

    /**
     * @return Returns the subRequests.
     */
    public ILockCost[] getSubRequests() {
        return subRequests;
    }

    /**
     *
     * @param subRequests
     *                The subRequests to set.
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void setSubRequests(ILockCost[] subRequests)
            throws XDBServerException {
        BatchCost bc = new BatchCost();
        for (ILockCost element : subRequests) {
            if (element instanceof SqlModifyTable) {
                ((SqlModifyTable) element).setBatchMode();
            }
            if (element instanceof IPreparable) {
                try {
                    ((IPreparable) element).prepare();
                } catch (Exception e) {
                    throw new XDBServerException(
                            "Can not add request to batch: " + e.getMessage());
                }
            }
            bc.addElement(element);
        }
        cost = new RequestCost(bc);
        this.subRequests = subRequests;
        status = STATUS_PREPARED;
    }

    /**
     *
     * @param engine
     * @param client
     * @return
     */
    ExecutionResult execute(Engine engine, XDBSessionContext client) {
        try {
            try {
                status = STATUS_EXECUTING;
                client.setStatementId(statementId);
                ILockCost sqlObject = cost.getSqlObject();
                if (sqlObject instanceof IExecutable) {
                    Collection<DBNode> nodeList = null;
                    if (sqlObject instanceof IXDBSql) {
                        nodeList = ((IXDBSql) sqlObject).getNodeList();
                    }
                    boolean goPersistent = !(sqlObject instanceof SqlSelect)
                            && !(sqlObject instanceof SqlBulkInsert)
                            && !(sqlObject instanceof SqlBeginTransaction)
                            && !(sqlObject instanceof SqlCommitTransaction)
                            && !(sqlObject instanceof SqlRollbackTransaction);
                    if (goPersistent) {
                        client.setPersistent(true);
                    }
                    try {
                        ExecutionResult result = null;
                        Timer execTimer = new Timer();
                        try {
                            // BUILD_CUT_START
                            requestTimer.startTimer();
                            // BUILD_CUT_END
                            execTimer.startTimer();
                            result = ((IExecutable) sqlObject).execute(engine);
                            execTimer.stopTimer();
                            // BUILD_CUT_START
                            requestTimer.stopTimer();
                            // BUILD_CUT_END
                        } catch (XDBServerException e) {
                            if (!client.isInTransaction()
                                    && !client.isInSubTransaction()) {
                                // Clean up underlying connections before
                                // executing next command
                                engine.rollbackTransaction(client, nodeList);
                            }
                            // else client will explicitly rollback

                            throw e;
                        }
                        if (goPersistent && nodeList != null
                                && nodeList.size() > 1
                                && !result.hasResultSet()
                                && !client.isInTransaction()
                                && !client.isInSubTransaction()
                                && !client.hasActiveResultSets()) {
                            engine.commitTransaction(client, nodeList);
                        }
                        // See if this was a long query
                        CommandLog.checkLongQuery(originalCommand, execTimer
                                .getDurationSeconds(), client);

                        return result;
                    } catch (Exception e) {
                        logger.catching(e);
                        if (!client.isInTransaction()
                                && !client.isInSubTransaction()) {
                            engine.rollbackTransaction(client, nodeList);
                        }
                        logger.throwing(e);
                        throw e;
                    } finally {
                        if (goPersistent) {
                            client.setPersistent(false);
                        }
                    }
                } else if (subRequests != null) {
                    boolean explicitTransaction = !client.isInTransaction();
                    if (explicitTransaction) {
                        engine.beginTransaction(client, null);
                    }
                    // BUILD_CUT_START
                    batchTimer.startTimer();
                    // BUILD_CUT_END
                    try {
                        Map<Integer,ExecutionResult> responses = new HashMap<Integer,ExecutionResult>(
                                subRequests.length);
                        int i = 0;
                        for (ILockCost sqlObjectItem : subRequests) {
                            // Only INSERT, UPDATE and DELETE are supported
                            if (sqlObjectItem instanceof SqlModifyTable) {
                                SqlModifyTable statement = (SqlModifyTable) sqlObjectItem;
                                try {
                                    responses.put(i++,
                                            statement.execute(engine));
                                } catch (Exception e) {
                                    logger.catching(e);
                                    responses.put(
                                            i++,
                                            ExecutionResult.createRowCountResult(
                                                    statement.getResultType(),
                                                    Statement.EXECUTE_FAILED));
                                }
                            } else {
                                logger.warn("Only INSERT, UPDATE and DELETE are allowed in batches");
                                responses.put(
                                        i++,
                                        ExecutionResult.createRowCountResult(
                                                ExecutionResult.COMMAND_UNKNOWN,
                                                Statement.EXECUTE_FAILED));
                            }
                        }
                        if (explicitTransaction) {
                            Collection<DBNode> empty = Collections.emptyList();
                            engine.commitTransaction(client, empty);
                        }
                        return ExecutionResult.createMultipleResult(
                                ExecutionResult.COMMAND_BATCH_EXEC, responses);
                    } catch (Exception e) {
                        logger.catching(e);
                        // Transaction could be terminated if session was closed
                        if (explicitTransaction && client.isInTransaction()) {
                            Collection<DBNode> empty = Collections.emptyList();
                            engine.rollbackTransaction(client, empty);
                        }
                        logger.throwing(e);
                        throw e;
                        // BUILD_CUT_START
                    } finally {
                        batchTimer.stopTimer();
                        // BUILD_CUT_END
                    }
                } else {
                    throw new XDBServerException("Empty request");
                }
            } catch (Exception ex) {
                logger.log(XLevel.TRACE, "Request failed, request: %0%",
                        new Object[] { client });
                logger.catching(ex);
                return ExecutionResult.createErrorResult(ex);
            }
        } finally {
            status = STATUS_PREPARED;
        }
    }

    public void cancel() {
        status = STATUS_DYING;
    }

    /**
     *
     * @return
     */
    public boolean cancelled() {
        return status == STATUS_DYING;
    }

    /**
     *
     * @return the current statement id
     */
    public long getStatementId() {
        return statementId;
    }
}
