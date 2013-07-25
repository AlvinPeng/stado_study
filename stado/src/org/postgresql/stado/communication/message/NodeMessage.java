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
package org.postgresql.stado.communication.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.NodeResultSetImpl;
import org.postgresql.stado.exception.XDBBaseException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.planner.StepDetail;


/**
 *
 *
 */
public class NodeMessage implements Serializable, Cloneable {
    /**
     *
     */
    private static final long serialVersionUID = -651288860476875902L;

    public static final XLogger CATEGORY_MESSAGE = XLogger
            .getLogger("MessageFlow");

    private static final XLogger logger = XLogger.getLogger(NodeMessage.class);

    public static final int MSG_KILL = 0;

    public static final int MSG_ABORT = 1;

    public static final int MSG_ABORT_ACK = 101;

    public static final int MSG_CONNECTION_BEGIN = 2;

    public static final int MSG_CONNECTION_BEGIN_ACK = 3;

    public static final int MSG_CONNECTION_END = 4;

    public static final int MSG_CONNECTION_END_ACK = 5;

    public static final int MSG_ERROR = 6;

    public static final int MSG_EXEC_COMMAND = 7;

    public static final int MSG_EXEC_COMMAND_RESULT = 8;

    public static final int MSG_EXEC_QUERY = 107;

    public static final int MSG_EXEC_QUERY_RESULT = 108;

    public static final int MSG_PREPARE_COMMAND = 71;

    public static final int MSG_PREPARE_COMMAND_ACK = 81;

    public static final int MSG_PREPARE_COMMAND_EXEC = 72;

    public static final int MSG_PREPARE_COMMAND_CLOSE = 73;

    public static final int MSG_PREPARE_COMMAND_CLOSE_ACK = 83;

    public static final int MSG_EXECUTE_STEP_INIT = 9;

    public static final int MSG_EXECUTE_STEP_INIT_ACK = 109;

    public static final int MSG_EXECUTE_STEP_RUN = 209;

    public static final int MSG_EXECUTE_STEP_END = 10;

    public static final int MSG_EXECUTE_STEP_END_ACK = 110;

    public static final int MSG_EXECUTE_STEP_SENT = 11;

    public static final int MSG_INIT_FROM_SYS = 12;

    public static final int MSG_INIT_FROM_SYS_ACK = 13;

    public static final int MSG_SHUTDOWN_FROM_SYS = 62;

    public static final int MSG_SHUTDOWN_FROM_SYS_ACK = 63;

    public static final int MSG_NODE_UP = 112;

    public static final int MSG_NODE_UP_ACK = 113;

    public static final int MSG_PING = 14;

    public static final int MSG_PING_ACK = 15;

    public static final int MSG_SEND_DATA = 16;

    public static final int MSG_SEND_DATA_ACK = 17;

    public static final int MSG_SEND_DATA_INIT = 19;

    public static final int MSG_TRAN_COMMIT = 22;

    public static final int MSG_TRAN_COMMIT_ACK = 23;

    public static final int MSG_TRAN_ROLLBACK = 24;

    public static final int MSG_TRAN_ROLLBACK_ACK = 25;

    public static final int MSG_DROP_TEMP_TABLES = 26;

    public static final int MSG_DROP_TEMP_TABLES_ACK = 27;

    public static final int MSG_TRAN_BEGIN_SAVEPOINT = 28;

    public static final int MSG_TRAN_BEGIN_SAVEPOINT_ACK = 29;

    public static final int MSG_TRAN_ROLLBACK_SAVEPOINT = 30;

    public static final int MSG_TRAN_ROLLBACK_SAVEPOINT_ACK = 31;

    public static final int MSG_TRAN_END_SAVEPOINT = 32;

    public static final int MSG_TRAN_END_SAVEPOINT_ACK = 33;

    public static final int MSG_EXECUTE_BATCH = 40;

    public static final int MSG_EXECUTE_BATCH_ACK = 41;

    public static final int MSG_RESULT_CLOSE = 142;

    public static final int MSG_RESULT_ROWS_REQUEST = 42;

    public static final int MSG_RESULT_ROWS = 43;

    public static final int MSG_INIT_PROPERTIES = 44;

    public static final int MSG_START_LOADERS = 45;

    public static final int MSG_START_LOADERS_ACK = 145;

    public static final int MSG_BEGIN_SEND_ROWS = 46;

    public static final int MSG_END_SEND_ROWS = 47;

    // This is likely temporary, until NodeThreads are pooled and persist
    public static final int MSG_STOP_THREAD = 1001;

    public static final int MAX_DATA_ROWS = 1000;

    // Size in chars, 30000 chars == 60000 bytes
    public static final int MAX_DATA_SIZE = 35000; // 35000;

    /**
     * Message Type
     */
    private int msgType;

    /**
     * Target node number. In case of broadcust it is <CODE>null</CODE>
     */
    protected Integer[] targetNodeIDs;

    /**
     * Source node number Address where to send response
     */
    private int sourceNodeID;

    /**
     * The request number within the session
     */
    private int requestId;

    /**
     * When message comes to node from network it needs to find its exact target
     * among multiple NodeThreads running there. <CODE>sessionID</CODE> is
     * allowing this.
     */
    private Integer sessionID;

    /** Parameterless constructor required for serialization */
    public NodeMessage() {
    }

    /** Creates a new instance of NodeMessage */
    protected NodeMessage(int messageType) {
        msgType = messageType;
    }

    /**
     * Factory method
     *
     * @param messageType
     */
    public static final NodeMessage getNodeMessage(int messageType) {
        switch (messageType) {
        case MSG_EXEC_QUERY_RESULT:
        case MSG_EXECUTE_STEP_SENT:
        case MSG_RESULT_CLOSE:
        case MSG_RESULT_ROWS_REQUEST:
        case MSG_RESULT_ROWS:
            return new ResultSetMessage(messageType);

        case MSG_EXECUTE_STEP_RUN:
        case MSG_EXECUTE_STEP_INIT:
            return new StepDetailMessage(messageType);

        case MSG_INIT_PROPERTIES:
        case MSG_EXECUTE_BATCH:
        case MSG_SEND_DATA:
        case MSG_SEND_DATA_ACK:
        case MSG_DROP_TEMP_TABLES:
        case MSG_PREPARE_COMMAND:
        case MSG_PREPARE_COMMAND_EXEC:
            return new DataRowsMessage(messageType);

        case MSG_EXECUTE_BATCH_ACK:
            return new BatchResultMessage(messageType);

        case MSG_EXEC_QUERY:
        case MSG_TRAN_BEGIN_SAVEPOINT:
        case MSG_TRAN_END_SAVEPOINT:
        case MSG_TRAN_ROLLBACK_SAVEPOINT:
        case MSG_SEND_DATA_INIT:
        case MSG_PREPARE_COMMAND_CLOSE:
            return new CommandMessage(messageType);

        case MSG_EXEC_COMMAND:
            return new CommandExtMessage(messageType);

        case MSG_EXEC_COMMAND_RESULT:
            return new UpdateResultMessage(messageType);

        case MSG_ABORT:
            return new AbortMessage(messageType);

        case MSG_INIT_FROM_SYS:
        case MSG_INIT_FROM_SYS_ACK:
        case MSG_SHUTDOWN_FROM_SYS:
        case MSG_SHUTDOWN_FROM_SYS_ACK:
        case MSG_CONNECTION_BEGIN:
        case MSG_START_LOADERS:
            return new ConnectMessage(messageType);

            // case MSG_START_LOADERS: // for distributed Loader, remove ?

        case MSG_BEGIN_SEND_ROWS:
        case MSG_END_SEND_ROWS:
            return new SendRowsMessage(messageType);

        default:
            return new NodeMessage(messageType);
        }
    }

    public static final byte[] getBytes(NodeMessage message) throws IOException {
        // Never compress by default
        return getBytes(message, Integer.MAX_VALUE);
    }

    public static final byte[] getBytes(NodeMessage message,
            int compressThreshold) throws IOException {
        byte[] aMessage = null;
        // TODO find out good initial size
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            // Indicate not compressed stream
            baos.write(0);
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            try {
                try {
                    oos.writeObject(message);
                } catch (NotSerializableException e) {
                    logger.catching(e);
                    throw new IOException("Can not serialize message "
                            + message);
                }
                oos.flush();
                aMessage = baos.toByteArray();
                logger.debug("Encode message, size: " + aMessage.length);
            } finally {
                oos.close();
            }
        } finally {
            baos.close();
        }
        if (aMessage.length < compressThreshold) {
            return aMessage;
        }

        // TODO find out good initial size
        baos = new ByteArrayOutputStream();
        try {
            // Indicate compressed stream
            baos.write(1);
            ZipOutputStream zos = new ZipOutputStream(baos);
            try {
                zos.putNextEntry(new ZipEntry("x"));
                // Skip compression indicator
                zos.write(aMessage, 1, aMessage.length - 1);
                zos.closeEntry();
                zos.flush();
                logger.debug("Compress message, size: " + baos.size());
                return baos.toByteArray();
            } finally {
                zos.close();
            }
        } finally {
            baos.close();
        }
    }

    public static final NodeMessage decodeBytes(byte[] bytes)
    throws IOException, ClassNotFoundException {
        return decodeBytes(bytes, 0, bytes.length);
    }

    public static final NodeMessage decodeBytes(byte[] bytes, int offset,
            int length)
            throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes, offset + 1,
                length - 1);
        try {
            if (bytes[offset] == 0) {
                ObjectInputStream ois = new ObjectInputStream(bais);
                try {
                    return (NodeMessage) ois.readObject();
                } finally {
                    ois.close();
                }
            } else {
                ZipInputStream zis = new ZipInputStream(bais);
                try {
                    zis.getNextEntry();
                    ObjectInputStream ois = new ObjectInputStream(zis);
                    try {
                        return (NodeMessage) ois.readObject();
                    } finally {
                        ois.close();
                    }
                } finally {
                    zis.close();
                }
            }
        } finally {
            bais.close();
        }
    }

    static public String getMessageTypeString(int messageType) {
        String msgTypeStr = "";

        switch (messageType) {
        case MSG_KILL:
            msgTypeStr = "MSG_KILL";
            break;
        case MSG_ABORT:
            msgTypeStr = "MSG_ABORT";
            break;
        case MSG_ABORT_ACK:
            msgTypeStr = "MSG_ABORT_ACK";
            break;
        case MSG_CONNECTION_BEGIN:
            msgTypeStr = "MSG_CONNECTION_BEGIN";
            break;
        case MSG_CONNECTION_BEGIN_ACK:
            msgTypeStr = "MSG_CONNECTION_BEGIN_ACK";
            break;
        case MSG_CONNECTION_END:
            msgTypeStr = "MSG_CONNECTION_END";
            break;
        case MSG_CONNECTION_END_ACK:
            msgTypeStr = "MSG_CONNECTION_END_ACK";
            break;
        case MSG_ERROR:
            msgTypeStr = "MSG_ERROR";
            break;
        case MSG_EXEC_COMMAND:
            msgTypeStr = "MSG_EXEC_COMMAND";
            break;
        case MSG_EXEC_COMMAND_RESULT:
            msgTypeStr = "MSG_EXEC_COMMAND_RESULT";
            break;
        case MSG_EXEC_QUERY:
            msgTypeStr = "MSG_EXEC_QUERY";
            break;
        case MSG_EXEC_QUERY_RESULT:
            msgTypeStr = "MSG_EXEC_QUERY_RESULT";
            break;
        case MSG_PREPARE_COMMAND:
            msgTypeStr = "MSG_PREPARE_COMMAND";
            break;
        case MSG_PREPARE_COMMAND_ACK:
            msgTypeStr = "MSG_PREPARE_COMMAND_ACK";
            break;
        case MSG_PREPARE_COMMAND_EXEC:
            msgTypeStr = "MSG_PREPARE_COMMAND_EXEC";
            break;
        case MSG_PREPARE_COMMAND_CLOSE:
            msgTypeStr = "MSG_PREPARE_COMMAND_CLOSE";
            break;
        case MSG_PREPARE_COMMAND_CLOSE_ACK:
            msgTypeStr = "MSG_PREPARE_COMMAND_CLOSE_ACK";
            break;
        case MSG_EXECUTE_STEP_INIT:
            msgTypeStr = "MSG_EXECUTE_STEP_INIT";
            break;
        case MSG_EXECUTE_STEP_INIT_ACK:
            msgTypeStr = "MSG_EXECUTE_STEP_INIT_ACK";
            break;
        case MSG_EXECUTE_STEP_RUN:
            msgTypeStr = "MSG_EXECUTE_STEP_RUN";
            break;
        case MSG_EXECUTE_STEP_END:
            msgTypeStr = "MSG_EXECUTE_STEP_END";
            break;
        case MSG_EXECUTE_STEP_END_ACK:
            msgTypeStr = "MSG_EXECUTE_STEP_END_ACK";
            break;
        case MSG_EXECUTE_STEP_SENT:
            msgTypeStr = "MSG_EXECUTE_STEP_SENT";
            break;
        case MSG_INIT_FROM_SYS:
            msgTypeStr = "MSG_INIT_FROM_SYS";
            break;
        case MSG_INIT_FROM_SYS_ACK:
            msgTypeStr = "MSG_INIT_FROM_SYS_ACK";
            break;
        case MSG_SHUTDOWN_FROM_SYS:
            msgTypeStr = "MSG_SHUTDOWN_FROM_SYS";
            break;
        case MSG_SHUTDOWN_FROM_SYS_ACK:
            msgTypeStr = "MSG_SHUTDOWN_FROM_SYS_ACK";
            break;
        case MSG_NODE_UP:
            msgTypeStr = "MSG_NODE_UP";
            break;
        case MSG_PING:
            msgTypeStr = "MSG_PING";
            break;
        case MSG_PING_ACK:
            msgTypeStr = "MSG_PING_ACK";
            break;
        case MSG_SEND_DATA:
            msgTypeStr = "MSG_SEND_DATA";
            break;
        case MSG_SEND_DATA_ACK:
            msgTypeStr = "MSG_SEND_DATA_ACK";
            break;
        case MSG_SEND_DATA_INIT:
            msgTypeStr = "MSG_SEND_DATA_INIT";
            break;
        case MSG_TRAN_COMMIT:
            msgTypeStr = "MSG_TRAN_COMMIT";
            break;
        case MSG_TRAN_COMMIT_ACK:
            msgTypeStr = "MSG_TRAN_COMMIT_ACK";
            break;
        case MSG_TRAN_ROLLBACK:
            msgTypeStr = "MSG_TRAN_ROLLBACK";
            break;
        case MSG_TRAN_ROLLBACK_ACK:
            msgTypeStr = "MSG_TRAN_ROLLBACK_ACK";
            break;
        case MSG_DROP_TEMP_TABLES:
            msgTypeStr = "MSG_DROP_TEMP_TABLES";
            break;
        case MSG_DROP_TEMP_TABLES_ACK:
            msgTypeStr = "MSG_DROP_TEMP_TABLES_ACK";
            break;
        case MSG_TRAN_BEGIN_SAVEPOINT:
            msgTypeStr = "MSG_TRAN_BEGIN_SAVEPOINT";
            break;
        case MSG_TRAN_BEGIN_SAVEPOINT_ACK:
            msgTypeStr = "MSG_TRAN_BEGIN_SAVEPOINT_ACK";
            break;
        case MSG_TRAN_ROLLBACK_SAVEPOINT:
            msgTypeStr = "MSG_TRAN_ROLLBACK_SAVEPOINT";
            break;
        case MSG_TRAN_ROLLBACK_SAVEPOINT_ACK:
            msgTypeStr = "MSG_TRAN_ROLLBACK_SAVEPOINT_ACK";
            break;
        case MSG_TRAN_END_SAVEPOINT:
            msgTypeStr = "MSG_TRAN_END_SAVEPOINT";
            break;
        case MSG_TRAN_END_SAVEPOINT_ACK:
            msgTypeStr = "MSG_TRAN_END_SAVEPOINT_ACK";
            break;
        case MSG_EXECUTE_BATCH:
            msgTypeStr = "MSG_EXECUTE_BATCH";
            break;
        case MSG_EXECUTE_BATCH_ACK:
            msgTypeStr = "MSG_EXECUTE_BATCH_ACK";
            break;
        case MSG_RESULT_ROWS_REQUEST:
            msgTypeStr = "MSG_RESULT_ROWS_REQUEST";
            break;
        case MSG_RESULT_CLOSE:
            msgTypeStr = "MSG_RESULT_CLOSE";
            break;
        case MSG_RESULT_ROWS:
            msgTypeStr = "MSG_RESULT_ROWS";
            break;
        case MSG_STOP_THREAD:
            msgTypeStr = "MSG_STOP_THREAD";
            break;
        case MSG_NODE_UP_ACK:
            msgTypeStr = "MSG_NODE_UP_ACK";
            break;
        case MSG_INIT_PROPERTIES:
            msgTypeStr = "MSG_INIT_PROPERTIES";
            break;
        case MSG_START_LOADERS:
            msgTypeStr = "MSG_START_LOADERS";
            break;
        case MSG_START_LOADERS_ACK:
            msgTypeStr = "MSG_START_LOADERS_ACK";
            break;
        default:
            msgTypeStr = (new Integer(messageType)).toString();
        }
        return msgTypeStr;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("NodeMessage[Type:").append(getMessageTypeString()).append(
                ", From:").append(sourceNodeID).append(", To: [");
        if (targetNodeIDs != null) {
            for (int i = 0; i < targetNodeIDs.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(targetNodeIDs[i]);
            }
        }
        sb.append("], Session: ").append(sessionID).append(", Request: ")
                .append(requestId).append(getDescription()).append("]");
        return sb.toString();
    }

    protected String getDescription() {
        return "";
    }

    public int getMessageType() {
        return msgType;
    }

    public String getMessageTypeString() {
        return getMessageTypeString(msgType);
    }

    /**
     * Getter for property targetNodeID.
     *
     * @return Value of property targetNodeID.
     */
    public Integer getTargetNodeID() {
        return targetNodeIDs == null || targetNodeIDs.length != 1 ? null
                : targetNodeIDs[0];
    }

    /**
     * Setter for property targetNodeID.
     *
     * @param targetNodeID
     *                New value of property targetNodeID.
     */
    public void setTargetNodeID(Integer targetNodeID) {
        targetNodeIDs = targetNodeID == null ? null
                : new Integer[] { targetNodeID };
    }

    /**
     * Setter for property targetNodeIDs.
     *
     * @param targetNodeIDs
     *                New value of property targetNodeIDs.
     */
    public void setTargetNodeIDs(Integer[] targetNodeIDs) {
        this.targetNodeIDs = targetNodeIDs;
    }

    /**
     * @return the list ot target nodes
     */
    public Collection<Integer> getNodeList() {
        if (targetNodeIDs == null) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(targetNodeIDs);
        }
    }

    /**
     * Getter for property sourceNodeID.
     *
     * @return Value of property sourceNodeID.
     */
    public int getSourceNodeID() {
        return sourceNodeID;
    }

    /**
     * Setter for property sourceNodeID.
     *
     * @param sourceNodeID
     *                New value of property sourceNodeID.
     */
    public void setSourceNodeID(int sourceNodeID) {
        this.sourceNodeID = sourceNodeID;
    }

    /**
     * Getter for property sessionID.
     *
     * @return Value of property sessionID.
     */
    public Integer getSessionID() {
        return sessionID;
    }

    /**
     * Setter for property sessionID.
     *
     * @param sessionID
     *                New value of property sessionID.
     */
    public void setSessionID(Integer sessionID) {
        this.sessionID = sessionID;
    }

    /**
     * @param value
     *                the request id
     */
    public void setRequestId(int value) {
        this.requestId = value;
    }

    /**
     * @return the request id
     */
    public int getRequestId() {
        return this.requestId;
    }

    /*
     * To track message status in a Connector
     */
    private transient int sendAttempt = 0;

    public int sendFailed() {
        return ++sendAttempt;
    }

    /*
     * Methods for message carrying result set MSG_EXEC_QUERY_RESULT
     * MSG_EXECUTE_STEP_SENT
     */

    public NodeResultSetImpl getResultSet() {
        throw new XDBServerException(
                "Method is not implemented: getResultSet()");
    }

    public String getResultSetID() {
        throw new XDBServerException(
                "Method is not implemented: getResultSetID()");
    }

    public void setResultSetID(String resultSetID) {
        throw new XDBServerException(
                "Method is not implemented: setResultSetID(String)");
    }

    public byte[] getResultSetData() {
        throw new XDBServerException(
                "Method is not implemented: getResultSetData()");
    }

    public void setResultSetData(byte[] resultSetData) {
        throw new XDBServerException(
                "Method is not implemented: setResultSetData(byte[])");
    }

    public boolean isResultSetHasMoreRows() {
        throw new XDBServerException(
                "Method is not implemented: isResultSetHasMoreRows()");
    }

    public void setResultSetHasMoreRows(boolean value) {
        throw new XDBServerException(
                "Method is not implemented: setResultSetHasMoreRows(boolean)");
    }

    /*
     * Methods for message carrying step detail MSG_SEND_DATA
     * MSG_EXECUTE_STEP_RUN MSG_EXECUTE_STEP_INIT
     */
    public void setStepDetail(StepDetail stepDetails) {
        throw new XDBServerException(
                "Method is not implemented: setStepDetail(StepDetail)");
    }

    public StepDetail getStepDetail() {
        throw new XDBServerException(
                "Method is not implemented: getStepDetail()");
    }

    /*
     * Methods for message carrying row data (array of strings, insert
     * statements or just values) MSG_INIT_PROPERTIES MSG_SEND_DATA
     * MSG_EXECUTE_BATCH
     */
    /**
     * Add row data, returns the row number (from 0)
     */
    public int addRowData(String rowString) {
        throw new XDBServerException(
                "Method is not implemented: setRowData(String)");
    }

    public String[] getRowData() {
        throw new XDBServerException("Method is not implemented: getRowData()");
    }

    public int getRowCount() {
        throw new XDBServerException("Method is not implemented: getRowCount()");
    }

    public boolean canAddRows() {
        throw new XDBServerException("Method is not implemented: canAddRows()");
    }

    public void setDataSeqNo(long value) {
        throw new XDBServerException(
                "Method is not implemented: setDataSeqNo(long)");
    }

    public long getDataSeqNo() {
        throw new XDBServerException(
                "Method is not implemented: getDataSeqNo()");
    }

    /*
     * Methods for message carrying batch results MSG_EXECUTE_BATCH_ACK
     */
    public void setBatchResult(int[] result) {
        throw new XDBServerException(
                "Method is not implemented: setBatchResult(int[])");
    }

    public int[] getBatchResult() {
        throw new XDBServerException(
                "Method is not implemented: getBatchResult()");
    }

    /*
     * Methods for message carrying sql statement MSG_SHUTDOWN_FROM_SYS
     * MSG_EXEC_COMMAND MSG_EXEC_QUERY MSG_START_LOADERS
     */
    public void setSqlCommand(String sqlCommandString) {
        throw new XDBServerException(
                "Method is not implemented: setSqlCommand(String)");
    }

    /*
     * MSG_EXEC_COMMAND
     */
    public String getSqlCommand() {
        throw new XDBServerException(
                "Method is not implemented: getSqlCommand()");
    }

    public boolean getAutocommit() {
        throw new XDBServerException(
                "Method is not implemented: getAutoCommit()");
    }

    public void setAutocommit(boolean autocommit) {
        throw new XDBServerException(
                "Method is not implemented: setAutoCommit(boolean)");
    }

    /*
     * MSG_DROP_TEMP_TABLES
     */
    public void setTempTables(Collection<String> tableNames) {
        throw new XDBServerException(
                "Method is not implemented: setTempTables(String)");
    }

    public Collection<String> getTempTables() {
        throw new XDBServerException(
                "Method is not implemented: getTempTables()");
    }

    /*
     * MSG_TRAN_BEGIN_SAVEPOINT MSG_TRAN_END_SAVEPOINT
     * MSG_TRAN_ROLLBACK_SAVEPOINT
     */
    public void setSavepoint(String savepoint) {
        throw new XDBServerException(
                "Method is not implemented: setSavepoint(String)");
    }

    public String getSavepoint() {
        throw new XDBServerException(
                "Method is not implemented: getSavepoint()");
    }

    /*
     * MSG_SEND_DATA_INIT MSG_START_LOADERS
     */
    public void setTargetTable(String tableName) {
        throw new XDBServerException(
                "Method is not implemented: setTargetTable(String)");
    }

    public String getTargetTable() {
        throw new XDBServerException(
                "Method is not implemented: getTargetTable()");
    }

    /*
     * Methods for message carrying result of update MSG_EXEC_COMMAND_RESULT
     */
    public void setNumRowsResult(int numRows) {
        throw new XDBServerException(
                "Method is not implemented: setNumRowsResult(int)");
    }

    public int getNumRowsResult() {
        throw new XDBServerException(
                "Method is not implemented: getNumRowsResult()");
    }

    /*
     * Methods for message carrying exception MSG_ABORT
     */
    public XDBBaseException getCause() {
        throw new XDBServerException("Method is not implemented: getCause()");
    }

    public void setCause(XDBBaseException ex) {
        throw new XDBServerException(
                "Method is not implemented: setCause(XDBBaseException)");
    }

    /*
     * MSG_INIT_FROM_SYS MSG_INIT_FROM_SYS_ACK MSG_SHUTDOWN_FROM_SYS
     * MSG_SHUTDOWN_FROM_SYS_ACK MSG_CONNECTION_BEGIN MSG_START_LOADERS
     */
    /**
     * @return the database name
     */
    public String getDatabase() {
        throw new XDBServerException("Method is not implemented: getDatabase()");
    }

    /**
     * @param database
     *                the database name
     */
    public void setDatabase(String database) {
        throw new XDBServerException(
                "Method is not implemented: setDatabase(String)");
    }

    /*
     * MSG_INIT_FROM_SYS
     */
    /**
     * @return the fully qualified name of class of JDBC driver
     */
    public String getJdbcDriver() {
        throw new XDBServerException(
                "Method is not implemented: getJdbcDriver()");
    }

    /**
     * @return the JDBC URI
     */
    public String getJdbcString() {
        throw new XDBServerException(
                "Method is not implemented: getJdbcString()");
    }

    /**
     * @return the user name
     */
    public String getJdbcUser() {
        throw new XDBServerException("Method is not implemented: getJdbcUser()");
    }

    /**
     * @return the password
     */
    public String getJdbcPassword() {
        throw new XDBServerException(
                "Method is not implemented: getJdbcPassword()");
    }

    /**
     * @return max number of connections in the pool
     */
    public int getMaxConns() {
        throw new XDBServerException("Method is not implemented: getMaxConns()");
    }

    /**
     * @return min number of connections in the pool
     */
    public int getMinConns() {
        throw new XDBServerException("Method is not implemented: getMinConns()");
    }

    /**
     * @return the timeout
     */
    public long getTimeOut() {
        throw new XDBServerException("Method is not implemented: getTimeOut()");
    }

    /**
     * @param string
     *                the fully qualified name of class of JDBC driver
     */
    public void setJdbcDriver(String string) {
        throw new XDBServerException(
                "Method is not implemented: setJdbcDriver(String)");
    }

    /**
     * @param string
     *                the JDBC URI
     */
    public void setJdbcString(String string) {
        throw new XDBServerException(
                "Method is not implemented: setJdbcString(String)");
    }

    /**
     * @param string
     *                the user name
     */
    public void setJdbcUser(String string) {
        throw new XDBServerException(
                "Method is not implemented: setJdbcUser(String)");
    }

    /**
     * @param string
     *                the password
     */
    public void setJdbcPassword(String string) {
        throw new XDBServerException(
                "Method is not implemented: setJdbcPassword(String)");
    }

    /**
     * @param maxConns
     *                max number of connections in the pool
     */
    public void setMaxConns(int maxConns) {
        throw new XDBServerException(
                "Method is not implemented: setMaxConns(int)");
    }

    /**
     * @param msgType
     *                the message type
     */
    public void setMessageType(int msgType) {
        this.msgType = msgType;
    }

    /**
     * @param minConns
     *                min number of connections in the pool
     */
    public void setMinConns(int minConns) {
        throw new XDBServerException(
                "Method is not implemented: setMinConns(int)");
    }

    /**
     * @param timeOut
     *                the timeout
     */
    public void setTimeOut(long timeOut) {
        throw new XDBServerException(
                "Method is not implemented: setTimeOut(long)");
    }

    /**
     * This method was introduced due to bug of sending message to multiple
     * recipients. When SendMessageHelper sent NodeMessage to list of targets it
     * iterated through the list, assigned target to message and passed it to
     * Agent's queue. When Agent started processing of its queue there were
     * number of references to single NodeMessage, and all the messages were
     * sent to one target. Now SendMessageHelper sends copies of the origin
     * messages to supplied list.
     *
     * @return Copy of the messasge
     * @see org.postgresql.stado.communication.SendMessageHelper
     * @see org.postgresql.stado.communication.CoordinatorAgent
     * @see org.postgresql.stado.communication.NodeAgent
     */
    @Override
    public Object clone() {
        NodeMessage result = null;
        try {
            result = (NodeMessage) super.clone();
            // Mutable array - override default behavior
            if (targetNodeIDs != null) {
                result.targetNodeIDs = new Integer[targetNodeIDs.length];
                // Integer is immutable, therefore just copy references
                System.arraycopy(targetNodeIDs, 0, result.targetNodeIDs, 0,
                        targetNodeIDs.length);
            }
        } catch (CloneNotSupportedException cnse) {
            // Never occurs since we are Cloneable
        }
        return result;
    }
}
