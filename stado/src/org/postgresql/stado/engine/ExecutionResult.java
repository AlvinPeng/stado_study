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
/**
 *
 */
package org.postgresql.stado.engine;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.postgresql.stado.metadata.NodeDBConnectionInfo;


/**
 * 
 *
 */
public class ExecutionResult {
    public static final int COMMAND_UNKNOWN = -1;

    public static final int COMMAND_SELECT = 0;

    public static final int COMMAND_ADD_NODES = 1;

    public static final int COMMAND_ALTER_TABLE = 2;

    public static final int COMMAND_ALTER_TABLESPACE = 3;

    public static final int COMMAND_ALTER_USER = 4;

    public static final int COMMAND_VACUUM_ANALYZE = 5;

    public static final int COMMAND_BEGIN_TRAN = 6;

    public static final int COMMAND_CLUSTER = 7;

    public static final int COMMAND_COMMIT_TRAN = 8;

    public static final int COMMAND_ROLLBACK_TRAN = 9;

    public static final int COMMAND_CREATE_INDEX = 10;

    public static final int COMMAND_CREATE_TABLE = 11;

    public static final int COMMAND_CREATE_TABLESPACE = 12;

    public static final int COMMAND_CREATE_USER = 13;

    public static final int COMMAND_CREATE_VIEW = 14;

    public static final int COMMAND_DELETE_TABLE = 15;

    public static final int COMMAND_INSERT_TABLE = 16;

    public static final int COMMAND_UPDATE_TABLE = 17;

    public static final int COMMAND_DROP_INDEX = 18;

    public static final int COMMAND_DROP_TABLE = 19;

    public static final int COMMAND_DROP_TABLESPACE = 20;

    public static final int COMMAND_DROP_NODES = 21;

    public static final int COMMAND_DROP_USER = 22;

    public static final int COMMAND_DROP_VIEW = 23;

    public static final int COMMAND_GRANT = 24;

    public static final int COMMAND_REVOKE = 25;

    public static final int COMMAND_BULK_INSERT = 26;

    public static final int COMMAND_SHOW = 27;

    public static final int COMMAND_MOVE_ROWS = 28;

    public static final int COMMAND_RENAME_TABLE = 29;

    public static final int COMMAND_TRUNCATE = 30;

    public static final int COMMAND_ISOLATION = 31;

    public static final int COMMAND_CREATE_DATABASE = 32;

    public static final int COMMAND_PERSIST_DATABASE = 33;

    public static final int COMMAND_DROP_DATABASE = 34;

    public static final int COMMAND_START_DATABASE = 35;

    public static final int COMMAND_STOP_DATABASE = 36;

    public static final int COMMAND_LOGIN = 37;

    public static final int COMMAND_LOGOUT = 38;

    public static final int COMMAND_KILL = 39;

    public static final int COMMAND_SERVER_PROPS = 40;

    public static final int COMMAND_ERROR = 41;

    public static final int COMMAND_BATCH_EXEC = 42;

    public static final int COMMAND_DIRECT_EXEC = 43;

    public static final int COMMAND_NODE_INFO = 44;

    public static final int COMMAND_COPY_IN = 45;

    public static final int COMMAND_COPY_OUT = 46;

    public static final int COMMAND_DEALLOCATE = 47;
    
    public static final int COMMAND_UNLISTEN = 48;

    public static final int COMMAND_EXPLAIN = 49;

    public static final int COMMAND_SET = 50;

    public static final int COMMAND_DECLARE_CURSOR = 51;
    
    public static final int COMMAND_CLOSE_CURSOR = 52;
    
    public static final int COMMAND_EMPTY_QUERY = 53;
    
    public static final int CONTENT_TYPE_EMPTY = 0;

    public static final int CONTENT_TYPE_ROWCOUNT = 1;

    public static final int CONTENT_TYPE_RESULTSET = 2;

    public static final int CONTENT_TYPE_EXCEPTION = 3;

    public static final int CONTENT_TYPE_SUBRESULTS = 4;

    public static final int CONTENT_TYPE_GENERATOR_VALUE = 5;

    public static final int CONTENT_TYPE_SERIALIZED_OBJECT = 6;

    public static final int CONTENT_TYPE_NODE_INFO = 7;

    private int kind;

    private int contentType;

    private int rowsAffected;

    private ResultSet rs = null;

    private Exception ex = null;

    private long rangeStart = 0;

    private byte[] serializedObject = null;

    private NodeDBConnectionInfo[] connectionInfos = null;

    private Map<Integer,ExecutionResult> subResults;

    /**
     *
     * @return
     */
    public static ExecutionResult createSuccessResult(int command) {
        return new ExecutionResult(command);
    }

    /**
     *
     * @param rowsAffected
     * @return
     */
    public static ExecutionResult createRowCountResult(int command,
            int rowsAffected) {
        return new ExecutionResult(command, rowsAffected);
    }

    /**
     *
     * @param rs
     * @return
     */
    public static ExecutionResult createResultSetResult(int command,
            ResultSet rs) {
        return new ExecutionResult(command, rs);
    }

    /**
     *
     * @param ex
     * @return
     */
    public static ExecutionResult createErrorResult(Exception ex) {
        return new ExecutionResult(ex);
    }

    /**
     *
     * @param rangeStart
     * @return
     */
    public static ExecutionResult createGeneratorRangeResult(int command,
            long rangeStart) {
        return new ExecutionResult(command, rangeStart);
    }

    /**
     *
     * @param data
     * @return
     */
    public static ExecutionResult createSerializedObjectResult(int command,
            byte[] data) {
        return new ExecutionResult(command, data);
    }

    /**
     *
     * @param connectionInfos
     * @return
     */
    public static ExecutionResult createNodeConnectionInfoResult(
            NodeDBConnectionInfo[] connectionInfos) {
        return new ExecutionResult(connectionInfos);
    }

    /**
     *
     * @param batchResults
     * @return
     */
    public static ExecutionResult createMultipleResult(int command,
            Map<Integer,ExecutionResult> subResults) {
        return new ExecutionResult(command, subResults);
    }

    /**
     *
     * @param rowsAffected
     */
    private ExecutionResult(int command) {
        contentType = CONTENT_TYPE_EMPTY;
        kind = command;
        this.rowsAffected = Statement.SUCCESS_NO_INFO;
    }

    /**
     *
     * @param rowsAffected
     */
    private ExecutionResult(int command, int rowsAffected) {
        contentType = CONTENT_TYPE_ROWCOUNT;
        kind = command;
        this.rowsAffected = rowsAffected;
    }

    /**
     *
     * @param rs
     */
    private ExecutionResult(int command, ResultSet rs) {
        contentType = CONTENT_TYPE_RESULTSET;
        kind = command;
        this.rs = rs;
    }

    /**
     *
     * @param ex
     */
    private ExecutionResult(Exception ex) {
        contentType = CONTENT_TYPE_EXCEPTION;
        kind = COMMAND_ERROR;
        this.ex = ex;
    }

    /**
     *
     * @param rangeStart
     */
    private ExecutionResult(int command, long rangeStart) {
        contentType = CONTENT_TYPE_GENERATOR_VALUE;
        kind = command;
        this.rangeStart = rangeStart;
    }

    /**
     *
     * @param serializedObject
     */
    private ExecutionResult(int command, byte[] serializedObject) {
        contentType = CONTENT_TYPE_SERIALIZED_OBJECT;
        kind = command;
        this.serializedObject = serializedObject;
    }

    /**
     *
     * @param connectionInfos
     */
    private ExecutionResult(NodeDBConnectionInfo[] connectionInfos) {
        contentType = CONTENT_TYPE_NODE_INFO;
        kind = COMMAND_NODE_INFO;
        this.connectionInfos = connectionInfos;
    }

    /**
     *
     * @param batchResults
     */
    private ExecutionResult(int command, Map<Integer,ExecutionResult> subResults) {
        contentType = CONTENT_TYPE_SUBRESULTS;
        kind = command;
        this.subResults = subResults;
    }

    /**
     *
     * @return
     */
    public int getKind() {
        return kind;
    }

    /**
     *
     * @return
     */
    public int getContentType() {
        return contentType;
    }

    /**
     *
     * @return
     */
    public boolean isError() {
        return kind == COMMAND_ERROR;
    }

    /**
     *
     * @return
     */
    public int getRowCount() {
        return rowsAffected;
    }

    /**
     *
     * @return
     */
    public ResultSet getResultSet() {
        return rs;
    }

    /**
     *
     * @return
     */
    public Exception getException() {
        return ex;
    }

    /**
     *
     * @return
     */
    public long getRangeStart() {
        return rangeStart;
    }

    /**
     *
     * @return
     */
    public byte[] getSerializedObject() {
        return serializedObject;
    }

    /**
     *
     * @return
     */
    public NodeDBConnectionInfo[] getNodeConnectionInfo() {
        return connectionInfos;
    }

    /**
     *
     * @return
     */
    public Map<Integer,ExecutionResult> getSubResults() {
        return subResults;
    }

    public boolean hasResultSet() {
        if (rs != null) {
            return true;
        }
        if (subResults != null) {
            for (ExecutionResult subResult : subResults.values()) {
                if (subResult.rs != null) {
                    return true;
                }
            }
        }
        return false;
    }

}
