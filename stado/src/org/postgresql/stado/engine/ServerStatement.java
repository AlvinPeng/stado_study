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
import java.sql.Types;

import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.io.CopyResponse;
import org.postgresql.stado.engine.io.ResponseMessage;
import org.postgresql.stado.engine.io.ResultSetResponse;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.parser.SqlCopyData;
import org.postgresql.stado.parser.SqlDropTempTables;


/**
 *  Code to support server-side
 *         prepared statements Request is parsed outside and registered with
 *         session using createStatement, then cursor name amd values for
 *         parameters are assigned using bindStatement and executed using
 *         executeRequest
 */
public class ServerStatement {
    private static final XLogger logger = XLogger
            .getLogger(ServerStatement.class);

    private String statementID = null;

    private String cursorID = null;

    private ExecutableRequest request;

    private XDBSessionContext client;

    private ResponseMessage description;

    private ExecutionResult result;

    private int fetchSize = 200;
    
    private boolean bindStepComplete = false; 
    
    ServerStatement(String statementID, ExecutableRequest request,
            XDBSessionContext client) {
        this.statementID = statementID;
        this.cursorID = null;
        this.request = request;
        this.client = client;
        this.description = null;
        this.result = null;
    }

    ServerStatement(String cursorID, ExecutionResult result,
            XDBSessionContext client) {
        this.statementID = null;
        this.cursorID = cursorID;
        this.request = null;
        this.client = client;
        this.description = null;
        this.result = result;
    }

    /**
     * @param paramValues
     */
    public void setParameterTypes(int[] paramTypes) {
        if (paramTypes != null && paramTypes.length > 0) {
            ILockCost sqlObject = request.getCost().getSqlObject();
            if (sqlObject instanceof IParametrizedSql) {
                ((IParametrizedSql) sqlObject).setParamDataTypes(paramTypes);
            } else {
                throw new XDBServerException(
                        "SQL object does not support parameters");
            }
        }
    }

    public int[] getParameterTypes() {
        ILockCost sqlObject = request.getCost().getSqlObject();
        if (sqlObject instanceof IParametrizedSql) {
            return ((IParametrizedSql) sqlObject).getParamDataTypes();
        } else {
            return null;
        }
    }

    /**
     * @param paramValues
     */
    public void bind(String cursorID, String[] paramValues) {
        this.cursorID = cursorID;
        if (paramValues != null && paramValues.length > 0) {
            ILockCost sqlObject = request.getCost().getSqlObject();
            if (sqlObject instanceof IParametrizedSql) {
                ((IParametrizedSql) sqlObject).setParamValues(paramValues);
            } else {
                throw new XDBServerException(
                        "SQL object does not support parameters");
            }
        }
    }

    /**
     * Returns description of current statement wrapped by a ResponseMessage
     * @return
     */
    public ResponseMessage describe() {
        if (description == null) {
            if (request.getCost().getSqlObject() instanceof SqlCopyData) {
                SqlCopyData copyData = (SqlCopyData) request.getCost().getSqlObject();
                if (copyData.isConsole()) {
                    description = new CopyResponse(copyData);
                }
            } else {
                if (result == null) {
                    // TODO RSR should be able to take column metadata from
                    // statement that has not been executed
                    
                    // Execute only if the bind step is complete
                    if (bindStepComplete)
                        execute(true);
                    else
                        return null;
                }
                if (result.getContentType() == ExecutionResult.CONTENT_TYPE_RESULTSET) {
                    description = new ResultSetResponse(0, cursorID, result
                            .getResultSet());
                } else if (result.getContentType() == ExecutionResult.CONTENT_TYPE_SUBRESULTS) {
                    ResultSet rs = result.getSubResults().values().iterator().next().getResultSet();

                    if (rs == null) {
                        ColumnMetaData[] meta = new ColumnMetaData[2];
                        meta[0] = new ColumnMetaData("", "Result", 10,
                                Types.INTEGER, 0, 0, "", (short) 0, false);
                        meta[1] = new ColumnMetaData("", "Node", 0,
                                Types.INTEGER, 0, 0, "", (short) 0, false);
                        ResultSetResponse rsr = new ResultSetResponse(0, cursorID, null);
                        rsr.setColumnMetaData(meta);
                        description = rsr;

                    } else {
                        ResultSetResponse rsr = new ResultSetResponse(0, cursorID, rs);
                        ColumnMetaData[] meta = rsr.getColumnMetaData();
                        ColumnMetaData[] newMeta = new ColumnMetaData[meta.length + 1];
                        System.arraycopy(meta, 0, newMeta, 0, meta.length);
                        newMeta[meta.length] = new ColumnMetaData("", "Node", 0,
                                Types.INTEGER, 0, 0, "", (short) 0, false);
                        rsr.setColumnMetaData(newMeta);
                        description = rsr;
                    }

                }
            }
        }
        return description;
    }

    public ExecutionResult execute() {
        return execute(false);
    }

    private boolean wasInternallyExecuted = false;

    private ExecutionResult execute(boolean internal) {
        if (internal) {
            if (result == null) {
                wasInternallyExecuted = true;
            } else {
                return result;
            }
        } else {
            if (result != null && wasInternallyExecuted) {
                wasInternallyExecuted = false;
                return result;
            }
        }
        result = client.executeRequest(request);
        if (description instanceof ResultSetResponse) {
            ((ResultSetResponse) description).setResultSet(result
                    .getResultSet());
        }
        return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.ICursor#getFetchSize()
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.ICursor#getResultSet()
     */
    public ResultSet getResultSet() {
        if (result == null) {
            execute();
        }
        return result.getResultSet();
    }

    public boolean hasResultSet() {
        return result != null && result.hasResultSet();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.engine.ICursor#setFetchSize(int)
     */
    public void setFetchSize(int fsize) {
        this.fetchSize = fsize;
    }

    public void close() {
        if (result != null) {
            if (result.getContentType() == ExecutionResult.CONTENT_TYPE_RESULTSET
                    && result.getResultSet() != null) {
                close(result.getResultSet());
            } else if (result.getContentType() == ExecutionResult.CONTENT_TYPE_SUBRESULTS) {
                for (ExecutionResult subresult : result.getSubResults().values()) {
                    if (subresult.getContentType() == ExecutionResult.CONTENT_TYPE_RESULTSET
                            && subresult.getResultSet() != null) {
                        close(subresult.getResultSet());
                    }
                }

            }
        }
    }

    private void close(ResultSet rs) {
        try {
            new SqlDropTempTables(rs, client).execute(Engine.getInstance());
        } catch (Exception se) {
            logger.catching(se);
        }
    }
    
    String getStatementID() {
        return statementID;
    }

    String getCursorID() {
        return cursorID;
    }
    
    public void setBindStepComplete(boolean state) {
        bindStepComplete = state;
    }
}
