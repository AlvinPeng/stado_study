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
package org.postgresql.stado.communication.message;

import java.sql.SQLException;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.NodeResultSetImpl;


/**
 * 
 * 
 */
public class ResultSetMessage extends NodeMessage {
    private static final long serialVersionUID = -4543391796944504611L;

    private static final XLogger logger = XLogger
            .getLogger(ResultSetMessage.class);

    private String resultSetID = null;

    private transient NodeResultSetImpl resultSet;

    private boolean resultSetHasMoreRows;

    private byte[] resultSetData = null;

    /** Parameterless constructor required for serialization */
    public ResultSetMessage() {
    }

    /**
     * @param messageType
     */
    protected ResultSetMessage(int messageType) {
        super(messageType);
    }

    @Override
    public NodeResultSetImpl getResultSet() {
        if (resultSet == null && resultSetID != null) {
            try {
                resultSet = new NodeResultSetImpl(this);
            } catch (SQLException e) {
                logger.catching(e);
            }
        }
        // DBQuery used internally by NodeProducerThread scrolls the ResultSet
        // when created. Here is workaround
        else if (resultSet != null) {
            try {
                resultSet.beforeFirst();
            } catch (java.sql.SQLException sqle) {
            }
        }
        return resultSet;
    }

    @Override
    public String getResultSetID() {
        return resultSetID;
    }

    @Override
    public void setResultSetID(String resultSetID) {
        this.resultSetID = resultSetID;
    }

    @Override
    public boolean isResultSetHasMoreRows() {
        return resultSetHasMoreRows;
    }

    @Override
    public void setResultSetHasMoreRows(boolean value) {
        resultSetHasMoreRows = value;
    }

    @Override
    public byte[] getResultSetData() {
        return resultSetData;
    }

    @Override
    public void setResultSetData(byte[] resultSetData) {
        this.resultSetData = resultSetData;
    }
}
