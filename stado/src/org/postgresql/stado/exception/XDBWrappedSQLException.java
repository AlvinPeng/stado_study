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
 * XDBWrappedSQLException.java
 * 
 *  
 */
package org.postgresql.stado.exception;

import java.sql.SQLException;

/**
 *  
 */
public class XDBWrappedSQLException extends XDBWrappedException {
    /**
     * 
     */
    private static final long serialVersionUID = 3547307180011580785L;

    private String sqlState;

    private int errorCode;

    private XDBWrappedSQLException nextException = null;

    /**
     * 
     */
    public XDBWrappedSQLException() {
        super();
    }

    /**
     * @param nodeID
     * @param cause
     */
    public XDBWrappedSQLException(int nodeID, SQLException cause) {
        super(nodeID, cause);
        sqlState = cause.getSQLState();
        errorCode = cause.getErrorCode();
        SQLException next = cause.getNextException();
        if (next != null) {
            nextException = new XDBWrappedSQLException(nodeID, next);
        }
    }

    /**
     * @return Returns the errorCode.
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * @return Returns the sqlState.
     */
    public String getSQLState() {
        return sqlState;
    }

    /**
     * @return Returns the nextException.
     */
    public XDBWrappedSQLException getNextException() {
        return nextException;
    }
}
