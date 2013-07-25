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
package org.postgresql.driver.core;

import org.postgresql.driver.PGNotification;

import java.sql.*;

/**
 * Provides access to protocol-level connection operations.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public interface ProtocolConnection {
    /**
     * Constant returned by {@link #getTransactionState} indicating that no
     * transaction is currently open.
     */
    static final int TRANSACTION_IDLE = 0;

    /**
     * Constant returned by {@link #getTransactionState} indicating that a
     * transaction is currently open.
     */
    static final int TRANSACTION_OPEN = 1;

    /**
     * Constant returned by {@link #getTransactionState} indicating that a
     * transaction is currently open, but it has seen errors and will
     * refuse subsequent queries until a ROLLBACK.
     */
    static final int TRANSACTION_FAILED = 2;

    /**
     * @return the hostname this connection is connected to.
     */
    String getHost();

    /**
     * @return the port number this connection is connected to.
     */
    int getPort();

    /**
     * @return the user this connection authenticated as.
     */
    String getUser();

    /**
     * @return the database this connection is connected to.
     */
    String getDatabase();

    /**
     * @return the server version of the connected server, formatted as X.Y.Z.
     */
    String getServerVersion();

    /**
     * @return the current encoding in use by this connection
     */
    Encoding getEncoding();
    
    /**
     * Returns whether the server treats string-literals according to the SQL
     * standard or if it uses traditional PostgreSQL escaping rules. Versions
     * up to 8.1 always treated backslashes as escape characters in
     * string-literals. Since 8.2, this depends on the value of the
     * <tt>standard_conforming_strings<tt> server variable.
     * 
     * @return true if the server treats string literals according to the SQL
     *   standard
     */
    boolean getStandardConformingStrings();

    /**
     * Get the current transaction state of this connection.
     * 
     * @return a ProtocolConnection.TRANSACTION_* constant.
     */
    int getTransactionState();

    /**
     * Retrieve and clear the set of asynchronous notifications pending on this
     * connection.
     *
     * @return an array of notifications; if there are no notifications, an empty
     *   array is returned.
     */
    PGNotification[] getNotifications() throws SQLException;

    /**
     * Retrieve and clear the chain of warnings accumulated on this connection.
     *
     * @return the first SQLWarning in the chain; subsequent warnings can be
     *   found via SQLWarning.getNextWarning().
     */
    SQLWarning getWarnings();

    /**
     * @return the QueryExecutor instance for this connection.
     */
    QueryExecutor getQueryExecutor();

    /**
     * Sends a query cancellation for this connection.
     * @throws SQLException if something goes wrong.
     */
    void sendQueryCancel() throws SQLException;

    /**
     * Close this connection cleanly.
     */
    void close();

    /**
     * Check if this connection is closed.
     *
     * @return true iff the connection is closed.
     */
    boolean isClosed();
    
    /**
     * 
     * @return the version of the implementation
     */
    public int getProtocolVersion();
}
