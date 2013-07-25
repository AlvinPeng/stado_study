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
package org.postgresql.driver;

import java.sql.*;

/**
 *  This interface defines the public PostgreSQL extensions to
 *  java.sql.Statement. All Statements constructed by the PostgreSQL
 *  driver implement PGStatement.
 */
public interface PGStatement
{
    // We can't use Long.MAX_VALUE or Long.MIN_VALUE for java.sql.date
    // because this would break the 'normalization contract' of the
    // java.sql.Date API.
    // The follow values are the nearest MAX/MIN values with hour,
    // minute, second, millisecond set to 0 - this is used for
    // -infinity / infinity representation in Java
    public static final long DATE_POSITIVE_INFINITY = 9223372036825200000l;
    public static final long DATE_NEGATIVE_INFINITY = -9223372036832400000l;


    /**
     * Returns the Last inserted/updated oid.
     * @return OID of last insert
        * @since 7.3
     */
    public long getLastOID() throws SQLException;

    /**
     * Turn on the use of prepared statements in the server (server side
     * prepared statements are unrelated to jdbc PreparedStatements)
     * As of build 302, this method is equivalent to
     *  <code>setPrepareThreshold(1)</code>.
     *
     * @deprecated As of build 302, replaced by {@link #setPrepareThreshold(int)}
        * @since 7.3
     */
    public void setUseServerPrepare(boolean flag) throws SQLException;

    /**
     * Checks if this statement will be executed as a server-prepared
     * statement. A return value of <code>true</code> indicates that the next
     * execution of the statement will be done as a server-prepared statement,
     * assuming the underlying protocol supports it.
     *
     * @return true if the next reuse of this statement will use a
     *  server-prepared statement
     */
    public boolean isUseServerPrepare();

    /**
     * Sets the reuse threshold for using server-prepared statements.
     *<p>
     * If <code>threshold</code> is a non-zero value N, the Nth and subsequent
     * reuses of a PreparedStatement will use server-side prepare.
     *<p>
     * If <code>threshold</code> is zero, server-side prepare will not be used.
     *<p>
     * The reuse threshold is only used by PreparedStatement and
     * CallableStatement objects; it is ignored for plain Statements.
     *
     * @since build 302
     * @param threshold the new threshold for this statement
     * @throws SQLException if an exception occurs while changing the threshold
     */
    public void setPrepareThreshold(int threshold) throws SQLException;

    /**
     * Gets the server-side prepare reuse threshold in use for this statement.
     *
     * @since build 302
     * @return the current threshold
     * @see #setPrepareThreshold(int)
     */
    public int getPrepareThreshold();
}
