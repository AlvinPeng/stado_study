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

import org.postgresql.driver.PGStatement;

import java.sql.*;
import java.util.Vector;

/**
 * Driver-internal statement interface. Application code should not use
 * this interface.
 */
public interface BaseStatement extends PGStatement, Statement
{
    /**
     * Create a synthetic resultset from data provided by the driver.
     *
     * @param fields the column metadata for the resultset
     * @param tuples the resultset data
     * @return the new ResultSet
     * @throws SQLException if something goes wrong
     */
    public ResultSet createDriverResultSet(Field[] fields, Vector tuples) throws SQLException;

    /**
     * Create a resultset from data retrieved from the server.
     *
     * @param originalQuery the query that generated this resultset; used when dealing with updateable resultsets
     * @param fields the column metadata for the resultset
     * @param tuples the resultset data
     * @param cursor the cursor to use to retrieve more data from the server; if null, no additional data is present.
     * @return the new ResultSet
     * @throws SQLException if something goes wrong
     */
    public ResultSet createResultSet(Query originalQuery, Field[] fields, Vector tuples, ResultCursor cursor) throws SQLException;

    /**
     * Execute a query, passing additional query flags.
     *
     * @param p_sql the query to execute
     * @param flags additional {@link QueryExecutor} flags for execution; these
     *  are bitwise-ORed into the default flags.
     * @throws SQLException if something goes wrong.
     */
    public boolean executeWithFlags(String p_sql, int flags) throws SQLException;

    /**
     * Execute a prepared query, passing additional query flags.
     *
     * @param flags additional {@link QueryExecutor} flags for execution; these
     *  are bitwise-ORed into the default flags.
     * @throws SQLException if something goes wrong.
     */
    public boolean executeWithFlags(int flags) throws SQLException;
}
