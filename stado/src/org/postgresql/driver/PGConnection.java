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

import org.postgresql.driver.copy.CopyManager;

/**
 *  This interface defines the public PostgreSQL extensions to
 *  java.sql.Connection. All Connections returned by the PostgreSQL driver
 *  implement PGConnection.
 */
public interface PGConnection
{
    /**
     * This method returns any notifications that have been received
     * since the last call to this method.
     * Returns null if there have been no notifications.
     * @since 7.3
     */
    public PGNotification[] getNotifications() throws SQLException;

    /**
     * This returns the COPY API for the current connection.
     * @since 8.4
     */
    public CopyManager getCopyAPI() throws SQLException;

    /**
     * This allows client code to add a handler for one of org.postgresql's
     * more unique data types. It is approximately equivalent to
     * <code>addDataType(type, Class.forName(name))</code>.
     *
     * @deprecated As of 8.0, replaced by
     *   {@link #addDataType(String,Class)}. This deprecated method does not
     *   work correctly for registering classes that cannot be directly loaded
     *   by the JDBC driver's classloader.
     * @throws RuntimeException if the type cannot be registered (class not
     *   found, etc).
     */
    public void addDataType(String type, String name);

    /**
     * This allows client code to add a handler for one of org.postgresql's
     * more unique data types. 
     *
     * <p><b>NOTE:</b> This is not part of JDBC, but an extension.
     *
     * <p>The best way to use this is as follows:
     *
     * <p><pre>
     * ...
     * ((org.postgresql.PGConnection)myconn).addDataType("mytype", my.class.name.class);
     * ...
     * </pre>
     *
     * <p>where myconn is an open Connection to org.postgresql.
     *
     * <p>The handling class must extend org.postgresql.driver.util.PGobject
     *
     * @since 8.0 
     *
     * @param type the PostgreSQL type to register
     * @param klass the class implementing the Java representation of the type;
     *    this class must implement {@link org.postgresql.driver.util.PGobject}).
     *
     * @throws SQLException if <code>klass</code> does not implement
     *    {@link org.postgresql.driver.util.PGobject}).
     *
     * @see org.postgresql.driver.util.PGobject
     */
    public void addDataType(String type, Class klass)
    throws SQLException;

    /**
     * Set the default statement reuse threshold before enabling server-side
     * prepare. See {@link org.postgresql.driver.PGStatement#setPrepareThreshold(int)} for 
     * details.
     *
     * @since build 302
     * @param threshold the new threshold
     */
    public void setPrepareThreshold(int threshold);

    /**
     * Get the default server-side prepare reuse threshold for statements created
     * from this connection.
     *
     * @since build 302
     * @return the current threshold
     */
    public int getPrepareThreshold();

}

