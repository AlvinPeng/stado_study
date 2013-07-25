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
package org.postgresql.driver.ds;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import java.sql.SQLException;
import java.io.Serializable;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

import org.postgresql.driver.ds.common.*;

/**
 * PostgreSQL implementation of ConnectionPoolDataSource.  The app server or
 * middleware vendor should provide a DataSource implementation that takes advantage
 * of this ConnectionPoolDataSource.  If not, you can use the PostgreSQL implementation
 * known as PoolingDataSource, but that should only be used if your server or middleware
 * vendor does not provide their own.  Why? The server may want to reuse the same
 * Connection across all EJBs requesting a Connection within the same Transaction, or
 * provide other similar advanced features.
 *
 * <p>In any case, in order to use this ConnectionPoolDataSource, you must set the property
 * databaseName.  The settings for serverName, portNumber, user, and password are
 * optional.  Note: these properties are declared in the superclass.</p>
 *
 * <p>This implementation supports JDK 1.3 and higher.</p>
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class PGConnectionPoolDataSource extends BaseDataSource implements Serializable, ConnectionPoolDataSource
{
    private boolean defaultAutoCommit = true;

    /**
     * Gets a description of this DataSource.
     */
    public String getDescription()
    {
        return "ConnectionPoolDataSource from " + org.postgresql.driver.Driver.getVersion();
    }

    /**
     * Gets a connection which may be pooled by the app server or middleware
     * implementation of DataSource.
     *
     * @throws java.sql.SQLException
     *     Occurs when the physical database connection cannot be established.
     */
    public PooledConnection getPooledConnection() throws SQLException
    {
        return new PGPooledConnection(getConnection(), defaultAutoCommit);
    }

    /**
     * Gets a connection which may be pooled by the app server or middleware
     * implementation of DataSource.
     *
     * @throws java.sql.SQLException
     *     Occurs when the physical database connection cannot be established.
     */
    public PooledConnection getPooledConnection(String user, String password) throws SQLException
    {
        return new PGPooledConnection(getConnection(user, password), defaultAutoCommit);
    }

    /**
     * Gets whether connections supplied by this pool will have autoCommit
     * turned on by default.  The default value is <tt>false</tt>, so that
     * autoCommit will be turned off by default.
     */
    public boolean isDefaultAutoCommit()
    {
        return defaultAutoCommit;
    }

    /**
     * Sets whether connections supplied by this pool will have autoCommit
     * turned on by default.  The default value is <tt>false</tt>, so that
     * autoCommit will be turned off by default.
     */
    public void setDefaultAutoCommit(boolean defaultAutoCommit)
    {
        this.defaultAutoCommit = defaultAutoCommit;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        writeBaseObject(out);
        out.writeBoolean(defaultAutoCommit);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        readBaseObject(in);
        defaultAutoCommit = in.readBoolean();
    }
}
