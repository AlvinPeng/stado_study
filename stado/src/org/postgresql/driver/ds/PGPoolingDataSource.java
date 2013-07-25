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

import javax.sql.DataSource;

/**
 * DataSource which uses connection pooling.  <font color="red">Don't use this if
 * your server/middleware vendor provides a connection pooling implementation
 * which interfaces with the PostgreSQL ConnectionPoolDataSource implementation!</font>
 * This class is provided as a convenience, but the JDBC Driver is really not
 * supposed to handle the connection pooling algorithm.  Instead, the server or
 * middleware product is supposed to handle the mechanics of connection pooling,
 * and use the PostgreSQL implementation of ConnectionPoolDataSource to provide
 * the connections to pool.
 *
 * <p>If you're sure you want to use this, then you must set the properties
 * dataSourceName, databaseName, user, and password (if required for the user).
 * The settings for serverName, portNumber, initialConnections, and
 * maxConnections are optional.  Note that <i>only connections
 * for the default user will be pooled!</i>  Connections for other users will
 * be normal non-pooled connections, and will not count against the maximum pool
 * size limit.</p>
 *
 * <p>If you put this DataSource in JNDI, and access it from different JVMs (or
 * otherwise load this class from different ClassLoaders), you'll end up with one
 * pool per ClassLoader or VM. This is another area where a server-specific
 * implementation may provide advanced features, such as using a single pool
 * across all VMs in a cluster.</p>
 *
 * <p>This implementation supports JDK 1.3 and higher.</p>
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 */
public class PGPoolingDataSource
    extends org.postgresql.driver.ds.jdbc4.AbstractJdbc4PoolingDataSource
    implements DataSource
{

    protected void addDataSource(String dataSourceName)
    {
        dataSources.put(dataSourceName, this);
    }

}
