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

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.PooledConnection;
import javax.sql.ConnectionEvent;

/**
 * PostgreSQL implementation of the PooledConnection interface.  This shouldn't
 * be used directly, as the pooling client should just interact with the
 * ConnectionPool instead.
 * @see org.postgresql.driver.ds.PGConnectionPoolDataSource
 *
 * @author Aaron Mulder (ammulder@chariotsolutions.com)
 * @author Csaba Nagy (ncsaba@yahoo.com)
 */
public class PGPooledConnection
    extends org.postgresql.driver.ds.jdbc4.AbstractJdbc4PooledConnection
    implements PooledConnection
{

    public PGPooledConnection(Connection con, boolean autoCommit, boolean isXA)
    {
        super(con, autoCommit, isXA);
    }

    public PGPooledConnection(Connection con, boolean autoCommit)
    {
        this(con, autoCommit, false);
    }

    protected ConnectionEvent createConnectionEvent(SQLException sqle)
    {
        return new ConnectionEvent(this, sqle);
    }

}
