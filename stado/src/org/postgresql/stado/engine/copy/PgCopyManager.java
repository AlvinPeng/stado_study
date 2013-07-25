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
package org.postgresql.stado.engine.copy;

import org.postgresql.driver.PGConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

/**
 * Wrapper for Postgres connection
 * Delegate method calls to Edb CopyManager's functions
 */
public class PgCopyManager extends CopyManager {
    private PGConnection connection;

    /**
     * Create new instance of CopyManager
     * @param connection
     *          the connection
     */
    PgCopyManager(PGConnection connection) {
        this.connection = connection;
        // TODO check driver version to verify if copy is supported
    }

    /**
     * Delegate the call to Postgres CopyManager
     * @param command
     *          must be a COPY FROM STDIN
     * @param in
     *          the data source
     * @return number of rows affected
     * @throws SQLException
     *          if SQL error occurs
     * @throws IOException
     *          problem of reading from the stream
     */
    public long copyIn(String command, InputStream in) throws SQLException, IOException {
        return connection.getCopyAPI().copyIn(command, in);
    }

    /**
     * Delegate the call to Postgres CopyManager
     * @param command
     *          must be a COPY FROM STDIN
     * @return an instance of PgCopyIn
     * @throws SQLException
     *          if SQL error occurs
     */
    public CopyIn copyIn(String command) throws SQLException {
        return new PgCopyIn(connection.getCopyAPI().copyIn(command));
    }

    /**
     * Delegate the call to Postgres CopyManager
     * @param command
     *          must be a COPY TO STDOUT
     * @param out
     *          the destination
     * @return number of rows returned
     * @throws SQLException
     *          if SQL error occurs
     * @throws IOException
     *          problem of writing to the stream
     */
    public long copyOut(String command, OutputStream out) throws SQLException, IOException {
        return connection.getCopyAPI().copyOut(command, out);
    }

    /**
     * Delegate the call to Postgres CopyManager
     * @param command
     *          must be a COPY TO STDOUT
     * @return an instance of PgCopyOut
     * @throws SQLException
     *          if SQL error occurs
     */
    public CopyOut copyOut(String command) throws SQLException {
        return new PgCopyOut(connection.getCopyAPI().copyOut(command));
    }
}
