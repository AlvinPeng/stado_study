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

import java.util.Properties;
import java.sql.SQLException;

import org.postgresql.driver.util.GT;
import org.postgresql.driver.util.PSQLException;
import org.postgresql.driver.util.PSQLState;

/**
 * Handles protocol-specific connection setup.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
public abstract class ConnectionFactory {
    /**
     * Protocol version to implementation instance map.
     * If no protocol version is specified, instances are
     * tried in order until an exception is thrown or a non-null
     * connection is returned.
     */
    private static final Object[][] versions = {
                { "3", new org.postgresql.driver.core.v3.ConnectionFactoryImpl() },
                { "2", new org.postgresql.driver.core.v2.ConnectionFactoryImpl() },
            };

    /**
     * Establishes and initializes a new connection.
     *<p>
     * If the "protocolVersion" property is specified, only that protocol
     * version is tried. Otherwise, all protocols are tried in order, falling
     * back to older protocols as necessary.
     *<p>
     * Currently, protocol versions 3 (7.4+) and 2 (pre-7.4) are supported.
     *
     * @param host the host to connect to
     * @param port the port to connect to
     * @param user the username to authenticate with; may not be null.
     * @param database the database on the server to connect to; may not be null.
     * @param info extra properties controlling the connection;
     *    notably, "password" if present supplies the password to authenticate with.
     * @param logger the logger to use for this connection
     * @return the new, initialized, connection
     * @throws SQLException if the connection could not be established.
     */
    public static ProtocolConnection openConnection(String host, int port, String user, String database, Properties info, Logger logger) throws SQLException {
        String protoName = info.getProperty("protocolVersion");

        for (int i = 0; i < versions.length; ++i)
        {
            String versionProtoName = (String) versions[i][0];
            if (protoName != null && !protoName.equals(versionProtoName))
                continue;

            ConnectionFactory factory = (ConnectionFactory) versions[i][1];
            ProtocolConnection connection = factory.openConnectionImpl(host, port, user, database, info, logger);
            if (connection != null)
                return connection;
        }

        throw new PSQLException(GT.tr("A connection could not be made using the requested protocol {0}.", protoName),
                                PSQLState.CONNECTION_UNABLE_TO_CONNECT);
    }

    /**
     * Implementation of {@link #openConnection} for a particular protocol version.
     * Implemented by subclasses of {@link ConnectionFactory}.
     *
     * @param host the host to connect to
     * @param port the port to connect to
     * @param user the username to authenticate with; may not be null.
     * @param database the database on the server to connect to; may not be null.
     * @param info extra properties controlling the connection;
     *    notably, "password" if present supplies the password to authenticate with.
     * @param logger the logger to use for this connection
     * @return the new, initialized, connection, or <code>null</code> if this protocol
     *    version is not supported by the server.
     * @throws SQLException if the connection could not be established for a reason other
     *    than protocol version incompatibility.
     */
    public abstract ProtocolConnection openConnectionImpl(String host, int port, String user, String database, Properties info, Logger logger) throws SQLException;
}
