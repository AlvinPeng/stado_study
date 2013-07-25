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
package org.postgresql.driver.core.v3;

import org.postgresql.driver.PGNotification;
import org.postgresql.driver.core.*;

import java.sql.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * ProtocolConnection implementation for the V3 protocol.
 *
 * @author Oliver Jowett (oliver@opencloud.com)
 */
class ProtocolConnectionImpl implements ProtocolConnection {
    ProtocolConnectionImpl(PGStream pgStream, String user, String database, Properties info, Logger logger) {
        this.pgStream = pgStream;
        this.user = user;
        this.database = database;
        this.logger = logger;
        this.executor = new QueryExecutorImpl(this, pgStream, info, logger);
        // default value for server versions that don't report standard_conforming_strings
        this.standardConformingStrings = false;
    }

    public String getHost() {
        return pgStream.getHost();
    }

    public int getPort() {
        return pgStream.getPort();
    }

    public String getUser() {
        return user;
    }

    public String getDatabase() {
        return database;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public synchronized boolean getStandardConformingStrings()
    {
        return standardConformingStrings;
    }

    public synchronized int getTransactionState()
    {
        return transactionState;
    }

    public synchronized PGNotification[] getNotifications() throws SQLException {
        PGNotification[] array = (PGNotification[])notifications.toArray(new PGNotification[notifications.size()]);
        notifications.clear();
        return array;
    }

    public synchronized SQLWarning getWarnings()
    {
        SQLWarning chain = warnings;
        warnings = null;
        return chain;
    }

    public QueryExecutor getQueryExecutor() {
        return executor;
    }

    public void sendQueryCancel() throws SQLException {
        PGStream cancelStream = null;

        // Now we need to construct and send a cancel packet
        try
        {
            if (logger.logDebug())
                logger.debug(" FE=> CancelRequest(pid=" + cancelPid + ",ckey=" + cancelKey + ")");

            cancelStream = new PGStream(pgStream.getHost(), pgStream.getPort());
            cancelStream.SendInteger4(16);
            cancelStream.SendInteger2(1234);
            cancelStream.SendInteger2(5678);
            cancelStream.SendInteger4(cancelPid);
            cancelStream.SendInteger4(cancelKey);
            cancelStream.flush();
            cancelStream.ReceiveEOF();
            cancelStream.close();
            cancelStream = null;
        }
        catch (IOException e)
        {
            // Safe to ignore.
            if (logger.logDebug())
                logger.debug("Ignoring exception on cancel request:", e);
        }
        finally
        {
            if (cancelStream != null)
            {
                try
                {
                    cancelStream.close();
                }
                catch (IOException e)
                {
                    // Ignored.
                }
            }
        }
    }

    public void close() {
        if (closed)
            return ;

        try
        {
            if (logger.logDebug())
                logger.debug(" FE=> Terminate");

            pgStream.SendChar('X');
            pgStream.SendInteger4(4);
            pgStream.flush();
            pgStream.close();
        }
        catch (IOException ioe)
        {
            // Forget it.
            if (logger.logDebug())
                logger.debug("Discarding IOException on close:", ioe);
        }

        closed = true;
    }

    public Encoding getEncoding() {
        return pgStream.getEncoding();
    }

    public boolean isClosed() {
        return closed;
    }

    //
    // Package-private accessors called during connection setup
    //

    void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    void setBackendKeyData(int cancelPid, int cancelKey) {
        this.cancelPid = cancelPid;
        this.cancelKey = cancelKey;
    }

    //
    // Package-private accessors called by the query executor
    //

    synchronized void addWarning(SQLWarning newWarning)
    {
        if (warnings == null)
            warnings = newWarning;
        else
            warnings.setNextWarning(newWarning);
    }

    synchronized void addNotification(PGNotification notification)
    {
        notifications.add(notification);
    }

    synchronized void setTransactionState(int state)
    {
        transactionState = state;
    }

    synchronized void setStandardConformingStrings(boolean value)
    {
        standardConformingStrings = value;
    }

    public int getProtocolVersion()
    {
        return 3;
    }

    private String serverVersion;
    private int cancelPid;
    private int cancelKey;

    private boolean standardConformingStrings;
    private int transactionState;
    private SQLWarning warnings;

    private boolean closed = false;

    private final ArrayList notifications = new ArrayList();

    private final PGStream pgStream;
    private final String user;
    private final String database;
    private final QueryExecutorImpl executor;
    private final Logger logger;
}
