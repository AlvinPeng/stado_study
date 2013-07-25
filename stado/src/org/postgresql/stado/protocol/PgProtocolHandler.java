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
/**
 *
 */
package org.postgresql.stado.protocol;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;


/**
 *
 *
 */
public class PgProtocolHandler extends
        AbstractProtocolHandler<PgProtocolSession> {
    /** the logger for ProtocolHandler */
    private static XLogger logger = XLogger.getLogger(PgProtocolHandler.class);

    private static PgProtocolHandler theProtocolHandler = null;

    /**
     *
     * @return
     */
    public static PgProtocolHandler getProtocolManager() {
        if (theProtocolHandler == null) {
            try {
                theProtocolHandler = new PgProtocolHandler();
            } catch (IOException ioe) {
                logger.catching(ioe);
            }
        }
        return theProtocolHandler;
    }

    protected PgProtocolHandler() throws IOException {
        super();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.protocol.AbstractProtocolHandler#closeClient(java.lang.Object)
     */
    @Override
    protected void closeClient(PgProtocolSession clientContext) {
        if (clientContext != null) {
            clientContext.close();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.protocol.ProtocolManager#rejectClient(java.nio.channels.SocketChannel,
     *      java.lang.Exception)
     */
    public void rejectClient(SocketChannel channel, Exception e) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.protocol.AbstractProtocolHandler#createClient()
     */
    @Override
    protected PgProtocolSession createClient(SocketChannel channel)
            throws Exception {
        return new PgProtocolSession(this, channel,
                XDBSessionContext.createSession());
    }
}
