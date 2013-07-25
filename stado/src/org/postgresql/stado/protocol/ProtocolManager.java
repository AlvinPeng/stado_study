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
package org.postgresql.stado.protocol;

import java.nio.channels.SocketChannel;

/**
 * The <code>ProtocolManager</code> provides an interface for different
 * protocols to be handled by the <code>XDBServer</code>
 *  
 * @see org.postgresql.stado.server.Server
 */
public interface ProtocolManager {
    /**
     * adds a client connection to be managed by the protocol manager
     * @param channel 
     * @param clientContext 
     */
    void addClient(SocketChannel channel);

    /**
     * remove a client connection from the protocol manager
     * @param channel 
     */
    void removeClient(SocketChannel channel);

    /**
     * Write error message to the channel   
     * @param channel
     * @param e
     */
    void rejectClient(SocketChannel channel, Exception e);
}
