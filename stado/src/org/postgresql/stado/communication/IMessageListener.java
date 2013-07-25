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
package org.postgresql.stado.communication;

/**
 * This interface have to be implemented by classes willing to receive
 * NodeMessages from a Connector
 * 
 * @see org.postgresql.stado.communication.message.NodeMessage
 * @see org.postgresql.stado.communication.AbstractConnector
 * @see org.postgresql.stado.communication.LocalConnector
 * 
 *  
 * @version 1.0
 */
public interface IMessageListener {
    /**
     * Someone calls this method when it have incoming message in queue for
     * process. Queue does not processing until method returns, but messages
     * keep arriving. Perform long tasks in another thread to avoid flooding of
     * the caller's message queue.
     * 
     * @param message
     *            The message
     * @return true if message "consumed", that is no more processing required
     */
    public boolean processMessage(
            org.postgresql.stado.communication.message.NodeMessage message);
}