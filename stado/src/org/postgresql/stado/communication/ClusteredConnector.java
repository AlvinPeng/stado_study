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
/*
 * ClusteredConnector.java
 * 
 *  
 */
package org.postgresql.stado.communication;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;

/**
 *  
 */
public abstract class ClusteredConnector extends AbstractConnector {
    private static final XLogger logger = XLogger
            .getLogger(ClusteredConnector.class);

    private final AbstractConnector[] connectors;

    private int lastIndex = 0;

    /**
     * 
     * @param nodeID 
     * @param domainCount 
     */
    public ClusteredConnector(int nodeID, int domainCount) {
        connectors = new AbstractConnector[domainCount];
        for (int i = 0; i < domainCount; i++) {
            connectors[i] = createConnector(nodeID, i);
        }
    }

    /**
     * 
     * @param nodeID 
     * @param domain 
     * @return 
     */

    protected abstract AbstractConnector createConnector(int nodeID, int domain);

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.communication.AbstractConnector#start()
     */
    @Override
    public void start() {
        for (int i = 0; i < connectors.length; i++) {
            connectors[i].start();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.communication.AbstractConnector#destroy()
     */
    @Override
    public void destroy() {
        for (int i = 0; i < connectors.length; i++) {
            connectors[i].destroy();
        }
        super.destroy();
    }

    /**
     * 
     * @return 
     */

    protected AbstractConnector pickConnector() {
        if (connectors.length == 1) {
            return connectors[0];
        }
        int bestLoad = Integer.MAX_VALUE;
        int bestIndex = lastIndex + 1;
        boolean loop;
        if (loop = bestIndex >= connectors.length) {
            bestIndex = 0;
        }
        // Cycle through connectors
        for (int i = bestIndex; loop == (i <= lastIndex); i = (loop = ++i >= connectors.length) ? 0
                : i) {
            int load;
            synchronized (connectors[i].outQueue) {
                load = connectors[i].outQueue.size();
            }
            if (load < bestLoad) {
                bestLoad = load;
                bestIndex = i;
            }
            if (load == 0) {
                // Got idle connector, no need to go further
                break;
            }
        }
        lastIndex = bestIndex;
        logger.debug("Use connector #" + lastIndex);
        return connectors[bestIndex];
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.communication.AbstractConnector#enqueueMessage(org.postgresql.stado.Engine.NodeMessage)
     */
    /**
     * 
     * @param message 
     */

    @Override
    public void enqueueMessage(NodeMessage message) {
        pickConnector().enqueueMessage(message);
    }

    /*
     * @param listener 
     * 
     * @see org.postgresql.stado.communication.AbstractConnector#addMessageListener(org.postgresql.stado..communication.IMessageListener)
     */
    
    @Override
    public void addMessageListener(IMessageListener listener) {
        for (int i = 0; i < connectors.length; i++) {
            connectors[i].addMessageListener(listener);
        }
    }

    /*
     * 
     * @param listener 
     * @see org.postgresql.stado.communication.AbstractConnector#removeMessageListener(org.postgresql.stado..communication.IMessageListener)
     */
    @Override
    public void removeMessageListener(IMessageListener listener) {
        for (int i = 0; i < connectors.length; i++) {
            connectors[i].removeMessageListener(listener);
        }
    }
}
