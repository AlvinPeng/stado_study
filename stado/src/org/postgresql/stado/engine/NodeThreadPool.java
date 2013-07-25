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
 *  
 */
package org.postgresql.stado.engine;

import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.exception.XDBServerException;

/**
 *  
 * 
 */
public class NodeThreadPool extends ObjectPool<NodeThread> {
    private int nodeID;

    /**
     * 
     * @param nodeID 
     * @param minSize 
     * @param maxSize 
     */
    public NodeThreadPool(int nodeID, int minSize, int maxSize) {
        super(minSize, maxSize);
        this.nodeID = nodeID;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.server.ObjectPool#createEntry()
     */
    /**

     * 

     * @throws org.postgresql.stado.exception.XDBServerException 

     * @return 

     */

    @Override
    protected NodeThread createEntry() throws XDBServerException {
        NodeThread nt = new NodeThread(nodeID);
        new Thread(nt).start();
        return nt;
    }

    /**
     * 
     * @param entry 
     * @see org.postgresql.stado.server.ObjectPool#destroyEntry(java.lang.Object)
     */

    @Override
    protected void destroyEntry(NodeThread entry) {
        entry.processMessage(NodeMessage
                .getNodeMessage(NodeMessage.MSG_STOP_THREAD));
    }

    /**
     * 
     * @throws org.postgresql.stado.exception.XDBServerException 
     * @return 
     */

    public NodeThread getNodeThread() throws XDBServerException {
        return getObject();
    }

    /**
     * 
     * @param thread 
     */

    public void releaseNodeThread(NodeThread thread) {
        if (thread.isAlive()) {
            releaseObject(thread);
        } else {
            // If thread is not alive no need to stop it - it is already stopped
            destroyObject(thread, false);
        }
    }
}
