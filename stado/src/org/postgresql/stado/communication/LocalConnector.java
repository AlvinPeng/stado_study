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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;


/**
 * LocalConnector is used to excange messages between participants running
 * within the same Java VM. May link more then two members.
 * 
 *  
 * @version 1.0
 */
public class LocalConnector extends AbstractConnector {

    private static final XLogger logger = XLogger
            .getLogger(LocalConnector.class);

    /**
     * Reference to the excange table.
     */
    private ConcurrentHashMap<Integer, BlockingQueue<NodeMessage>> msgQTable;

    private boolean started = false;

    /**
     * Places queue for incoming NodeMessages to excange table.
     * @param nodeNum 
     * @param msgQTable 
     */
    public LocalConnector(int nodeNum,
            ConcurrentHashMap<Integer, BlockingQueue<NodeMessage>> msgQTable) {
        final String method = "LocalConnector";
        logger.entering(method,
                new Object[] { new Integer(nodeNum), msgQTable });
        try {

            this.msgQTable = msgQTable;

            // While there is only LocalConnector for every node this code is
            // correct
            msgQTable.put(nodeNum, inQueue);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Start the connector by creating two threads for sending and prcessing. We
     * don't need thread for receiving because messages fall down directly to
     * our inQueue
     */
    @Override
    public void start() {
        if (started) {
            return;
        }
        started = true;
        addWorkerThread(new SendingThread());
        addWorkerThread(this);
    }

    /**
     * Implementation of AbstractSendingThread to send messages through excange
     * table.
     */
    protected class SendingThread extends AbstractSendingThread {
        /**
         * Puts the message to appropriate queue.
         * @param message 
         * @throws java.lang.Exception 
         */
        @Override
        protected void send(NodeMessage message) throws Exception {
            final String method = "send";
            logger.entering(method, new Object[] { message });
            try {

                BlockingQueue<NodeMessage> destination = msgQTable.get(message
                        .getTargetNodeID());
                destination.offer(message);

            } finally {
                logger.exiting(method);
            }
        }
    }
}