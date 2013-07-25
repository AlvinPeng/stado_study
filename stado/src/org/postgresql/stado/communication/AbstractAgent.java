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
package org.postgresql.stado.communication;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.exception.XDBUnexpectedMessageException;


/**
 * Common superclass for all Agents Contains basic Communication and message
 * dispatching functionality
 * 
 *  
 */
public abstract class AbstractAgent implements IMessageListener {

    private static final XLogger logger = XLogger
            .getLogger(AbstractAgent.class);

    public static final long MAX_PROCESS_IDLE = 600000L;

    /**
     * Running processes needed to communicate to processes running on other
     * nodes
     */
    private Map<Integer, ProcessContext> processes = new HashMap<Integer, ProcessContext>();

    /**
     * Initializes Connector to CoordinatorAgent. Adds itself as listener to it.
     */
    protected abstract void init();

    /**
     * Destroys Connector to CoordinatorAgent.
     */
    protected abstract void close();

    /**
     * Returns connector to communicate to specified target
     * 
     * @param target
     *            to communicate to
     * @return The Connector
     */
    public abstract AbstractConnector getConnector(Integer target);

    /**
     * Sends the message to its target using appropriate connector
     * @param message 
     */
    public void sendMessage(NodeMessage message) {
        final String method = "sendMessage";
        logger.entering(method, new Object[] { message });
        try {

            if (message == null) {
                return;
            }
            Integer target = message.getTargetNodeID();
            if (target != null && getNodeID() == target.intValue()) {
                logger.debug("Node " + getNodeID() + ": loopback message sent");
                processMessage(message);
            } else {
                AbstractConnector connector = getConnector(target);
                if (connector == null) {
                    if (target == null) {
                        // try and send copies of the message without broadcast
                        Collection nodeList = message.getNodeList();
                        for (Iterator it = nodeList.iterator(); it.hasNext();) {
                            Integer node = (Integer) it.next();
                            NodeMessage copy = (NodeMessage) message.clone();
                            copy.setTargetNodeID(node);
                            // Safe recursion, next time target won't be null
                            sendMessage(copy);
                        }
                    } else {
                        logger.log(org.apache.log4j.Level.WARN,
                                "Can not send message %0%",
                                new Object[] { message });
                    }
                } else {
                    connector.enqueueMessage(message);
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Helper metod to create message of specified type from the Agent to
     * original message's sender.
     * 
     * @param origin
     *            Original message
     * @param msgType
     *            Message type for reply
     * @return Newly created message
     */
    protected NodeMessage reply(NodeMessage origin, int msgType) {
        final String method = "reply";
        logger.entering(method, new Object[] { origin, new Integer(msgType) });
        NodeMessage out = null;
        try {

            out = NodeMessage.getNodeMessage(msgType);
            out.setSourceNodeID(getNodeID());
            out.setTargetNodeID(new Integer(origin.getSourceNodeID()));
            out.setSessionID(origin.getSessionID());
            out.setRequestId(origin.getRequestId());
            return out;

        } finally {
            logger.exiting(method, out);
        }
    }

    /**
     * Returns identifier of the Agent
     * 
     * @return ID
     */
    public abstract int getNodeID();

    /**
     * Registers new active process
     * 
     * @return process that was registered with this sessionID
     * @param listener 
     * @param sessionID identifier of the process
     */
    public IMessageListener addProcess(Integer sessionID,
            IMessageListener listener) {
        final String method = "addProcess";
        logger.entering(method, new Object[] { sessionID, listener });
        ProcessContext out = null;
        try {

            synchronized (processes) {
                out = processes.put(sessionID,
                        new ProcessContext(listener));
            }
            return (out == null ? null : out.getListener());

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Unregisters active process
     * 
     * @param sessionID
     *            identifier of the process
     * @return process that was registered with this sessionID
     */
    public IMessageListener removeProcess(Integer sessionID) {
        final String method = "removeProcess";
        logger.entering(method, new Object[] { sessionID });
        ProcessContext out = null;
        try {

            synchronized (processes) {
                out = processes.remove(sessionID);
            }
            return (out == null ? null : out.getListener());

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Process message before default processing To be overridden by descendants
     * 
     * @param message
     *            The message
     * @return true if message consumed, false otherwise
     */
    public boolean beforeProcessMessage(NodeMessage message) {
        if (message.getMessageType() == NodeMessage.MSG_PING
                && message.getSessionID() == null) {
            sendMessage(reply(message, NodeMessage.MSG_PING_ACK));
            return true;
        }
        return false;
    }

    /**
     * Process message after default processing Respond with MSG_ABORT message
     * Descendants can override this behavior
     * 
     * @param message
     *            The message
     * @return true if message consumed, false otherwise
     */
    public boolean afterProcessMessage(NodeMessage message) {
        final String method = "afterProcessMessage";
        logger.entering(method, new Object[] { message });
        try {

            if (message.getMessageType() == NodeMessage.MSG_ABORT) {
                // Ignore abort message
                return false;
            }
            if (message.getMessageType() == NodeMessage.MSG_PING) {
                // Just do not reply on ping if target it not found
                return false;
            }
            NodeMessage abort = reply(message, NodeMessage.MSG_ABORT);
            abort.setCause(new XDBUnexpectedMessageException(getNodeID(),
                    "Message is not delivered: target process not found",
                    message));
            sendMessage(abort);
            return true;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Processes message received by some agent
     * 
     * @param message
     *            The message
     * @return true if message consumed, false otherwise
     * @see org.postgresql.stado.communication.IMessageListener#processMessage(org.postgresql.stado.communication.message.NodeMessage)
     */
    public final boolean processMessage(NodeMessage message) {
        final String method = "processMessage";
        logger.entering(method, new Object[] { message });
        try {

            NodeMessage.CATEGORY_MESSAGE.info("Received message " + message);
            if (beforeProcessMessage(message)) {
                return true;
            }

            if (dispatchMessage(message)) {
                return true;
            }

            return afterProcessMessage(message);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param message 
     * @return 
     */
    protected boolean dispatchMessage(NodeMessage message) {
        ProcessContext target = null;
        synchronized (processes) {
            target = processes.get(message.getSessionID());
        }

        if (target != null) {
            return target.incomingMessage(message);
        }
        return false;
    }

    private class ProcessContext {
        private IMessageListener listener;

        private long timestamp;

        private boolean pinged = false;

        /**
         * 
         * @param listener 
         */
        public ProcessContext(IMessageListener listener) {
            this.listener = listener;
            timestamp = System.currentTimeMillis();
        }

        /**
         * 
         * @param message 
         * @return 
         */
        public synchronized boolean incomingMessage(NodeMessage message) {
            boolean consumed = listener.processMessage(message);
            if (consumed) {
                timestamp = System.currentTimeMillis();
                if (pinged && message.getSourceNodeID() == 0
                        && message.getMessageType() != NodeMessage.MSG_ABORT) {
                    // Coordinator is alive, clear the flag
                    pinged = false;
                }
            }
            return consumed;
        }

        /**
         * 
         * @return 
         */
        public synchronized long getIdleTime() {
            return System.currentTimeMillis() - timestamp;
        }

        /**
         * 
         * @return 
         */
        public IMessageListener getListener() {
            return listener;
        }
    }

    protected class ProcessAnalizer implements Runnable {
        public void run() {
            synchronized (processes) {
                for (Iterator<Map.Entry<Integer, ProcessContext>> it = processes
                        .entrySet().iterator(); it.hasNext();) {
                    Map.Entry<Integer, ProcessContext> entry = it.next();
                    ProcessContext context = entry.getValue();
                    if (context.getIdleTime() > MAX_PROCESS_IDLE) {
                        if (getNodeID() == 0 || context.pinged) {
                            NodeMessage message = NodeMessage
                                    .getNodeMessage(NodeMessage.MSG_CONNECTION_END);
                            message.setSourceNodeID(getNodeID());
                            message.setTargetNodeID(new Integer(getNodeID()));
                            message.setSessionID(entry.getKey());
                            sendMessage(message);
                        } else {
                            NodeMessage message = NodeMessage
                                    .getNodeMessage(NodeMessage.MSG_PING);
                            message.setSourceNodeID(getNodeID());
                            message.setTargetNodeID(new Integer(0));
                            message.setSessionID(entry.getKey());
                            sendMessage(message);
                        }
                    }
                }
            }
        }
    }
}