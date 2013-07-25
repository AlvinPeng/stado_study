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

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.exception.XDBBaseException;
import org.postgresql.stado.exception.XDBWrappedException;


/**
 * AbstractConnector common ancestor for all the Connectors. It is work like
 * pipe conducting messages between participants. Contains queues for incoming
 * and outgoing messages, message listeners and processes to serve actual
 * connections. Changes in 2.0: Use more effective queues from
 * java.util.concurrent package.
 * 
 *  
 * @version 2.0
 */
public abstract class AbstractConnector implements Runnable {

    private static final XLogger logger = XLogger
            .getLogger(AbstractConnector.class);

    /**
     * Here are processes, which are interested in messages from the nodes.
     * 
     * @see IMessageListener
     */
    private Collection<IMessageListener> messageListeners;

    /**
     * Queue for incoming messages
     */
    protected BlockingQueue<NodeMessage> inQueue;

    /**
     * Queue for outgoing messages
     */
    protected BlockingQueue<NodeMessage> outQueue;

    /**
     * Threads to interrupt on destroy
     */
    private Collection<Thread> threads = new LinkedList<Thread>();

    private boolean destroyed = false;

    /**
     * Initializes the connector
     */
    public AbstractConnector() {
        final String method = "AbstractConnector";
        logger.entering(method);
        try {

            // Use HashSet because one instance of the connector may be shared
            // to access different nodes, end for every node owner will register
            messageListeners = new HashSet<IMessageListener>();
            inQueue = new LinkedBlockingQueue<NodeMessage>();
            outQueue = new LinkedBlockingQueue<NodeMessage>();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Registers process interested in messages from the nodes.
     * @param listener 
     */
    public void addMessageListener(IMessageListener listener) {
        final String method = "addMessageListener";
        logger.entering(method, new Object[] { listener });
        try {

            synchronized (messageListeners) {
                messageListeners.add(listener);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Unregisters process, not interested in messages from the nodes any more.
     * @param listener 
     */
    public void removeMessageListener(IMessageListener listener) {
        final String method = "removeMessageListener";
        logger.entering(method, new Object[] { listener });
        try {

            synchronized (messageListeners) {
                messageListeners.remove(listener);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Forwards the NodeMessage to registered listener.
     * @param message 
     */
    protected void processMessage(NodeMessage message) {
        final String method = "processMessage";
        logger.entering(method, new Object[] { message });
        try {

            IMessageListener[] copy = null;
            synchronized (messageListeners) {
                copy = messageListeners
                        .toArray(new IMessageListener[messageListeners.size()]);
            }

            boolean consumed = false;

            for (int i = 0; i < copy.length; i++) {
                consumed |= copy[i].processMessage(message);
            }

            if (!consumed) {
                XLogger.getLogger("Server").warn(
                        "Message was not consumed: " + message);
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Put message to the end of the queue for outgoing messages.
     * @param message 
     */
    public void enqueueMessage(NodeMessage message) {
        final String method = "enqueueMessage";
        logger.entering(method, new Object[] { message });
        try {

            NodeMessage.CATEGORY_MESSAGE.debug("Message is enqueued, queue: "
                    + outQueue);
            outQueue.offer(message);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @param r 
     */
    protected synchronized void addWorkerThread(Runnable r) {
        if (!destroyed) {
            Thread theThread = new Thread(r);
            theThread.start();
            threads.add(theThread);
        }
    }

    /**
     * Sets flag signalling to internal threads to stop and clean up themselves.
     */
    public synchronized void destroy() {
        final String method = "destroy";
        logger.entering(method);
        try {

            destroyed = true;
            for (Thread theThread : threads) {
                theThread.interrupt();
            }
            threads.clear();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Removes one message from incoming queue and directs it to process
     * interested in it.
     * @throws java.lang.InterruptedException 
     */
    public void processIncomingMessage() throws InterruptedException {
        NodeMessage message = inQueue.poll(5,
                java.util.concurrent.TimeUnit.SECONDS);
        if (message != null) {
            processMessage(message);
        }
    }

    /**
     * When connector is running in separate thread processes messages from
     * incoming queue until destroyed.
     */
    public void run() {
        while (true) {
            try {
                processIncomingMessage();
            } catch (InterruptedException ie) {
                // Normal shutdown
                break;
            } catch (Throwable t) {
                // Try and catch everything that may happen
                logger.catching(t);
            }
        }
    }

    /**
     * Subclass does here all required initialization and starts internal
     * processes.
     */
    public abstract void start();

    /**
     * Common ancestor for process sending messages from outgoing queue to other
     * participants of the connection.
     */
    protected abstract class AbstractSendingThread implements Runnable {

        /**
         * Reads messages one by one from outgoing queue and send them to other
         * participants of the connection using send(NodeMessage) method.
         */
        public void run() {
            final String method = "run";
            logger.entering(method);
            try {

                while (true) {
                    try {
                        NodeMessage message = outQueue.poll(5,
                                java.util.concurrent.TimeUnit.SECONDS);
                        try {
                            if (message != null) {
                                NodeMessage.CATEGORY_MESSAGE
                                        .info("Sending message: " + message);
                                send(message);
                            }
                        } catch (Exception ex) {
                            logger.catching(ex);
                            sendFailed(message, ex);
                        }
                    } catch (InterruptedException ie) {
                        // Normal shutdown
                        break;
                    }
                }

            } finally {
                logger.exiting(method);
            }
        }

        /**
         * Called when sending error occurs. Re-enqueue message by default
         * @param message 
         * @param ex 
         */
        protected void sendFailed(NodeMessage message, Exception ex) {
            final String method = "sendFailed";
            logger.entering(method, new Object[] { message, ex });
            try {

                while (message.sendFailed() < 3) {
                    try {
                        send(message);
                        return;
                    } catch (Exception e) {
                        logger.catching(e);
                    }
                }
                XLogger
                        .getLogger("Server")
                        .error(
                                "Can not deliver message, sending MSG_ABORT back to Agent");
                NodeMessage abort = NodeMessage
                        .getNodeMessage(NodeMessage.MSG_ABORT);
                Integer target = message.getTargetNodeID();
                abort.setSourceNodeID(target == null ? -1 : target.intValue());
                abort.setTargetNodeID(new Integer(message.getSourceNodeID()));
                abort.setSessionID(message.getSessionID());
                abort.setRequestId(message.getRequestId());
                abort
                        .setCause(ex instanceof XDBBaseException ? (XDBBaseException) ex
                                : new XDBWrappedException(message
                                        .getSourceNodeID(), ex));
                processMessage(abort);

            } finally {
                logger.exiting(method);
            }
        }

        /**
         * Abstract method performing actual send.
         * @param message 
         * @throws java.lang.Exception 
         */
        protected abstract void send(NodeMessage message) throws Exception;
    }

    /**
     * Common ancestor for process receiving messages from other participants of
     * the connection to incoming queue.
     */
    protected abstract class AbstractReceivingThread implements Runnable {
        /**
         * Retrieve messages from other participants of the connection by
         * calling receive() method and put it to incoming queue.
         */
        public void run() {
            final String method = "run";
            logger.entering(method);
            try {

                while (true) {
                    NodeMessage[] messages = null;

                    try {
                        messages = receive();
                    } catch (InterruptedException ex) {
                        break;
                    } catch (Exception ex) {
                        if (Thread.currentThread().isInterrupted()) {
                            break;
                        }
                        receiveFailed(ex);
                    }

                    if (messages != null && messages.length > 0) {
                        for (int i = 0; i < messages.length; i++) {
                            NodeMessage.CATEGORY_MESSAGE.info(getClass()
                                    .getName()
                                    + ": Received message: " + messages[i]);
                            inQueue.offer(messages[i]);
                        }
                    }
                }

            } finally {
                logger.exiting(method);
            }
        }

        /**
         * Called when sending error occurs. Ignore exception by default
         * @param ex 
         */
        protected void receiveFailed(Exception ex) {
        }

        /**
         * Abstract method performing actual receive. Can be both blocking and
         * non-blocking.
         * @throws java.lang.Exception 
         * @return 
         */
        protected abstract NodeMessage[] receive() throws Exception;
    }
}