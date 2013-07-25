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

import java.util.*;

import org.postgresql.stado.common.ActivityLog;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.communication.message.SendRowsMessage;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBBaseException;
import org.postgresql.stado.exception.XDBMessageMonitorException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;




/**
 *   Tracks received messages
 */
public class MessageMonitor {
    private static final XLogger logger = XLogger
            .getLogger(MessageMonitor.class);

    private static final long TIMEOUT_RATE = Property.getLong(
            "xdb.messagemonitor.timeout.rate", 4L);

    private static final long TIMEOUT_MILLIS = Property.getLong(
            "xdb.messagemonitor.timeout.millis", 3600000L);

    private long timeout;

    // Sources of messages we are expect
    // Key is NodeID, Value is ProcessID
    private List<Integer> sourceList = null;

    // Registered messages
    private ArrayList<NodeMessage> messages = null;

    // When we start waiting messages. Used as flag indicating
    // somewhat thread has been waiting for messages already
    private long startWaiting = 0;

    private NodeMessage abort = null;

    private int currentRequestId;

    private XDBSessionContext client;
    
    /** Constructor */
    public MessageMonitor(XDBSessionContext client) {
        this.client = client;
        timeout = 0;
    }

    /**
     * Set up monitor to track messages
     * 
     * @param requestId
     *            Type of messages we are interested in
     * @param sources
     *            Sources of messages we are expect. Key is NodeID, Value is
     *            ProcessID
     * @throws XDBServerException
     *             When we already monitor messages
     */
    public synchronized void setMonitor(int requestId, Collection sources)
            throws XDBServerException {
        final String method = "setMonitor";
        logger.entering(method,
                new Object[] { new Integer(requestId), sources });
        try {

            if (sources != null) {
                abort = null;
                timeout = 0;
                this.currentRequestId = requestId;
                sourceList = new ArrayList<Integer>(sources.size());

                for (Iterator it = sources.iterator(); it.hasNext();) {
                    Object next = it.next();
                    sourceList.add((next instanceof DBNode) ? ((DBNode) next)
                            .getNode().getNodeid() : (Integer) next);
                }

                messages = new ArrayList<NodeMessage>(sourceList.size());
            } else {
                XDBServerException ex = new XDBServerException(
                        "Monitor already tracks messages");
                logger.throwing(ex);
                throw ex;
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Blocks caller thread until all messages arrived or timeout expired.
     * Clears monitor before exit. NOTE: Size of resulting array may be less
     * then sourceList if monitor was aborted.
     * 
     * @return Messages that have been received. Returned only first message for
     *         every node.
     * @throws XDBServerException
     *             Someone already call waitForMessages or setMonitor() has not
     *             been called or timeout expired or there was an Abort message.
     */
    public synchronized NodeMessage[] waitForMessages()
            throws XDBServerException {

        // use default timeout
        return waitForMessages(timeout);
    }

    /**
     * Blocks caller thread until all messages arrived or waitTimeout expired.
     * Clears monitor before exit. NOTE: Size of resulting array may be less
     * then sourceList if monitor was aborted.
     * 
     * @return Messages that have been received. Returned only first message for
     *         every node.
     * @param waitTimeout 
     * @throws XDBServerException Someone already call waitForMessages or setMonitor() has not
     *             been called or timeout expired or there was an Abort message.
     */
    public synchronized NodeMessage[] waitForMessages(long waitTimeout)
            throws XDBServerException {
        final String method = "waitForMessages";
        logger.entering(method);
        try {

            if (startWaiting > 0) {
                XDBServerException ex = new XDBServerException(
                        "Someone already waits for messages");
                logger.throwing(ex);
                throw ex;
            }

            if (sourceList == null) {
                XDBServerException ex = new XDBServerException(
                        "Monitor was not set up to track messages");
                logger.throwing(ex);
                throw ex;
            }

            startWaiting = System.currentTimeMillis();

            try {
                while (!sourceList.isEmpty()) {
                    long rest = waitTimeout + startWaiting
                            - System.currentTimeMillis();
                    if (waitTimeout > 0L && rest < 0L) {
                        XDBServerException ex = new XDBMessageMonitorException(
                                0, "Timeout expired (" + waitTimeout + ")",
                                sourceList);
                        logger.throwing(ex);
                        throw ex;
                    }

                    if (abort != null) {
                        // clear message once abort processed
                        XDBBaseException be = abort.getCause();
                        XDBServerException ex;
                        if (be != null) {
                            ex = new XDBMessageMonitorException(be, sourceList);
                        } else {
                            ex = new XDBMessageMonitorException(abort
                                    .getSourceNodeID(), "Error has occurred",
                                    sourceList);
                        }
                        abort = null;
                        logger.throwing(ex);
                        throw ex;
                    }

                    try {
                        wait(rest > 0 ? rest : 0L);
                        if (waitTimeout == 0) {
                            // if we are here, it mean a message was received
                            // set up timeout here
                            waitTimeout = Math.max(
                                    (System.currentTimeMillis() - startWaiting)
                                            * TIMEOUT_RATE, TIMEOUT_MILLIS);
                            logger.debug("New timeout has been calculated: "
                                    + waitTimeout);
                        }
                    } catch (InterruptedException ie) {
                    }
                    logger.debug("Waiting for: "
                            + (System.currentTimeMillis() - startWaiting)
                            + " ms, waitTimeout: " + waitTimeout);
                }

                return messages.toArray(new NodeMessage[messages.size()]);
            } finally {
                sourceList = null;
                messages = null;
                startWaiting = 0;
                timeout = 0;
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * 
     * @throws org.postgresql.stado.exception.XDBServerException 
     * @return 
     */

    public synchronized NodeMessage[] checkMessages() throws XDBServerException {
        // final String method = "checkMessages";
        // logger.entering(method);
        try {

            if (sourceList == null) {
                XDBServerException ex = new XDBServerException(
                        "Monitor was not set up to track messages");
                logger.throwing(ex);
                throw ex;
            }
            if (startWaiting == 0) {
                startWaiting = System.currentTimeMillis();
            }
            try {
                if (abort != null) {
                    // clear message once abort processed
                    XDBBaseException be = abort.getCause();
                    XDBServerException ex;
                    if (be != null) {
                        ex = new XDBMessageMonitorException(be, sourceList);
                    } else {
                        ex = new XDBMessageMonitorException(abort
                                .getSourceNodeID(), "Error has occurred",
                                sourceList);
                    }
                    abort = null;
                    logger.throwing(ex);
                    throw ex;
                }

                if (sourceList.isEmpty()) {
                    NodeMessage[] out = messages
                            .toArray(new NodeMessage[messages.size()]);
                    sourceList = null;
                    messages = null;
                    startWaiting = 0;
                    timeout = 0;
                    return out;
                } else {
                    if (timeout > 0
                            && (startWaiting + timeout) < System
                                    .currentTimeMillis()) {
                        XDBServerException ex = new XDBMessageMonitorException(
                                0, "Timeout expired (" + timeout + ")",
                                sourceList);
                        logger.throwing(ex);
                        throw ex;
                    }

                    if (timeout == 0 && !messages.isEmpty())
                    // if (!messages.isEmpty())
                    {
                        timeout = Math.max(
                                (System.currentTimeMillis() - startWaiting)
                                        * TIMEOUT_RATE, TIMEOUT_MILLIS);
                        logger.debug("New timeout has been calculated: "
                                + timeout);
                    }
                    return null;
                }
            } catch (Exception e) {
                sourceList = null;
                messages = null;
                startWaiting = 0;
                timeout = 0;
                if (e instanceof XDBServerException) {
                    throw (XDBServerException) e;
                } else {
                    throw new XDBServerException("Exception has been thrown", e);
                }
            }

        } finally {
            // logger.exiting(method);
        }
    }

    /**
     * Register message arrival
     * 
     * @param msg
     *            received message
     */
    public synchronized void register(NodeMessage msg) {
        final String method = "register";
        logger.entering(method, new Object[] { msg });
        try {

            if (sourceList != null && msg.getRequestId() == currentRequestId) {

                // Check to see if it is an informational message about sending
                // rows
                if (msg.getMessageType() == NodeMessage.MSG_BEGIN_SEND_ROWS)
                {
                    ActivityLog.startShipRows(client.getStatementId(), 
                            msg.getSourceNodeID(),
                            ((SendRowsMessage) msg).getDestNodeForRows());
                }
                else if (msg.getMessageType() == NodeMessage.MSG_END_SEND_ROWS)
                {
                    ActivityLog.endShipRows(client.getStatementId(), 
                            msg.getSourceNodeID(),
                            ((SendRowsMessage) msg).getDestNodeForRows(),
                            ((SendRowsMessage) msg).getNumRowsSent());                        
                }                              
                else if (sourceList.remove(new Integer(msg.getSourceNodeID()))) {
                    logger.debug("Getting closer: " + sourceList);
                    messages.add(msg);
                    notify();
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Cleanup monitor and release waiting thread
     * @param aNodeMessage 
     * @return 
     */
    public synchronized boolean abort(NodeMessage aNodeMessage) {
        final String method = "abort";
        logger.entering(method, new Object[] { aNodeMessage });
        try {

            // Only really abort if it had to do with the current request
            if (aNodeMessage.getRequestId() == currentRequestId) {
                abort = aNodeMessage;
                notifyAll();
            }
            return abort != null;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Returns current request id for this monitor
     * @return 
     */
    public int getRequestId() {
        return currentRequestId;
    }
}
