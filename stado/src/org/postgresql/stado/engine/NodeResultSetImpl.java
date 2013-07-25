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
 * NodeResultSetImpl.java
 * 
 *  
 */
package org.postgresql.stado.engine;

import java.sql.SQLException;

import org.apache.log4j.Level;
import org.postgresql.stado.common.ResultSetImpl;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.AbstractAgent;
import org.postgresql.stado.communication.CoordinatorAgent;
import org.postgresql.stado.communication.IMessageListener;
import org.postgresql.stado.communication.NodeAgent;
import org.postgresql.stado.communication.SendMessageHelper;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.engine.io.ResultSetResponse;
import org.postgresql.stado.engine.io.XMessage;
import org.postgresql.stado.exception.XDBBaseException;
import org.postgresql.stado.exception.XDBServerException;


/**
 *  
 */
public class NodeResultSetImpl extends ResultSetImpl implements
        IMessageListener {
    private static final XLogger logger = XLogger
            .getLogger(NodeResultSetImpl.class);

    private AbstractAgent agent = null;

    private SendMessageHelper sendHelper = null;

    private NodeMessage nodeMessage = null;

    private Integer sessionID = null;

    private NodeMessage nextMessage = null;

    /**
     * 
     * @param nodeMessage
     * @throws java.sql.SQLException
     */
    public NodeResultSetImpl(NodeMessage nodeMessage) throws SQLException {
        super();
        this.nodeMessage = nodeMessage;
        int target = nodeMessage.getTargetNodeID().intValue();
        sessionID = nodeMessage.getSessionID();
        agent = target == 0 ? (AbstractAgent) CoordinatorAgent.getInstance()
                : (AbstractAgent) NodeAgent.getNodeAgent(target);
        sendHelper = new SendMessageHelper(target, sessionID, agent);
        this.responseMessage = getNextResponse(nodeMessage, null);

        resetRawRows();
        this.columnMeta = responseMessage.getColumnMetaData();
    }

    /**
     * Collect messages with rows of remote ResultSet
     * 
     * @see org.postgresql.stado.communication.IMessageListener#processMessage(org.postgresql.stado.communication.message.NodeMessage)
     * @param message
     * @return
     */
    public synchronized boolean processMessage(NodeMessage message) {
        if (nextMessage != null) {
            // Development error
            XDBServerException ex = new XDBServerException(
                    "Previous message has not been consumed");
            logger.throwing(ex);
            throw ex;

        }
        if ((message.getMessageType() == NodeMessage.MSG_RESULT_ROWS || message
                .getMessageType() == NodeMessage.MSG_ABORT)
                && nodeMessage.getSourceNodeID() == message.getSourceNodeID()
                && nodeMessage.getRequestId() == message.getRequestId()) {
            nextMessage = message;
            notify();
            return true;
        }
        return false;
    }

    /**
     * 
     * @throws java.sql.SQLException
     */
    @Override
    protected synchronized void setNextResultSet() throws SQLException {
        logger.log(Level.INFO, "Asking node %0% for more rows",
                new Object[] { new Integer(nodeMessage.getSourceNodeID()) });
        NodeMessage requestNM = NodeMessage
                .getNodeMessage(NodeMessage.MSG_RESULT_ROWS_REQUEST);
        requestNM.setResultSetID(nodeMessage.getResultSetID());
        sendHelper.sendReplyMessage(nodeMessage, requestNM);
        // TODO wait timeout ?
        while (nextMessage == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new SQLException("Result set is closed");
            }
        }
        if (nextMessage.getMessageType() == NodeMessage.MSG_ABORT) {
            XDBBaseException ex = nextMessage.getCause();
            if (ex != null) {
                logger.catching(ex);
            }
            SQLException se = new SQLException(
                    "Can not fetch more rows: " + ex == null ? "reason unknown"
                            : ex.getMessage());
            logger.throwing(se);
            throw se;
        }
        responseMessage = getNextResponse(nextMessage, responseMessage);
        nextMessage = null;
    }

    /**
     * Extract data from incoming message
     * 
     * @param message
     * @param response
     * @return
     */
    private ResultSetResponse getNextResponse(NodeMessage message,
            ResultSetResponse response) {
        if (response == null) {
            response = new ResultSetResponse();
        }
        byte[] resultSetData = message.getResultSetData();
        // reading response
        byte[] header = new byte[XMessage.HEADER_SIZE];
        System.arraycopy(resultSetData, 0, header, 0, XMessage.HEADER_SIZE);
        response.setHeaderBytes(header);
        int len = response.getPacketLength() - XMessage.HEADER_SIZE;
        byte[] inputBytes = new byte[len];
        System.arraycopy(resultSetData, XMessage.HEADER_SIZE, inputBytes, 0,
                len);
        response.setMessage(inputBytes);
        return response;
    }

    @Override
    public void close() throws SQLException {
        if (!responseMessage.isLastPacket()) {
            // Close RS on node
            NodeMessage closeMsg = NodeMessage
                    .getNodeMessage(NodeMessage.MSG_RESULT_CLOSE);
            closeMsg.setResultSetID(nodeMessage.getResultSetID());
            sendHelper.sendReplyMessage(nodeMessage, closeMsg);
        }
        // Remove the RS from ME
        NodeMessage closeMsg = NodeMessage
                .getNodeMessage(NodeMessage.MSG_RESULT_CLOSE);
        closeMsg.setResultSetID(nodeMessage.getResultSetID());
        closeMsg.setSessionID(nodeMessage.getSessionID());
        closeMsg.setRequestId(nodeMessage.getRequestId());
        closeMsg.setSourceNodeID(nodeMessage.getTargetNodeID());
        sendHelper.sendMessage(nodeMessage.getTargetNodeID(), closeMsg);
    }
}
