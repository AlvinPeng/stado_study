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
 * BatchHandler.java
 *
 *  
 */

package org.postgresql.stado.engine;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.SendMessageHelper;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;



/**
 * This is used to help manage batches, where multiple commands are sent to the
 * server at once for efficiency, and are, in turn executed via JDBC batches.
 *
 * 
 */
public class BatchHandler {
    private static final XLogger logger = XLogger.getLogger(BatchHandler.class);

    SendMessageHelper sendHelper;

    LinkedBlockingQueue<NodeMessage> producerQueue;

    XDBSessionContext client;

    /**
     * Stores current message for each node
     */
    private HashMap<Integer, NodeMessage> nodeMsgTable;

    /**
     * executeBatch needs to return an array of integers, so, we have Hashtables
     * to map the original statements to the ones on the nodes.
     */
    private HashMap<Integer, List<Integer>> mappingTables;

    /** for mapping batch results */
    private int lineNum;

    private int requestId; // current requestId used by all messages for this
                            // batch

    /**
     * Creates a new instance of BatchHandler
     * @param sendHelper
     * @param producerQueue
     * @param client
     */
    public BatchHandler(SendMessageHelper sendHelper,
            LinkedBlockingQueue<NodeMessage> producerQueue,
            XDBSessionContext client) {

        final String method = "BatchHandler";
        logger.entering(method, new Object[] { sendHelper, producerQueue });
        try {

            this.sendHelper = sendHelper;
            this.producerQueue = producerQueue;
            this.client = client;
            requestId = client.getRequestId();

            nodeMsgTable = new HashMap<Integer, NodeMessage>(32);
            mappingTables = new HashMap<Integer, List<Integer>>(32);
            lineNum = 0;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Initializes to prepare for bulk insertion
     */
    public void initBatch() {
        final String method = "initBatch";
        logger.entering(method);
        try {

            lineNum = 0;
            nodeMsgTable.clear();
            nodeMsgTable = new HashMap<Integer, NodeMessage>();
            mappingTables = new HashMap<Integer, List<Integer>>();
            requestId = client.getRequestId();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param sqlStatement
     * @param nodeId
     * @return
     */

    private boolean addStatementToNode(String sqlStatement, int nodeId) {
        NodeMessage buildNodeMessage = nodeMsgTable.get(nodeId);
        List<Integer> nodeLines;
        if (buildNodeMessage == null) {
            buildNodeMessage = NodeMessage
                    .getNodeMessage(NodeMessage.MSG_EXECUTE_BATCH);
            buildNodeMessage.setRequestId(requestId);
            nodeMsgTable.put(nodeId, buildNodeMessage);

            nodeLines = new ArrayList<Integer>(NodeMessage.MAX_DATA_ROWS);
            mappingTables.put(nodeId, nodeLines);
        } else {
            nodeLines = mappingTables.get(nodeId);
        }
        buildNodeMessage.addRowData(sqlStatement);
        nodeLines.add(lineNum);
        return !buildNodeMessage.canAddRows();
    }

    /**
     * Insert the values at all the nodes in the node list. Note that we do it
     * individually since until "broadcast" is implemented.
     * @param sqlStatement
     * @param nodeList
     * @return
     */
    // -----------------------------------------------------------------------
    public boolean addToBatchOnNodeList(String sqlStatement,
            Collection<DBNode> nodeList) {
        final String method = "addToBatchOnNodeList";
        logger.entering(method, new Object[] { sqlStatement, nodeList });
        try {

            boolean messageFull = false;
            // Now loop through and send
            for (DBNode dbNode : nodeList) {
                messageFull |= addStatementToNode(sqlStatement, dbNode
                        .getNodeId());
            }

            lineNum++;
            return messageFull;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Insert the values at all the nodes in the node list. Note that we do it
     * individually since until "broadcast" is implemented.
     * @param statements
     * @return
     */
    // -----------------------------------------------------------------------
    public boolean addToBatchOnNodes(Map<DBNode, String> statements) {
        final String method = "addToBatchOnNodeList";
        logger.entering(method, new Object[] { statements });
        try {

            boolean messageFull = false;
            // Now loop through and send
            for (Map.Entry<DBNode, String> entry : statements.entrySet()) {
                messageFull |= addStatementToNode(entry.getValue(), entry
                        .getKey().getNodeId());
            }

            lineNum++;
            return messageFull;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Adds command to batch on single node
     * @param sqlStatement
     * @param iNodeId
     * @return
     */
    public boolean addToBatchOnNode(String sqlStatement, int iNodeId) {
        final String method = "addToBatchOnNode";
        logger.entering(method, new Object[] { sqlStatement,
                new Integer(iNodeId) });
        try {

            boolean messageFull = addStatementToNode(sqlStatement, iNodeId);
            lineNum++;
            return messageFull;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @return Returns the requestId.
     */
    public int getRequestId() {
        return requestId;
    }

    /**
     * Execute the batch
     * @param strict
     * @return
     */
    public int[] executeBatch(boolean strict) {
        final String method = "executeBatch";
        logger.entering(method);
        int[] batchResults = new int[lineNum];
        if (lineNum == 0) {
            return batchResults;
        }
        try {

            NodeMessage aNodeMessage;
            int result;
            int originalPosition;
            boolean batchFailed = false;
            Arrays.fill(batchResults, Statement.SUCCESS_NO_INFO);

            MessageMonitor monitor = new MessageMonitor(client);
            monitor.setMonitor(requestId, nodeMsgTable.keySet());
            for (Map.Entry<Integer, NodeMessage> entry : nodeMsgTable
                    .entrySet()) {
            Integer anInteger = entry.getKey();
            aNodeMessage = entry.getValue();
            sendHelper.sendMessage(anInteger, aNodeMessage);
         }

            nodeMsgTable.clear();

            // Wait for acknowledgements
            do {
                aNodeMessage = null;
                try {
                    aNodeMessage = producerQueue.poll(5,
                            java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException ignore) {
                }

                if (aNodeMessage == null) {
                    continue;
                }

                NodeMessage.CATEGORY_MESSAGE
                        .debug("BH.executeBatch(): received message: "
                                + aNodeMessage);

                if (aNodeMessage.getMessageType() != NodeMessage.MSG_EXECUTE_BATCH_ACK) {
                    XDBServerException ex = new XDBServerException(
                            ErrorMessageRepository.UNEXPECTED_MESSAGE_RECIEVED
                                    + "(" + aNodeMessage.getMessageType()
                                    + " , " + NodeMessage.MSG_EXECUTE_BATCH_ACK
                                    + " )",
                            0,
                            ErrorMessageRepository.UNEXPECTED_MESSAGE_RECIEVED_CODE);
                    logger.throwing(ex);
                    throw ex;
                }
                monitor.register(aNodeMessage);
                if (batchFailed) {
                    continue;
                }
                // ok, now we update the return array, and map it back to
                // the original positions
                int nodeResult[] = aNodeMessage.getBatchResult();
                List<Integer> nodeLines = mappingTables.get(aNodeMessage
                        .getSourceNodeID());

                for (int i = 0; i < nodeResult.length; i++) {
                    // Get result for each
                    result = nodeResult[i];

                    if (strict && result == Statement.EXECUTE_FAILED) {
                        batchFailed = true;
                        break;
                    }
                    // determine where it was in original batch,
                    // before it was split up and sent to nodes
                    originalPosition = nodeLines.get(i);

                    // update results in array that refers to original position
                    // double check if we have a failure, in case we replicated,
                    // and it worked on one node, but not another
                    if (batchResults[originalPosition] == Statement.SUCCESS_NO_INFO) {
                        batchResults[originalPosition] = result;
                    } else if (batchResults[originalPosition] == Statement.EXECUTE_FAILED) {
                        if (result != Statement.EXECUTE_FAILED) {
                            // Execution is failed on some nodes, need to
                            // rollback the batch
                            batchFailed = true;
                            break;
                        }
                    } else {
                        if (result == Statement.EXECUTE_FAILED) {
                            // Execution is failed on some nodes, need to
                            // rollback the batch
                            batchFailed = true;
                            break;
                        } else if (result != Statement.SUCCESS_NO_INFO) {
                            batchResults[originalPosition] += result;
                        }
                    }
                }
            } while (monitor.checkMessages() == null);
            if (batchFailed) {
                for (int i = 0; i < batchResults.length; i++) {
                    if (batchResults[i] == Statement.SUCCESS_NO_INFO) {
                        batchResults[i] = Statement.EXECUTE_FAILED;
                    }
                }
            }
            return batchResults;

        } finally {
            logger.exiting(method, batchResults);
        }
    }

    /**
     *
     */
    public void clearBatch() {
        initBatch();
    }
}
