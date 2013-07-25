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
 * ProducerSender.java
 *
 *
 *
 */
package org.postgresql.stado.engine;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLevel;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.communication.SendMessageHelper;
import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.communication.message.SendRowsMessage;
import org.postgresql.stado.engine.copy.CopyOut;
import org.postgresql.stado.engine.loader.Loader;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBGeneratorException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysSerialGenerator;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.metadata.partitions.ReplicatedPartitionMap;
import org.postgresql.stado.misc.RSHelper;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.planner.StepDetail;


/**
 * Handles sending data messages to the individual nodes, based on a ResultSet.
 * Note that it is used for both the coordinator and NodeProducerThreads.
 * aQueryCombiner will be non-null if this is run from the coordinator.
 *
 *
 */
public class ProducerSender {

    private static final XLogger logger = XLogger.getLogger(ProducerSender.class);

    private static final int MAX_UNACKED_MESSAGES = 10;

    private long dataSeqNo;

    private AckTracker anAckTracker = null;

    private LinkedBlockingQueue<NodeMessage> producerQueue;

    private SendMessageHelper sendHelper;

    private int maxUnackedCount = 0;

    /**
     * If we are aborting, try and clean up gracefully. Fixes situation where NT
     * thinks NPT has handled abort, but it really has not.
     */
    private boolean abortFlag = false;

    /**
     * Stores current message for each node
     */
    private HashMap<Integer, NodeMessage> nodeMsgTable;

    private AtomicReference<ResultSet> currentResultSet = new AtomicReference<ResultSet>();

    private AtomicReference<Loader> currentLoader = new AtomicReference<Loader>();


    /**

     * Creates a new instance of ProducerSender

     * @param sendHelper

     * @param producerQueue

     */
    public ProducerSender(SendMessageHelper sendHelper,
            LinkedBlockingQueue<NodeMessage> producerQueue) {
        final String method = "ProducerSender";
        logger.entering(method, new Object[] { sendHelper, producerQueue });
        try {

            this.sendHelper = sendHelper;
            this.producerQueue = producerQueue;
            dataSeqNo = 0;
            nodeMsgTable = new HashMap<Integer, NodeMessage>();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param stepDetail
     */

    private void initInserts(StepDetail stepDetail) {
        final String method = "initInserts";
        logger.entering(method, new Object[] { stepDetail });
        try {

            Collection<Integer> nodeList = stepDetail.consumerNodeList;
            if (!Props.XDB_USE_LOAD_FOR_STEP) {
                dataSeqNo = 0;
                anAckTracker = new AckTracker();
                anAckTracker.init(new HashMap(), nodeList);
                if (nodeList == null || nodeList.size() == 0) {
                    maxUnackedCount = MAX_UNACKED_MESSAGES;
                } else {
                    // maxUnackedCount = (int) Math.round(MAX_UNACKED_MESSAGES *
                    // Math.sqrt(nodeList.size()));
                    maxUnackedCount = nodeList.size() + 1;
                }
            }
        } finally {
            logger.exiting(method);
        }
    }
    /**
     * Sends ResultSet to nodes.
     * @param copyOut
     * @param aStepDetail
     * @param jdbcLock
     * @throws java.sql.SQLException
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void sendToNodes(CopyOut copyOut, StepDetail aStepDetail,
            Object jdbcLock, int sourceNodeId, int requestId)
    throws SQLException, XDBServerException {
        IntermediateSerialGeneratorClient aSerialGenerator = null;
        if (aStepDetail.getSerialColumnPosition() > 0) {
            // TODO generator ID will be taken from StepDetail
            aSerialGenerator = new IntermediateSerialGeneratorClient();
        }
        Loader loader = new Loader(copyOut,
                aStepDetail.getInsertColumnString());
        Collection<Integer> nodeList = aStepDetail.consumerNodeList;

        // Send messages if we are using the grid monitor
        if (Props.XDB_ENABLE_ACTIVITY_LOG) {
            if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD
                    || aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD_FINAL) {
                SendRowsMessage beginSendRowsMessage = (SendRowsMessage) NodeMessage.getNodeMessage(NodeMessage.MSG_BEGIN_SEND_ROWS);
                beginSendRowsMessage.setRequestId(requestId);
                beginSendRowsMessage.setDestNodeForRows(1);
                sendHelper.sendMessage(new Integer(0),
                        beginSendRowsMessage);
            } else {
                for (Object element : aStepDetail.consumerNodeList) {
                    Integer destNodeInt = (Integer) element;

                    SendRowsMessage beginSendRowsMessage = (SendRowsMessage) NodeMessage.getNodeMessage(NodeMessage.MSG_BEGIN_SEND_ROWS);
                    beginSendRowsMessage.setRequestId(requestId);
                    beginSendRowsMessage.setDestNodeForRows(destNodeInt.intValue());
                    sendHelper.sendMessage(new Integer(0),
                            beginSendRowsMessage);
                }
            }
        }

        if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_BROADCAST_AND_COORD
                || aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD
                || aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD_FINAL
                || aStepDetail.combineOnCoordFirst) {
            if (nodeList == null) {
                nodeList = Collections.singleton(aStepDetail.getDestNode());
            } else if (!nodeList.contains(aStepDetail.getDestNode())) {
                nodeList = new HashSet<Integer>(nodeList);
                nodeList.add(aStepDetail.getDestNode());
            }
        }
        NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Sending results to Nodes:"
                + nodeList);
        int hashPosition = -1;
        if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_HASH) {
            if (aStepDetail.getHashColumnPosition() > 0) {
                hashPosition = aStepDetail.getHashColumnPosition() - 1;
            }
        }

        PartitionMap partMap = aStepDetail.getPartitionMap();
        if (partMap == null) {
            // Force rebuild PartitionMap
            partMap = new ReplicatedPartitionMap();
            partMap.generateDistribution(nodeList);
        }
        loader.setTargetTableInfoExt(aStepDetail.targetTable,
                hashPosition, aStepDetail.getGroupHashColumns(), partMap,
                aStepDetail.getHashDataType(),
                aStepDetail.getSerialColumnPosition(),
                aSerialGenerator,
                aStepDetail.getDestType() == StepDetail.DEST_TYPE_NODEID,
                aStepDetail.suppressSendingNodeId,
                aStepDetail.nodeInfos);
                currentLoader.set(loader);
        try {
            // loader.setVerbose(logger.isDebugEnabled());
            loader.prepareLoad();
            boolean success = false;
            try {
                NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Run Writers");
                loader.runWriters();
                success = true;
            } finally {
                NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Finish load");
                loader.finishLoad(success);

                if (Props.XDB_ENABLE_ACTIVITY_LOG) {
                    HashMap<Integer, Long> writerMap = loader.getRowCountMap();

                    if (writerMap != null) {
                        for (Object element : writerMap.keySet()) {
                            Integer destNodeInt = (Integer) element;

                            SendRowsMessage endSendRowsMessage = (SendRowsMessage) NodeMessage.getNodeMessage(NodeMessage.MSG_END_SEND_ROWS);

                            endSendRowsMessage.setRequestId(requestId);
                            endSendRowsMessage.setDestNodeForRows(destNodeInt.intValue());
                            endSendRowsMessage.setNumRowsSent(writerMap.get(destNodeInt));

                            sendHelper.sendMessage(new Integer(0),
                                    endSendRowsMessage);
                        }
                    }
                }
            }
            return;
        } catch (Exception e) {
            logger.catching(e);
            XDBServerException ex = new XDBServerException(e.getMessage());
            logger.throwing(ex);
            throw ex;
        } finally {
            currentLoader.set(null);
        }
    	
    }
    
    /**
     * Sends ResultSet to nodes.
     * @param aResultSet
     * @param aStepDetail
     * @param jdbcLock
     * @throws java.sql.SQLException
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void sendToNodes(ResultSet aResultSet, StepDetail aStepDetail,
            Object jdbcLock, int sourceNodeId, int requestId)
    throws SQLException, XDBServerException {
        final String method = "sendToNodes";
        logger.entering(method, new Object[] { aResultSet, aStepDetail,
                jdbcLock });
        try {

            currentResultSet.set(aResultSet);
            IntermediateSerialGeneratorClient aSerialGenerator = null;
            if (aStepDetail.getSerialColumnPosition() > 0) {
                // TODO generator ID will be taken from StepDetail
                aSerialGenerator = new IntermediateSerialGeneratorClient();
            }
            if (Props.XDB_USE_LOAD_FOR_STEP) {
                Loader loader = new Loader(aResultSet,
                        aStepDetail.getInsertColumnString());
                Collection<Integer> nodeList = aStepDetail.consumerNodeList;

                // Send messages if we are using the grid monitor
                if (Props.XDB_ENABLE_ACTIVITY_LOG) {
                    if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD
                            || aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD_FINAL) {
                        SendRowsMessage beginSendRowsMessage = (SendRowsMessage) NodeMessage.getNodeMessage(NodeMessage.MSG_BEGIN_SEND_ROWS);
                        beginSendRowsMessage.setRequestId(requestId);
                        beginSendRowsMessage.setDestNodeForRows(1);
                        sendHelper.sendMessage(new Integer(0),
                                beginSendRowsMessage);
                    } else {
                        for (Object element : aStepDetail.consumerNodeList) {
                            Integer destNodeInt = (Integer) element;

                            SendRowsMessage beginSendRowsMessage = (SendRowsMessage) NodeMessage.getNodeMessage(NodeMessage.MSG_BEGIN_SEND_ROWS);
                            beginSendRowsMessage.setRequestId(requestId);
                            beginSendRowsMessage.setDestNodeForRows(destNodeInt.intValue());
                            sendHelper.sendMessage(new Integer(0),
                                    beginSendRowsMessage);
                        }
                    }
                }

                if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_BROADCAST_AND_COORD
                        || aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD
                        || aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD_FINAL
                        || aStepDetail.combineOnCoordFirst) {
                    if (nodeList == null) {
                        nodeList = Collections.singleton(aStepDetail.getDestNode());
                    } else if (!nodeList.contains(aStepDetail.getDestNode())) {
                        nodeList = new HashSet<Integer>(nodeList);
                        nodeList.add(aStepDetail.getDestNode());
                    }
                }
                NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Sending results to Nodes:"
                        + nodeList);
                int hashPosition = -1;
                if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_HASH) {
                    if (aStepDetail.getHashColumnPosition() > 0) {
                        hashPosition = aStepDetail.getHashColumnPosition() - 1;
                    }
                }
                PartitionMap partMap = aStepDetail.getPartitionMap();
                if (partMap == null) {
                    // Force rebuild PartitionMap
                    partMap = new ReplicatedPartitionMap();
                    partMap.generateDistribution(nodeList);
                }
                loader.setTargetTableInfoExt(aStepDetail.targetTable,
                        hashPosition, aStepDetail.getGroupHashColumns(), partMap,
                        aStepDetail.getHashDataType(),
                        aStepDetail.getSerialColumnPosition(),
                        aSerialGenerator,
                        aStepDetail.getDestType() == StepDetail.DEST_TYPE_NODEID,
                        aStepDetail.suppressSendingNodeId,
                        aStepDetail.nodeInfos);
                        currentLoader.set(loader);
                try {
                    // loader.setVerbose(logger.isDebugEnabled());
                    loader.prepareLoad();
                    boolean success = false;
                    try {
                        NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Run Writers");
                        loader.runWriters();
                        success = true;
                    } finally {
                        NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Finish load");
                        loader.finishLoad(success);

                        if (Props.XDB_ENABLE_ACTIVITY_LOG) {
                            HashMap<Integer, Long> writerMap = loader.getRowCountMap();

                            if (writerMap != null) {
                                for (Object element : writerMap.keySet()) {
                                    Integer destNodeInt = (Integer) element;

                                    SendRowsMessage endSendRowsMessage = (SendRowsMessage) NodeMessage.getNodeMessage(NodeMessage.MSG_END_SEND_ROWS);

                                    endSendRowsMessage.setRequestId(requestId);
                                    endSendRowsMessage.setDestNodeForRows(destNodeInt.intValue());
                                    endSendRowsMessage.setNumRowsSent(writerMap.get(destNodeInt));

                                    sendHelper.sendMessage(new Integer(0),
                                            endSendRowsMessage);
                                }
                            }
                        }
                    }
                    return;
                } catch (Exception e) {
                    logger.catching(e);
                    XDBServerException ex = new XDBServerException(
                            "Can not send data to Nodes", e);
                    logger.throwing(ex);
                    throw ex;
                } finally {
                    currentLoader.set(null);
                }
            }
            // Use batches to send intermediate results
            int iColumnCount = 0;
            int iPartitionedNode;
            boolean[] isQuoted = null;
            boolean hasRows;
            if (jdbcLock == null) {
                jdbcLock = aResultSet.getStatement().getConnection();
            }
            int iCount = 0;

            // Check to see if it can just go to a single node,
            // based on the next table to join with and its
            // partition column
            // -----------------------------------------
            synchronized (jdbcLock) {
                logger.log(XLevel.TRACE,
                        "Entered critical section for connection: %0%",
                        new Object[] { jdbcLock });
                ResultSetMetaData rsmd = aResultSet.getMetaData();
                logger.log(
                        XLevel.TRACE,
                        "Got ResultSet metadata, for connection: %0%, ResultSet %1%",
                        new Object[] { jdbcLock, aResultSet });
                iColumnCount = rsmd.getColumnCount();

                // If dealing with the last step in the subquery of a correlated
                // subquery, we don't want to send the last column, XNODEID
                if (aStepDetail.suppressSendingNodeId) {
                    iColumnCount--;
                }

                // Determine whether or not to quote
                isQuoted = RSHelper.getQuoteInfo(aResultSet);

                // now, loop through the resultset, and buildup string
                // while (true) {
                // Use this to avoid concurrency issues sharing the same jdbc
                // connection
                hasRows = aResultSet.next();
                logger.log(
                        XLevel.TRACE,
                        "Scrolled to next row, for connection: %0%, ResultSet %1%",
                        new Object[] { jdbcLock, aResultSet });

                logger.log(XLevel.TRACE,
                        "Leaving critical section for connection: %0%",
                        new Object[] { jdbcLock });
                jdbcLock.notify();
            }

            initInserts(aStepDetail);

            StringBuffer sbTempValues = new StringBuffer();

            String baseInsert;
            // for database compatibility- if using a serial, we need
            // to explicitly list the columns
            if (aStepDetail.targetSchema != null
                    && aStepDetail.targetSchema.indexOf("XSERIALID") > 0) {
                baseInsert = "INSERT INTO " + IdentifierHandler.quote(aStepDetail.targetTable) + "("
                + aStepDetail.getInsertColumnString() + ") VALUES ";
            } else {
                baseInsert = "INSERT INTO " + IdentifierHandler.quote(aStepDetail.targetTable)
                + " VALUES ";
            }

            while (hasRows) {
                iCount++;

                logger.log(
                        XLevel.TRACE,
                        "Converting data from columns, for connection: %0%, ResultSet %1%",
                        new Object[] { jdbcLock, aResultSet });

                sbTempValues.setLength(0);
                String outputRow = null;
                if (!Props.XDB_JUST_DATA_VALUES) {
                    sbTempValues.append(baseInsert);
                }

                sbTempValues.append("(");

                for (int i = 1; i <= iColumnCount; i++) {
                    if (i > 1) {
                        sbTempValues.append(',');
                    }

                    if (aStepDetail.getSerialColumnPosition() == i) {
                        try {
                            sbTempValues.append(""
                                    + aSerialGenerator.allocateValue());
                        } catch (XDBGeneratorException ge) {
                            throw new XDBServerException(
                                    "Can not generate serial value: "
                                    + ge.getMessage());
                        }
                    } else if (aResultSet.getString(i) == null) {
                        sbTempValues.append("NULL");
                    } else {
                        if (isQuoted[i - 1]) {
                            sbTempValues.append("'");

                            String colStr = aResultSet.getString(i);
                            // first check before replacing to
                            // save extra allocs
                            if (colStr.indexOf("'") >= 0) {
                                colStr = colStr.replaceAll("'", "''");
                            }
                            sbTempValues.append(colStr);

                            sbTempValues.append("'");
                        } else {
                            String colStr = aResultSet.getString(i);
                            sbTempValues.append(colStr);
                        }
                    }
                }
                sbTempValues.append(")");
                outputRow = sbTempValues.toString();

                logger.log(
                        XLevel.TRACE,
                        "Converted data from columns, for connection: %0%, ResultSet %1%",
                        new Object[] { jdbcLock, aResultSet });

                if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_BROADCAST
                        || aStepDetail.getDestType() == StepDetail.DEST_TYPE_BROADCAST_AND_COORD) {
                    try {
                        insertOnNodeList(outputRow, aStepDetail);
                    } catch (Exception e) {
                        logger.catching(e);
                        XDBServerException ex = new XDBServerException(
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE
                                + "( " + 0 + "," + outputRow + " ) "
                                + e.getMessage(),
                                e,
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                } else if (aStepDetail.combineOnCoordFirst) {
                    try {
                        insertOnNode(0, outputRow);
                    } catch (Exception e) {
                        logger.catching(e);
                        XDBServerException ex = new XDBServerException(
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE
                                + "( " + 0 + "," + "[ " + outputRow
                                + " ]" + " )",
                                e,
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                } else if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_ONE) {
                    try {
                        insertOnNode(aStepDetail.getDestNode(), outputRow);
                    } catch (Exception e) {
                        logger.catching(e);
                        XDBServerException ex = new XDBServerException(
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE
                                + "( " + 0 + "," + outputRow + " )",
                                e,
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                } else if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_HASH) {
                    if (aStepDetail.getHashColumnPosition() > 0) {
                        // use position
                        iPartitionedNode = aStepDetail.getNodeId(
                                aResultSet.getString(aStepDetail.getHashColumnPosition()));
                    } else {
                        // send based on group by
                        // for an even distribution, we concatenate all of the
                        // items in the group by clause
                        StringBuilder sbGroupExpr = new StringBuilder(80);
                        for (int groupHashCol : aStepDetail.getGroupHashColumns()) {
                            sbGroupExpr.append(aResultSet.getString(groupHashCol));
                        }
                        iPartitionedNode = aStepDetail.getNodeId(sbGroupExpr.toString());
                    }

                    try {
                        insertOnNode(iPartitionedNode, outputRow);
                    } catch (Exception e) {
                        logger.catching(e);
                        XDBServerException ex = new XDBServerException(
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE
                                + "( " + 0 + "," + outputRow + " )",
                                e,
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                } else if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_NODEID) {
                    // use the last column, it always contains the XNODEID
                    // we need for the destination
                    int nodeId = aResultSet.getInt(aResultSet.getMetaData().getColumnCount());

                    try {
                        insertOnNode(nodeId, outputRow);
                    } catch (Exception e) {
                        logger.catching(e);
                        XDBServerException ex = new XDBServerException(
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE
                                + "( " + 0 + "," + outputRow + " )",
                                e,
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                } else if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD) {
                    try {
                        insertOnNode(0, outputRow);
                    } catch (Exception e) {
                        logger.catching(e);
                        XDBServerException ex = new XDBServerException(
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE
                                + "( " + 0 + "," + outputRow + " )",
                                e,
                                ErrorMessageRepository.INSERT_FAILED_ON_NODE_CODE);
                        logger.throwing(ex);
                        throw ex;
                    }
                }

                synchronized (jdbcLock) {
                    logger.log(XLevel.TRACE,
                            "Entered critical section for connection: %0%",
                            new Object[] { jdbcLock });
                    hasRows = aResultSet.next();
                    logger.log(
                            XLevel.TRACE,
                            "Scrolled to next row, for connection: %0%, ResultSet %1%",
                            new Object[] { jdbcLock, aResultSet });
                    logger.log(XLevel.TRACE,
                            "Leaving critical section for connection: %0%",
                            new Object[] { jdbcLock });
                    jdbcLock.notify();
                }
            }

            try {
                logger.info("Finishing Inserts");
                finishInserts(aStepDetail.getDestType());
                logger.info("Finished Inserts");
            } catch (Exception e) {
                logger.catching(e);
                XDBServerException ex = new XDBServerException(
                        ErrorMessageRepository.INSERT_FAILED, e,
                        ErrorMessageRepository.INSERT_FAILED_CODE);
                logger.throwing(ex);
                throw ex;
            }

        } finally {
            currentResultSet.set(null);
            logger.exiting(method);
        }
    }

    /**
     * Insert the values at all the nodes in the node list. Note that we do it
     * individually since until "broadcast" is implemented.
     *
     * @param insertValueString
     * @param aStepDetail
     * @throws org.postgresql.stado.exception.XDBServerException
     */

    // -----------------------------------------------------------------------
    private void insertOnNodeList(String insertValueString,
            StepDetail aStepDetail) throws XDBServerException {
        final String method = "insertOnNodeList";
        logger.entering(method, new Object[] { insertValueString, aStepDetail });
        try {

            if (!Props.XDB_USE_LOAD_FOR_STEP) {
                NodeMessage buildNodeMessage = nodeMsgTable.get(null);

                if (buildNodeMessage == null) {
                    ++dataSeqNo;
                    buildNodeMessage = NodeMessage.getNodeMessage(NodeMessage.MSG_SEND_DATA);
                    buildNodeMessage.setDataSeqNo(dataSeqNo);
                    Integer[] targetNodeIDs;
                    boolean addCoord = aStepDetail.getDestType() == StepDetail.DEST_TYPE_BROADCAST_AND_COORD
                    || aStepDetail.combineOnCoordFirst;
                    if (aStepDetail.consumerNodeList == null) {
                        targetNodeIDs = addCoord ? new Integer[] { new Integer(
                                0) } : null;
                    } else {
                        targetNodeIDs = new Integer[aStepDetail.consumerNodeList.size()
                                                    + (addCoord ? 1 : 0)];
                        aStepDetail.consumerNodeList.toArray(targetNodeIDs);
                        if (addCoord) {
                            targetNodeIDs[targetNodeIDs.length - 1] = new Integer(
                                    0);
                        }
                    }
                    buildNodeMessage.setTargetNodeIDs(targetNodeIDs);
                    nodeMsgTable.put(null, buildNodeMessage);
                }

                buildNodeMessage.addRowData(insertValueString);

                if (!buildNodeMessage.canAddRows()) {
                    sendHelper.sendMessageByDestInMessage(buildNodeMessage);
                    nodeMsgTable.remove(null);
                    anAckTracker.addPacket(new Long(
                            buildNodeMessage.getDataSeqNo()), buildNodeMessage);
                    checkAcks(maxUnackedCount);
                }
            }

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param maxUnacked
     * @throws org.postgresql.stado.exception.XDBServerException
     */

    private void checkAcks(int maxUnacked) throws XDBServerException {
        final String method = "checkAcks";
        logger.entering(method);
        try {
            while (anAckTracker != null
                    && anAckTracker.getUnackedKeyCount() > maxUnacked
                    || !producerQueue.isEmpty()) {
                NodeMessage aNodeMessage;

                // We wait on our message queue for instructions on what to do
                aNodeMessage = null;
                if (abortFlag) {
                    return;
                }
                aNodeMessage = producerQueue.poll(5,
                        java.util.concurrent.TimeUnit.SECONDS);
                if (aNodeMessage == null) {
                    continue;
                }

                if (aNodeMessage.getMessageType() == NodeMessage.MSG_SEND_DATA_ACK) {
                    anAckTracker.register(
                            new Long(aNodeMessage.getDataSeqNo()),
                            aNodeMessage.getSourceNodeID());
                    logger.log(
                            XLevel.TRACE,
                            "maxUnacked = %0%; unackedCount = %1%",
                            new Object[] {
                                    new Integer(maxUnacked),
                                    new Integer(
                                            anAckTracker.getUnackedKeyCount()) });
                } else {
                    // Most probably we get MSG_ABORT
                    XDBServerException ex = new XDBServerException(
                            ErrorMessageRepository.UNEXPECTED_MESSAGE_RECIEVED
                            + "("
                            + NodeMessage.getMessageTypeString(aNodeMessage.getMessageType())
                            + " , "
                            + NodeMessage.getMessageTypeString(NodeMessage.MSG_SEND_DATA_ACK)
                            + ") ",
                            aNodeMessage.getCause(),
                            ErrorMessageRepository.UNEXPECTED_MESSAGE_RECIEVED_CODE);
                    logger.throwing(ex);
                    throw ex;
                }
            }

        } catch (InterruptedException ie) {
            // Just exit the loop
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param iNodeId
     * @param insertValueString
     * @throws org.postgresql.stado.exception.XDBServerException
     */

    private void insertOnNode(int iNodeId, String insertValueString)
    throws XDBServerException {
        final String method = "insertOnNode";
        logger.entering(method, new Object[] { new Integer(iNodeId),
                insertValueString });
        try {

            if (!Props.XDB_USE_LOAD_FOR_STEP) {
                NodeMessage buildNodeMessage;
                buildNodeMessage = nodeMsgTable.get(iNodeId);

                if (buildNodeMessage == null) {
                    buildNodeMessage = NodeMessage.getNodeMessage(NodeMessage.MSG_SEND_DATA);
                    buildNodeMessage.setDataSeqNo(++dataSeqNo);

                    // msgRowCount = 0;
                    nodeMsgTable.put(iNodeId, buildNodeMessage);
                }

                buildNodeMessage.addRowData(insertValueString);

                // see if the message for target node is full and we should send
                if (!buildNodeMessage.canAddRows()) {
                    sendHelper.sendMessage(new Integer(iNodeId),
                            buildNodeMessage);
                    nodeMsgTable.remove(iNodeId);
                    anAckTracker.addPacket(new Long(
                            buildNodeMessage.getDataSeqNo()), buildNodeMessage,
                            Collections.singleton(iNodeId));
                    checkAcks(maxUnackedCount);
                    buildNodeMessage = null;
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Send any remaining data messages to each of the target nodes.
     * @param destType
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public void finishInserts(int destType) throws XDBServerException {
        final String method = "finishInserts";
        logger.entering(method, new Object[] { new Integer(destType) });
        try {

            if (!Props.XDB_USE_LOAD_FOR_STEP && !abortFlag) {
                logger.log(XLevel.TRACE, "Messages To Send: %0%",
                        new Object[] { new Integer(nodeMsgTable.size()) });
                for (Entry<Integer, NodeMessage> entry : nodeMsgTable.entrySet()) {
                    NodeMessage aNodeMessage = entry.getValue();

                    if (entry.getKey() == null) {
                        sendHelper.sendMessageByDestInMessage(aNodeMessage);
                        anAckTracker.addPacket(aNodeMessage.getDataSeqNo(),
                                aNodeMessage);
                    } else {
                        sendHelper.sendMessage(entry.getKey(), aNodeMessage);
                        anAckTracker.addPacket(aNodeMessage.getDataSeqNo(),
                                aNodeMessage,
                                Collections.singleton(entry.getKey()));
                    }
                }

                checkAcks(0);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param flag
     */

    public void setAbort(boolean flag) {
        abortFlag = flag;
        if (flag) {
            Loader loader = currentLoader.get();
            if (loader != null) {
                loader.cancel();
            }
            ResultSet aResultSet = currentResultSet.get();
            try {
                if (aResultSet != null) {
                    Statement aStatement = aResultSet.getStatement();
                    if (aStatement != null) {
                        aStatement.cancel();
                    }
                    aResultSet.close();
                }
            } catch (SQLException se) {
                logger.catching(se);
            }
        }
    }

    private class IntermediateSerialGeneratorClient extends SysSerialGenerator {

        /*
         * @see org.postgresql.stado.MetaData.SysSerialGenerator#update()

         * @throws org.postgresql.stado.exception.XDBGeneratorException
         */

        @Override
        protected void update() throws XDBGeneratorException {
            update(1);
        }
    }
}
