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
 * StepDetail.java
 *
 *
 */

package org.postgresql.stado.planner;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.SqlWordWrap;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.NodeDBConnectionInfo;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.partitions.HashPartitionMap;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.metadata.partitions.ReplicatedPartitionMap;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 * StepDetail
 *
 * Contains all cpre information needed to execute a step.
 *
 *
 */
public class StepDetail implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 644465276880869900L;

    private static final XLogger logger = XLogger.getLogger(StepDetail.class);

    /** Session info */
    private transient XDBSessionContext client;

    /** Request Id */
    public int requestId;

    /** Step number in the query */
    public int stepNo;

    /** For this node step, whether or not it produces data. */
    public boolean isProducer;

    /** For this node step, whether or not it consumes data. */
    public boolean isConsumer;

    /** The actual query to execute on this step. */
    public String queryString;

    /** What table to insert intermediate results into. */
    public String targetTable;

    /** CREATE TABLE command for intermediate table. */
    public String targetSchema;

    /** List of temp tables to drop at the end of this step. */
    public List<String> dropList;

    /** Destination type. See DEST_TYPE_* */
    private short destType;

    /** if destType is DEST_TYPE_ONE, the target node. */
    private int destNode;

    /** List containing list of destination nodes,
     * if DEST_TYPE_BROADCAST or DEST_TYPE_BROADCAST_AND_COORD
     */
    public ArrayList<Integer> consumerNodeList;

    /** Indicates if we need to combine the results on the
     * coordinator first before routing to the final destination.
     * This is because of distinct.
     * that means that destType is the *final* destination, and does
     * not take this into account
     */
    public boolean combineOnCoordFirst = false;

    /** if DEST_TYPE_HASH, the column to hash on */
    private int hashColumnPosition = 0;
    
    /** set this if we should calculate group hash based on all columns
     * in the list. */
    private int[] groupHashColumns;

    private ExpressionType hashDataType = null;

    /** if DEST_TYPE_HASH, the partition map to use */
    private PartitionMap partitionMap;

    /** Send to all nodes */
    public static final short DEST_TYPE_BROADCAST = 1;

    /** Send results only to coordinator */
    public static final short DEST_TYPE_COORD = 2;

    /** Send results to just a single node */
    public static final short DEST_TYPE_ONE = 3;

    /** Send results based on hash calculation of a column */
    public static final short DEST_TYPE_HASH = 4;

    /** Needs to go to all nodes + coordinator, due to special OUTER case
     */
    public static final short DEST_TYPE_BROADCAST_AND_COORD = 5;

    /** For the final step- don't send the data, it will be streamed
     * from the coordinator
     */
    public static final short DEST_TYPE_COORD_FINAL = 6;

    /** Send results to a specific node. */
    public static final short DEST_TYPE_NODEID = 7;

    /** for help with metadata into */
    private transient SysDatabase database;

    public boolean isFinalUnionPart = false;

    /** This is only used with union.
     * We create 1 or 2 result sets, depending on
     * the sequence of UNION and UNION ALL
     */
    public short unionResultGroup = 0;

    /** only used for union part of top plan */
    public List finalUnionPartSortInfo;

    /** only used for union part of top plan */
    public boolean finalUnionPartIsDistinct = false;

    /** The select statement minus the select clause, to allow
     * easy rebuilding of statement with new projection list. */
    protected String nonProjectionSelectPart;

    /** If this is a single step that just looks up from replicated tables. */
    protected boolean isLookupStep = false;

    /** It this is an outer setp. */
    private boolean isOuterStep = false;

    /**
     * When dealing with correlated subqueries and using the NODEID destination
     * technique, on the last step we want to select XNODEID to get the
     * destination node, but we don't want to send the nodeid itself, to save
     * bandwidth and make the insertion more effecient.
     */
    public boolean suppressSendingNodeId = false;

    /** If we are using load for step, we need to pass down to nodes;
     * is needed for XDBLoader
     */
    public NodeDBConnectionInfo[] nodeInfos = null;

    /** this is used for correlated joins */
    private String indexColumnString;

    /** Column list that we are loading up */
    private String insertColumnString;

    /** For outer joins, the name of the outer node id column */
    private String outerNodeIdColumn;

    /** If we generated a serial column, its ordinal position. */
    private short serialColumnPosition = -1;

    /** For CREATE TABLE AS support */
    private Map<Integer, String> createTablespaceMap;

    /**
     * Parameterless constructor is required for serialization
     */
    public StepDetail() {

    }

    /**
     * Constructor
     *
     * @param client
     *            user session
     */
    public StepDetail(XDBSessionContext client) {
        this.client = client;
        dropList = new ArrayList<String>();

        database = MetaData.getMetaData().getSysDatabase(client.getDBName());

        if (Props.XDB_USE_LOAD_FOR_STEP) {
            Collection<DBNode> nodeList = database.getDBNodeList();
            nodeInfos = new NodeDBConnectionInfo[nodeList.size()];
            int i = 0;
            for (DBNode aDBNode : nodeList) {
                nodeInfos[i++] = aDBNode.getNodeDBConnectionInfo();
            }
        }
    }

    /**
     * @param tableName
     *            table to add to this step's drop list
     */
    public void addDropTable(String tableName) {
        final String method = "addDropTable";
        logger.entering(method);

        try {
            dropList.add(tableName);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Gets the node id that the data resides on
     *
     * @param colValue
     *            column value
     * @return the nodeid the row resides on
     */
    public int getNodeId(String colValue) {
        final String method = "getNodeId";
        logger.entering(method);

        try {
            return partitionMap.getPartitions(colValue).iterator().next();
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * returns a copy of this StepDetail. Note, just copies references.
     *
     * @return copy
     */
    public StepDetail copy() {
        StepDetail newStepDetail = new StepDetail(client);

        newStepDetail.requestId = this.requestId;
        newStepDetail.stepNo = this.stepNo;
        newStepDetail.isProducer = this.isProducer;
        newStepDetail.isConsumer = this.isConsumer;
        newStepDetail.queryString = this.queryString;
        newStepDetail.nonProjectionSelectPart = this.nonProjectionSelectPart;
        newStepDetail.targetTable = this.targetTable;
        newStepDetail.targetSchema = this.targetSchema;
        newStepDetail.dropList = this.dropList;
        newStepDetail.destType = this.destType;
        newStepDetail.destNode = this.destNode;
        newStepDetail.combineOnCoordFirst = this.combineOnCoordFirst;
        newStepDetail.hashColumnPosition = this.hashColumnPosition;
        newStepDetail.groupHashColumns = this.groupHashColumns;
        newStepDetail.hashDataType = this.hashDataType;
        newStepDetail.partitionMap = this.partitionMap;
        newStepDetail.consumerNodeList = this.consumerNodeList;
        newStepDetail.database = this.database;
        newStepDetail.suppressSendingNodeId = this.suppressSendingNodeId;
        newStepDetail.nodeInfos = this.nodeInfos;
        newStepDetail.indexColumnString = this.indexColumnString;
        newStepDetail.insertColumnString = this.insertColumnString;
        newStepDetail.isOuterStep = this.isOuterStep;
        newStepDetail.outerNodeIdColumn = this.outerNodeIdColumn;
        newStepDetail.serialColumnPosition = this.serialColumnPosition;
        // it is ok to just refer to the same Map for our purposes here.
        newStepDetail.createTablespaceMap = this.createTablespaceMap;

        return newStepDetail;
    }

    // BUILD_CUT_START
    /**
     * Output Step info as String
     *
     * @return String representation of StepDetail
     */
    @Override
    public String toString() {
        StringBuffer sbStep = new StringBuffer(256);

        sbStep.append("\n requestId = ")
                .append(requestId)
                .append('\n')
                .append(" StepNo = ")
                .append(stepNo)
                .append('\n')
                .append(" isProducer = ")
                .append(isProducer)
                .append('\n');
        if (serialColumnPosition > 0) {
            sbStep.append(" serialColumnPosition = ")
                    .append(serialColumnPosition)
                    .append('\n');
        }
        sbStep.append(" isConsumer = ")
                .append(isConsumer)
                .append('\n')
                .append("queryString =\n")
                .append(SqlWordWrap.wrap(queryString, 120))
                .append('\n');
        if (destType != DEST_TYPE_COORD_FINAL) {
            sbStep.append(" targetTable = ")
            		.append(targetTable)
            		.append('\n');

        	sbStep.append(" targetSchema = ")
                	.append(targetSchema)
                	.append('\n');
        }
        sbStep.append(" DropList = ");

        for (String dropTable : dropList) {
            sbStep.append(dropTable + " ");
        }
        sbStep.append('\n').append(" destType = ");

        switch (destType) {
        case DEST_TYPE_BROADCAST:
            sbStep.append("DEST_TYPE_BROADCAST\n");
            break;

        case DEST_TYPE_COORD:
            sbStep.append("DEST_TYPE_COORD\n");
            break;

        case DEST_TYPE_ONE:
            sbStep.append("DEST_TYPE_ONE\n")
                    .append(" - destNode = ")
                    .append(destNode)
                    .append('\n');
            break;

        case DEST_TYPE_HASH:
            sbStep.append("DEST_TYPE_HASH\n")
                    .append(" hashColumnPosition = ")
                    .append(hashColumnPosition)
                    .append('\n')
                    .append(" groupHashColumns = ");
            if (groupHashColumns != null) {
                for (int i=0; i<groupHashColumns.length; i++) {
                    sbStep.append(groupHashColumns[i])
                            .append(' ');
                }
            }
            sbStep.append('\n');            
            break;

        case DEST_TYPE_BROADCAST_AND_COORD:
            sbStep.append("DEST_TYPE_BROADCAST_AND_COORD\n");
            break;

        case DEST_TYPE_COORD_FINAL:
            sbStep.append("DEST_TYPE_COORD_FINAL\n");
            break;

        case DEST_TYPE_NODEID:
            sbStep.append("DEST_TYPE_NODEID\n");
            break;

        default:
            sbStep.append("(none set) " + destType);
            sbStep.append('\n');
            break;
        }

        sbStep.append(" combineOnCoordFirst = ")
                .append(combineOnCoordFirst)
                .append('\n');

        sbStep.append(" consumerNodeList = ");

        if (this.consumerNodeList != null) {
            for (Integer nodeInt : consumerNodeList) {
                sbStep.append(" ").append(nodeInt);
            }
            sbStep.append('\n');
        }
        sbStep.append('\n');

        return sbStep.toString();
    }

    // BUILD_CUT_ALT
    // public String toString () { return null; }
    // BUILD_CUT_END

    /**
     * Converts the leaf passed in into a more detailed StepDetail object using
     * this instance.
     *
     * @param isCorrelatedHashable
     * @param aLeaf -
     *            current leaf
     * @param nextLeaf -
     *            the leaf after that (we need to peak ahead)
     * @param nextNextLeaf -
     *            the leaf after nextLeaf (detects extra outer step)
     * @param correlatedDepth -
     *            current depth level for nest correlated subqueries
     */
    public void convertLeafToStep(Leaf aLeaf, Leaf nextLeaf, Leaf nextNextLeaf,
            int correlatedDepth, boolean isCorrelatedHashable) {
        final String method = "convertLeafToStep";
        logger.entering(method);

        try {

            this.stepNo = aLeaf.getLeafStepNo();
            this.isLookupStep = aLeaf.isLookupStep();
            this.outerNodeIdColumn = aLeaf.getOuterNodeIdColumn();
            this.serialColumnPosition = aLeaf.getSerialColumnPosition();

            // If we aren't combining, nor the last step
            // then the nodes are all consumers
            if (nextLeaf == null) {
                this.isConsumer = false;
            } else {
                if (!(nextLeaf.isCombineOnMain() || aLeaf.isCombinerStep() || aLeaf.isExtraStep())) {
                    this.isConsumer = true;
                } else {
                    this.isConsumer = false;

                    // see if we are dealing with aggregates
                        // if we have a group by clause, we will aggregate
                        // again down at the nodes
                    if (nextLeaf.isCombinerStep()
                            && aLeaf.groupByColumns.size() - aLeaf.getAddedGroupCount() > 0) {
                            this.isConsumer = true;
                    }
                }
            }

            // See if the nodes are producers
            if (!(aLeaf.isCombineOnMain() || aLeaf.isCombinerStep() || aLeaf.isExtraStep())) {
                this.isProducer = true;
            } else {
                this.isProducer = false;

                if (aLeaf.isCombinerStep() && aLeaf.groupByColumns.size() > 0) {
                    this.isProducer = true;
                }
            }

            this.targetTable = aLeaf.getTargetTableName();
            this.targetSchema = aLeaf.getTempTargetCreateStmt();

            // If we are a producer, determine destination info.
            this.destType = -1;

            // update StepDetail's producer info appropriately.
            if (this.isProducer) {
                updateProducerStepInfo(aLeaf, nextLeaf, nextNextLeaf, this);
            }

            if (correlatedDepth > 0 && !isCorrelatedHashable) {
                this.addNodeIdToStep(correlatedDepth, nextLeaf != null);
            }
            return;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * This is similar to convertLeafToStep, but creates the step information
     * for the Coordinator, which may need to be different than what happens at
     * the nodes.
     *
     * Note that if the coordinator is neither a consumer nor producer, it
     * returns null
     *
     * @param correlatedDepth
     * @param isCorrelatedHashable
     * @param aLeaf current leaf
     * @param nextLeaf the leaf after that * we need to peak ahead
     * @param nextNextLeaf the leaf after nextLeaf * detects extra outer step
     */
    public void convertLeafToStepCoord(Leaf aLeaf, Leaf nextLeaf,
            Leaf nextNextLeaf, int correlatedDepth, boolean isCorrelatedHashable) {
        final String method = "convertLeafToStepCoord";
        logger.entering(method);

        try {
            this.stepNo = aLeaf.getLeafStepNo();
            this.outerNodeIdColumn = aLeaf.getOuterNodeIdColumn();

            // If we are on the last step, results will be combined at the
            // coordinator
            if (nextLeaf == null) {
                this.isConsumer = true;
            } else {
                if (!(nextLeaf.isCombineOnMain() || aLeaf.isCombinerStep() || aLeaf.isExtraStep())) {
                    if (nextLeaf.subplan != null)
                    {
                        // force it to consume for correlated
                        this.isConsumer = true;
                    } else {
                        this.isConsumer = false;
                    }
                } else {
                    this.isConsumer = true;

                    // check for aggregation, if we have a group by clause
                    if (nextLeaf.isCombinerStep()) {
                        if (aLeaf.groupByColumns.size() - aLeaf.getAddedGroupCount() > 0) {
                            this.isConsumer = false;
                        }
                    }

                }

                // but, do set if we are dealing with a special case of outer
                if (nextNextLeaf != null && nextNextLeaf.isExtraStep()) {
                    this.isConsumer = true;
                }
            }

            // See if we nodes are producers
            if (aLeaf.isCombineOnMain() || aLeaf.isCombinerStep()
                    || aLeaf.isExtraStep()) {
                this.isProducer = true;

                if (aLeaf.isCombinerStep()
                        && aLeaf.groupByColumns.size() - aLeaf.getAddedGroupCount() > 0) {
                    this.isProducer = false;
                }
            } else {
                this.isProducer = false;
            }

            // If we are neither a producer nor consumer, return null
            if (!this.isConsumer && !this.isProducer) {
                return;
            }

            this.targetTable = aLeaf.getTargetTableName();
            this.targetSchema = aLeaf.getTempTargetCreateStmt();

            this.destType = -1;

            // update StepDetail's producer info appropriately.
            if (this.isProducer) {
                updateProducerStepInfo(aLeaf, nextLeaf, nextNextLeaf, this);
            }

            if (correlatedDepth > 0 && !isCorrelatedHashable) {
                this.addNodeIdToStep(correlatedDepth, nextLeaf != null);
            }

            return;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Set elements in aStepDetail for producing the data, like where to send
     * the results, the query to use, etc.
     *
     * @param aStepDetail
     * @param aLeaf current leaf
     * @param nextLeaf the leaf after that - we need to peak ahead
     * @param nextNextLeaf the leaf after nextLeaf - detects extra outer step
     */
    private void updateProducerStepInfo(Leaf aLeaf, Leaf nextLeaf,
            Leaf nextNextLeaf, StepDetail aStepDetail) {
        final String method = "updateProducerStepInfo";
        logger.entering(method);

        try {
            SysTable sysTab = null;

            aStepDetail.queryString = aLeaf.getSelect();
            aStepDetail.nonProjectionSelectPart = aLeaf.getNonProjectionSelectPart();
            aStepDetail.dropList = new ArrayList<String>(aLeaf.tempTableDropList);

            if (nextLeaf == null) {
                aStepDetail.setDestTypeCoordinator();
            } else {
                // check for correlated subquery; then it must go to both
                if (nextLeaf.subplan != null) {
                    aStepDetail.setDestTypeCoordinator();
                } else if (nextNextLeaf != null && nextNextLeaf.isExtraStep()) {
                    // Special outer case
                    aStepDetail.setDestTypeBroadcastAndCoordinator();
                } else if (nextLeaf.isCombinerStep()) {
                    if (aLeaf.groupByColumns.size() - aLeaf.getAddedGroupCount() > 0) {
                        // If we are dealing with aggregates with a group by,
                        // create our own Hash. We flagged this special case above.

                        PartitionMap partitionMap = new HashPartitionMap();
                        Collection<DBNode> dbNodeList = database.getDBNodeList();
                        ArrayList<Integer> nodeIdList = new ArrayList<Integer>(
                                dbNodeList.size());
                        for (DBNode node : dbNodeList) {
                            nodeIdList.add(node.getNodeId());
                        }
                        partitionMap.generateDistribution(nodeIdList);
                        Leaf.Projection groupColumn = aLeaf.groupByColumns.get(0);
                        if (groupColumn.groupByPosition == 0) {
                        	int i = 1;
                        	for (Leaf.Projection column : aLeaf.selectColumns) {
                        		if (column.projectString.equalsIgnoreCase(groupColumn.projectString)) {
                                    aStepDetail.setDestTypeHash(i, partitionMap);
                                    ExpressionType anET = new ExpressionType();
                                    anET.setExpressionType(ExpressionType.VARCHAR_TYPE, 0, 0, 0);                            
                                    aStepDetail.setHashDataType(anET);
                        		}
                        		i++;
                        	}
                        } else {
                            if (aLeaf.groupByColumns.size() == 1) {
                                aStepDetail.setDestTypeHash(
                                        groupColumn.groupByPosition, partitionMap);                                
                                ExpressionType anET = new ExpressionType();
                                anET.setExpressionType(ExpressionType.VARCHAR_TYPE, 0, 0, 0);                            
                                aStepDetail.setHashDataType(anET);
                            } else {                       
                                int[] groupHashPosList = new int[aLeaf.groupByColumns.size()];
                                int i = 0;
                                for (Leaf.Projection groupExpr : aLeaf.groupByColumns) {
                                    groupHashPosList[i++] = groupExpr.groupByPosition;
                                    // only use the max config value amount 
                                    // # of expressions
                                    if (Props.XDB_MAX_GROUP_HASH_COUNT > 0 
                                            && i >= Props.XDB_MAX_GROUP_HASH_COUNT) {
                                        break;
                                    }
                                }                            
                                aStepDetail.setDestTypeHashList(
                                        groupHashPosList, partitionMap);
                                ExpressionType anET = new ExpressionType();
                                anET.setExpressionType(ExpressionType.VARCHAR_TYPE, 0, 0, 0);                            
                                aStepDetail.setHashDataType(anET);
                            }
                        }
                    } else {
                        aStepDetail.setDestTypeCoordinator();
                    }
                } else if (nextLeaf.isCombineOnMain() || aLeaf.isCombinerStep()
                        || aLeaf.isExtraStep()) {
                    aStepDetail.setDestTypeCoordinator();
                } else {
                    // If not yet set, check partitioning info
                    // Determine which table to base partitioning decsions on

                    // See if we already set this in QueryPlan.
                    if (nextLeaf.getHashColumn() != null) {
                        sysTab = SysTable.getPartitionTable(
                                nextLeaf.getHashTableName(), database);
                        int i = 0;
                        for (Leaf.Projection projection : aLeaf.selectColumns) {
                        	i++;
                        	if (nextLeaf.getHashColumn().equals(Leaf.normalizeHashColumnName(projection.projectString))) {
                                aStepDetail.setDestTypeHash(i, sysTab.getPartitionMap());
                                aStepDetail.setHashDataType(new ExpressionType(sysTab.getPartitionedColumn()));
                                return;
                        	}
                        }
                    }

                    // TODO: There is currently a problem when we
                    // created a new relation with a subquery in VIEWs.
                    // To work around that, we catch an Exception that will
                    // be thrown by SysTable.getPartitionTable, and just broadcast.
                    // This needs to be improved.
                    try {
                        sysTab = SysTable.getPartitionTable(nextLeaf.getTableName(),
                                database);
                    } catch (Exception e) {
                        aStepDetail.setDestTypeBroadcast();
                        return;
                    }
                    // handle case when data is on one node
                    if (sysTab.getPartitionScheme() == SysTable.PTYPE_ONE) {
                        aStepDetail.setDestTypeOne(sysTab.getPartitionMap().joinPartitions().iterator().next());
                    }
                    // If we are dealing with a lookup, have run on coordinating
                    // node
                    else if (sysTab.getPartitionScheme() == SysTable.PTYPE_LOOKUP) {
                        aStepDetail.setDestTypeOne(database.getCoordinatorNodeID());
                    }
                    // See if we assigned hash information in QueryPlan for
                    // correlated.
                    else if (nextLeaf.isSingleStepCorrelated()
                            && nextLeaf.getSingleCorrelatedHash() != null) {
                        // Handle case where next step is simple correlated join
                    	int i = 1;
                    	for (Leaf.Projection column : aLeaf.selectColumns) {
                    		if (nextLeaf.getSingleCorrelatedHash().equals(Leaf.normalizeHashColumnName(column.projectString))) {
                                aStepDetail.setDestTypeHash(i, sysTab.getPartitionMap());
                                aStepDetail.setHashDataType(new ExpressionType(sysTab.getPartitionedColumn()));
                                break;
                    		}
                    		i++;
                    	}
                    } else {
                        // We do not have a join on the partitioned info,
                        // so we are going to have to do a broadcast.
                        logger.debug("DEST_TYPE = broadcast");
                        aStepDetail.setDestTypeBroadcast();
                    }
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * This is based on convertLeafToStep, but is for handling the case when we
     * create an extra step for correlated subqueries.
     *
     * @param aLeaf
     *            the current step to process
     */
    public void convertLeafToStepCorrDown1(Leaf aLeaf) {
        final String method = "convertLeafToStepCorrDown";
        logger.entering(method);

        try {
            //work-around...
            this.stepNo = 1000 + aLeaf.getLeafStepNo();

            this.isConsumer = true;
            this.isProducer = true;
            this.outerNodeIdColumn = aLeaf.getOuterNodeIdColumn();
            this.queryString = aLeaf.getCorrelatedSelectString();

            // We are adding an extra step where we want to resend and hash
            // just the column values we need to try and only "send down"
            // distinct
            // values.
            // At this point, the correlatedJoinTableName has an "A" appended at
            // the end of it. We need to create a similar table for this
            // intermediate step, so we append another "A"...

            this.targetTable = aLeaf.correlatedJoinTableName + "A";
            this.targetSchema = aLeaf.getCreateCorrelatedTableString().replaceFirst(
                    aLeaf.correlatedJoinTableName, this.targetTable);

            // change this to hash

            // Generate hash table partition map for results
            PartitionMap partitionMap = new HashPartitionMap();
            Collection<DBNode> dbNodeList = database.getDBNodeList();
            ArrayList<Integer> nodeIdList = new ArrayList<Integer>(dbNodeList.size());
            for (DBNode node : dbNodeList) {
                nodeIdList.add(node.getNodeId());
            }
            partitionMap.generateDistribution(nodeIdList);
            // Hash on first column
            setDestTypeHash(1, partitionMap);
            setHashDataType(aLeaf.correlatedChildHashableExpression.getExprDataType());
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * This is based on convertLeafToStep, but is for handling the case when we
     * create an extra step for correlated subqueries.
     *
     * @param aLeaf
     *            the current step to process
     */
    public void convertLeafToStepCorrDown2(Leaf aLeaf) {
        final String method = "convertLeafToStepCorrDown";
        logger.entering(method);

        try {
            //work-around
            this.stepNo = 1000 + aLeaf.getLeafStepNo();

            this.isConsumer = true;
            this.isProducer = true;
            this.outerNodeIdColumn = aLeaf.getOuterNodeIdColumn();

            this.targetTable = aLeaf.correlatedJoinTableName;
            this.targetSchema = aLeaf.getCreateCorrelatedTableString();

            this.queryString = "SELECT DISTINCT * FROM "
                    + IdentifierHandler.quote(targetTable + "A");

            // Drop "AA" table from previous step.
            // (We create "A" table on current step.)
            this.dropList.add(this.targetTable + "A");

            // set later
            setDestTypeBroadcast();
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Special handling for correlated queries. We want to change a step so that
     * it distributes parent table across all nodes to be joined later. We also
     * can then use it to somewhat efficiently get distinct values for sending
     * down to correlated subquery.
     *
     * @param currentLeaf
     *            the current step to process
     * @param previousLeaf
     *            the current step to process
     */
    public void convertLeafToStepCorrelatedDown(Leaf currentLeaf,
            Leaf previousLeaf) {
        final String method = "convertLeafToStepCorrelatedDown";
        logger.entering(method);

        try {
            this.stepNo = currentLeaf.getLeafStepNo();

            this.isConsumer = true;
            this.isProducer = true;
            this.outerNodeIdColumn = currentLeaf.getOuterNodeIdColumn();

            this.targetTable = previousLeaf.getTargetTableName();
            this.targetSchema = previousLeaf.getTempTargetCreateStmt();

            String hashColumn;
            if (currentLeaf.correlatedParentHashableExpression.getAlias() != null
                    && currentLeaf.correlatedParentHashableExpression.getAlias().length() > 0) {
                hashColumn = currentLeaf.correlatedParentHashableExpression.getAlias();
            } else {
                hashColumn = currentLeaf.correlatedParentHashableExpression.getExprString();
            }
            hashColumn = Leaf.normalizeHashColumnName(hashColumn);
            
            setHashDataType(currentLeaf.correlatedParentHashableExpression.getExprDataType());
            PartitionMap partitionMap = new HashPartitionMap();
            Collection<DBNode> dbNodeList = database.getDBNodeList();
            ArrayList<Integer> nodeIdList = new ArrayList<Integer>(dbNodeList.size());
            for (DBNode node : dbNodeList) {
                nodeIdList.add(node.getNodeId());
            }
            partitionMap.generateDistribution(nodeIdList);
            
            for (int i = 0; i < previousLeaf.selectColumns.size(); i++) {
            	if (hashColumn.equals(Leaf.normalizeHashColumnName(previousLeaf.selectColumns.get(i).projectString))) {
            		setDestTypeHash(i + 1, partitionMap);
            	}
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * For correlated subqueries, we add in XNODEID so we can later properly
     * join the parent with the child subquery.
     *
     * @param correlatedDepth
     * @param addToTargetSchema
     */
    protected void addNodeIdToStep(int correlatedDepth,
            boolean addToTargetSchema) {
        if (queryString != null) {
            int fromPos = queryString.indexOf(" FROM ");

            // TODO: loop and add, depending on depth for nested
            // correlated subqueries.
            this.queryString = this.queryString.substring(0, fromPos)
                    + ", XNODEID" + correlatedDepth
                    + queryString.substring(fromPos);
        }

        if (targetSchema != null && addToTargetSchema) {
            int fromPos = targetSchema.lastIndexOf(")");
            this.targetSchema = this.targetSchema.substring(0, fromPos)
                    + ", XNODEID" + correlatedDepth + " INTEGER)";
        } else {
            suppressSendingNodeId = true;
        }
    }

    /**
     * This is based on convertLeafToStep, but is for handling the case when we
     * create an extra step for correlated subqueries.
     *
     * @param correlatedDepth
     * @param aLeaf the current step to process
     */
    public void convertLeafToStepCorrDownNodeId(Leaf aLeaf, int correlatedDepth) {
        final String method = "convertLeafToStepCorrDown";
        logger.entering(method);

        try {
            // work-around
            this.stepNo = 1000 + aLeaf.getLeafStepNo();

            this.isConsumer = true;
            this.isProducer = true;

            this.queryString = aLeaf.getCorrelatedSelectString();
            this.targetTable = aLeaf.correlatedJoinTableName;
            this.targetSchema = aLeaf.getCreateCorrelatedTableString();

            // We already distributed for "parent" part of join later,
            // now we broadcast down to send down values
            setDestTypeBroadcast();
            this.addNodeIdToStep(correlatedDepth + 1, true);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Special handling for correlated queries. We use this variant when there
     * is no easy correlated join condition, when things are more complicated.
     *
     * @param correlatedDepth
     * @param currentLeaf the current step to process
     * @param previousLeaf the current step to process
     */
    public void convertLeafToStepCorrelatedNodeIdDown(Leaf currentLeaf,
            Leaf previousLeaf, int correlatedDepth) {
        final String method = "convertLeafToStepCorrelatedNodeIdDown";
        logger.entering(method);

        try {
            this.stepNo = currentLeaf.getLeafStepNo();

            this.isConsumer = true;
            this.isProducer = true;

            this.targetTable = previousLeaf.getTargetTableName();
            this.targetSchema = previousLeaf.getTempTargetCreateStmt();

            setDestTypeNodeID();

            // We need to insert in XNODEID
            addNodeIdToStep(correlatedDepth + 1, true);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param indexColumnString
     */

    public void setIndexColumnString(String indexColumnString) {
        this.indexColumnString = indexColumnString;
    }

    /**
     * Returns a temporary index definition that we will create
     * for correlated subqueries, which helps performance on PostgreSQL
     *
     * @return
     */

    public String getCorrelatedIndex() {
        String outString = null;

        if (indexColumnString != null) {
            outString = "CREATE INDEX IDX_TMP_" + this.targetTable + " ON "
                    + IdentifierHandler.quote(this.targetTable)
                    + " (" + indexColumnString + ")";
        }

        return outString;
    }

    /**
     * Returns position of serial id
     *
     * @return
     */
    public String getInsertColumnString() {
        return insertColumnString;
    }

    /**
     * Set whether or not this is an outer step
     *
     * @param value
     */

    public void setIsOuterStep(boolean value) {
        isOuterStep = value;
    }

    /**
     *
     * @return whether or not this is an outer step
     */

    public boolean getIsOuterStep() {
        return isOuterStep;
    }

    /**
     *
     * @return
     */

    public String getOuterNodeIdColumn() {
        return outerNodeIdColumn;
    }

    /**
     *e
     * @return
     */

    public short getSerialColumnPosition() {
        return serialColumnPosition;
    }

    /**
     *
     * @return
     */
    public ExpressionType getHashDataType() {
        return hashDataType;
    }

    /**
     * Set the hash data type.
     *
     * @param dataType the hash data type
     */
    public void setHashDataType(ExpressionType dataType) {
        hashDataType = dataType;
    }

    /**
     * Set the tablespace to use when creating the table for the specified node
     *
     * @param nodeId target node id
     * @param tablespaceName the tablespace to use on the node
     */
    public void addCreateTablespace(int nodeId, String tablespaceName)
    {
        if (createTablespaceMap == null) {
            createTablespaceMap = new HashMap<Integer, String> ();
        }
        createTablespaceMap.put(Integer.valueOf(nodeId), tablespaceName);
    }

    /**
     * @return if the create table command uses tablespaces
     */
    public boolean usesTablespace () {
        return createTablespaceMap != null;
    }

    /**
     * @return the tablespace clause to use for the specified node
     */
    public String getTablespaceClause(int nodeId) {
        String tablespaceClause = null;
        if (createTablespaceMap != null) {
            tablespaceClause = createTablespaceMap.get(Integer.valueOf(nodeId));
        }
        return tablespaceClause;
    }

    public void setDestTypeNodeID() {
        destType = StepDetail.DEST_TYPE_NODEID;
        partitionMap = null;
        hashColumnPosition = 0;
        destNode = 0;
    }

    public void setDestTypeOne(int destNode) {
        destType = StepDetail.DEST_TYPE_ONE;
        partitionMap = new ReplicatedPartitionMap();
        partitionMap.generateDistribution(Collections.singleton(destNode));
        hashColumnPosition = 0;
        this.destNode = destNode;
    }

    public void setDestTypeCoordinatorFinal() {
        destType = StepDetail.DEST_TYPE_COORD_FINAL;
        destNode = database.getCoordinatorNodeID();
    }

    public void setDestTypeCoordinator() {
        destType = StepDetail.DEST_TYPE_COORD;
        partitionMap = null;
        hashColumnPosition = 0;
        destNode = database.getCoordinatorNodeID();
    }

    public void setDestTypeBroadcastAndCoordinator() {
        setDestTypeBroadcast(true);
    }
    
    public void setDestTypeBroadcast() {
        setDestTypeBroadcast(false);
    }
    
    private void setDestTypeBroadcast(boolean andCoordinator) {
        destType = andCoordinator ? StepDetail.DEST_TYPE_BROADCAST_AND_COORD 
                        : StepDetail.DEST_TYPE_BROADCAST;
        partitionMap = null;
        hashColumnPosition = 0;
        destNode = andCoordinator ? database.getCoordinatorNodeID() : 0;
    }

    public void setDestTypeHash(int hashColumnPosition, PartitionMap partMap) {
        destType = StepDetail.DEST_TYPE_HASH;
        partitionMap = partMap;
        this.hashColumnPosition = hashColumnPosition;
        destNode = 0;
    }

    public void setDestTypeHashList(int[] hashColumns, PartitionMap partMap) {
        destType = StepDetail.DEST_TYPE_HASH;
        partitionMap = partMap;
        hashColumnPosition = -1;
        groupHashColumns = hashColumns;
        destNode = 0;
    }    
    
    public int getDestType() {
        return destType;
    }

    public int getHashColumnPosition() {
        return hashColumnPosition;
    }

    public int getDestNode() {
        return destNode;
    }

    public PartitionMap getPartitionMap() {
        return partitionMap;
    }

    public void setPartitionMap(PartitionMap partMap) {
        partitionMap = partMap;
    }
    
    public int[] getGroupHashColumns() {
        return groupHashColumns;
    }
}
