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
/* * ExecutionStep.java
 *
 *
 */
package org.postgresql.stado.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.partitions.HashPartitionMap;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.ExpressionType;


/**
 * ExecutionStep is used for the steps that are executed, the details of
 * which appear in StepDetail.java.
 *
 * There is some special handling in here for correlated subqueries worthy of
 * extra discussion.
 *
 * Changes were made to allow for more parallelism.
 *
 * Basically, we want to send down all unique correlated values from the parent
 * down to the child query for execution. That happens as part of
 * correlatedSendDownStep, which will be non-null in that case. Note that it is
 * set in the step immediately preceeding the correlated join. On that next
 * step, correlatedSubPlan will be non-null, and will be executed first. It will
 * already have the needed values sent down due to correlatedSendDownStep.
 *
 * Note that in convertFromLeaf we do some gyrations that manipulate that last
 * step of the subplan. Basically, we force it to send its results to all valid
 * nodes in the database, distributing tuples based on hashing a child member of
 * a correlated join expression. Each node will now have about 1/nth of the data
 * across n nodes.
 *
 * The changes done here made post-QueryPlan processing. This means that we do
 * several (ugly) transformations in convertFromLeaf to support this.
 * Unfortunately, it does make the code appear confusing. It may have been
 * better to actually make most of these changes in QueryPlan, but the code
 * still mayhave been difficult to follow since we will have made changes in a
 * couple of places to support this.
 *
 * Contains all needed info for handling a step of an ExecutionPlan
 *
 * @author Administrator
 */
public class ExecutionStep {
    private static final XLogger logger = XLogger.getLogger(ExecutionStep.class);

    /** session information */
    private XDBSessionContext client = null;

    /** detailed step information for the nodes */
    public StepDetail aStepDetail;

    /** detailed step information for the coordinator */
    public StepDetail coordStepDetail;

    /** describes what each node does during a step*/
    public HashMap<Integer, NodeUsage> nodeUsageTable;

    /** number of nodes that are producers in this step */
    public int producerCount;

    /** number of nodes taht are consumers in this step */
    public int consumerCount;

    /** whether or not this is a generated extra step */
    public boolean isExtraStep;

    /** whether or not this is the final step of the plan */
    public boolean isFinalStep = false;

    /** destination node list */
    public List<DBNode> destNodeList;

    /** correlated subplans to execute in this step, if any */
    public ExecutionPlan correlatedSubPlan = null;

    /** uncorrelated subplans to execute in this step, if any */
    public List<ExecutionPlan> uncorrelatedSubPlanList = null;

    /** if non null, extra step to execute to send down for a correlated
     * subquery at the end of this step */
    public ExecutionStep correlatedSendDownStep = null;

    /** if non null, second extra step to execute to send down for a correlated
     * subquery at the end of this step */
    public ExecutionStep correlatedSendDownStep2 = null;

    /** for outer joins, we may have extra steps to execute to resolve
     back to original outer rows. */
    public ExecutionPlan outerSubPlan;

    /** The plan that this step belongs to. */
    private ExecutionPlan parentPlan;

    /** original Leaf that was used to convert this step */
    private Leaf origLeaf;

    /** */
    public boolean isLookupStep = false;

    /**
     * Creates a new instance of ExecutionStep
     *
     * @param parentPlan
     * @param client
     */
    protected ExecutionStep(ExecutionPlan parentPlan, XDBSessionContext client) {
        this.client = client;
        nodeUsageTable = new HashMap<Integer, NodeUsage>();
        this.parentPlan = parentPlan;
    }

    /**
     * Converts Leaf information. Note that we need to peek ahead a couple of
     * steps to set destination information correctly.
     *
     * @param currentLeaf
     * @param nextLeaf
     * @param nextNextLeaf
     * @param previousLeaf
     * @param parentNodeUsageTable
     * @param parentTargetNodeList
     * @param isUnionSubquery
     * @param planType
     */
    protected void convertFromLeaf(Leaf currentLeaf, Leaf nextLeaf,
            Leaf nextNextLeaf, Leaf previousLeaf,
            HashMap<Integer, NodeUsage> parentNodeUsageTable,
            List<DBNode> parentTargetNodeList, boolean isUnionSubquery,
            int planType) {
        final String method = "convertFromLeaf";

        logger.entering(method);

        try {
            origLeaf = currentLeaf;

            // see if we have a correlated subquery
            if (currentLeaf.subplan != null) {
                correlatedSubPlan = new ExecutionPlan(this.parentPlan,
                        currentLeaf.subplan, null, null, false, client,
                        parentPlan.correlatedDepth + 1, currentLeaf.isCorrelatedHashable());
            }

            isExtraStep = currentLeaf.isExtraStep();

            // Convert Leaf into StepDetail
            // This assumes the general case- if going to the nodes, nodes
            // are both consumers and producers. We create variations later.
            aStepDetail = new StepDetail(client);
            aStepDetail.convertLeafToStep(currentLeaf, nextLeaf, nextNextLeaf,
                    parentPlan.correlatedDepth, parentPlan.isCorrelatedHashable);

            isLookupStep = aStepDetail.isLookupStep;

            coordStepDetail = new StepDetail(client);
            coordStepDetail.convertLeafToStepCoord(currentLeaf, nextLeaf,
                    nextNextLeaf, parentPlan.correlatedDepth,
                    parentPlan.isCorrelatedHashable);

            // If coodinator is neither a producer nor consumer, set to null
            // so we don't bother using it later
            if (!coordStepDetail.isConsumer && !coordStepDetail.isProducer) {
                coordStepDetail = null;
            }

            producerCount = 0;

            // Determine actual node usage

            // Make sure we have at least one producer
            if (aStepDetail.isProducer) {
                producerCount = currentLeaf.queryNodeList.size();

                for (DBNode dbNode : currentLeaf.queryNodeList) {
                    NodeUsage aNodeUsageElement = new NodeUsage(
                            dbNode.getNode().getNodeid(),
                            aStepDetail.isProducer, false);

                    nodeUsageTable.put(
                            new Integer(dbNode.getNode().getNodeid()),
                            aNodeUsageElement);
                }
            }

            consumerCount = 0;

            // Now, update and add for consumers
            // Make sure there is at least one consumer
            if (aStepDetail.isConsumer) {
                consumerCount = nextLeaf.queryNodeList.size();

                aStepDetail.consumerNodeList = new ArrayList<Integer>();

                for (DBNode dbNode : nextLeaf.queryNodeList) {
                    aStepDetail.consumerNodeList.add(dbNode.getNode().getNodeid());

                    // See if it was already put in usage table
                    NodeUsage aNodeUsageElement = nodeUsageTable.get(new Integer(
                            dbNode.getNode().getNodeid()));

                    if (aNodeUsageElement != null) {
                        aNodeUsageElement.isConsumer = true;
                    } else {
                        aNodeUsageElement = new NodeUsage(
                                dbNode.getNode().getNodeid(), false,
                                aStepDetail.isConsumer);

                        nodeUsageTable.put(new Integer(
                                dbNode.getNode().getNodeid()),
                                aNodeUsageElement);
                    }
                }

                if (coordStepDetail != null) {
                    coordStepDetail.consumerNodeList = aStepDetail.consumerNodeList;
                }
            }

            if (nextLeaf != null) {
                destNodeList = nextLeaf.queryNodeList;
            } else {
                destNodeList = null;
            }

            // process any uncorrelated subqueries
            for (QueryPlan uncorSubPlan : currentLeaf.uncorrelatedSubplanList) {

                ExecutionPlan uncorExecPlan = new ExecutionPlan(
                        this.parentPlan, uncorSubPlan, this.nodeUsageTable,
                        currentLeaf.queryNodeList, false, client);

                // Need to set destination for this, though
                if (this.uncorrelatedSubPlanList == null) {
                    this.uncorrelatedSubPlanList = new ArrayList<ExecutionPlan>();
                }

                this.uncorrelatedSubPlanList.add(uncorExecPlan);
            }

            // We now look for special cases, and perhaps change some of
            // the values we already set.
            // Handle the case where we are processing an uncorrelated subquery
            // here on the last step, that needs to join later with its parent.
            if (parentTargetNodeList != null && nextLeaf == null
                    && !isUnionSubquery) {
                aStepDetail.isConsumer = true;

                if (currentLeaf.finalInClausePartitioningTable != null) {
                    SysDatabase database = MetaData.getMetaData().getSysDatabase(
                            client.getDBName());
                    SysTable sysTab = database.getSysTable(currentLeaf.finalInClausePartitioningTable);
                    ExpressionType dataType = new ExpressionType(
                            sysTab.getPartitionedColumn());
                    aStepDetail.setDestTypeHash(1, sysTab.getPartitionMap());
                    aStepDetail.setHashDataType(dataType);

                    if (!coordStepDetail.isProducer) {
                        coordStepDetail = null; // .isConsumer = false;
                    } else {
                        coordStepDetail.isConsumer = false;
                        coordStepDetail.setDestTypeHash(1, sysTab.getPartitionMap());
                        coordStepDetail.setHashDataType(dataType);
                    }
                } else {
                    aStepDetail.setDestTypeBroadcast();

                    if (!coordStepDetail.isProducer) {
                        coordStepDetail = null; // .isConsumer = false;
                    } else {
                        coordStepDetail.isConsumer = false;
                        coordStepDetail.setDestTypeBroadcast();
                    }
                }

                destNodeList = parentTargetNodeList;
                consumerCount = destNodeList.size();

                // We also need to add information from the parent's node usage
                // table.
                // All nodes that are producers in the parent must become this
                // step's consumers.
                aStepDetail.consumerNodeList = new ArrayList<Integer>();

                for (NodeUsage parentUsage : parentNodeUsageTable.values()) {
                    aStepDetail.consumerNodeList.add(new Integer(
                            parentUsage.nodeId));

                    NodeUsage aNodeUsageElement = nodeUsageTable.get(new Integer(
                            parentUsage.nodeId));

                    if (aNodeUsageElement != null) {
                        aNodeUsageElement.isConsumer = true;
                    } else {
                        aNodeUsageElement = new NodeUsage(parentUsage.nodeId,
                                false, true);

                        nodeUsageTable.put(new Integer(parentUsage.nodeId),
                                aNodeUsageElement);
                    }
                }

                if (coordStepDetail != null) {
                    coordStepDetail.consumerNodeList = aStepDetail.consumerNodeList;
                }
            }

            // Handle relation subqueries.
            // Change destination to HASH to all nodes, instead of coordinator
            if (planType == QueryPlan.RELATION && nextLeaf == null
                    && currentLeaf.subplan == null) {
                convertToNodeStep();
            }

            // Also, distribute union subquery results instead of sending to
            // coordinator.
            if (nextLeaf == null
                    && isUnionSubquery
                    && coordStepDetail != null
                    && parentPlan.parentExecutionPlan != null
                    && parentPlan.parentExecutionPlan.parentExecutionPlan != null) {
                convertToNodeStep();
            }

            /*
             * Do transformations for correlated join.
             */
            if (currentLeaf.subplan != null) // &&
            // currentLeaf.correlatedHashable)
            {
                prepareCorrelatedJoin(currentLeaf, nextLeaf);
            }

            // We need to have special handling for correlated subqueries.
            // We want to go ahead and make sure that the results are also
            // sent down to the nodes for the next step, to rejoin back with
            // parent.
            // Note that the subplan may have its own separate set of nodes
            if (nextLeaf != null && nextLeaf.subplan != null) {
                prepareCorrelatedParentAndDownTables(currentLeaf, nextLeaf);
            }

            // see if we are dealing with an outer join
            // we create a subplan for it
            if (currentLeaf.outerSubplan != null) {
                outerSubPlan = new ExecutionPlan(this.parentPlan,
                        currentLeaf.outerSubplan, this.nodeUsageTable,
                        currentLeaf.queryNodeList, false, client);

                // Now we make some modifications
                ExecutionStep firstStep = outerSubPlan.stepList.get(0);
                firstStep.aStepDetail.setDestTypeNodeID();
                firstStep.aStepDetail.setIsOuterStep(true);

                aStepDetail.nonProjectionSelectPart = this.aStepDetail.nonProjectionSelectPart.replace(
                        "&xnodecount&",
                        Integer.toString(firstStep.producerCount));
                //
                aStepDetail.queryString = this.aStepDetail.queryString.replace(
                        "&xnodecount&",
                        Integer.toString(firstStep.producerCount));
                aStepDetail.dropList.add(firstStep.aStepDetail.targetTable);
                // aStepDetail.dropList.add (currentLeaf.joinTableName);

                // don't do any XNODEID substitutions
                // this.aStepDetail.setIsOuterStep(true);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Change the step so that it uses the nodes instead of the coordinator
     */
    private void convertToNodeStep() {
        aStepDetail.isConsumer = true;

        StepDetail stepDetail;

        // update, depending on which one is the producer
        if (aStepDetail.isProducer) {
            stepDetail = aStepDetail;
        } else {
            stepDetail = coordStepDetail;
        }
        SysDatabase database = client.getSysDatabase();

        // distribute data evenly across all available nodes
        destNodeList = new ArrayList<DBNode>();
        stepDetail.consumerNodeList = new ArrayList<Integer>();
        PartitionMap partitionMap = new HashPartitionMap();
        Collection<DBNode> dbNodeList = database.getDBNodeList();
        ArrayList<Integer> nodeIdList = new ArrayList<Integer>(dbNodeList.size());
        for (DBNode aDBNode : dbNodeList) {
            destNodeList.add(aDBNode);
            stepDetail.consumerNodeList.add(aDBNode.getNodeId());
            nodeIdList.add(aDBNode.getNodeId());
        }
        partitionMap.generateDistribution(nodeIdList);
        stepDetail.setDestTypeHash(1, partitionMap);
        ExpressionType anET = new ExpressionType();
        anET.setExpressionType(ExpressionType.VARCHAR_TYPE, 0, 0, 0);                            
        aStepDetail.setHashDataType(anET);

        if (coordStepDetail != null && !coordStepDetail.isProducer) {
            coordStepDetail = null; // .isConsumer = false;
        }

        consumerCount = destNodeList.size();

        for (DBNode dbNode : database.getDBNodeList()) {

            // See if it was already put in usage table
            NodeUsage aNodeUsageElement = this.nodeUsageTable.get(new Integer(
                    dbNode.getNode().getNodeid()));

            if (aNodeUsageElement != null) {
                aNodeUsageElement.isConsumer = true;
            } else {
                aNodeUsageElement = new NodeUsage(dbNode.getNode().getNodeid(),
                        false, true); // aStepDetail.isConsumer

                this.nodeUsageTable.put(new Integer(
                        dbNode.getNode().getNodeid()), aNodeUsageElement);
            }
        }
    }

    /**
     * Change destination info, etc for correlated join.
     *
     * It would be better to change the code in StepDetail itself, but it is
     * more likely to lead to other things breaking, so we just do a bunch of
     * transformations here, copying over what we need from the coordinator step
     * to our step here.
     *
     * @param currentLeaf
     * @param nextLeaf
     *
     */
    private void prepareCorrelatedJoin(Leaf currentLeaf, Leaf nextLeaf) {
        final String method = "prepareCorrelatedJoin";
        logger.entering(method);

        try {
            SysDatabase database = client.getSysDatabase();
            int databaseNodeCount = database.getDBNodeList().size();

            ExecutionStep lastSubplanStep = correlatedSubPlan.stepList.get(
                    correlatedSubPlan.stepList.size()-1);

            // change the last step of the subplan to tell the nodes to be
            // consumers, too.
            if (lastSubplanStep.aStepDetail.isProducer) {
                // make sure it knows to be a consumer now, too.
                lastSubplanStep.aStepDetail.isConsumer = true;

                // ... and update usage table
                lastSubplanStep.consumerCount = databaseNodeCount;

                for (DBNode dbNode : database.getDBNodeList()) {

                    if (lastSubplanStep.aStepDetail.consumerNodeList == null) {
                        lastSubplanStep.aStepDetail.consumerNodeList = new ArrayList<Integer>();
                    }
                    lastSubplanStep.aStepDetail.consumerNodeList.add(new Integer(
                            dbNode.getNode().getNodeid()));
                    // See if it was already put in usage table
                    NodeUsage aNodeUsageElement = lastSubplanStep.nodeUsageTable.get(new Integer(
                            dbNode.getNode().getNodeid()));

                    if (aNodeUsageElement != null) {
                        aNodeUsageElement.isConsumer = true;
                    } else {
                        aNodeUsageElement = new NodeUsage(
                                dbNode.getNode().getNodeid(), false, true);

                        lastSubplanStep.nodeUsageTable.put(new Integer(
                                dbNode.getNode().getNodeid()),
                                aNodeUsageElement);
                    }
                }

                // we no longer want to send to coordinator
                lastSubplanStep.coordStepDetail = null;

                if (currentLeaf.correlatedChildHashableExpression == null) {
                    lastSubplanStep.aStepDetail.setDestTypeNodeID();
                } else {
                    String hashColumn;
                    if (currentLeaf.correlatedChildHashableExpression.getAlias().length() > 0) {
                        hashColumn = currentLeaf.correlatedChildHashableExpression.getAlias();
                    } else {
                        hashColumn = currentLeaf.correlatedChildHashableExpression.getExprString();
                    }
                    hashColumn = Leaf.normalizeHashColumnName(hashColumn);

                    lastSubplanStep.aStepDetail.setHashDataType(currentLeaf.correlatedChildHashableExpression.getExprDataType());
                    // distribute data evenly across all available nodes

                    PartitionMap partitionMap = new HashPartitionMap();
                    Collection<DBNode> dbNodeList = database.getDBNodeList();
                    ArrayList<Integer> nodeIdList = new ArrayList<Integer>(
                            dbNodeList.size());
                    for (DBNode node : dbNodeList) {
                        nodeIdList.add(node.getNodeId());
                    }
                    partitionMap.generateDistribution(nodeIdList);
                    Leaf lastSubplanLeaf = currentLeaf.subplan.getLastLeaf();
                    for (int i = 0; i < lastSubplanLeaf.selectColumns.size(); i++) {
                    	if (hashColumn.equals(Leaf.normalizeHashColumnName(lastSubplanLeaf.selectColumns.get(i).projectString))) {
                    		lastSubplanStep.aStepDetail.setDestTypeHash(i + 1, partitionMap);
                    	}
                    }
                }

                // Set index info
                String idxString = "";

                List<String> corrColumns = currentLeaf.getCorrelatedColumns();
                if (corrColumns != null) {
                    for (String strColumn : corrColumns) {
                        if (idxString.length() > 0) {
                            idxString += ",";
                        }
                        idxString += strColumn;
                    }
                }
                if (idxString.length() > 0) {
                    lastSubplanStep.aStepDetail.setIndexColumnString(idxString);
                }
            }

            // Change the current step so that it executes at the nodes
            // instead of the coordinator.
            // This is a bit unclean, but I would rather leave the basics of the
            // other code since it is working untouched, and then make these
            // changes here after the fact.
            this.aStepDetail.isProducer = true;
            this.aStepDetail.isConsumer = true;
            this.producerCount = databaseNodeCount; // MetaDataMaster.getDatabaseNodeList().size();

            // We always set dest type to HASH here.
            // Note that ExecutionPlan will later override and set to
            // DEST_TYPE_COORD_FINAL if it is really the last step
            this.coordStepDetail = null;

            PartitionMap partitionMap = new HashPartitionMap();
            Collection<DBNode> dbNodeList = database.getDBNodeList();
            ArrayList<Integer> nodeIdList = new ArrayList<Integer>(dbNodeList.size());
            for (DBNode node : dbNodeList) {
                nodeIdList.add(node.getNodeId());
            }
            partitionMap.generateDistribution(nodeIdList);
            aStepDetail.setDestTypeHash(1, partitionMap);
            ExpressionType exprType = new ExpressionType();
            exprType.setExpressionType(ExpressionType.VARCHAR_TYPE, -1, -1, -1);
            aStepDetail.setHashDataType(exprType);
            aStepDetail.consumerNodeList = new ArrayList<Integer>();

            for (DBNode dbNode : database.getDBNodeList()) {
                // See if it was already put in usage table
                Integer nodeInt = new Integer(dbNode.getNode().getNodeid());

                NodeUsage aNodeUsageElement = this.nodeUsageTable.get(new Integer(
                        dbNode.getNode().getNodeid()));

                if (aNodeUsageElement != null) {
                    aNodeUsageElement.isProducer = true;
                    aNodeUsageElement.isConsumer = true;
                } else {
                    aNodeUsageElement = new NodeUsage(
                            dbNode.getNode().getNodeid(), true, true);

                    this.nodeUsageTable.put(nodeInt, aNodeUsageElement);
                }

                if (!aStepDetail.consumerNodeList.contains(nodeInt)) {
                    aStepDetail.consumerNodeList.add(nodeInt);
                }
                this.consumerCount = aStepDetail.consumerNodeList.size();
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Prepares sending results of query down to a correlated query. Basically,
     * it makes sure that the results of the current step get distributed down
     * at the nodes, via a hash. That is to be the current "parent" query. Then,
     * two additional steps, correlatedSendDownStep, correlatedSendDownStep2 are
     * created that query. The first gets needed values from parent that are
     * needed in correlated join and hashes to distribute results and
     * consolidate unique values. The second step then gets all the unique
     * values to send down.
     *
     * @param currentLeaf
     * @param nextLeaf
     */
    private void prepareCorrelatedParentAndDownTables(Leaf currentLeaf,
            Leaf nextLeaf) {
        final String method = "prepareCorrelatedParentAndDownTables";
        logger.entering(method);

        try {
            // We need to change this Step so we don't send to coordinator,
            // and send to back around to nodes instead.
            // This may not be null if dealing with multiple correlated
            // statements one after the other
            if (coordStepDetail != null
                    && coordStepDetail.consumerNodeList != null) {
                aStepDetail.consumerNodeList = coordStepDetail.consumerNodeList;
            }
            coordStepDetail = null;

            if (nextLeaf.correlatedParentHashableExpression != null) {
                aStepDetail.convertLeafToStepCorrelatedDown(nextLeaf,
                        currentLeaf);
            } else {
                // We need to rely on the NodeId method of handling
                aStepDetail.convertLeafToStepCorrelatedNodeIdDown(nextLeaf,
                        currentLeaf, parentPlan.correlatedDepth);
            }

            // We need to update usage table

            SysDatabase database = MetaData.getMetaData().getSysDatabase(
                    client.getDBName());

            for (DBNode dbNode : database.getDBNodeList()) {
                if (aStepDetail.consumerNodeList == null) {
                    aStepDetail.consumerNodeList = new ArrayList<Integer>(
                            database.getDBNodeList().size());
                }
                if (!aStepDetail.consumerNodeList.contains(dbNode.getNodeId())) {
                    aStepDetail.consumerNodeList.add(new Integer(
                            dbNode.getNode().getNodeid()));
                }
                // See if it was already put in usage table
                NodeUsage aNodeUsageElement = nodeUsageTable.get(new Integer(
                        dbNode.getNode().getNodeid()));

                if (aNodeUsageElement != null) {
                    aNodeUsageElement.isConsumer = true;
                } else {
                    aNodeUsageElement = new NodeUsage(
                            dbNode.getNode().getNodeid(), false, true); // aStepDetail.isConsumer

                    nodeUsageTable.put(
                            new Integer(dbNode.getNode().getNodeid()),
                            aNodeUsageElement);
                }
            }

            // Special steps for getting needed joining values down to nodes
            // for correlated subquery
            if (nextLeaf.correlatedParentHashableExpression != null) {
                correlatedSendDownStep = new ExecutionStep(null, client);
                correlatedSendDownStep.convertExtraCorrelated(currentLeaf,
                        nextLeaf);

                correlatedSendDownStep2 = new ExecutionStep(null, client);
                correlatedSendDownStep2.convertExtraCorrelated2(currentLeaf,
                        nextLeaf);
            } else {
                correlatedSendDownStep = new ExecutionStep(null, client);
                correlatedSendDownStep.convertExtraCorrelatedNodeId(
                        currentLeaf, nextLeaf, parentPlan.correlatedDepth);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * For sending results up to now from parent query to correlated child
     * subquery.
     *
     * @param currentLeaf
     * @param nextLeaf
     */
    private void convertExtraCorrelated(Leaf currentLeaf, Leaf nextLeaf) {
        final String method = "convertExtraCorrelated";
        logger.entering(method);

        try {
            coordStepDetail = null;

            aStepDetail = new StepDetail(client);
            aStepDetail.convertLeafToStepCorrDown1(nextLeaf);

            isExtraStep = currentLeaf.isExtraStep();

            Leaf corrLeaf = nextLeaf.subplan.getFirstLeaf();

            SysDatabase database = MetaData.getMetaData().getSysDatabase(
                    client.getDBName());
            consumerCount = database.getDBNodeList().size();

            producerCount = corrLeaf.queryNodeList.size();

            aStepDetail.consumerNodeList = new ArrayList<Integer>();

            for (DBNode aDBNode : database.getDBNodeList()) {
                aStepDetail.consumerNodeList.add(new Integer(
                        aDBNode.getNodeId()));

                // See if it was already put in usage table
                NodeUsage aNodeUsageElement = nodeUsageTable.get(new Integer(
                        aDBNode.getNodeId()));

                if (aNodeUsageElement != null) {
                    aNodeUsageElement.isConsumer = true;
                } else {
                    aNodeUsageElement = new NodeUsage(aDBNode.getNodeId(),
                            true, true);

                    nodeUsageTable.put(new Integer(aDBNode.getNodeId()),
                            aNodeUsageElement);
                }
            }

            destNodeList = corrLeaf.queryNodeList;
			// correlatedSendDownStepDetail.consumerNodeList =
			// corrLeaf.queryNodeList;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * For sending results up to now from parent query to correlated child
     * subquery.
     *
     * @param currentLeaf
     * @param nextLeaf
     */
    private void convertExtraCorrelated2(Leaf currentLeaf, Leaf nextLeaf) {
        final String method = "convertExtraCorrelated";
        logger.entering(method);

        try {
            aStepDetail = new StepDetail(client);
            coordStepDetail = null;

            aStepDetail.convertLeafToStepCorrDown2(nextLeaf);

            isExtraStep = currentLeaf.isExtraStep();

            Leaf corrLeaf = nextLeaf.subplan.getFirstLeaf();

            consumerCount = corrLeaf.queryNodeList.size();
            producerCount = corrLeaf.queryNodeList.size();

            aStepDetail.consumerNodeList = new ArrayList<Integer>();

            for (DBNode aDBNode : corrLeaf.queryNodeList) {
                aStepDetail.consumerNodeList.add(new Integer(
                        aDBNode.getNodeId()));

                // See if it was already put in usage table
                NodeUsage aNodeUsageElement = nodeUsageTable.get(new Integer(
                        aDBNode.getNodeId()));

                if (aNodeUsageElement != null) {
                    aNodeUsageElement.isConsumer = true;
                } else {
                    // always a producer, too
                    aNodeUsageElement = new NodeUsage(aDBNode.getNodeId(),
                            true, true);

                    nodeUsageTable.put(new Integer(aDBNode.getNodeId()),
                            aNodeUsageElement);
                }
            }

            // We also need to through and make sure we did not leave out
            // any producers
            SysDatabase database = MetaData.getMetaData().getSysDatabase(
                    client.getDBName());

            for (DBNode aDBNode : database.getDBNodeList()) {
                // See if it was already put in usage table
                NodeUsage aNodeUsageElement = nodeUsageTable.get(new Integer(
                        aDBNode.getNodeId()));

                if (aNodeUsageElement != null) {
                    // we already took care of it above
                    continue;
                } else {
                    // always a producer
                    aNodeUsageElement = new NodeUsage(aDBNode.getNodeId(),
                            true, false);

                    nodeUsageTable.put(new Integer(aDBNode.getNodeId()),
                            aNodeUsageElement);
                }
            }

            destNodeList = corrLeaf.queryNodeList;;
        } finally {
            logger.exiting(method);
        }

    }

    /**
     * For sending results up to now from parent query to correlated child
     * subquery.
     *
     * @param currentLeaf
     * @param nextLeaf
     * @param correlatedDepth
     */
    private void convertExtraCorrelatedNodeId(Leaf currentLeaf, Leaf nextLeaf,
            int correlatedDepth) {
        final String method = "convertExtraCorrelatedNodeId";
        logger.entering(method);

        try {
            // Determine actual node usage
            coordStepDetail = null;

            aStepDetail = new StepDetail(client);
            aStepDetail.convertLeafToStepCorrDownNodeId(nextLeaf,
                    correlatedDepth);

            isExtraStep = currentLeaf.isExtraStep();

            Leaf corrLeaf = nextLeaf.subplan.getFirstLeaf();

            SysDatabase database = MetaData.getMetaData().getSysDatabase(
                    client.getDBName());
            consumerCount = database.getDBNodeList().size();

            producerCount = corrLeaf.queryNodeList.size();

            aStepDetail.consumerNodeList = new ArrayList<Integer>();

            for (DBNode aDBNode : database.getDBNodeList()) {
                aStepDetail.consumerNodeList.add(new Integer(
                        aDBNode.getNodeId()));

                // See if it was already put in usage table
                NodeUsage aNodeUsageElement = nodeUsageTable.get(new Integer(
                        aDBNode.getNodeId()));

                if (aNodeUsageElement != null) {
                    aNodeUsageElement.isConsumer = true;
                } else {
                    aNodeUsageElement = new NodeUsage(aDBNode.getNodeId(),
                            true, true); // aStepDetail.isConsumer

                    nodeUsageTable.put(new Integer(aDBNode.getNodeId()),
                            aNodeUsageElement);
                }
            }

            destNodeList = corrLeaf.queryNodeList;
        } finally {
            logger.exiting(method);
        }

    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        StringBuffer sbStep = new StringBuffer(400);

        sbStep.append("\n ExecutionStep\n");
        sbStep.append(" -------------\n");

        sbStep.append(" producerCount = ");
        sbStep.append(producerCount);
        sbStep.append('\n');
        sbStep.append(" consumerCount = ");
        sbStep.append(consumerCount);
        sbStep.append('\n');
        sbStep.append(" isExtraStep = ");
        sbStep.append(isExtraStep);
        sbStep.append('\n');
        sbStep.append(" isFinalStep = ");
        sbStep.append(isFinalStep);
        sbStep.append('\n');

        if (destNodeList != null) {
            sbStep.append(" destNodeList = ");

            for (DBNode aDBNode : destNodeList) {
                sbStep.append(" ").append(aDBNode.getNodeId());
            }
            sbStep.append('\n');
        }

        if (aStepDetail != null) {
            sbStep.append("\n aStepDetail\n");
            sbStep.append(" -----------\n");
            sbStep.append(aStepDetail);
        }

        if (coordStepDetail != null) {
            sbStep.append("\n coordStepDetail\n");
            sbStep.append(" ---------------\n");
            sbStep.append(coordStepDetail);
        }

        if (nodeUsageTable != null && nodeUsageTable.size() > 0) {
            sbStep.append("\n nodeUsageTable\n");
            sbStep.append(" --------------\n");
            for (NodeUsage aNodeUsageElem : nodeUsageTable.values()) {
                sbStep.append(aNodeUsageElem);
            }
            sbStep.append('\n');
        }

        if (correlatedSendDownStep != null) {
            sbStep.append("\n correlatedSendDownStep\n");
            sbStep.append(" ------------------------\n");
            sbStep.append(correlatedSendDownStep);
            sbStep.append(" end correlatedSendDownStep\n\n");
        }

        if (correlatedSendDownStep2 != null) {
            sbStep.append("\n correlatedSendDownStep2\n");
            sbStep.append(" ------------------------\n");
            sbStep.append(correlatedSendDownStep2);
            sbStep.append(" end correlatedSendDownStep2\n\n");
        }

        return sbStep.toString();
    }

    /**
     * Update all the values in the node usage table, setting isConsumer to the
     * specified value.
     *
     * @param isConsumer
     * @param consumerNodeList
     */
    private void updateNodeProducerTableConsumer(boolean isConsumer,
            List<Integer> consumerNodeList) {

        for (NodeUsage aNodeUsage : nodeUsageTable.values()) {
            aNodeUsage.isConsumer = isConsumer;
            if (isConsumer) {
                consumerNodeList.add(aNodeUsage.nodeId);
            }
        }
    }

    /**
     * Look through nodeUsageTable to get the producer node
     *
     * @return
     */
    private int getProducerNode() {

        for (NodeUsage aNodeUsage : nodeUsageTable.values()) {
            if (aNodeUsage.isProducer) {
                return aNodeUsage.nodeId;
            }
        }
        return -1;
    }

    /**
     * Set one single node as the destination
     *
     * @param destNodeId
     * @param consumerNodeList
     */
    private void updateNodeProducerTableSetTargetConsumer(int destNodeId,
            List<Integer> consumerNodeList) {

        for (NodeUsage aNodeUsage : nodeUsageTable.values()) {
            if (aNodeUsage.nodeId == destNodeId) {
                aNodeUsage.isConsumer = true;
                consumerNodeList.add(aNodeUsage.nodeId);
            } else {
                aNodeUsage.isConsumer = false;
            }
        }
    }

    /**
     * Correct destinations for this step, based on the next step
     *
     * @param nextStep
     */
    protected void correctDestinations(ExecutionStep nextStep) {

        // This checks to make sure we aren't dealing with new relations that
        // were just defined and are not used on the next step.
        // This may happen with VIEWs
        if (this.aStepDetail.isProducer) {
            if (!nextStep.origLeaf.containsTable(aStepDetail.targetTable)) {
                /* make sure it is a relation. */
                if (parentPlan.parentExecutionPlan != null
                        && parentPlan.parentExecutionPlan.relationPlanList.contains(parentPlan)) {
                    return;
                }
            }
        } else {
            if (!nextStep.origLeaf.containsTable(coordStepDetail.targetTable)) {
                return;
            }
        }

        // next step is on coordinator
        if (nextStep.coordStepDetail != null
                && nextStep.coordStepDetail.isProducer) {
            if (coordStepDetail != null && coordStepDetail.isProducer) {
                if (coordStepDetail.getDestType() != StepDetail.DEST_TYPE_COORD
                        || coordStepDetail.getDestType() != StepDetail.DEST_TYPE_COORD_FINAL) {
                    // correct it
                    coordStepDetail.setDestTypeCoordinator();
                    aStepDetail.isConsumer = false;
                    aStepDetail.consumerNodeList = new ArrayList<Integer>();
                    consumerCount = 0;

                    // update node usage table
                    updateNodeProducerTableConsumer(false,
                            aStepDetail.consumerNodeList);
                }
            } else if (aStepDetail.getDestType() != StepDetail.DEST_TYPE_COORD
                    || aStepDetail.getDestType() != StepDetail.DEST_TYPE_COORD_FINAL) {
                aStepDetail.setDestTypeCoordinator();
                aStepDetail.isConsumer = false;
                aStepDetail.consumerNodeList = new ArrayList<Integer>();
                consumerCount = 0;

                // update node usage table
                updateNodeProducerTableConsumer(false,
                        aStepDetail.consumerNodeList);

                if (coordStepDetail == null) {
                    coordStepDetail = new StepDetail(client);
                    coordStepDetail.requestId = aStepDetail.requestId;
                    coordStepDetail.stepNo = aStepDetail.stepNo;
                    coordStepDetail.isProducer = false;
                    coordStepDetail.isConsumer = true;
                    coordStepDetail.targetSchema = aStepDetail.targetSchema;
                    coordStepDetail.targetTable = aStepDetail.targetTable;
                    coordStepDetail.setDestTypeCoordinator();
                }
            }
        } else {
            // Next step is done on nodes
            if (coordStepDetail != null && coordStepDetail.isProducer) {
                // Current step produces

                // See if next step is on a single node
                if (nextStep.producerCount == 1) {
                    coordStepDetail.setDestTypeOne(nextStep.getProducerNode());

                    // find single destination in nodeUsageTable
                    aStepDetail.isConsumer = true;
                    aStepDetail.consumerNodeList = new ArrayList<Integer>();
                    updateNodeProducerTableSetTargetConsumer(
                            coordStepDetail.getDestNode(),
                            aStepDetail.consumerNodeList);
                    consumerCount = aStepDetail.consumerNodeList.size();
                    return;
                }
            } else {
                // We are producing down at the nodes
                // See if next step is on a single node
                if (nextStep.producerCount == 1) {
                    aStepDetail.setDestTypeOne(nextStep.getProducerNode());
                    // find single destination in nodeUsageTable
                    aStepDetail.isConsumer = true;
                    aStepDetail.consumerNodeList = new ArrayList<Integer>();
                    updateNodeProducerTableSetTargetConsumer(
                            aStepDetail.getDestNode(), aStepDetail.consumerNodeList);
                    consumerCount = aStepDetail.consumerNodeList.size();
                    return;
                }
            }

            if (aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD
                    || aStepDetail.getDestType() == StepDetail.DEST_TYPE_COORD_FINAL) {
                // It is trying to send to the coordinator.
                aStepDetail.isConsumer = true;
                aStepDetail.consumerNodeList = new ArrayList<Integer>();
                aStepDetail.setDestTypeBroadcast();
                updateNodeProducerTableConsumer(true,
                        aStepDetail.consumerNodeList);
                consumerCount = aStepDetail.consumerNodeList.size();
            }

        }
    }

    /**
     * This is used when we want to have the scheduler pick which node to
     * execute a lookup on.
     *
     * @param nodeId
     */
    public void setSingleExecNode(int nodeId) {
        if (aStepDetail.isProducer) {
            NodeUsage aNodeUsageElement = new NodeUsage(nodeId, true, false);
            nodeUsageTable = new HashMap<Integer, NodeUsage>();
            nodeUsageTable.put(new Integer(nodeId), aNodeUsageElement);
        }
    }

    /**
     * Gets the SqlExpression to use for partitioning based on a parameter
     * value.
     *
     * @return the parameter SqlExpression to use for partitioning
     */
    protected SqlExpression getPartitionParameterExpression() {

        return origLeaf.getPartitionParameterExpression();
    }
}
