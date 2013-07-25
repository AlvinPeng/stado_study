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
 * ExecutionPlan.java
 *
 *
 */

package org.postgresql.stado.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.parser.ExpressionType;



/**
 * Converts a QueryPlan into an ExecutionPlan, one that prepares the nodes with
 * Step information
 *
 */
public class ExecutionPlan {

    // whether or not to do select projections from
    // (select endstuff from lasttable) at the end.
    // Some databases do not like it.
    public static final boolean TRANSFORM_PROJECTIONS = Property.getBoolean(
            "xdb.transformProjections", false);

    /** This subplans parent plan */
    protected ExecutionPlan parentExecutionPlan;

    /** The individual steps in this plan */
    public List<ExecutionStep> stepList;

    /** The final projections on the last step */
    public String finalProjString = "";

    /** the final temp table name on the last step */
    public String finalTempTableName = "";

    /** if this is a union, the individual plans for statements unioned */
    public List<ExecutionPlan> unionPlanList;

    /** Subqueries that are scalar (return one value) */
    public List<ExecutionPlan> scalarPlanList;

    /** Relational subqueries (FROM clause) */
    public List<ExecutionPlan> relationPlanList;

    /** Used for saving the results of a scalar query */
    public String scalarResult = "null";

    /** scalar index */
    public int scalarPlaceholderNo;

    /** whether or not we combine the results at the coordinator */
    public boolean combineResults = false;

    /** whether or not it is a top level union */
    public boolean isTopLevelUnion = false;

    /** whether or not this is the final unioned query */
    public boolean isFinalUnionPart = false;

    /** union type */
    public int unionType = QueryPlan.UNIONTYPE_NONE;

    /** list of temp tables to drop at the nodes after this plan */
    public List<String> nodeTempTableDropList;

    /** list of "temp" tables on the coordinator to drop after this plan */
    public List<String> coordTempTableDropList;

    /** The correlated depth of this plan */
    public int correlatedDepth = 0;

    /** whether or not we can hash by the correlated column to reduce
     * sending rows. */
    protected boolean isCorrelatedHashable = false;

    /** session info */
    private XDBSessionContext client;

    /** helper for PreparedStatements */
    protected ExecutionPlanPreparedHandler anEPParameterHelper;


    private long finalLimit = -1;


    private long finalOffset = -1;


    /**
     * Creates a new instance of ExecutionPlan
     *
     * @param parentExecPlan
     * @param aQueryPlan
     * @param parentNodeUsageTable
     * @param parentTargetNodeList
     * @param isUnionSubquery
     * @param client
     */
    public ExecutionPlan(ExecutionPlan parentExecPlan, QueryPlan aQueryPlan,
            HashMap<Integer, NodeUsage> parentNodeUsageTable,
            List<DBNode> parentTargetNodeList,
            boolean isUnionSubquery, XDBSessionContext client) {
        this(parentExecPlan, aQueryPlan, parentNodeUsageTable,
                parentTargetNodeList, isUnionSubquery, client, 0, false);
    }

    /**
     * Constructor
     *
     * @param parentExecPlan
     * @param aQueryPlan
     * @param parentNodeUsageTable
     * @param parentTargetNodeList
     * @param isUnionSubquery
     * @param client
     * @param correlatedDepth
     * @param isCorrelatedHashable
     */
    public ExecutionPlan(ExecutionPlan parentExecPlan, QueryPlan aQueryPlan,
            HashMap<Integer, NodeUsage> parentNodeUsageTable,
            List<DBNode> parentTargetNodeList,
            boolean isUnionSubquery, XDBSessionContext client,
            int correlatedDepth, boolean isCorrelatedHashable) {

        Leaf currentLeaf;
        Leaf nextLeaf;
        Leaf nextNextLeaf;
        Leaf previousLeaf = null;
        ExecutionStep anExecutionStep;
        ExecutionPlan subExecPlan;

        this.parentExecutionPlan = parentExecPlan;
        this.client = client;
        this.correlatedDepth = correlatedDepth;
        this.isCorrelatedHashable = isCorrelatedHashable;

        stepList = new ArrayList<ExecutionStep>();
        unionPlanList = new ArrayList<ExecutionPlan>();
        scalarPlanList = new ArrayList<ExecutionPlan>();
        relationPlanList = new ArrayList<ExecutionPlan>();

        this.unionType = aQueryPlan.unionType;

        // See if we are dealing with UNIONs
        if (aQueryPlan.isUnion) {
            buildUnionPlan(aQueryPlan, parentNodeUsageTable,
                    parentTargetNodeList);
            return;
        }

        // Now check for subqueries that are relation or scalar subqueries
        for (QueryPlan subQueryPlan : aQueryPlan.subplanList) {

            subExecPlan = new ExecutionPlan(this, subQueryPlan,
                    parentNodeUsageTable, parentTargetNodeList, false, client);

            if (subQueryPlan.scalarLeaf != null) {
                // save scalar-related info we will need for execution.
                subExecPlan.scalarPlaceholderNo = subQueryPlan.placeHolderNo;
                scalarPlanList.add(subExecPlan);
            } else {
                // must be relation
                relationPlanList.add(subExecPlan);
            }
        }

        finalProjString = aQueryPlan.finalProjString;

        // loop through all the steps
        // get the select statement, and execute.
        aQueryPlan.initLeafIteration();

        // We need to peak ahead 2 leaves to correctly build up the steps.
        for (currentLeaf = aQueryPlan.nextLeaf(), nextLeaf = aQueryPlan
                .nextLeaf(); currentLeaf != null; previousLeaf = currentLeaf, currentLeaf = nextLeaf, nextLeaf = nextNextLeaf) {

            // peek ahead
            nextNextLeaf = aQueryPlan.nextLeaf();
            anExecutionStep = createNewStep();
            anExecutionStep.convertFromLeaf(currentLeaf, nextLeaf,
                    nextNextLeaf, previousLeaf, parentNodeUsageTable,
                    parentTargetNodeList, isUnionSubquery, aQueryPlan.planType);

            finalTempTableName = currentLeaf.getTargetTableName();

            /*
             * Handle the case where we are on the very last step (top level of
             * query, not subquery). We don't want to create a final temp table
             * and instead combine ResultSets.
             *
             * We want to do that for non-aggregate queries, and for aggregate
             * queries that include a group by. For aggregate queries without a
             * group by, we still want them to go to the coordinator
             * (SELECT COUNT(*) FROM t).
             *
             * If the final step is being executed on the coordinator, we don't
             * want to bother creating a "final" table, we just set up the final
             * ResultSet, which is done later in QueryProcessor.
             */
            if (nextLeaf == null
                    && parentTargetNodeList == null
                    && (aQueryPlan.isTopLevelPlan || aQueryPlan.isFinalUnionPart)) {
                isFinalUnionPart = aQueryPlan.isFinalUnionPart;

                // see if combining from nodes
                if (anExecutionStep.aStepDetail.isProducer
                        && !(currentLeaf.isCombinerStep() && currentLeaf.groupByColumns
                                .size() == 0)) {
                    // Flag for combining results
                    if (isFinalUnionPart) {
                        anExecutionStep.aStepDetail.isFinalUnionPart = true;
                        anExecutionStep.aStepDetail.finalUnionPartSortInfo = aQueryPlan.sortInfo;
                        anExecutionStep.aStepDetail.finalUnionPartIsDistinct = aQueryPlan.isDistinct;
                    }

                    anExecutionStep.coordStepDetail = null;
                    anExecutionStep.isFinalStep = true;
                    anExecutionStep.aStepDetail.setDestTypeCoordinatorFinal();
                    modifyFinalSelectOnNodes(anExecutionStep, aQueryPlan,
                            TRANSFORM_PROJECTIONS);
                } else {
                    // We just want the last step to execute at the combiner,
                    // without putting it into a temp table.

                    // Flag for combining results
                    if (isFinalUnionPart) {
                        anExecutionStep.coordStepDetail.isFinalUnionPart = true;
                        anExecutionStep.coordStepDetail.finalUnionPartSortInfo = aQueryPlan.sortInfo;
                        anExecutionStep.coordStepDetail.finalUnionPartIsDistinct = aQueryPlan.isDistinct;
                    }

                    anExecutionStep.coordStepDetail.isConsumer = false;
                    anExecutionStep.isFinalStep = true;
                    anExecutionStep.coordStepDetail.setDestTypeCoordinatorFinal();
                    anExecutionStep.coordStepDetail.targetSchema = "";
                    anExecutionStep.coordStepDetail.targetTable = "";
                    coordTempTableDropList = anExecutionStep.coordStepDetail.dropList;
                    modifyFinalSelectOnCoordinator(anExecutionStep, aQueryPlan,
                            TRANSFORM_PROJECTIONS);
                }
            }
        }
    }

    /**
     * Changes projections for final step when no group by is present.
     *
     * @param anExecutionStep
     * @param aQueryPlan
     * @param transformProjections
     */
    private void modifyFinalSelectOnNodes(ExecutionStep anExecutionStep,
            QueryPlan aQueryPlan, boolean transformProjections) {

        StringBuilder finalSelect = new StringBuilder(256);
        finalSelect.append("SELECT ");

        if (aQueryPlan.isDistinct) {
            finalSelect.append("DISTINCT ");
        }

        if (aQueryPlan.orderByClause.length() > 0) {

            // To make sure projections are correct, do relation subquery
            if (transformProjections) {
                anExecutionStep.aStepDetail.queryString = finalSelect
                        .append(aQueryPlan.finalProjString)
                        .append(aQueryPlan.addedFinalProjections)
                        .append(" FROM (")
                        .append(anExecutionStep.aStepDetail.queryString)
                        .append(") as axtmzyx")
                        .append(" order by ")
                        .append(aQueryPlan.orderByClause)
                        .toString();
            } else {
                anExecutionStep.aStepDetail.queryString = finalSelect
                        .append(aQueryPlan.finalProjString)
                        .append(aQueryPlan.addedFinalProjections)
                        .append(" ")
                        .append(anExecutionStep.aStepDetail.nonProjectionSelectPart)
                        .append(" order by ")
                        .append(aQueryPlan.orderByClause)
                        .toString();
            }
        } else {
            if (transformProjections) {
                // To make sure projections are correct, do relation subquery
                anExecutionStep.aStepDetail.queryString = finalSelect
                        .append(aQueryPlan.finalProjString)
                        .append(" FROM (")
                        .append(anExecutionStep.aStepDetail.queryString)
                        .append(") as xtmzyx")
                        .toString();
            } else {
                anExecutionStep.aStepDetail.queryString = finalSelect
                        .append(aQueryPlan.finalProjString)
                        .append(" ")
                        .append(anExecutionStep.aStepDetail.nonProjectionSelectPart)
                        .toString();
            }
        }

        // Flag that we should "stream" and combine ResultSets on the fly
        this.combineResults = true;
        nodeTempTableDropList = anExecutionStep.aStepDetail.dropList;

        // Add limit and offset info
        addLimitAndOffset (anExecutionStep, aQueryPlan);

        addIntoTable(anExecutionStep.aStepDetail, anExecutionStep.aStepDetail,
        		anExecutionStep.nodeUsageTable, aQueryPlan);
    }

    /**
     * If target table is specified modify final step to create final table
     * instead of sending results to the Coordinator
     * @param srcDetail
     * @param dstDetail
     * @param nodeUsageTable
     * @param aQueryPlan
     */
    private void addIntoTable(StepDetail srcDetail, StepDetail dstDetail,
            Map<Integer, NodeUsage> nodeUsageTable, QueryPlan aQueryPlan) {
        SysTable intoTable = aQueryPlan.getIntoTable();
        if (intoTable != null) {
            if (combineResults) {
                if ((aQueryPlan.getLimit() > 0 || aQueryPlan.getOffset() > 0) && nodeUsageTable.size() != 1) {
                    return;
                } else {
                    combineResults = false;
                }
            }
            // This is not initialized if query is like select count(*) ...
            srcDetail.targetTable = dstDetail.targetTable;
            srcDetail.consumerNodeList = new ArrayList<Integer>(
                    intoTable.getPartitionMap().allPartitions().size());
            // Share the list
            dstDetail.consumerNodeList = srcDetail.consumerNodeList;
            for (Integer nodeID : intoTable.getPartitionMap().allPartitions()) {
                srcDetail.consumerNodeList.add(nodeID);
                NodeUsage nu = nodeUsageTable.get(nodeID);
                if (nu == null) {
                    nu = new NodeUsage(nodeID, false, false);
                    nodeUsageTable.put(nodeID, nu);
                }
                nu.isConsumer = true;
            }
            dstDetail.isConsumer = true;
            if (intoTable.getPartitionScheme() == SysTable.PTYPE_HASH) {
                String hashColumn = intoTable.getPartitionColumn();
                //
                SysColumn partColumn = intoTable.getPartitionedColumn();
                // Possible if CREATE TABLE AS specifies non-existing column
                if (partColumn == null) {
                    throw new XDBServerException("Partitioning column not found: " + hashColumn);
                }
                int partColumnPosition = intoTable.getSysColumn(
                        partColumn.getColName()).getColSeq();
                srcDetail.setDestTypeHash(partColumnPosition, intoTable.getPartitionMap());
                srcDetail.setHashDataType(new ExpressionType(partColumn));
                if (aQueryPlan.isExistingInto()) {
                    srcDetail.targetTable = intoTable.getTableName();
                    srcDetail.targetSchema = null;
                }
            } else if (intoTable.getPartitionScheme() == SysTable.PTYPE_ROBIN) {
                Collection<Integer> parts = intoTable.getPartitionMap()
                        .getPartitions(null);
                srcDetail.setDestTypeOne(parts.iterator().next());
                if (aQueryPlan.isExistingInto()) {
                    srcDetail.targetTable = intoTable.getTableName();
                    srcDetail.targetSchema = null;
                }
            } else {
                srcDetail.setDestTypeBroadcast();
                // Required if target table is round robin, no harm otherwise
                srcDetail.setPartitionMap(intoTable.getPartitionMap());
            }
        }
    }

    /**
     * Append LIMIT and OFFSET to step, if applicable
     *
     * @param anExecutionStep
     * @param aQueryPlan
     */
    private void addLimitAndOffset (ExecutionStep anExecutionStep,
            QueryPlan aQueryPlan) {

        // Handle LIMIT and OFFSET for last step.
        // If there is only node being used, we do not use
        // CombinedResultSet object, so just pass along verbatim.
        if (anExecutionStep.nodeUsageTable.size() == 1) {
            if (aQueryPlan.getLimit() > -1) {
                anExecutionStep.aStepDetail.queryString += " LIMIT "
                        + aQueryPlan.getLimit();
            }
            if (aQueryPlan.getOffset() > -1) {
                anExecutionStep.aStepDetail.queryString += " OFFSET "
                        + aQueryPlan.getOffset();
            }
        } else {
            // more than 1 node. It is up to CombinedResultSet
            // to merge them
            if (aQueryPlan.getLimit() > -1) {
                if (aQueryPlan.getOffset() > -1) {
                    // Add limit and offset together,
                    // CombinedResultSet will take care of offset on merge
                    anExecutionStep.aStepDetail.queryString += " LIMIT "
                            + (aQueryPlan.getLimit() + aQueryPlan.getOffset());
                } else {
                    anExecutionStep.aStepDetail.queryString += " LIMIT "
                            + aQueryPlan.getLimit();
                }
            }
            finalLimit = aQueryPlan.getLimit();
            finalOffset = aQueryPlan.getOffset();
        }
    }

    public long getLimit() {
        return finalLimit;
    }

    public long getOffset() {
        return finalOffset;
    }

    /**
     * Modifies the final select, in case it is being executed on the
     * coordinator.
     *
     * @param anExecutionStep
     * @param aQueryPlan
     * @param transformProjections
     */
    private void modifyFinalSelectOnCoordinator(ExecutionStep anExecutionStep,
            QueryPlan aQueryPlan, boolean transformProjections) {
        String finalSelect = "SELECT ";

        if (aQueryPlan.isDistinct) {
            finalSelect += "DISTINCT ";
        }

        if (aQueryPlan.orderByClause.length() > 0) {
            // To make sure projections are correct, do relation subquery
            if (transformProjections) {
                // I believe this is incorrect for unions
                // but we should move away from TRANSFORM_PROJECTIONS
                anExecutionStep.coordStepDetail.queryString = finalSelect
                        + aQueryPlan.finalProjString
                        + aQueryPlan.addedFinalProjections + " FROM ("
                        + anExecutionStep.coordStepDetail.queryString + ")"
                        + " order by " + aQueryPlan.orderByClause;
            } else {
                anExecutionStep.coordStepDetail.queryString = finalSelect
                        + aQueryPlan.finalProjString
                        + " "
                        + anExecutionStep.coordStepDetail.nonProjectionSelectPart
                        + " order by " + aQueryPlan.orderByClause;
            }
        } else {
            // To make sure projections are correct, do relation subquery
            if (transformProjections) {
                anExecutionStep.coordStepDetail.queryString = finalSelect
                        + aQueryPlan.finalProjString + " FROM ("
                        + anExecutionStep.coordStepDetail.queryString
                        + ") as xtmzyx";
            } else {
                anExecutionStep.coordStepDetail.queryString = finalSelect
                        + aQueryPlan.finalProjString
                        + " "
                        + anExecutionStep.coordStepDetail.nonProjectionSelectPart;
            }
        }

        // Handle LIMIT and OFFSET.
        // We are just executing on the coordinator, so we can append here
        if (aQueryPlan.getLimit() > -1) {
            anExecutionStep.coordStepDetail.queryString += " LIMIT "
                    + aQueryPlan.getLimit();
        }
        if (aQueryPlan.getOffset() > -1) {
            anExecutionStep.coordStepDetail.queryString += " OFFSET "
                    + aQueryPlan.getOffset();
        }
        addIntoTable(anExecutionStep.coordStepDetail, anExecutionStep.aStepDetail,
        		anExecutionStep.nodeUsageTable, aQueryPlan);
    }

    /**
     * Special handling if we are dealing with a UNION
     *
     * @param aQueryPlan
     * @param parentNodeUsageTable
     * @param parentTargetNodeList
     */
    private void buildUnionPlan(QueryPlan aQueryPlan,
            HashMap<Integer, NodeUsage> parentNodeUsageTable,
            List<DBNode> parentTargetNodeList) {

        ExecutionPlan subExecPlan;
        ExecutionStep anExecutionStep;

        unionPlanList = new ArrayList<ExecutionPlan>();
        nodeTempTableDropList = new ArrayList<String>();
        coordTempTableDropList = new ArrayList<String>();

        ArrayList<String> finalCoordTempTableDropList = new ArrayList<String>();

        for (QueryPlan subQueryPlan : aQueryPlan.unionSubplanList) {
            // If there is an order by clause, it is ok to add it here
            if (subQueryPlan.orderByClause.length() > 0) {
                Leaf finalLeaf = subQueryPlan.getLastLeaf();
                finalLeaf.setSelectStatement(finalLeaf.getSelectStatement()
                        + " order by "
                        + subQueryPlan.orderByClause);
            }

            subExecPlan = new ExecutionPlan(this, subQueryPlan,
                    parentNodeUsageTable, parentTargetNodeList, true, client);

            unionPlanList.add(subExecPlan);

            // Make sure we drop temp tables properly
            ExecutionStep lastStep = subExecPlan.stepList.get(subExecPlan.stepList.size() - 1);

            if (lastStep.aStepDetail.isProducer) {
                nodeTempTableDropList.addAll(lastStep.aStepDetail.dropList);
            } else {
                if (lastStep.coordStepDetail != null) {
                    coordTempTableDropList
                            .addAll(lastStep.coordStepDetail.dropList);
                }
            }

            finalCoordTempTableDropList.add(subQueryPlan.finalTableName);

            // We also want to provide some additional information
            // about how we can combine the unions, for MultinodeExecutor.
            // See also QueryPlan.createPlanSegmentFromTree.
            // We should end up with 0 or more 1's, followed by
            // 0 or more 2's.
            if (subQueryPlan.unionType == QueryPlan.UNIONTYPE_UNIONALL) {
                if (lastStep.aStepDetail != null) {
                    lastStep.aStepDetail.unionResultGroup = 2;
                }
                if (lastStep.coordStepDetail != null) {
                    lastStep.coordStepDetail.unionResultGroup = 2;
                }
            } else {
                if (lastStep.aStepDetail != null) {
                    lastStep.aStepDetail.unionResultGroup = 1;
                }
                if (lastStep.coordStepDetail != null) {
                    lastStep.coordStepDetail.unionResultGroup = 1;
                }
            }
        }

        finalTempTableName = aQueryPlan.finalTableName;

        // Save order by clause for later
        finalProjString = aQueryPlan.finalProjString;

        if (!aQueryPlan.isTopLevelPlan) {
            // There will just be one Leaf, containing all subqueries

            /*
             * Note that we always have these run from the nodes, not the
             * coordinator. It is ok, because we previously did a hashed
             * distribution. That is also convenient for combining UNIONs,
             * independent if we have Q1 UNION Q2 UNION ALL Q3, the hash ensures
             * that each node will be dealing with unique data.
             */
            Leaf aLeaf = aQueryPlan.getFirstLeaf();

            anExecutionStep = createNewStep();

            // We are a union query in a subquery.
            // See if the parent is also a union query and the top plan.
            // In that case, we flag that it is "final"
            if (parentExecutionPlan != null
                    && parentExecutionPlan.parentExecutionPlan == null
                    && !parentExecutionPlan.unionPlanList.isEmpty()) {
                // update Step, set nodeUsage properly
                anExecutionStep.convertFromLeaf(aLeaf, null, null, null,
                        parentNodeUsageTable, parentTargetNodeList, false,
                        aQueryPlan.planType);

                anExecutionStep.aStepDetail.setDestTypeCoordinatorFinal();
                anExecutionStep.isFinalStep = true;
                anExecutionStep.aStepDetail.isFinalUnionPart = true;
                anExecutionStep.aStepDetail.finalUnionPartSortInfo = aQueryPlan.topQueryPlan.sortInfo;
            } else {
                // We are nested more deeply.

                // update Step, set nodeUsage properly
                anExecutionStep.convertFromLeaf(aLeaf, null, null, null,
                        parentNodeUsageTable, parentTargetNodeList, true,
                        aQueryPlan.planType);
            }

            anExecutionStep.aStepDetail.dropList
                    .addAll(finalCoordTempTableDropList);
        } else {
            isTopLevelUnion = true;
        }
    }

    /**
     * Creates a new ExecutionStep, adds it to the step list and returns it.
     * @return
     */
    private ExecutionStep createNewStep() {
        ExecutionStep anExecutionStep = new ExecutionStep(this, client);

        stepList.add(anExecutionStep);

        return anExecutionStep;
    }

    /**
     * Due to destination node issues, what this method does is step
     * through the ExecutionPlan in the same order that it should be executed,
     * and look for inconsistencies where the last step of subplan sends it to
     * the wrong destination.
     *
     * TODO: remove other code that determines destinations and use this
     * instead since it will overwrite those.
     *
     * @see correctStepDestinations
     */
    public void correctDestinations() {
        correctStepDestinations();
    }

    /**
     * Due to destination node issues, what this method does is step
     * through the ExecutionPlan in the same order that it should be executed,
     * and look for inconsistencies where the last step of subplan sends it to
     * the wrong destination.
     *
     * @see correctDestinations
     * @return the last ExecutionStep of the plan
     */
    private ExecutionStep correctStepDestinations() {

        ExecutionStep lastExecStep = null;

        if (!unionPlanList.isEmpty()) {
            correctUnionQueryDestinations();
        }

        // Check to see if we have any scalar subqueries and correct
        for (ExecutionPlan scalarPlan : scalarPlanList) {
            scalarPlan.correctStepDestinations();
        }

        // correct any relation subplans
        for (ExecutionPlan relationPlan : relationPlanList) {
            // check last step
            ExecutionStep lastRelationStep = relationPlan
                    .correctStepDestinations();
            lastRelationStep.correctDestinations(stepList.get(0));
        }

        // Now do the steps
        for (ExecutionStep anExecStep : stepList) {

            // See if we need to handle any correlated subplans first
            if (anExecStep.correlatedSubPlan != null) {
                // This may be unnecessary, but being safe
                anExecStep.correlatedSubPlan.correctStepDestinations();
            }

            QueryPlan.CATEGORY_QUERYFLOW.info(anExecStep);

            // see if we have any uncorrelated subqueries
            if (anExecStep.uncorrelatedSubPlanList != null) {
                for (ExecutionPlan uncorSubPlan : anExecStep.uncorrelatedSubPlanList) {
                    // need to set destination for this
                    ExecutionStep uncorLastExecStep = uncorSubPlan
                            .correctStepDestinations();

                    if (uncorLastExecStep != null) {
                        uncorLastExecStep.correctDestinations(anExecStep);
                    }
                }
            }

            // double check previous step with this one
            if (lastExecStep != null) {
                lastExecStep.correctDestinations(anExecStep);
            }

            lastExecStep = anExecStep;
        }

        return lastExecStep;
    }

    /**
     * Corrects destinations in UNION queries
     */
    private void correctUnionQueryDestinations() {

        for (ExecutionPlan subPlan : unionPlanList) {
            subPlan.correctStepDestinations();
        }
    }

    /**
     * Returns the top level plan
     *
     * @return the top level ExecutionPlan
     */
    protected ExecutionPlan getTopParentPlan () {
        if (parentExecutionPlan == null) {
            return this;
        } else {
            return parentExecutionPlan.getTopParentPlan();
        }
    }

    /**
     * Prepares PreparedStatement paramters based on the types.
     *
     * @param paramTypes - the parameter types used
     */
    public void prepareParameters (int[] paramTypes) {

        anEPParameterHelper = new ExecutionPlanPreparedHandler(client, paramTypes);
        anEPParameterHelper.preparePlanParameters (this);
    }

    /**
     * Replaces the appropriate parameter values from the parameter list
     * for PreparedStatements
     *
     * @param paramValues - the values to substitute.
     */
    public void substituteParameterValues (String[] paramValues) {
        anEPParameterHelper.substituteParameterValues(paramValues);
    }

    public boolean isSingleStep() {
    	return (relationPlanList == null || relationPlanList.isEmpty())
    			&& (scalarPlanList == null || scalarPlanList.isEmpty())
    			&& (unionPlanList == null || unionPlanList.isEmpty())
    			&& stepList != null && stepList.size() == 1;
    }

    public String toString() {
    	StringBuffer sbPlan = new StringBuffer(1024);
    	for (ExecutionPlan unionPlan : unionPlanList) {
    		sbPlan.append("\n Union Subplan:\n");
    		sbPlan.append(unionPlan);
    	}
    	
    	for (ExecutionPlan scalarPlan : scalarPlanList) {
    		sbPlan.append("\n Scalar Subplan:\n");
    		sbPlan.append(scalarPlan);
    	}
    	
    	for (ExecutionPlan relationPlan : relationPlanList) {
    		sbPlan.append("\n Relation Subplan:\n");
    		sbPlan.append(relationPlan);
    	}

    	for (ExecutionStep execStep : stepList) {
    		if (execStep.correlatedSubPlan != null) {
        		sbPlan.append("\n Correlated Subplan:\n");
        		sbPlan.append(execStep.correlatedSubPlan);
    		}
    		
            if (execStep.uncorrelatedSubPlanList != null) {
                for (ExecutionPlan uncorSubPlan : execStep.uncorrelatedSubPlanList) {
            		sbPlan.append("\n Uncorrelated Subplan:\n");
            		sbPlan.append(uncorSubPlan);
                }
            }

    		if (execStep.outerSubPlan != null) {
        		sbPlan.append("\n Outer Subplan:\n");
        		sbPlan.append(execStep.outerSubPlan);
    		}
    		
    		sbPlan.append("\n");
    		sbPlan.append(execStep);
    		
            if (execStep.correlatedSendDownStep != null) {
        		sbPlan.append("\n");
        		sbPlan.append(execStep.correlatedSendDownStep);
        		
                if (execStep.correlatedSendDownStep2 != null) {
            		sbPlan.append("\n");
            		sbPlan.append(execStep.correlatedSendDownStep2);
                }
            }
    	}
    	return sbPlan.toString();
    }
}
