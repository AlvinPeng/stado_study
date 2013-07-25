/**
 * ***************************************************************************
 * Copyright (C) 2008 EnterpriseDB Corporation. Copyright (C) 2011 Stado Global
 * Development Group.
 *
 * This file is part of Stado.
 *
 * Stado is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * Stado is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * Stado. If not, see <http://www.gnu.org/licenses/>.
 *
 * You can find Stado at http://www.stado.us
 *
 ***************************************************************************
 */
/*
 * Optimizer.java
 */
package org.postgresql.stado.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysCheck;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.misc.SortedLongVector;
import org.postgresql.stado.parser.handler.IdentifierHandler;

/**
 * Optimizer is responsible for determining the processing order of the tables.
 * It utilizies QueryTree and builds up and evaluates candidate trees, choosing
 * the best one based on cost
 *
 * Notes: Always evaluate non-correlated subqueries first for a relation Try and
 * evelauate correlated subqueries last, or near the end Always perform join of
 * parent-child partitions together, which is determined early in query plan
 * Always perform joins with "lookup" tables together This last rule should be
 * revisited- these joins should be biased to be moved to the end if possible,
 * except if there are additional qualifying conditions
 *
 */
public class Optimizer {

    /**
     * for helping fold in lookups
     */
    private class LookupCandidate {

        RelationNode relationNode;
        RelationNode otherRelationNode;
        QueryCondition queryCondition;
        short outerType;

        /**
         * Constructor
         *
         * @param relationNode
         * @param otherRelationNode
         * @param queryCondition
         * @param outerType
         */
        LookupCandidate(RelationNode relationNode,
                RelationNode otherRelationNode, QueryCondition queryCondition,
                short outerType) {
            this.relationNode = relationNode;
            this.otherRelationNode = otherRelationNode;
            this.queryCondition = queryCondition;
            this.outerType = outerType;
        }
    }
    private static final XLogger logger = XLogger.getLogger(Optimizer.class);
    /**
     * Processing factor in costing, based on # of rows
     */
    private static final float PROC_FACTOR = .001f;
    // Used for examining processing outer'ed tables that are
    // joined along partitioned column together.
    /**
     * Too complex, don't process together
     */
    private static final int OUTER_PARTED_NO = 0;
    /**
     * Both tables are on level 0.
     */
    private static final int OUTER_PARTED_LEVEL_0 = 1;
    /**
     * Both are on the same level, and only one of the tables joins with a table
     * on a lower level.
     */
    private static final int OUTER_PARTED_SAME_ONE = 2;
    /**
     * Each are on a different level, separated by 1, and no other tables are in
     * their respective outer groups.
     */
    private static final int OUTER_PARTED_DIFFERENT = 3;
    /**
     * When comparing candidate trees, after each iteration, prune the candidate
     * list to those with the lowest cost, up to MAX_TREES
     */
    private static final int MAX_TREES = 5;
    /**
     * Minimum number of trees to try and carry forward in candidate tree
     * building
     */
    private static final int MIN_TREES = 5;
    /**
     * For each candidate tree in the list, we will only take the top
     * STEP_TRIM_AMOUNT trees to be compared to the other candidate tree
     * permuatations.
     */
    private static final int STEP_TRIM_AMOUNT = 3;
    /**
     * User's session info
     */
    XDBSessionContext client;
    /**
     * the database we are working with
     */
    private final SysDatabase database;
    /**
     * The list of candidate trees we are evaluating
     */
    SortedLongVector candidateTreeList;

    /**
     * Constructor
     *
     * @param client the session associated with this connection
     */
    public Optimizer(XDBSessionContext client) // , QueryTree aQueryTree)
    {
        this.client = client;
        database = MetaData.getMetaData().getSysDatabase(client.getDBName());
    }

    /**
     * Determines whether the subquery only contains lookups and parent-child
     * joins.
     *
     * This is for correlated subqueries, to try and skip an extra step. In
     * order for this to work (folding in the extra step), there can be only a
     * single correlated subquery join, and the condition must later be set to
     * appear at the RelationNode in the parent.
     *
     * @param subTreeNode RelationNode containing correlated subquery
     * @param aQueryTree parent QueryTree
     * @return
     */
    private boolean containsOnlyLookupsAndParChilds(RelationNode subTreeNode) {
        final String method = "containsOnlyLookupsAndParChilds";
        logger.entering(method);

        try {
            RelationNode aRelationNode, otherRelationNode;
            String partitionColumn;
            int nonLookupCount = 0;

            if (subTreeNode.getSubqueryTree().getUnionQueryTreeList() != null
                    && subTreeNode.getSubqueryTree().getUnionQueryTreeList().size() > 0) {
                return false;
            }

            if (subTreeNode.getSubqueryTree().getNoncorSubqueryList() != null
                    && subTreeNode.getSubqueryTree().getUnionQueryTreeList().size() > 0) {
                return false;
            }

            // See if it contains only lookups
            for (RelationNode aRelNode : subTreeNode.getSubqueryTree().getRelationNodeList()) {
                if (aRelNode.isCorrelatedPlaceholder()) {
                    continue;
                }

                // Nested correlated subqueries
                if (aRelNode.isCorrelatedSubquery()) {
                    continue;
                }

                if (aRelNode.isTable() && !aRelNode.isLookup()) {
                    if (++nonLookupCount >= 2) {
                        return false;
                    }
                }
            }

            // We only have lookups, all clear
            if (nonLookupCount == 0) {
                return true;
            }

            // check to see if join is on partitioned column
            RelationNode joinRelationNode = null;

            for (QueryCondition cond : subTreeNode.getSubqueryTree().getConditionList()) {
                if (!cond.isTwoRelationJoin()) {
                    return false;
                } else {
                    aRelationNode = cond.getRelationNodeList().get(0);
                    otherRelationNode = cond.getRelationNodeList().get(1);

                    // don't check if "join" is on a subquery. (yet...)
                    if (aRelationNode.isSubquery()
                            || otherRelationNode.isSubquery()) {
                        return false;
                    }

                    // skip, if condition involves lookups only
                    if (aRelationNode.isCorrelatedPlaceholder()) {
                        if (otherRelationNode.isLookup()) {
                            continue;
                        }
                    } else if (otherRelationNode.isCorrelatedPlaceholder()) {
                        if (aRelationNode.isLookup()) {
                            continue;
                        }
                    } else {
                        if (aRelationNode.isLookup()
                                && otherRelationNode.isLookup()) {
                            continue;
                        }
                    }

                    // see if this is the/a correlated join
                    boolean isCorrJoin = false;

                    if (aRelationNode.isCorrelatedPlaceholder()) {
                        isCorrJoin = true;
                        RelationNode tempNode = aRelationNode;
                        aRelationNode = otherRelationNode;
                        otherRelationNode = tempNode;
                    }

                    if (otherRelationNode.isCorrelatedPlaceholder()) {
                        isCorrJoin = true;
                    }

                    if (!isCorrJoin) {
                        return false;
                    } else {
                        partitionColumn = aRelationNode.getPartitionColumnName();

                        // See if the condition uses the column
                        if (partitionColumn != null
                                && partitionColumn.length() > 0) {

                            joinRelationNode = isCorrelatedPartitionedJoin(
                                    cond.getExpr().getLeftExpr(),
                                    cond.getExpr().getRightExpr(),
                                    aRelationNode, partitionColumn);

                            if (joinRelationNode != null) {
                                break;
                            }

                            joinRelationNode = isCorrelatedPartitionedJoin(
                                    cond.getExpr().getRightExpr(),
                                    cond.getExpr().getLeftExpr(),
                                    aRelationNode, partitionColumn);
                        }
                    }
                }
            }

            if (joinRelationNode == null) {
                return false;
            }

            // if we found a correlated join, just check and make sure that
            // the other RelationNodes involved are lookups (if any)
            for (RelationNode relNode :
                    subTreeNode.getSubqueryTree().getRelationNodeList()) {

                // skip if it is the placeholder, or the one that joins with it
                // that is part of the parent-child join
                if (relNode.isCorrelatedPlaceholder() || relNode == joinRelationNode) {
                    continue;
                }

                // Make sure it is a table.
                // We don't yet drill down deeper for relation subqueries, etc.
                if (!relNode.isTable()) {
                    return false;
                }

                // make sure it is a lookup
                if (!relNode.isLookup()) {
                    // not a lookup- we can't safely reduce this
                    return false;
                }
            }
            return true;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @return RelationNode to join with, if correlated partition join,
     * otherwise null
     *
     * @condition1 first condition element to compare
     * @condition2 second condition element to compare
     * @aRelationNode current relation node
     * @partitionColumn the column to compare
     */
    private RelationNode isCorrelatedPartitionedJoin(SqlExpression expr1,
            SqlExpression expr2, RelationNode aRelationNode,
            String partitionColumn) {

        RelationNode joinRelationNode = null;

        if (expr1.isColumn()) {
            AttributeColumn column1 = expr1.getColumn();

            // See if the table being joined with here
            // is on its partitioning key.
            // then check other side
            if (column1.getTableName().equalsIgnoreCase(
                    aRelationNode.getTableName())
                    && column1.columnName.equalsIgnoreCase(partitionColumn)
                    && expr2.isColumn()) {

                AttributeColumn column2 = expr2.getColumn();

                // Do not handle for WITH yet
                if (column1.relationNode.isWithDerived() ||
                        column2.relationNode.isWithDerived()) {
                    return null;
                }
                if (column2.isPartitionColumn()) {
                    // ok, we have a valid correlated
                    // join on partitioned columns
                    SysTable aSysTable = column1.getSysTable();
                    SysTable otherSysTable = column2.getSysTable();

                    if (!(aSysTable == otherSysTable
                            && Props.XDB_AVOID_CORRELATED_SELF_JOINS)
                            && aSysTable.onSameNodes(otherSysTable)) {

                        // Make sure they are partitioned on
                        // the same nodes
                        joinRelationNode = aRelationNode;
                    }
                }
            }
        }
        return joinRelationNode;
    }

    /**
     * Allows for checking uncorrelated IN subqueries, to see if we can fold
     * condition in with parent, in case a parent-child relationship exists.
     *
     * @param aQC
     * @param aRelationNode
     * @return
     */
    private boolean checkParentChildInCondition(QueryCondition aQC) {

        if (aQC == null) {
            return false;
        }

        // Make sure it is an IN operation
        if (aQC.getOperator().compareTo("IN") != 0) {
            return false;
        }

        // Make sure the left hand side is an expression that is just
        // a column that is the partion column for a table.
        if (aQC.getLeftCond().getCondType() == QueryCondition.QC_SQLEXPR
                && aQC.getLeftCond().getExpr().isColumn()
                && aQC.getLeftCond().getExpr().getMapped() == SqlExpression.ORIGINAL) {
            AttributeColumn anAC = aQC.getLeftCond().getExpr().getColumn();

            if (anAC.isPartitionColumn()) {
                // parent is hashable column. Now we check the child.
                if (aQC.getRightCond().getCondType() == QueryCondition.QC_SQLEXPR
                        && aQC.getRightCond().getExpr().getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                    SqlExpression aSE = aQC.getRightCond().getExpr().getSubqueryTree().getProjectionList()
                            .get(0);

                    if (!aSE.isColumn()) {
                        return false;
                    }

                    AttributeColumn projAC = aSE.getColumn();
                    String tableName = projAC.getTableName();

                    if (projAC.isPartitionColumn()) {

                        // ok, the uncorrelated subquery is also partitioned.
                        // We have a chance for a match.
                        // Make sure they are partitioned on the same nodes
                        if (!projAC.relationNode.onSameNodes(
                                anAC.relationNode)) {
                            return false;
                        }

                        // Now we check to make sure there are not any other
                        // non-lookup tables involved.
                        if (aQC.getRightCond().getExpr().getSubqueryTree().getUnionQueryTreeList() != null
                                && aQC.getRightCond().getExpr().getSubqueryTree().getUnionQueryTreeList()
                                .size() > 0) {
                            return false;
                        }

                        if (aQC.getRightCond().getExpr().getSubqueryTree().getNoncorSubqueryList() != null
                                && aQC.getRightCond().getExpr().getSubqueryTree().getNoncorSubqueryList()
                                .size() > 0) {
                            return false;
                        }

                        for (RelationNode subqueryRelNode :
                                aQC.getRightCond().getExpr()
                                .getSubqueryTree().getRelationNodeList()) {

                            if (!subqueryRelNode.isTable()) {
                                // Don't yet check nesting
                                return false;
                            }

                            if (subqueryRelNode.getTableName()
                                    .compareToIgnoreCase(tableName) == 0) {
                                continue;
                            }

                            if (!subqueryRelNode.isLookup()) {
                                return false;
                            }
                        }

                        // looks good, we can fold it in
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Determines optimized QueryTree. QueryTree initially just has a list of
     * RelationNodes, containing all the info we need. From that, we build up
     * canidate trees using QueryNodes.
     *
     * @return Optimized QueryTree
     * @param aQueryTree Input skeleton QueryTree
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    public QueryTree determineQueryPath(QueryTree aQueryTree)
            throws XDBServerException {
        final String method = "determineQueryPath";
        logger.entering(method);
        QueryTree resultTree;
        boolean allLookups;

        try {
            // We do a series of steps to create QueryNodes from RelationNodes,
            // and get them ready for Optimization

            // We first create QueryNodes for all of the relationNodes.
            if (aQueryTree.getQueryNodeTable().isEmpty()) {
                    
                for (RelationNode subTreeNode : aQueryTree.getTopWithSubqueryList()) {

                    QueryTree subQueryTree = determineQueryPath(subTreeNode.getSubqueryTree());

                    // assign new subquery tree
                    subTreeNode.setSubqueryTree(subQueryTree);
                    subTreeNode.setEstRowsReturned(subQueryTree.getRootNode().getEstRowsReturned());
                    subTreeNode.setEstCost(subQueryTree.getRootNode().getEstCost());
                    subTreeNode.setRowsize(subQueryTree.getRootNode().getRowsize());
                    //subTreeNode.setSelectRowSize(subQueryTree.getRootNode().getSelectRowSize());
                    subTreeNode.setEstRowsReturned(subQueryTree.getRootNode().getEstRowsReturned());                        
                }

                for (RelationNode aRelationNode : aQueryTree.getRelationNodeList()) {

                    QueryNode aQueryNode = aQueryTree.newQueryNode(aRelationNode);
                    aQueryTree.addToQueryNodeTable(aQueryNode);                 
                }
            }
            
            // Now we need to go back and update the join list for all of the
            // QueryNodes.
            for (QueryNode queryNode : aQueryTree.getQueryNodeTable().values()) {
                // Get all relations that its relation nodes joins with,
                // and update joinList.
                for (RelationNode otherRelationNode : queryNode.getRelationNode().getJoinList()) {
                    queryNode.getJoinList().add(otherRelationNode.getNodeId());
                }
            }

            // Update QueryCondition join lists

            // Loop through all nodes to get their conditions
            for (QueryNode queryNode : aQueryTree.getQueryNodeTable().values()) {
                for (QueryCondition aQC : queryNode.getRelationNode().getConditionList()) {
                    // loop through and build up nodeIdList.
                    // Note that the QueryNodes use the same ids initially
                    // as their RelationNodes
                    for (RelationNode relationNode : aQC.getRelationNodeList()) {
                        aQC.getNodeIdList().add(Integer.valueOf(relationNode.getNodeId()));
                    }
                }
            }

            // Also, loop through and update all conditions from main tree
            for (QueryCondition aQC : aQueryTree.getConditionList()) {
                for (RelationNode relationNode : aQC.getRelationNodeList()) {
                    aQC.getNodeIdList().add(Integer.valueOf(relationNode.getNodeId()));
                }
            }

            // process relation subqueries next (FROM clause)
            for (RelationNode subTreeNode : aQueryTree.getRelationSubqueryList()) {
                
                // Just continue if it is our WITH around the other relation
                if (subTreeNode.getSubqueryTree() == null) {
                    continue;
                }
                
                QueryTree subQueryTree = determineQueryPath(subTreeNode.getSubqueryTree());

                // assign new subquery tree
                subTreeNode.setSubqueryTree(subQueryTree);

                // update cost and row values for optimizer calcs
                QueryNode subQueryNode = aQueryTree.getNodeById(
                        Integer.valueOf(subTreeNode.getNodeId()));
                
                if (subQueryNode == null) {
                    throw new XDBServerException("Internal error with relation subqueries");
                }

                subQueryNode.setEstRowsReturned(subQueryTree.getRootNode().getEstRowsReturned());
                subQueryNode.setEstCost(subQueryTree.getRootNode().getEstCost());
                subQueryNode.setRowsize(subQueryTree.getRootNode().getRowsize());
                subQueryNode.setSelectRowSize(subQueryTree.getRootNode().getSelectRowSize());
                subTreeNode.setEstRowsReturned(subQueryTree.getRootNode().getEstRowsReturned());
            }

            for (SqlExpression aSqlExpression : aQueryTree.getScalarSubqueryList()) {
                QueryTree subQueryTree =
                        determineQueryPath(aSqlExpression.getSubqueryTree());

                // assign new subquery tree
                aSqlExpression.setSubqueryTree(subQueryTree);
            }

            // process non-correlated subqueries
            for (java.util.Iterator<RelationNode> it = aQueryTree.getNoncorSubqueryList().iterator(); it.hasNext();) {
                RelationNode subTreeNode = it.next();
                // If it only had lookups, just fold the condition in with the
                // parent QueryCondition. Otherwise, process normally.
                allLookups = subTreeNode.getSubqueryTree().containsOnlyLookups(database);

                QueryTree subQueryTree =
                        determineQueryPath(subTreeNode.getSubqueryTree());

                subTreeNode.setSubqueryTree(subQueryTree);

                // Update cost info in parent
                QueryNode subQueryNode = aQueryTree.getNodeById(
                        Integer.valueOf(subTreeNode.getNodeId()));

                subQueryNode.setEstRowsReturned(subQueryTree.getRootNode().getEstRowsReturned());
                subQueryNode.setEstCost(subQueryTree.getRootNode().getEstCost());
                subQueryNode.setRowsize(subQueryTree.getRootNode().getRowsize());
                subQueryNode.setSelectRowSize(subQueryTree.getRootNode().getSelectRowSize());
                subTreeNode.setEstRowsReturned(subQueryTree.getRootNode().getEstRowsReturned());

                // remove ourself from the condition, since we want to remove
                // our node-- condition should be placed based on any other
                // nodes involved.

                // Enumeration condEnum = subTreeNode.conditionList.elements();

                // Right now this is kind of a pain to do-
                // We look for this node used in the parent's query condition
                // list

                QueryCondition uncorrelatedQC = null;

                for (QueryCondition aQC : aQueryTree.getConditionList()) {
                    // look for place holder node id
                    for (RelationNode aRelNode : aQC.getRelationNodeList()) {
                        if (subTreeNode.getNodeId() == aRelNode.getNodeId()) {
                            aQC.getRelationNodeList().remove(aRelNode);

                            // save for later
                            uncorrelatedQC = aQC;
                            break;
                        }
                    }

                    // look for place holder node id
                    for (Integer anInt : aQC.getNodeIdList()) {
                        if (subTreeNode.getNodeId() == anInt.intValue()) {
                            aQC.getNodeIdList().remove(anInt);
                            break;
                        }
                    }
                }

                // Also, check if we can fold it in if we are dealing with an
                // IN or NOT clause with a parent-child join
                boolean isParentChildIn = checkParentChildInCondition(
                        uncorrelatedQC);

                // If we are dealing only with lookups, just rebuild the
                // condition string, and remove from noncorSubqueryList.
                // Later when the condition is processed, it will be "pushed"
                // down to the proper target node- the appropriate join with
                // the parent (eg, IN clause), or the bottom most left node
                // in the case of EXISTS
                if (allLookups || isParentChildIn) {
                    subTreeNode.getParentNoncorExpr().setExprString(subTreeNode.getSubqueryTree()
                            .rebuildString());

                    subTreeNode.getParentNoncorExpr().setTempExpr(true);

                    // remove from list
                    it.remove();
                }
            }

            // Note: we are going to do correlated queries later, below

            // Traverse initial tree, going to each node, and determine its base
            // cost.
            // The base cost is used later to determine which table to start
            // with.

            // getInitialCost is called recursively
            // it determines the initial cost for each individual table,
            // and saves it with the node.
            getInitialCost(aQueryTree);

            // now, we do a little simplification

            // See if it is a parent table joining with its child, or a
            // table joining with a "lookup" table.
            // If that is the case, transform the tree to make a right subtree
            // join,
            // then estimate the base "cost" of the relation.
            // We do this because we *know* it will always be cheaper to join
            // this way.
            checkSpecialRelations(aQueryTree);

            logger.log(Level.DEBUG,
                    " Intermediate after checkSpecialRelations\n%0",
                    new Object[]{aQueryTree});

            candidateTreeList = new SortedLongVector();

            resultTree = buildAndEvaluateTrees(aQueryTree, candidateTreeList);

            // at the end here we have special handling for any uncorrelated
            // queries. We go back and update the QueryNode whose condition list
            // now contains the subquery. This allows us to properly track
            // temp table usage later.
            for (RelationNode subTreeNode : aQueryTree.getNoncorSubqueryList()) {
                // look for QueryCondition containing expr.
                QueryNode condNode = findConditionNode(resultTree.getRootNode(),
                        subTreeNode.getParentNoncorExpr());

                if (condNode != null) {
                    condNode.getUncorrelatedCondTreeList().add(subTreeNode);
                    // subTreeNode.subqueryTree.parentTreeCondUsageNode =
                    // condNode;
                }
            }

            // unions
            for (QueryTree unionSubTree : aQueryTree.getUnionQueryTreeList()) {

                QueryTree newUnionSubTree = determineQueryPath(unionSubTree);
                resultTree.getUnionQueryTreeList().add(newUnionSubTree);
            }

            logger.debug("QueryTree: " + resultTree.toString());

            return resultTree;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Tries to determine in which QueryNode's condition list the SqlExpression
     * is used.
     *
     * This is useful for uncorrelated query handling.
     *
     * @return the QueryNode where the condition was found.
     * @param aSE
     * @param currentNode The QueryNode from which to search for the expression
     */
    private QueryNode findConditionNode(QueryNode currentNode, SqlExpression aSE) {
        final String method = "findConditionNode";
        logger.entering(method);

        try {
            QueryNode checkNode = null;

            for (QueryCondition aQC : currentNode.getConditionList()) {
                if (aQC.containsSqlExpression(aSE)) {
                    return currentNode;
                }
            }

            if (currentNode.getLeftNode() != null) {
                checkNode = findConditionNode(currentNode.getLeftNode(), aSE);

                if (checkNode == null && currentNode.getRightNode() != null) {
                    checkNode = findConditionNode(currentNode.getRightNode(), aSE);
                }
            }

            return checkNode;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Checks to see if expression is a column in the specified table
     *
     * @param aQueryCondition
     * @param tableName
     * @return the AttributeColumn if it is a match, otherwise null
     */
    private AttributeColumn checkExpressionMatch(SqlExpression expr,
            String tableName) {

        AttributeColumn anAC = null;

        if (expr != null && expr.isColumn()) {

            anAC = expr.getColumn();
            if (!tableName.equalsIgnoreCase(anAC.getTableName())) {
                return null;
            }
        }

        return anAC;
    }

    /**
     * Searches for lookup and parent-child relations. Note that we want not
     * only parent-child relationships, but those table pairs that have data
     * partitioned on the same physical node, and whose join condition satisfies
     * that relationship.
     *
     * @param aQueryTree The QueryTree to check
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void checkSpecialRelations(QueryTree aQueryTree)
            throws XDBServerException {
        final String method = "checkSpecialRelations";
        logger.entering(method);
        RelationNode aRelationNode, otherRelationNode;

        String partitionColumn;
        AttributeColumn leftColumn, rightColumn;
        List<String> usedLookups; // track if a lookup has already been merged
        List<String> usedParentChild; // track if parent-child has already been merged

        short outerType = OUTER_PARTED_NO;

        try {
            usedLookups = new ArrayList<String>();
            usedParentChild = new ArrayList<String>();

            // We loop through two separate times, once to detect parent-child
            // relationships and single node joins,
            // then a second time to detect lookups,
            // in that order. That we, we ensure we do all parent-childs first,
            // then potentially add the lookups to the same step as well.
            for (QueryCondition cond : aQueryTree.getConditionList()) {
                if (cond.isTwoRelationJoin()) {
                    aRelationNode = cond.getRelationNodeList().get(0);
                    otherRelationNode = cond.getRelationNodeList().get(1);

                    // avoid self-joins. They don't seem to work well in
                    // PostgreSQL.
                    if (aRelationNode.getTableName().equalsIgnoreCase(
                            otherRelationNode.getTableName())) {
                        continue;
                    }

                    // If both of these have been used, don't bother checking
                    if (usedParentChild.contains(aRelationNode.getAlias())
                            && usedParentChild.contains(otherRelationNode.getAlias())) {
                        continue;
                    }

                    // don't check if "join" is on a subquery. (yet...)
                    if ((aRelationNode.isSubquery() && !aRelationNode.isWithDerived())
                            || (otherRelationNode.isSubquery() && !otherRelationNode.isWithDerived())) {
                        continue;
                    }

                    // don't check if "join" is on correlated subquery
                    if (aRelationNode.isCorrelatedPlaceholder()
                            || otherRelationNode.isCorrelatedPlaceholder()) {
                        continue;
                    }

                    if (aRelationNode.isHashPartitioned()
                            && otherRelationNode.isHashPartitioned()) {

                        // Make sure we are just dealing with simple columns
                        if (cond.getExpr() == null
                                || cond.getExpr().getLeftExpr() == null
                                || cond.getExpr().getRightExpr() == null) {
                            continue;
                        }

                        if (cond.getExpr().getLeftExpr().isColumn()) {
                            leftColumn = cond.getExpr().getLeftExpr()
                                    .getColumn();
                        } else {
                            continue;
                        }

                        if (cond.getExpr().getRightExpr().isColumn()) {
                            rightColumn = cond.getExpr().getRightExpr()
                                    .getColumn();
                        } else {
                            continue;
                        }

                        // Make sure they are partitioned on the same nodes
                        if (!aRelationNode.onSameNodes(otherRelationNode)) {
                            continue;
                        }

                        // do partition check
                        partitionColumn = aRelationNode.getPartitionColumnName();

                        // See if we are dealing with 2 tables partitioned on
                        // the same column
                        if (partitionColumn != null
                                && partitionColumn.length() > 0) {

                            if (checkAndMergePartitionedColumns(
                                    aRelationNode, otherRelationNode,
                                    aQueryTree, partitionColumn,
                                    usedParentChild,
                                    leftColumn, rightColumn)) {

                                continue;
                            }

                            //Now try flipping the columns
                            if (checkAndMergePartitionedColumns(
                                    aRelationNode, otherRelationNode,
                                    aQueryTree, partitionColumn,
                                    usedParentChild,
                                    rightColumn, leftColumn)) {

                                continue;
                            }
                        }
                    } else if (aRelationNode.isOnSingleNode()
                            && otherRelationNode.isOnSingleNode()) {

                        // Make sure they are on the same nodes
                        // amart tables are on the same nodes if and only if
                        // partition maps are equal
                        if (aRelationNode.getPartitionMap().equals(
                                otherRelationNode.getPartitionMap())) {
                            // Check outers to see if it is ok to process
                            // together.
                            outerType = preMergeOuterCheck(aRelationNode,
                                    otherRelationNode, aQueryTree);

                            if (outerType != OUTER_PARTED_NO) {
                                mergeNodes(aQueryTree, otherRelationNode,
                                        aRelationNode, outerType);
                            }
                        }
                    }
                }
            }

            // Now check lookups
            // Some lookups might be used multiple times, so we
            // need to choose which other relation to join it with.
            // We first go through and collect info
            Map<RelationNode, List<LookupCandidate>> lookupMap = new HashMap<RelationNode, List<LookupCandidate>>();

            for (QueryCondition cond : aQueryTree.getConditionList()) {

                if (cond.isTwoRelationJoin()) {
                    aRelationNode = cond.getRelationNodeList().get(0);
                    otherRelationNode = cond.getRelationNodeList().get(1);

                    // don't check if "join" is on a subquery. (yet...)
                    if (aRelationNode.isSubquery()
                            || otherRelationNode.isSubquery()) {
                        continue;
                    }

                    // don't check if "join" is on correlated subquery
                    if (aRelationNode.isCorrelatedPlaceholder()
                            || otherRelationNode.isCorrelatedPlaceholder()) {
                        continue;
                    }

                    if (aRelationNode.isLookup()
                            && !usedLookups.contains(aRelationNode.getAlias())) {

                        // Check outers to see if it is ok to process together.
                        outerType = preMergeOuterCheck(aRelationNode,
                                otherRelationNode, aQueryTree);

                        // only prcoess if they are on the same level,
                        // or if they are on different levels, they
                        // the relation node is on a higher level (inner table)
                        if (outerType != OUTER_PARTED_NO
                                && !(outerType == OUTER_PARTED_DIFFERENT && aRelationNode
                                .getOuterLevel() < otherRelationNode
                                .getOuterLevel())) {
                            List<LookupCandidate> vConditions = lookupMap.get(aRelationNode);

                            if (vConditions == null) {
                                vConditions = new ArrayList<LookupCandidate>();
                            }

                            LookupCandidate aLookupCand = new LookupCandidate(
                                    aRelationNode, otherRelationNode, cond,
                                    outerType);

                            vConditions.add(aLookupCand);
                            lookupMap.put(aRelationNode, vConditions);
                        }
                    } else if (otherRelationNode.isLookup()
                            && !usedLookups.contains(otherRelationNode.getAlias())) {
                        outerType = preMergeOuterCheck(aRelationNode,
                                otherRelationNode, aQueryTree);

                        // only prcoess if they are on the same level,
                        // or if they are on different levels, they
                        // the relation node is on a higher level (inner table)
                        if (outerType != OUTER_PARTED_NO
                                && !(outerType == OUTER_PARTED_DIFFERENT && otherRelationNode
                                .getOuterLevel() < aRelationNode
                                .getOuterLevel())) {
                            List<LookupCandidate> vConditions = lookupMap
                                    .get(otherRelationNode);

                            if (vConditions == null) {
                                vConditions = new ArrayList<LookupCandidate>();
                            }
                            LookupCandidate aLookupCand = new LookupCandidate(
                                    aRelationNode, otherRelationNode, cond,
                                    outerType);
                            vConditions.add(aLookupCand);
                            lookupMap.put(otherRelationNode, vConditions);
                        }
                    }
                }
            }

            // Iterate over the lookups that we stored.
            // If anyone is used multiple times, we evaluate best choice
            for (Map.Entry<RelationNode, List<LookupCandidate>> entry : lookupMap.entrySet()) {
                RelationNode lookupRelationNode = entry.getKey();
                List<LookupCandidate> vConditions = entry.getValue();

                // If there is only 1, just choose it
                if (vConditions.size() == 1) {
                    LookupCandidate aLookup = vConditions.get(0);
                    mergeNodes(aQueryTree, aLookup.otherRelationNode,
                            aLookup.relationNode, aLookup.outerType);
                } else {
                    // We need to choose which one to use
                    LookupCandidate bestCandidate = null;

                    for (LookupCandidate aCandidate : vConditions) {

                        double bestSelectivity = -1;
                        AttributeColumn anAC = null;

                        SysTable sysTable = lookupRelationNode.getSysTable();

                        if (aCandidate.queryCondition.getExpr() != null) {
                            anAC = checkExpressionMatch(
                                    aCandidate.queryCondition.getExpr().getLeftExpr(),
                                    sysTable.getTableName());

                            // check other part of condition
                            if (anAC == null) {
                                anAC = checkExpressionMatch(
                                        aCandidate.queryCondition.getExpr().getRightExpr(),
                                        sysTable.getTableName());
                            }
                        }

                        if (anAC != null) {
                            // ok, we have a bonafide candidate here,
                            // examine it.
                            if (sysTable.isPrimaryKey(anAC.columnName)
                                    || sysTable.isUniqueIndex(anAC.columnName)) {
                                // We want to use this one
                                mergeNodes(aQueryTree,
                                        aCandidate.otherRelationNode,
                                        aCandidate.relationNode,
                                        aCandidate.outerType);
                                bestCandidate = null;
                                break;
                            }

                            // we see if this is the best candidate so far.
                            // We want the most selective one
                            // (lowest selectivity factor)
                            double selectivity = anAC.getSysColumn(database)
                                    .getSelectivity();

                            if (bestSelectivity == -1
                                    || selectivity < bestSelectivity) {
                                bestCandidate = aCandidate;
                                bestSelectivity = selectivity;
                            }
                        }
                    }

                    if (bestCandidate != null) {
                        mergeNodes(aQueryTree, bestCandidate.otherRelationNode,
                                bestCandidate.relationNode,
                                bestCandidate.outerType);
                    }
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * If the nodes are similarly partitioned, merge them here
     *
     * @param aRelationNode,
     * @param otherRelationNode,
     * @param aQueryTree,
     * @param partitionColumn,
     * @param usedParentChild,
     * @param column1,
     * @param column2
     *
     * @return true, if we merged
     */
    private boolean checkAndMergePartitionedColumns(
            RelationNode aRelationNode,
            RelationNode otherRelationNode,
            QueryTree aQueryTree,
            String partitionColumn,
            List<String> usedParentChild,
            AttributeColumn column1,
            AttributeColumn column2) {

        if (column1.getTableName().equalsIgnoreCase(
                aRelationNode.getTableName())
                && column1.columnName.equalsIgnoreCase(partitionColumn)) {

            // ok, we might have a match. check other side
            String otherPartitionColumn = otherRelationNode.getPartitionColumnName();

            if (column2.getTableName().equalsIgnoreCase(
                    otherRelationNode.getTableName())
                    && column2.columnName.equalsIgnoreCase(otherPartitionColumn)) {
                // Check outers to see if it is ok to
                // process together.
                short outerType = preMergeOuterCheck(
                        aRelationNode, otherRelationNode, aQueryTree);

                if (outerType != OUTER_PARTED_NO) {
                    mergeNodes(aQueryTree, otherRelationNode,
                            aRelationNode, outerType);

                    usedParentChild.add(aRelationNode.getAlias());
                    usedParentChild.add(otherRelationNode.getAlias());
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Merges (joins) two QueryNodes, based on their respective corresponding
     * QueryNodes.
     *
     * @param aQueryTree The QueryTree we are processing
     * @param aRelationNode first RelationNode to join
     * @param otherRelationNode other RelationNode to join
     * @param outerType outer type info to take into account
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void mergeNodes(QueryTree aQueryTree, RelationNode aRelationNode,
            RelationNode otherRelationNode, short outerType)
            throws XDBServerException {
        final String method = "mergeNodes";
        logger.entering(method);
        try {
            QueryNode aQueryNode = aQueryTree.getNodeById(Integer.valueOf(
                    aRelationNode.getNodeId()));

            QueryNode otherQueryNode = aQueryTree.getNodeById(
                    Integer.valueOf(otherRelationNode.getNodeId()));

            while (aQueryNode.getParent() != null) {
                aQueryNode = aQueryNode.getParent();
            }

            while (otherQueryNode.getParent() != null) {
                otherQueryNode = otherQueryNode.getParent();
            }

            if (aQueryNode != otherQueryNode) {
                mergeNodes(aQueryTree, aQueryNode, otherQueryNode, outerType);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Merges (joins) two QueryNodes. A new QueryNode is created with the other
     * input nodes become its left and right nodes in the tree.
     *
     * @param aQueryNode
     * @param otherQueryNode
     * @param aQueryTree The QueryTree we are processing
     * @param outerType outer type info to take into account
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void mergeNodes(QueryTree aQueryTree, QueryNode aQueryNode,
            QueryNode otherQueryNode, short outerType)
            throws XDBServerException {
        final String method = "mergeNodes";
        logger.entering(method);
        try {

            QueryNode newJoinNode = aQueryTree.newQueryNode();

            // Get new join cost (populates newJoinNode)
            estimateJoinCost(aQueryTree, aQueryNode, otherQueryNode,
                    newJoinNode);

            // Try combining the other way to see if it is cheaper.
            // We take the cheapest one, because the underlying db should do
            // it the smarter way
            QueryNode newJoinNode2 = aQueryTree.newQueryNode();

            estimateJoinCost(aQueryTree, otherQueryNode, aQueryNode,
                    newJoinNode2);

            // do lower cost first, unless it has a lower outer level
            if (newJoinNode2.getEstCost() < newJoinNode.getEstCost()
                    && aQueryNode.getOuterLevel() <= otherQueryNode.getOuterLevel()) {
                newJoinNode.setEstCost(newJoinNode2.getEstCost());
                newJoinNode.setEstRowsReturned(newJoinNode2.getEstRowsReturned());
                newJoinNode.setRightNode(otherQueryNode);
                newJoinNode.setLeftNode(aQueryNode);
            } else {
                newJoinNode.setRightNode(aQueryNode);
                newJoinNode.setLeftNode(otherQueryNode);
            }
            newJoinNode.getLeftNode().setParent(newJoinNode);
            newJoinNode.getRightNode().setParent(newJoinNode);
            newJoinNode.setNodeType(QueryNode.JOIN);
            newJoinNode.setPreserveSubtree(true);

            if (outerType == OUTER_PARTED_SAME_ONE) {
                newJoinNode.setSubtreeOuter(true);
                newJoinNode.setOuterLevel(aQueryNode.getOuterLevel());
                newJoinNode.setOuterGroup(aQueryNode.getOuterGroup());
            }

            if (outerType == OUTER_PARTED_DIFFERENT) {
                if (newJoinNode.getLeftNode().getOuterLevel() > newJoinNode.getRightNode().getOuterLevel()) {
                    // switch these
                    newJoinNode.setRightNode(otherQueryNode);
                    newJoinNode.setLeftNode(aQueryNode);
                }
                newJoinNode.getRightNode().setSubtreeOuter(true);
                newJoinNode.getLeftNode().setSubtreeOuter(true);
                newJoinNode.setOuterLevel(newJoinNode.getLeftNode().getOuterLevel());
                newJoinNode.setOuterGroup(newJoinNode.getLeftNode().getOuterGroup());
            }

            // adjust Join list
            adjustSubtreeJoins(newJoinNode, aQueryTree);

            // Now update the list of available nodes in QueryTree
            // We just want the new parent "available"
            aQueryTree.addToQueryNodeTable(newJoinNode);

            // only remove if we combined a node that has children.
            if (aQueryNode.getLeftNode() != null) {
                aQueryTree.getQueryNodeTable()
                        .remove(Integer.valueOf(aQueryNode.getNodeId()));
            }
            if (otherQueryNode.getLeftNode() != null) {
                aQueryTree.getQueryNodeTable().remove(Integer.valueOf(
                        otherQueryNode.getNodeId()));
            }

            newJoinNode.setRowReductionFactor(newJoinNode.getRightNode().getRowReductionFactor()
                    * newJoinNode.getLeftNode().getRowReductionFactor());
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Make sure that it is ok to merge two nodes that are joined on
     * partitioning key. <br> It should only allow them to be processed together
     * under the following scenarios:<br> 1.) Both tables are on level 0.<br>
     * 2.) Both are on the same level, and only one of the tables joins with a
     * table on a lower level.<br> 3.) Each are on a different level, separated
     * by 1, and no other tables are in their respective outer groups.<br>
     *
     * <br> If none of these scenarios apply, these should not be processed
     * together.
     *
     * @param node1 RelationNode to check
     * @param node2 other RelationNode to check
     * @param aQueryTree the QueryTree we are processing
     * @return the outer type - OUTER_PARTED_NO (0) - Do not process together,
     * OUTER_PARTED_LEVEL_0 (1) - both are on level 0, OUTER_PARTED_SAME_ONE (2)
     * - both are on same level, OUTER_PARTED_DIFFERENT (3) - these are on
     * different levels
     */
    private short preMergeOuterCheck(RelationNode node1, RelationNode node2,
            QueryTree aQueryTree) {
        final String method = "preMergeOuterCheck";
        logger.entering(method);
        try {
            short checkState;
            logger.debug(" outerLevel 1: " + node1.getOuterLevel());
            logger.debug(" outerLevel 2: " + node2.getOuterLevel());

            // 1.) Both tables are on level 0.
            if (node1.getOuterLevel() == 0 && node2.getOuterLevel() == 0) {
                return OUTER_PARTED_LEVEL_0; // ok to merge
                
            } else if (node1.getOuterLevel() == node2.getOuterLevel()) {
                 // 2.) Both are on the same level, and only one of the tables joins
                 // with a table on a lower level.
                 // OR
                 // At least one is from the base node
                checkState = 0;

                // Check node1's joins
                for (int i = 0; i < node1.getJoinList().size(); i++) {
                    RelationNode testRelationNode = node1.getJoinList().get(i);

                    if (testRelationNode == node2) {
                        // this is the join condition we already know about
                        break;
                    }

                    // We have a new join condition
                    // Compare levels
                    if (testRelationNode.getOuterLevel() != node1
                            .getOuterLevel()) {
                        if (node1.getOuterLevel()
                                - testRelationNode.getOuterLevel() != 1) {
                            // don't bother- continue
                            continue;
                        }

                        if (checkState == 0) {
                            // Note that at least one node joins with
                            // immediately
                            // preceeding level
                            checkState = 1;
                            break;
                        }
                    }
                }

                // Check node1's joins
                for (int i = 0; i < node2.getJoinList().size(); i++) {
                    RelationNode testRelationNode = node2.getJoinList().get(i);

                    if (testRelationNode == node1) {
                        // this is the join condition we already know about
                        break;
                    }

                    // We have a new join condition
                    // Compare levels
                    if (testRelationNode.getOuterLevel() != node2
                            .getOuterLevel()) {
                        if (node2.getOuterLevel()
                                - testRelationNode.getOuterLevel() != 1) {
                            // don't bother- continue
                            continue;
                        }

                        if (checkState == 1) {
                            // Both nodes join with previous level, not safe to
                            // join here
                            return 0;
                        }

                        if (checkState == 0) {
                            checkState = 2;
                        }
                    }
                }

                if (checkState == 1 || checkState == 2) {
                    // Only one of them joins with previous level,
                    // It is safe to process them together
                    return OUTER_PARTED_SAME_ONE;
                }

                if (checkState == 0) {
                    // Cartesian product? Is it safe?
                    return OUTER_PARTED_NO;
                }
            } else if (Math.abs(node1.getOuterLevel() - node2.getOuterLevel()) == 1) {
                // 3.) Each are on a different level, separated by 1, and no other
                // tables are in their respective outer groups.

                // Check to make sure there are no other nodes in their
                // respective groups
                for (int i = 0; i < aQueryTree.getRelationNodeList().size(); i++) {
                    RelationNode aRelationNode = aQueryTree.getRelationNodeList()
                            .get(i);

                    if (aRelationNode.getOuterLevel() == node1.getOuterLevel()) {
                        if (aRelationNode == node1) {
                            // ok, skip
                            continue;
                        } else {
                            // there are other RelationNodes in our group,
                            // forget doing together
                            return OUTER_PARTED_NO;
                        }
                    }

                    if (aRelationNode.getOuterLevel() == node2.getOuterLevel()) {
                        if (aRelationNode == node2) {
                            // ok, skip
                            continue;
                        } else {
                            // there are other RelationNodes in our group,
                            // forget doing together
                            return OUTER_PARTED_NO;
                        }
                    }
                }

                // If we reach this point, it is safe to do these together on
                // different levels.
                return OUTER_PARTED_DIFFERENT;

            } else if (node1.getOuterLevel() == 0 || node2.getOuterLevel() == 0) {
                // One of them is the base node, but is already joining with 
                // another table. Make sure the other node does not join
                // with any other table
                RelationNode testRelationNode;
                
                if (node1.getOuterLevel() == 0) {
                    testRelationNode = node2;
                } else {
                    testRelationNode = node1;
                }
                
                if (testRelationNode.getJoinList().size() == 1) {
                    return OUTER_PARTED_DIFFERENT;
                }
                return OUTER_PARTED_NO;
            }

            // Do not process together
            return OUTER_PARTED_NO;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * We track which QueryNodes in the tree are available to join with which
     * other nodes. This is updated as the QueryTree is built.
     *
     * @param movingNode The QueryNode whose join info should be udpated
     * @param aQueryTree The current QueryTree
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void adjustSubtreeJoins(QueryNode movingNode, QueryTree aQueryTree)
            throws XDBServerException {
        final String method = "adjustSubtreeJoins";
        logger.entering(method);
        try {

            adjustSubtreeJoinsDetail(movingNode, movingNode.getLeftNode(),
                    movingNode.getRightNode(), aQueryTree);

            adjustSubtreeJoinsDetail(movingNode, movingNode.getRightNode(),
                    movingNode.getLeftNode(), aQueryTree);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * We track which QueryNodes in the tree are available to join with which
     * other nodes. This is updated as the QueryTree is built.
     *
     * @param movingNode The QueryNode whose join info should be udpated
     * @param node1 first node being joined
     * @param node2 second node being joined
     * @param aQueryTree The current QueryTree
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void adjustSubtreeJoinsDetail(QueryNode movingNode,
            QueryNode node1,
            QueryNode node2,
            QueryTree aQueryTree)
            throws XDBServerException {
        final String method = "adjustSubtreeJoinsDetail";
        logger.entering(method);

        int otherJoinNodeId;
        int refNodeId;
        boolean nodeFound = false;

        // update the "join conditions" available to the new subtree
        for (int i = 0; i < node1.getJoinList().size(); i++) {
            otherJoinNodeId = node1.getJoinList().get(i)
                    .intValue();

            if (otherJoinNodeId == node2.getNodeId()) {
                continue; // skip, we don't want to add
            }

            movingNode.getJoinList().add(otherJoinNodeId);

            // change opposite reference on other node.
            nodeFound = false;

            QueryNode otherJoinNode = aQueryTree.getNodeById(otherJoinNodeId);

            for (int j = 0; j < otherJoinNode.getJoinList().size(); j++) {
                refNodeId = otherJoinNode.getJoinList().get(j)
                        .intValue();

                if (refNodeId == node1.getNodeId()) {
                    // replace on list
                    otherJoinNode.getJoinList().remove(j);
                    otherJoinNode.getJoinList().add(movingNode.getNodeId());
                    nodeFound = true;
                    break;
                }
            }

            if (!nodeFound) {
                String errorMessage = ErrorMessageRepository.REFERENCE_NODE_NOTFOUND;
                throw new XDBServerException(errorMessage + " :"
                        + node1.getNodeId(), 0,
                        ErrorMessageRepository.REFERENCE_NODE_NOTFOUND_CODE);
            }
        }
    }

    /**
     * This gets the initial cost and sizing information for each individual
     * table, before it is joined with any other tables.
     *
     * @param aQueryTree The QueryTree for which to get initial cost.
     */
    // -------------------------------------------------------------------------
    private void getInitialCost(QueryTree aQueryTree) {
        final String method = "getInitialCost";
        logger.entering(method);
        try {
            long numRows, estRowsReturned, estBaseReadCost;
            long testEstBaseReadCost;
            QueryCondition cond;
            SqlExpression proj;
            double rowReductionFactor;
            SysColumn aSysColumn;
            long cost;
            int selectRowSize;
            AttributeColumn compColumn;
            RelationNode currRelNode;
            long numPages;
            for (QueryNode currentNode : aQueryTree.getQueryNodeTable().values()) {

                if (currentNode.isTable()) {
                    currRelNode = currentNode.getRelationNode();

                    logger.debug("table: " + currRelNode.getTableName());

                    SysTable aSysTable = currRelNode.getSysTable();

                    // get the number of rows
                    // numRows =
                    // MetaDataMaster.getRowCount(currRelNode.tableName);
                    numRows = aSysTable.getRowCount();

                    aSysTable.getSubtableCount();

                    // see if any conditions exist on select,
                    // to reduce returned set, and speed up access

                    // Try and take statistics into account later below
                    // If none, we assume that if a condition exists, it reduces
                    // the returned dataset's number of rows to 10%.

                    // Initialize We may further reduce the factor later below

                    rowReductionFactor = 1;
                    numPages = 0;
                    if (aSysTable.getEstRowsPerPage() > 0) {
                        numPages = Math.round(numRows
                                / aSysTable.getEstRowsPerPage());
                    }

                    if (numPages == 0) {

                        numPages = 1;
                    }

                    estBaseReadCost = numPages
                            + Math.round(PROC_FACTOR * numRows);

                    // We still do loop through all conditions to see if it is
                    // on an index, to estimate the costs of reading the table

                    // We may also update estRowsReturned as a result.

                    // These are all assumed to be ANDed at this level,
                    // though a condition may have an OR as part of its
                    // expression

                    for (int i = 0; i < currRelNode.getConditionList().size(); i++) {
                        cond = currRelNode.getConditionList()
                                .get(i);

                        // see if we are dealing with a simple condition, eg,
                        // FirstName = 'fred'
                        if (cond.isAtomic()) {
                            if (cond.getLeftCond().getExpr().isColumn()) {
                                compColumn = cond.getLeftCond().getExpr().getColumn();
                            } else if (cond.getRightCond().getExpr().isColumn()) {
                                compColumn = cond.getRightCond().getExpr().getColumn();
                            } else {
                                continue;
                            }

                            aSysColumn = compColumn.getSysColumn(database);

                            testEstBaseReadCost = calculateIndexCost(
                                    aSysColumn, numRows);

                            double subtableReductionFactor = 1.0;
                            // We check if we have subtables, and if the
                            // partitioning
                            // expression is based on this column
                            for (SysCheck aSysCheck : aSysTable.getSysChecks()) {

                                if (aSysCheck.getCheckDef().indexOf(
                                        compColumn.columnName) >= 0) {
                                    subtableReductionFactor = 1.0 / Math
                                            .sqrt(aSysTable.getSubtableCount());
                                }
                            }

                            testEstBaseReadCost *= subtableReductionFactor;

                            if (testEstBaseReadCost >= 0
                                    && testEstBaseReadCost < estBaseReadCost) {
                                estBaseReadCost = testEstBaseReadCost;
                            }

                            if (aSysColumn.getSelectivity() > 0) {
                                rowReductionFactor = rowReductionFactor
                                        * aSysColumn.getSelectivity();
                            } else {
                                if (rowReductionFactor == 1) {
                                    rowReductionFactor *= 0.10 * subtableReductionFactor;
                                } else {
                                    // rowReductionFactor /= 2;
                                    rowReductionFactor *= 0.10 * subtableReductionFactor;
                                }
                            }
                        } else {
                            // no info available, just guess and reduce by 1/10
                            // for =, or 1/3 for other operators.
                            if (cond.getOperator() == "=") {
                                rowReductionFactor /= 10;
                            } else {
                                rowReductionFactor /= 3;
                            }
                        }
                    }

                    estRowsReturned = (long) (numRows * rowReductionFactor);

                    if (estRowsReturned == 0) {
                        estRowsReturned = 1;
                    }

                    // We also need to estimate the size of the rows returned,
                    // to concern ourselves
                    // with network bandwidth.

                    selectRowSize = 1;

                    for (int i = 0; i < currRelNode.getProjectionList().size(); i++) {
                        // Note that a project value may also be an
                        // expression,
                        // eg. cost * quantity
                        proj = currRelNode.getProjectionList().get(i);

                        selectRowSize += proj.getExprDataType().getByteLength();
                    }

                    // TODO: need to later look for groups/sums

                    cost = estBaseReadCost;

                    // now, save this info in node.
                    currRelNode.setRowsize(aSysTable.getRowSize());
                    currRelNode.setEstCost(cost);
                    currRelNode.setEstRowsReturned(estRowsReturned);

                    currentNode.setRowsize(currRelNode.getRowsize());
                    currentNode.setSelectRowSize(selectRowSize);
                    currentNode.setBaseNumRows(numRows);
                    currentNode.setEstCost(currRelNode.getEstCost());
                    currentNode.setEstRowsReturned(currRelNode.getEstRowsReturned());

                    currentNode.setRowReductionFactor(rowReductionFactor);
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Calculates estimated cost of reading based on available indexes for the
     * column.
     *
     * @param aSysColumn The column to check
     * @param numTableRows The number of rows affected
     * @return
     */
    private long calculateIndexCost(SysColumn aSysColumn, long numTableRows) {
        final String method = "calculateIndexCost";
        logger.entering(method);
        try {

            long estReadCost = -1;
            long numPages; // number of pages in the index

            if (aSysColumn.bestIndex != null) {
                numPages = numTableRows / aSysColumn.bestIndex.estRowsPerPage;

                if (aSysColumn.bestIndexColPos == 1) {
                    if (aSysColumn.getSelectivity() > 0) {
                        estReadCost = Math.round(numPages
                                * aSysColumn.getSelectivity())
                                + Math.round(PROC_FACTOR * numTableRows);
                    } else {
                        estReadCost = Math.round(numPages * 0.10)
                                + Math.round(PROC_FACTOR * numTableRows);
                    }
                } else {
                    // Since our column is not the first in the index,
                    // we have to scan the index pages,
                    // but there are fewer of these than table pages
                    // (Table scan may still be better sometimes though)
                    if (aSysColumn.getSelectivity() > 0) {
                        estReadCost = numPages
                                + Math.round(numTableRows
                                * aSysColumn.getSelectivity())
                                // approx. table fetches
                                + Math.round(PROC_FACTOR * numTableRows);
                    } else {
                        estReadCost = numPages + Math.round(numTableRows * .10)
                                // approx. table fetches
                                + Math.round(PROC_FACTOR * numTableRows);
                    }
                }
            }

            return estReadCost;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Calculates the cost of joining by trying to take index info into account
     *
     * @return The cost of the join
     * @param columnName
     * @param node1 QueryNode representing "outer" part of join
     * @param tableName Table we are joining with
     * @param innerNumPages Number of data pages for the table we will join with
     * @param innerNumTableRows Number of rows in the table we will join with.
     * @param estRowsLookup Estimated number of rows that we will have to lookup
     */
    private long calculateIndexJoinCost(QueryNode node1, String tableName,
            String columnName, long innerNumPages, long innerNumTableRows,
            long estRowsLookup) {
        final String method = "calculateIndexJoinCost";
        logger.entering(method);
        try {

            SysColumn aSysColumn;
            long resultCardinality;
            double selectivity;
            long estReadCost = -1;
            long numPages; // number of pages in the index
            long outerNumPages;
            long baseCost;
            // if joining on partitioned column, reduce join cost
            double partReduction = 1.0;

            SysTable aSysTable = database.getSysTable(tableName);
            aSysColumn = aSysTable.getSysColumn(columnName);
            selectivity = aSysColumn.getSelectivity();

            // check to see if we are joining with a partitioned column
            if (aSysColumn == aSysTable.getPartitionedColumn()) {
                partReduction = 1.0 / ((Collection) aSysTable.getNodeList())
                        .size();
            }

            // this should really be set, but do this just in case...
            if (selectivity == 0) {
                resultCardinality = estRowsLookup > innerNumTableRows ? estRowsLookup
                        : innerNumTableRows;
                selectivity = 0.1;
            } else {
                resultCardinality = Math.round(estRowsLookup
                        * innerNumTableRows * selectivity);
            }

            if (aSysColumn.bestIndex != null) {
                numPages = innerNumTableRows
                        / aSysColumn.bestIndex.estRowsPerPage;

                outerNumPages = Math.round(estRowsLookup * node1.getRowsize()
                        / SysTable.PAGE_SIZE);

                if (aSysColumn.bestIndexColPos == 1) {

                    if (aSysColumn.bestIndex.idxtype == 'U'
                            || aSysColumn.bestIndex.idxtype == 'P') {

                        // we multiply by 2 - once for the index fetch, once for
                        // the table
                        estReadCost = outerNumPages
                                + Math.round(2 * estRowsLookup * partReduction)
                                + Math.round(PROC_FACTOR * resultCardinality
                                * partReduction);
                    } else {
                        estReadCost = +Math.round(4 * estRowsLookup
                                * partReduction)
                                + Math.round(PROC_FACTOR * resultCardinality
                                * partReduction);
                    }
                } else {
                    // using a composite index where column is not first
                    // element. Index is scanned
                    estReadCost = outerNumPages
                            + Math.round(estRowsLookup * numPages
                            * partReduction) // index page cost
                            + Math.round(estRowsLookup * selectivity
                            * partReduction) // data page accesses
                            + Math.round(PROC_FACTOR * resultCardinality
                            * partReduction);
                }

                // innerNumPages = Math.round(node2.baseNumRows /
                // innerRowsPerPage);
                baseCost = outerNumPages
                        + Math.round(node1.getEstRowsReturned() * innerNumPages
                        * partReduction)
                        + Math.round(PROC_FACTOR * resultCardinality
                        * partReduction);

                if (baseCost < estReadCost) {
                    estReadCost = baseCost;
                }
            }

            return estReadCost;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Estimates the cost of joining 2 QueryNdes. This is used when building up
     * various candidate QueryTrees
     *
     * @param aQueryTree The QueryTree we are building up
     * @param node1 Existing QueryNode to join
     * @param node2 Candidate QueryNode to join with
     * @param saveNode The new QueryNode joining node that joins the nodes
     */
    private void estimateJoinCost(QueryTree aQueryTree, QueryNode node1,
            QueryNode node2, QueryNode saveNode) {
        final String method = "estimateJoinCost";
        logger.entering(method);
        try {

            long estRowsReturned, estBaseReadCost;
            long joiningCost;
            long testJoiningCost;
            QueryCondition cond;
            int rowsize = 0;
            float rowsPerPage = 0;
            int numPages;
            AttributeColumn leftColumn, rightColumn;
            SysColumn aSysColumn = null;
            QueryNode otherSubtreeNode = null;
            boolean otherNodeOnNode1 = false;
            long testRowsReturned;

            // keep these on same line for build process!
            if (node1.getNodeType() == QueryNode.RELATION) {
                logger.debug(" - node1.tableName: "
                        + node1.getRelationNode().getTableName());
            }
            if (node2.getNodeType() == QueryNode.RELATION) {
                logger.debug(" - node2.tableName: "
                        + node2.getRelationNode().getTableName());
            }

            // We still do loop through all conditions to see if it is
            // on an index, to estimate the costs of reading the table

            // We may also update estRowsReturned as a result.

            // These are all assumed to be ANDed at this level,
            // though a condition may have an OR as part of its expression
            // TODO: add OR handling

            // Set base values we will need

            // Note that node2 may be a subtree of "special" nodes already
            // combined

            if (node2.getRowsize() <= 0) {
                node2.setRowsize(4);
            }
            rowsPerPage = SysTable.PAGE_SIZE / node2.getRowsize();
            numPages = Math.round(node2.getBaseNumRows() / rowsPerPage);

            if (numPages == 0) {
                numPages = 1;
            }

            joiningCost = 0;
            estRowsReturned = 0;
            // Now check joins from main tree
            for (int i = 0; i < aQueryTree.getConditionList().size(); i++) {
                testJoiningCost = 0;
                cond = aQueryTree.getConditionList().get(i);

                logger.debug(" - checking cond: " + cond.getCondString());

                if (cond.isJoin()) {
                    if (cond.getLeftCond() != null
                            && cond.getLeftCond().getCondType() == QueryCondition.QC_SQLEXPR
                            && cond.getLeftCond().getExpr().isColumn()) {
                        leftColumn = cond.getLeftCond().getExpr().getColumn();
                    } else {
                        continue;
                    }

                    if (cond.getRightCond() != null
                            && cond.getRightCond().getCondType() == QueryCondition.QC_SQLEXPR
                            && cond.getRightCond().getExpr().isColumn()) {
                        rightColumn = cond.getRightCond().getExpr().getColumn();
                    } else {
                        continue;
                    }

                    if ((leftColumn.columnGenre & AttributeColumn.MAPPED) != 0
                            || (rightColumn.columnGenre & AttributeColumn.MAPPED) != 0) {
                        continue;
                    }

                    aSysColumn = null;

                    // Left node columns will currently
                    // already have been folded into a temp table in the
                    // current implementation, so we do
                    // NOT want to take it into consideration when costing.
                    otherNodeOnNode1 = false;

                    if (node2.subtreeFind(leftColumn.getTableName(),
                            leftColumn.getTableAlias()) != null) {
                        otherSubtreeNode = node1.subtreeFind(rightColumn
                                .getTableName(), rightColumn.getTableAlias());

                        // Make sure it is joining with the right
                        if (otherSubtreeNode != null
                                && !leftColumn.relationNode.isRelationSubquery()) {

                            aSysColumn = leftColumn.getSysColumn(database);

                            testJoiningCost = calculateIndexJoinCost(node1,
                                    leftColumn.getTableName(),
                                    leftColumn.columnName, numPages,
                                    node2.getEstRowsReturned(),
                                    node1.getEstRowsReturned());
                        }
                    }

                    if (node2.subtreeFind(rightColumn.getTableName(),
                            rightColumn.getTableAlias()) != null) {
                        otherSubtreeNode = node1.subtreeFind(leftColumn
                                .getTableName(), leftColumn.getTableAlias());

                        // Make sure it is joining with the left
                        if (otherSubtreeNode != null) {
                            otherNodeOnNode1 = true;
                            if (!rightColumn.relationNode.isRelationSubquery()) {
                                aSysColumn = rightColumn.getSysColumn(database);

                                testJoiningCost = calculateIndexJoinCost(node1,
                                        rightColumn.getTableName(),
                                        rightColumn.columnName, numPages,
                                        node2.getEstRowsReturned(),
                                        node1.getEstRowsReturned());
                            }
                        }
                    }

                    // did not find a condition
                    if (aSysColumn == null) {
                        continue;
                    }

                    // skip if no index on it
                    if (testJoiningCost == -1) {
                        // but first reduce by selectivity
                        if (joiningCost > 0) {
                            // just further reduce estRowsReturned
                            estRowsReturned *= aSysColumn.getSelectivity();
                        }
                        continue;
                    }

                    if (testJoiningCost <= joiningCost || joiningCost == 0) {

                        joiningCost = testJoiningCost;
                        estRowsReturned = 0;

                        // test if join column is unique, use the other number
                        // of rows
                        if (aSysColumn.bestIndex != null) {
                            if ((aSysColumn.bestIndex.idxtype == 'U' || aSysColumn.bestIndex.idxtype == 'P')
                                    && aSysColumn.bestIndex.keycnt == 1
                                    && node2.getNodeType() == QueryNode.RELATION) {
                                if (otherNodeOnNode1) {
                                    estRowsReturned = node2.getEstRowsReturned();
                                } else {
                                    estRowsReturned = node1.getEstRowsReturned();
                                }
                            }
                        }

                        if (estRowsReturned == 0) {
                            testRowsReturned = Math.round(node2.getEstRowsReturned()
                                    * node1.getRowReductionFactor());

                            estRowsReturned = node1.getEstRowsReturned() > testRowsReturned ? node1.getEstRowsReturned()
                                    : testRowsReturned;
                        }
                    }
                }
            }

            // double check if we found a joinable index
            // if not, estimate the number of rows returned
            if (estRowsReturned == 0) {
                // No available stats; We need to guess here...
                testRowsReturned = Math.round(node2.getEstRowsReturned()
                        * node1.getRowReductionFactor());

                estRowsReturned = node1.getEstRowsReturned() > testRowsReturned ? node1.getEstRowsReturned()
                        : testRowsReturned;
            }
            if (estRowsReturned == 0) {
                estRowsReturned = 1;
            }

            // Now cost it out

            // this may be the case for subqueries
            if (node1.getRowsize() == 0) {
                node1.setRowsize(1);
            }

            // Start off cost based on the number of pages in left node.

            // estBaseReadCost = node1.estRowsReturned / node1.rowsize;
            estBaseReadCost = 0;

            // need to guess
            if (joiningCost == 0) {
                long outerNumPages = Math.round(node1.getEstRowsReturned()
                        / (1.0 * SysTable.PAGE_SIZE / node1.getRowsize()));

                joiningCost = outerNumPages + node1.getEstRowsReturned() * numPages
                        + Math.round(PROC_FACTOR * estRowsReturned);
            }

            // assume we have to lookup all of them in the right node's table(s)
            estBaseReadCost += joiningCost;

            // our row size is the select row size of the others
            // rowsize = node1.selectRowSize + node2.selectRowSize;
            rowsize = 1;

            rowsize += setJoinNodeProjections(node1, saveNode);
            rowsize += setJoinNodeProjections(node2, saveNode);

            // TODO: need to later look for groups/sums

            // now, save this info in node:
            saveNode.setRowsize(rowsize);
            saveNode.setSelectRowSize(rowsize);
            saveNode.setEstCost(estBaseReadCost);
            saveNode.setEstRowsReturned(estRowsReturned);

            if (node1.getNodeType() == QueryNode.RELATION) {
                logger.debug(" - left node: "
                        + node1.getRelationNode().getTableName());
            } else {
                logger.debug(" left node: joined");
            }
            if (node2.getNodeType() == QueryNode.RELATION) {
                logger.debug(" - right node: "
                        + node2.getRelationNode().getTableName());
            } else {
                logger.debug(" - right node: joined");
            }
            logger.debug("--- join node cost = " + saveNode.getEstCost());
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @return total projection size
     * @param aQueryNode QueryNode to process
     * @param saveNode where to copy the projections to
     */
    private int setJoinNodeProjections(QueryNode aQueryNode, QueryNode saveNode) {

        int rowsize = 0;

        // note that a project value may also be an expression,
        // eg. cost * quantity
        if (!aQueryNode.isUncorrelatedSubquery()) {
            for (SqlExpression projection : aQueryNode.getProjectionList()) {
                rowsize += projection.getExprDataType().getByteLength();
                logger.debug(" adding proj: " + projection.getExprString());
                saveNode.getProjectionList().add(projection);
            }
        }
        return rowsize;
    }

    /**
     * Estimates the cost of joining QueryNodes in "down-join". That is, instead
     * of node1's results typically being sent to node2, node1 is itself a join
     * of at least 2 tables, and node2's results will be sent over and joined at
     * the same time the node1 join occurs. This would be at least a 3-table
     * join. It is useful for cases where we have a join like this:
     * small_table_1-big_table-small_table_2.
     *
     * @return false, if not a valid down-join, otherwise true
     * @param aQueryTree
     * @param node1 candidate right node
     * @param node2 current root node
     * @param saveNode
     */
    private boolean estimateDownJoinCost(QueryTree aQueryTree,
            QueryNode node1,
            QueryNode node2,
            QueryNode saveNode) {
        final String method = "estimateDownJoinCost";
        logger.entering(method);
        try {

            long estRowsReturned, estBaseReadCost;
            long joiningCost;
            QueryCondition cond;
            int rowsize = 0;
            float rowsPerPage = 0;
            int numPages;
            // long cost;
            SqlExpression proj;
            AttributeColumn leftColumn, rightColumn;
            SysColumn aSysColumn = null;
            QueryNode otherSubtreeNode = null;
            long testRowsReturned;
            // We only want to do this if we can find a condition on a table in
            // node2's
            // subtree that joins with node1's *right* subtree
            boolean foundConditionOnBaseTable = false;

            rowsPerPage = SysTable.PAGE_SIZE / node2.getRowsize();
            numPages = Math.round(node2.getBaseNumRows() / rowsPerPage);

            if (numPages == 0) {
                numPages = 1;
            }

            joiningCost = 0;
            estRowsReturned = 0;
            // Now check joins from main tree
            for (int i = 0; i < aQueryTree.getConditionList().size()
                    && !foundConditionOnBaseTable; i++) {
                cond = aQueryTree.getConditionList().get(i);

                logger.debug(" - checking cond: " + cond.getCondString());

                if (cond.isJoin()) {
                    if (cond.getLeftCond() != null
                            && cond.getLeftCond().getCondType() == QueryCondition.QC_SQLEXPR
                            && cond.getLeftCond().getExpr().isColumn()) {
                        leftColumn = cond.getLeftCond().getExpr().getColumn();
                    } else {
                        continue;
                    }

                    if (cond.getRightCond() != null
                            && cond.getRightCond().getCondType() == QueryCondition.QC_SQLEXPR
                            && cond.getLeftCond().getExpr().isColumn()) {
                        rightColumn = cond.getRightCond().getExpr().getColumn();
                    } else {
                        continue;
                    }

                    if (leftColumn == null || rightColumn == null) {
                        continue;
                    }

                    aSysColumn = null;

                    // Left node columns will currently
                    // already have been folded into a temp table in the
                    // current implementation, so we do
                    // NOT want to take it into consideration when costing.
                    if (node2.getRightNode() != null
                            && node2.getRightNode()
                            .subtreeFind(leftColumn.getTableName(),
                            leftColumn.getTableAlias()) != null) {

                        otherSubtreeNode = node1.subtreeFind(rightColumn
                                .getTableName(), rightColumn.getTableAlias());

                        // Make sure it is joining with the right
                        if (otherSubtreeNode != null) {
                            foundConditionOnBaseTable = true;

                            aSysColumn = leftColumn.getSysColumn(database);
                        }
                    }

                    if (node2.getRightNode() != null
                            && node2.getRightNode().subtreeFind(rightColumn
                            .getTableName(), rightColumn
                            .getTableAlias()) != null) {
                        otherSubtreeNode = node1.subtreeFind(leftColumn
                                .getTableName(), leftColumn.getTableAlias());

                        // Make sure it is joining with the left
                        if (otherSubtreeNode != null) {
                            foundConditionOnBaseTable = true;
                            aSysColumn = rightColumn.getSysColumn(database);
                        }
                    }

                    // did not find a condition
                    if (aSysColumn == null) {
                        continue;
                    }
                }
            }

            // At this point, we see if we have found a valid condition.
            // If not, we must be dealing with a case where node2 is joining
            // with
            // node1's leftNode (not right). That is, other previous results.
            // We therefore do not want to consider a down-join here.
            if (!foundConditionOnBaseTable) {
                return false;
            }

            // Rework est # of rows
            // double check if we found a joinable index
            // if not, estimate the number of rows returned
            if (estRowsReturned == 0) {
                testRowsReturned = Math.round(node2.getEstRowsReturned()
                        * node1.getRowReductionFactor());

                estRowsReturned = node1.getEstRowsReturned() > testRowsReturned ? node1.getEstRowsReturned()
                        : testRowsReturned;
            }
            if (estRowsReturned == 0) {
                estRowsReturned = 1;
            }

            // Now cost it out

            // this may be the case for subqueries
            if (node1.getRowsize() == 0) {
                node1.setRowsize(1);
            }

            estBaseReadCost = 0;

            if (joiningCost == 0) {
                //
                long shippedPages = Math.round(node1.getEstRowsReturned()
                        / (1.0 * SysTable.PAGE_SIZE / node1.getRowsize()));
                shippedPages = shippedPages <= 0 ? 1 : shippedPages;

                double shipFactor = Math.log(shippedPages);

                joiningCost = shippedPages
                        + Math.round(node2.getEstRowsReturned() * shipFactor)
                        + Math.round(PROC_FACTOR * estRowsReturned);
            }

            // assume we have to lookup all of them in the right node's table(s)
            estBaseReadCost += joiningCost;

            // our row size is the select row size of the others
            // rowsize = node1.selectRowSize + node2.selectRowSize;
            rowsize = 1;

            if (!node1.isUncorrelatedSubquery()) {
                for (int i = 0; i < node1.getProjectionList().size(); i++) {
                    // Note that a project value may also be an expression,
                    // eg. cost * quantity
                    proj = node1.getProjectionList().get(i);

                    rowsize += proj.getExprDataType().getByteLength();

                    logger.debug(" adding proj: " + proj.getExprString());
                    saveNode.getProjectionList().add(proj);
                }
            }

            if (!node2.isUncorrelatedSubquery()) {
                for (int i = 0; i < node2.getProjectionList().size(); i++) {
                    // Note that a project value may also be an expression,
                    // eg. cost * quantity
                    proj = node2.getProjectionList().get(i);

                    rowsize += proj.getExprDataType().getByteLength();

                    logger.debug(" adding proj: " + proj.getExprString());
                    saveNode.getProjectionList().add(proj);
                }
            }

            // TODO: need to later look for groups/sums

            // now, save this info in node:
            saveNode.setRowsize(rowsize);
            saveNode.setSelectRowSize(rowsize);
            saveNode.setEstCost(estBaseReadCost);
            saveNode.setEstRowsReturned(estRowsReturned);

            return true;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Used to track which nodes have been "used" or incorporated into the tree.
     *
     * @param aQueryNode The QueryNode to mark
     * @param unusedQueryNodes The list of remaining unused QueryNodes
     */
    private void markUsedNodes(QueryNode aQueryNode, List<Integer> unusedQueryNodes) {
        final String method = "markUsedNodes";
        logger.entering(method);
        try {

            logger.debug("unusedQueryNodes.size() = "
                    + unusedQueryNodes.size());
            // Always try and remove it, it could be a subquery or subtree join
            unusedQueryNodes.remove(Integer.valueOf(aQueryNode.getNodeId()));

            if (aQueryNode.getNodeType() == QueryNode.JOIN) {
                markUsedNodes(aQueryNode.getLeftNode(), unusedQueryNodes);
                markUsedNodes(aQueryNode.getRightNode(), unusedQueryNodes);
            }
            logger.debug("unusedQueryNodes.size() = "
                    + unusedQueryNodes.size());
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * As part of optimization process, this builds up a list of candidate
     * QueryTrees.
     *
     * @return new optimized QueryTree
     * @param originalQueryTree The skeleton QueryTree to optimize
     * @param candidateTrees Vector containing candidateTrees
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private QueryTree buildAndEvaluateTrees(QueryTree originalQueryTree,
            SortedLongVector candidateTrees) throws XDBServerException {
        final String method = "buildAndEvaluateTrees";
        logger.entering(method);
        try {
            QueryTree newQueryTree;
            boolean isCorrelated = false;
            SortedLongVector candidateNodes;
            int numTrees;

            candidateNodes = new SortedLongVector();

            logger.debug("QueryNodeList.size() = "
                    + originalQueryTree.getQueryNodeTable().size());

            // find the node to start with.
            // If outer joins are present, always take the lowest level (outer
            // most) next
            for (QueryNode aQueryNode : originalQueryTree.getQueryNodeTable().values()) {

                // Populate unusedQueryNodes for tracking

                // Don't track ones that we have already combined,
                // due to special relationships.
                if (aQueryNode.getParent() != null) {
                    continue;
                }

                // don't bother with non-correlated
                if (aQueryNode.isUncorrelatedSubquery()) {
                    continue;
                }
                
                // For WITH subqueries, if we are in the top level tree,
                // only add to the list if used in top level
                if (originalQueryTree.getTopMostParentQueryTree() == null 
                        && !aQueryNode.getRelationNode().isTopMostUsedWith()) {
                    continue;
                }

                if (!aQueryNode.isWith()) {
                    originalQueryTree.getUnusedQueryNodeList().add(Integer.valueOf(
                        aQueryNode.getNodeId()));
                }

                // Don't consider ones with subqueries for the initial tree
                if (aQueryNode.getNodeType() == QueryNode.RELATION
                        && (aQueryNode.isUncorrelatedSubquery() || aQueryNode.isCorrelatedSubquery())) {
                    continue;
                }

                // if we already found a correlated node placeholder, don't
                // bother checking
                if (isCorrelated) {
                    continue;
                }

                if (aQueryNode.getNodeType() == QueryNode.JOIN
                        || !aQueryNode.isCorrelatedSubquery()) {
                    // Also check if SUBQUERY_CORRELATED_PH
                    // *Always* start with that one.
                    if (aQueryNode.getNodeType() == QueryNode.RELATION
                            && aQueryNode.isCorrelatedPlaceholder()) {
                        isCorrelated = true;
                        // clear and just have correlated node added.
                        candidateNodes = new SortedLongVector();
                    }
                }

                // Add to sorted list
                candidateNodes.addElement(aQueryNode.getEstCost(), aQueryNode);

                logger.debug("Adding unused node:");
                logger.debug(aQueryNode.toString("-"));
            }

            // Now retrieve the top x values from the candidateNodes list
            // to create x trees with
            numTrees = candidateNodes.size();
            if (MAX_TREES < numTrees) {
                numTrees = MAX_TREES;
            }

            for (int i = 0; i < numTrees; i++) {
                QueryNode aQN = (QueryNode) candidateNodes.get(i);

                // We only bother starting with nodes whose outerLevel = 0
                // For the first step
                if (aQN.getOuterLevel() == 0) {
                    originalQueryTree.setRootNode(aQN);

                    newQueryTree = originalQueryTree.copy();

                    markUsedNodes(newQueryTree.getRootNode(),
                            newQueryTree.getUnusedQueryNodeList());

                    // save the tree to our list
                    candidateTrees.addElement(newQueryTree);
                }
            }

            logger.debug("orig queryNodeList.size = "
                    + originalQueryTree.getQueryNodeTable().size());

            // Continue building candidate trees based on our list
            newQueryTree = buildTrees(candidateTrees);

            return newQueryTree;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * builds up candidate trees and evaluates their cost, choosing the lowest
     * cost QueryTree.
     *
     * @return lowest cost QueryTree
     * @param treeList list of starting candidate trees
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private QueryTree buildTrees(SortedLongVector treeList)
            throws XDBServerException {
        final String method = "buildTrees";
        logger.entering(method);
        try {

            QueryTree aQueryTree;
            QueryTree finalTree;
            List nextTreeSet;
            SortedLongVector newCandidateTrees;
            boolean endFlag = false;
            int trimAmount;

            for (int level = 2; level < 1000 && !endFlag; level++) {
                newCandidateTrees = new SortedLongVector();

                // We loop through each candidate and get a set of possible
                // trees
                for (int i = 0; i < treeList.size(); i++) {
                    aQueryTree = (QueryTree) treeList.get(i);

                    if (aQueryTree.getUnusedQueryNodeList().size()
                            - aQueryTree.getCorrelatedSubqueryList().size() <= 0) {

                        newCandidateTrees.addElement(aQueryTree);

                        endFlag = true;
                        continue;
                    }

                    // get some possible derivatives of current tree.
                    nextTreeSet = getNextTreeSet(aQueryTree);

                    // Add them to our new target list.
                    for (int j = 0; j < nextTreeSet.size(); j++) {
                        QueryTree newQueryTree = (QueryTree) nextTreeSet
                                .get(j);

                        newCandidateTrees.addElement(newQueryTree.getCost(),
                                newQueryTree);
                    }
                }

                // OK, we have now created a new batch of trees.
                // We take the top n number of them
                // Allow it to grow linearly

                // But first, tree and take at least MIN_TREES
                trimAmount = newCandidateTrees.size() / (level + 1);

                if (trimAmount < MIN_TREES) {
                    trimAmount = MIN_TREES;

                    if (trimAmount >= newCandidateTrees.size()) {
                        trimAmount = newCandidateTrees.size();
                    }
                }

                for (int i = newCandidateTrees.size() - 1; i > trimAmount - 1
                        && i != 0; i--) {
                    logger.debug("Removing candidate tree");
                    logger.debug("-----------------------");
                    QueryTree delTree = (QueryTree) newCandidateTrees
                            .get(i);
                    logger.debug(delTree);

                    newCandidateTrees.removeElementAt(i);
                }

                // reset the tree List
                treeList = newCandidateTrees;
            }

            // At this point we have finished building up our candidate trees.
            // We now take the best one, and finish work on it.
            for (int i = 1; i < treeList.size(); i++) {
                logger.debug("Rejected candidate tree");
                QueryTree rejectTree = (QueryTree) treeList.get(i);
                logger.debug(rejectTree);
            }

            if (treeList.size() == 0) {
                throw new XDBServerException("Unexpected empty query tree list");
            }
            finalTree = (QueryTree) treeList.get(0);
            finalTree = finishTree(finalTree);

            return finalTree;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Given a QueryTree, will return a Vector containing the lowest cost trees
     * (prunes) based on possible joins
     *
     * @return Vector contains list of possible next step QueryTrees
     * @param aQueryTree the QueryTree to evaluate
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private List getNextTreeSet(QueryTree aQueryTree)
            throws XDBServerException {
        final String method = "getNextTreeSet";
        logger.entering(method);
        try {

            QueryNode joinNode;
            QueryNode otherJoinNode;
            QueryNode rootNode;
            QueryTree newQueryTree;
            short lowestOuterLevel = -1;
            SortedLongVector candidateJoinNodes;
            SortedLongVector newTreeSet;
            int trimAmount;
            List<Integer> joinList;

            candidateJoinNodes = new SortedLongVector();
            newTreeSet = new SortedLongVector();

            rootNode = aQueryTree.getRootNode();

            logger.debug("1 joinList.size() = "
                    + aQueryTree.getRootNode().getJoinList().size());
            logger.debug("new root node");
            logger.debug(rootNode.toString(" -"));
            logger.supertrace(aQueryTree);

            // Vector candidateNodes = new Vector ();
            joinNode = null;

            // See if we are dealing with a cartesian product...
            if (rootNode.getJoinList().size() == 0) {
                logger.debug("cartesian product encountered.");

                // Set the join list to all unused ones.
                Iterator<Integer> itNodeId = aQueryTree.getUnusedQueryNodeList().iterator();

                // Exclude unused WITH subqueries
                while (itNodeId.hasNext()) {
                    
                    Integer nodeId = itNodeId.next();
                    QueryNode tempNode = aQueryTree.getNodeById(nodeId.intValue());
                    
                    // if we are a top tree and it is unused at the top level, 
                    // remove here so we don't have a cartesian product
                    if (tempNode.isUnusedTopWith()) {
                        itNodeId.remove();
                    }
                }
                
                if (aQueryTree.getUnusedQueryNodeList().size() == 0)
                {
                    //just return original
                    newTreeSet.addElement(aQueryTree.getRootNode().getEstCost(),
                            aQueryTree);
                    return newTreeSet;
                }
                
                joinList = aQueryTree.getUnusedQueryNodeList();
            } else {
                joinList = rootNode.getJoinList();
            }

            // First, determine the lowest outer level
            for (int i = 0; i < joinList.size(); i++) {
                Integer tempInt = joinList.get(i);

                QueryNode tempNode = aQueryTree.getNodeById(tempInt.intValue());

                if (lowestOuterLevel == -1) {
                    lowestOuterLevel = tempNode.getOuterLevel();
                }
            }

            for (int i = 0; i < joinList.size(); i++) {
                // look up node based on Id number
                Integer tempInt = joinList.get(i);

                QueryNode tempNode = aQueryTree.getNodeById(tempInt.intValue());

                // Only bother copying if it is the lowest outer level
                if (tempNode.getOuterLevel() != lowestOuterLevel) {
                    continue;
                }

                newQueryTree = aQueryTree.copy();

                joinNode = newQueryTree.newQueryNode();

                logger.debug("joinId = " + tempInt.intValue());

                otherJoinNode = newQueryTree.getNodeById(tempInt);

                if (otherJoinNode.getNodeType() == QueryNode.RELATION) {
                    if (otherJoinNode.isCorrelatedSubquery()) {
                        continue;
                    }

                    // Also, ignore uncorrelated nodes
                    if (otherJoinNode.isUncorrelatedSubquery()) {
                        continue;
                    }
                }

                // Ok to join these in new tree, joining at top
                addNormalJoinAtTop(newQueryTree, joinNode, otherJoinNode);

                // We now test doing an alternative join.
                // We test cost if we do a "tree-down join"
                QueryTree treeDownQueryTree = aQueryTree.copy();

                joinNode = treeDownQueryTree.newQueryNode();

                otherJoinNode = treeDownQueryTree.getNodeById(tempInt);

                // See if a tree-down join is possible.
                // if so, see if its cost was less than the normal join
                if (addTreeDownJoin(treeDownQueryTree, joinNode, otherJoinNode)) {
                    logger.debug("Normal join cost = " + newQueryTree.getCost()
                            + "; Tree-down join cost = "
                            + treeDownQueryTree.getCost());

                    // logger.debug(treeDownQueryTree.toString());
                    // logger.debug(newQueryTree.toString());

                    /*
                     * use tree down tree if cost is less, or if the right node
                     * has many fewer results than the left (that means it
                     * probably *should* have chosen it, but did not.)
                     */
                    if (treeDownQueryTree.getCost() < newQueryTree.getCost()
                            || 4 * treeDownQueryTree.getRootNode().getRightNode().getEstRowsReturned() < newQueryTree.getRootNode().getLeftNode().getEstRowsReturned()) {
                        if (treeDownQueryTree.getCost() > newQueryTree
                                .getCost()) {
                            treeDownQueryTree.getRootNode().setEstCost(newQueryTree.getRootNode().getEstCost());
                        }
                        // add the tree to the next set.
                        newTreeSet.addElement(
                                treeDownQueryTree.getRootNode().getEstCost(),
                                treeDownQueryTree);
                    } else {
                        // add the tree to the next set.
                        newTreeSet.addElement(newQueryTree.getRootNode().getEstCost(),
                                newQueryTree);
                    }
                } else {
                    // add the tree to the next set.
                    newTreeSet.addElement(newQueryTree.getRootNode().getEstCost(),
                            newQueryTree);
                }
            }

            // From the candidates take the top x and generate new trees
            // Just take the best trees
            trimAmount = STEP_TRIM_AMOUNT;

            if (newTreeSet.size() < trimAmount) {
                trimAmount = candidateJoinNodes.size();
            }

            for (int i = newTreeSet.size() - 1; i > trimAmount; i--) {
                newTreeSet.removeElementAt(i);
            }

            return newTreeSet;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Updates the tree by joining the specified nodes at the top of the tree.
     *
     * @param newQueryTree
     * @param joinNode
     * @param otherJoinNode
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void addNormalJoinAtTop(QueryTree newQueryTree, QueryNode joinNode,
            QueryNode otherJoinNode) throws XDBServerException {

        QueryNode newRootNode = newQueryTree.getRootNode();

        estimateJoinCost(newQueryTree, newRootNode, otherJoinNode, joinNode);

        newQueryTree.setRootNode(joinNode);
        joinNode.setRightNode(otherJoinNode);
        joinNode.setLeftNode(newRootNode);
        joinNode.getLeftNode().setParent(joinNode);
        joinNode.getRightNode().setParent(joinNode);
        joinNode.setNodeType(QueryNode.JOIN);

        adjustSubtreeJoins(joinNode, newQueryTree);

        newQueryTree.addToQueryNodeTable(joinNode);

        // Note the table nodes we have used.
        markUsedNodes(joinNode.getRightNode(), newQueryTree.getUnusedQueryNodeList());

        // candidateJoinNodes.addElement (joinNode.estCost, joinNode);

        logger.debug("joinNode.estCost = " + joinNode.getEstCost());
        if (otherJoinNode.getNodeType() == QueryNode.RELATION) {
            logger.debug("otherJoinNode.tableName = "
                    + otherJoinNode.getRelationNode().getTableName());
            logger.debug("otherJoinNode.relationNode.outerLevel = "
                    + otherJoinNode.getRelationNode().getOuterLevel());
        }

        if (joinNode.getRightNode().getNodeType() == QueryNode.RELATION
                && joinNode.getRightNode().getRelationNode().getOuterLevel() > newQueryTree.getLastOuterLevel()) {
            joinNode.getRightNode().setSubtreeOuter(true);
            newQueryTree.setLastOuterLevel(joinNode.getRightNode().getRelationNode()
                    .getOuterLevel());
        }

        joinNode.setOuterLevel(joinNode.getRightNode().getOuterLevel());
    }

    /**
     * Updates the tree by joining the specified nodes at the top of the tree.
     *
     * @param newQueryTree
     * @param joinNode
     * @param otherJoinNode
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    private boolean addTreeDownJoin(QueryTree newQueryTree, QueryNode joinNode,
            QueryNode otherJoinNode) throws XDBServerException {

        // See if the system is configured to check for these.
        if (!Props.XDB_ALLOW_DOWN_JOIN) {
            return false;
        }

        QueryNode currentRootNode = newQueryTree.getRootNode();

        // only do this if we are already dealing with a normal join case
        // at top of current tree
        if (currentRootNode.getNodeType() == QueryNode.RELATION
                || currentRootNode.isPreserveSubtree()
                || currentRootNode.isTreeDownJoin()) {
            // not available for tree-down-join
            return false;
        }

        /*
         * // Don't consider if it is not either a join, // or a simple table
         * (don't do correlated subqueries). if (otherJoinNode.nodeType ==
         * QueryNode.RELATION && otherJoinNode.relationNode.nodeType !=
         * RelationNode.SUBQUERY_CORRELATED) { return false; }
         */

        // There are problems trying to do this if there are multiple
        // correlated subqueries in the tree. It will work with 2 correlated
        // subqueries in some cases, but to be safe, we will limit it to 1 here.
        if (newQueryTree.getCorrelatedSubqueryList().size() > 1) {
            return false;
        }

        // This is based on estimateJoinCost.
        // Note we flip the order of currentRootNode and otherJoinNode for
        // costing purposes.
        if (!estimateDownJoinCost(newQueryTree, otherJoinNode, currentRootNode,
                joinNode)) {
            // not valid candidate for tree-down-join
            return false;
        }

        newQueryTree.setRootNode(joinNode);
        joinNode.setRightNode(otherJoinNode);
        joinNode.setLeftNode(currentRootNode);
        joinNode.getLeftNode().setParent(joinNode);
        joinNode.getRightNode().setParent(joinNode);
        joinNode.setNodeType(QueryNode.JOIN);

        // Note that it is a tree down join
        joinNode.setTreeDownJoin(true);

        adjustSubtreeJoins(joinNode, newQueryTree);

        newQueryTree.addToQueryNodeTable(joinNode);

        // Note the table nodes we have used.
        markUsedNodes(joinNode.getRightNode(), newQueryTree.getUnusedQueryNodeList());

        logger.debug("joinNode.estCost = " + joinNode.getEstCost());
        if (otherJoinNode.getNodeType() == QueryNode.RELATION) {
            logger.debug("otherJoinNode.tableName = "
                    + otherJoinNode.getRelationNode().getTableName());
            logger.debug("otherJoinNode.relationNode.outerLevel = "
                    + otherJoinNode.getRelationNode().getOuterLevel());
        }

        if (joinNode.getRightNode().getNodeType() == QueryNode.RELATION
                && joinNode.getRightNode().getRelationNode().getOuterLevel() > newQueryTree.getLastOuterLevel()) {
            joinNode.getRightNode().setSubtreeOuter(true);
            newQueryTree.setLastOuterLevel(joinNode.getRightNode().getRelationNode()
                    .getOuterLevel());
        }

        return true;
    }

    /**
     * Finishes the chosen basic optimized QueryTree is chosen
     *
     * @return the finished QueryTree
     * @param newQueryTree the QueryTree to finish
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private QueryTree finishTree(QueryTree newQueryTree)
            throws XDBServerException {
        final String method = "finishTree";
        logger.entering(method);
        try {
            boolean foundMatch = false;

            newQueryTree.relabel();

            // We add the correlated nodes here at the end
            // first, we need to add the QueryNode that represents it.
            // We start at the top of the tree and try and push it down.
            // We check to see if right table nodes exist in joinList.
            for (RelationNode subTreeNode : newQueryTree.getCorrelatedSubqueryList()) {

                logger.debug("adding correlation...");
                QueryNode aQueryNode = null;

                // First, check if we are dealing with a special case-
                // just lookups in subquery

                boolean foldSubquery = false;

                if (containsOnlyLookupsAndParChilds(subTreeNode)) {
                    // just put the condition at the root node,
                    // so that we can save a step.
                    // We also do NOT add the relation node to the tree

                    // Right now this is kind of a pain to do-
                    // We look for this node used in the parent's query
                    // condition list

                    for (QueryCondition aQC : newQueryTree.getConditionList()) {
                        // look for place holder node id
                        for (RelationNode aRelNode : aQC.getRelationNodeList()) {
                            if (subTreeNode.getNodeId() == aRelNode.getNodeId()) {
                                aQC.getRelationNodeList().remove(aRelNode);
                                foldSubquery = true;
                                break;
                            }
                        }

                        if (!foldSubquery) {
                            continue;
                        }

                        // look for place holder node id
                        for (Integer anInt : aQC.getNodeIdList()) {
                            if (subTreeNode.getNodeId() == anInt.intValue()) {
                                aQC.getNodeIdList().remove(anInt);
                                break;
                            }
                        }
                    }

                    /* if we are dealing with a correlated subquery in the
                     * SELECT list, and it appears safe at this point,
                     * go ahead and fold.
                     */
                    if (!foldSubquery && subTreeNode.getParentCorrelatedExpr().isProjection()) {
                        foldSubquery = true;
                    }

                    if (foldSubquery) {
                        // In the subtree, remove the place holder node.
                        for (RelationNode aRelNode : subTreeNode.getSubqueryTree().getRelationNodeList()) {
                            if (aRelNode.isCorrelatedPlaceholder()) {
                                // first, update the join list of any other
                                // nodes that want to join with it
                                for (RelationNode subRelNode : aRelNode.getJoinList()) {
                                    subRelNode.getJoinList().remove(aRelNode);
                                }

                                // Also, remove the node from any
                                // QueryConditions
                                for (QueryCondition aQC :
                                        subTreeNode.getSubqueryTree().getConditionList()) {
                                    aQC.getRelationNodeList().remove(aRelNode);
                                }

                                subTreeNode.getSubqueryTree().getRelationNodeList()
                                        .remove(aRelNode);
                                break;
                            }
                        }
                    }
                }

                // if we do not fold in subquery, take care of here
                if (!foldSubquery) {
                    // first, determine subquery tree
                    QueryTree subQueryTree = determineQueryPath(subTreeNode.getSubqueryTree());

                    // hint to planner
                    subQueryTree.setCorrelatedSubtree(true);

                    // update cost and row values for optimizer calcs
                    QueryNode subQueryNode = newQueryTree.getNodeById(
                            Integer.valueOf(subTreeNode.getNodeId()));

                    subQueryNode.setEstRowsReturned(subQueryTree.getRootNode().getEstRowsReturned());
                    subQueryNode.setEstCost(subQueryTree.getRootNode().getEstCost());
                    subQueryNode.setRowsize(subQueryTree.getRootNode().getRowsize());
                    subTreeNode.setEstRowsReturned(subQueryTree.getRootNode().getEstRowsReturned());

                    // assign new subquery tree
                    subTreeNode.setSubqueryTree(subQueryTree);

                    // Get its corresponding QueryNode
                    aQueryNode = newQueryTree.getNodeById(subTreeNode.getNodeId());
                    QueryNode aTreeNode = newQueryTree.getRootNode();
                    foundMatch = false;

                    String labelCheck = "";

                    while (aTreeNode != null && !foundMatch) {
                        logger.debug(" - searching...");

                        if (aTreeNode.getNodeType() == QueryNode.JOIN) {
                            // get label to compare to
                            if (aTreeNode.isPreserveSubtree()) {
                                labelCheck = aTreeNode.getLabel();
                            } else {
                                labelCheck = aTreeNode.getRightNode().getLabel();
                            }
                        } else if (aTreeNode.isPreserveSubtree()
                                || aTreeNode.getNodeType() == QueryNode.RELATION) {
                            labelCheck = aTreeNode.getLabel();
                        }

                        logger.debug(" - labelcheck = " + labelCheck);

                        // Check the correlated node's join list so that we can
                        // find the node that "joins" with it.
                        for (int j = 0; j < aQueryNode.getJoinList().size()
                                && !foundMatch; j++) {

                            QueryNode compareNode = newQueryTree
                                    .getNodeById(aQueryNode.getJoinList()
                                    .get(j).intValue());

                            logger.debug(" - compareNode label = "
                                    + compareNode.getLabel());
                            logger.debug(" - compareNode sub = "
                                    + compareNode.getLabel().substring(0, labelCheck
                                    .length()));

                            // see if the join node is in subtree
                            if (compareNode.getLabel().substring(0,
                                    labelCheck.length()).compareTo(labelCheck) == 0) {
                                foundMatch = true;
                            }
                        }

                        // continue traversing down left
                        if (!foundMatch) {
                            aTreeNode = aTreeNode.getLeftNode();
                        }
                    }

                    if (foundMatch) {
                        // check to see if parent is a tree down join,
                        // then we want to do this one node higher on tree.
                        if (aTreeNode.getParent() != null
                                && aTreeNode.getParent().isTreeDownJoin()) {
                            aTreeNode = aTreeNode.getParent();
                        }

                        // Handle case of multiple correlated subqueries.
                        // In that case, we may have added already.
                        // We need to put new ones on top of those.
                        while (aTreeNode.getParent() != null
                                && !aTreeNode.getParent().getRightNode().isCorrelatedSubquery()) {
                            aTreeNode = aTreeNode.getParent();
                        }

                        // insert subquery node into tree above
                        QueryNode tempNode = newQueryTree.newQueryNode();

                        // need to do this for projections
                        estimateJoinCost(newQueryTree, aTreeNode, aQueryNode,
                                tempNode);

                        // We create a new join node and put aQueryNode in the
                        // tree
                        QueryNode joinNode = tempNode;

                        if (aTreeNode.getParent() != null) {
                            joinNode.setParent(aTreeNode.getParent());
                            aTreeNode.getParent().setLeftNode(joinNode);
                        } else {
                            newQueryTree.setRootNode(joinNode);
                        }

                        joinNode.setRightNode(aQueryNode);
                        joinNode.setLeftNode(aTreeNode);
                        joinNode.getLeftNode().setParent(joinNode);
                        joinNode.getRightNode().setParent(joinNode);
                        joinNode.setNodeType(QueryNode.JOIN);

                        // relabel tree
                        newQueryTree.relabel();
                    } else if (aTreeNode == null) {
                        // we should have found it somewhere
                        throw new XDBServerException(
                                ErrorMessageRepository.CORELATED_QUERY_ERROR,
                                0,
                                ErrorMessageRepository.CORELATED_QUERY_ERROR_CODE);
                    }
                }
            }

            // relabel tree so adjust methods work properly
            newQueryTree.relabel();

            logger.debug("Intermed tree");
            logger.debug("-------------");
            logger.debug(newQueryTree);

            // ok, now we want to clean up all conditions and make
            // sure they are set at the proper node, then clean up
            // selected (projected) columns.
            adjustConditions(newQueryTree);
            adjustProjections(newQueryTree);

            return newQueryTree;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Cleans up all conditions and makes sure they are set at the proper node.
     *
     * @newQueryTree QueryTree to adjust
     * @param newQueryTree
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void adjustConditions(QueryTree newQueryTree)
            throws XDBServerException {
        final String method = "adjustConditions";
        logger.entering(method);
        try {
            QueryNode aQueryNode;
            boolean breakFlag = false;
            int depthCheck;
            char currentChar;
            String targetLabel = "";

            for (QueryCondition aQueryCondition : newQueryTree.getConditionList()) {

                logger.debug("Condition = " + aQueryCondition.getCondString());

                // handle the case when the condition does not involve any
                // nodes,
                // eg where "2 in (1,2,3)"
                if (aQueryCondition.getNodeIdList().size() == 0) {
                    // find the bottom left-most node and add it there.
                    aQueryNode = newQueryTree.getRootNode();

                    while (aQueryNode.getLeftNode() != null
                            && !aQueryNode.isPreserveSubtree()) {
                        aQueryNode = aQueryNode.getLeftNode();
                    }

                    setConditionAtJoinNode(newQueryTree, aQueryNode.getLabel(),
                            aQueryCondition);
                } else {
                    // we have already set atomic conditions at the individual
                    // nodes
                    if (aQueryCondition.isAtomic()) {
                        continue;
                    }

                    // now handle the condition.
                    // determine the depth check. No need to go beyond shortest
                    // one
                    depthCheck = -1;

                    List<String> labelList = new ArrayList<String>();

                    for (int j = 0; j < aQueryCondition.getNodeIdList().size(); j++) {
                        aQueryNode = newQueryTree
                                .getNodeById(aQueryCondition.getNodeIdList()
                                .get(j).intValue());

                        // RelationNode aRelNode = (RelationNode)
                        // aQueryCondition.relationNodeList.get(j);

                        if (aQueryNode.getLabel().length() < depthCheck
                                || depthCheck == -1) {
                            depthCheck = aQueryNode.getLabel().length();
                        }
                        labelList.add(aQueryNode.getLabel());
                    }

                    // Check case if there is just one node involved
                    // in this tree like in the case of IN for subqueries
                    if (labelList.size() == 1) {
                        setConditionAtJoinNode(newQueryTree, labelList.get(0),
                                aQueryCondition);

                        continue;
                    }

                    // Check labels of QueryNodes involved in condition.
                    // Labels are in format like LLRL, representing tree
                    // position.
                    // The common most label is where the condition should go
                    targetLabel = "";
                    breakFlag = false;
                    String firstLabel = labelList.get(0);

                    for (int k = 0; k < depthCheck; k++) {
                        currentChar = firstLabel.charAt(k);

                        for (String anotherLabel : labelList) {
                            if (anotherLabel.charAt(k) != currentChar) {
                                breakFlag = true;
                                break;
                            }
                        }
                        if (breakFlag) {
                            setConditionAtJoinNode(newQueryTree, targetLabel,
                                    aQueryCondition);
                            break;
                        }
                        targetLabel += currentChar;
                    }

                    if (!breakFlag) {
                        throw new XDBServerException(
                                ErrorMessageRepository.MOVINGCONDITION_NOTFOUND,
                                XDBServerException.SEVERITY_MEDIUM,
                                ErrorMessageRepository.MOVINGCONDITION_NOTFOUND_CODE);
                    }
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Sets the specified QueryCondition at the target node. This is useful when
     * a condition involves multiple nodes (even more than 2) and we want to
     * find the common-most point at which we can apply it.
     *
     * @param targetTree the QueryTree to search
     * @param targetLabel the QueryNode label to search for
     * @param aQueryCondition the QueryCondition to set
     */
    private void setConditionAtJoinNode(QueryTree targetTree,
            String targetLabel, QueryCondition aQueryCondition) {
        final String method = "setConditionAtJoinNode";
        logger.entering(method);
        try {
            logger.debug("scan target = " + targetLabel);

            QueryNode currentNode = targetTree.getRootNode();

            // start at one, since L is at position 0

            for (int i = 1; i < targetLabel.length(); i++) {
                if (targetLabel.charAt(i) == 'L') {
                    currentNode = currentNode.getLeftNode();
                } else {
                    currentNode = currentNode.getRightNode();
                }
            }
            logger.debug("adding cond at " + currentNode.getLabel());

            currentNode.getConditionList().add(aQueryCondition);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Traverses nodes in a depth-first fashion and makes sure selected columns
     * appear at all nodes up the tree where they need to be, while making sure
     * they are not needed beyond that
     *
     * @param aQueryTree QueryTree to adjust
     */
    private void adjustProjections(QueryTree aQueryTree) {
        final String method = "adjustProjections";
        logger.entering(method);
        try {
            // Do this in a depth-first fashion.
            traverseProjections(aQueryTree.getRootNode(), aQueryTree);

            // Use expressions in GROUP BY
            if (aQueryTree.getGroupByList() != null) {
                for (SqlExpression expr : aQueryTree.getGroupByList()) {
                    projectExpressions(expr, aQueryTree);
                }
            }

            // Also make sure that we use all Having columns
            addHavingProjections(aQueryTree);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Traverses projections, for updating projection usage
     *
     * @param aQueryNode current QueryNode
     * @param aQueryTree QueryTree being traversed.
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void traverseProjections(QueryNode aQueryNode, QueryTree aQueryTree)
            throws XDBServerException {
        final String method = "traverseProjections";
        logger.entering(method);
        try {
            logger.debug("traverseProjections()");

            // recursively search for all TABLE query nodes
            if (aQueryNode.getNodeType() == QueryNode.JOIN) {
                traverseProjections(aQueryNode.getLeftNode(), aQueryTree);
                traverseProjections(aQueryNode.getRightNode(), aQueryTree);
            } else {
                // We hit a TABLE or subquery node.
                // Let's process those cond cols
                logger.debug("tp TABLE "
                        + aQueryNode.getRelationNode().getTableName() + " "
                        + aQueryNode.getRelationNode().getAlias() + " "
                        + aQueryNode.getLabel());

                List targetColumns = new ArrayList();

                for (int i = 0; i < aQueryNode.getRelationNode().getCondColumnList()
                        .size(); i++) {
                    AttributeColumn column =
                            aQueryNode.getRelationNode().getCondColumnList().get(i);
                    logger.debug("tp column " + column.columnName);

                    // Work-around because duplicates are messing things up
                    // here.
                    // See case 644572.
                    // If an alias was set on an earlier column with the same
                    // name, use it here as well.

                    // Note we only search up to the i index from the outer loop
                    for (int j = 0; j < i; j++) {
                        AttributeColumn compareColumn =
                                aQueryNode.getRelationNode().getCondColumnList().get(j);

                        if (compareColumn.getTableName().equals(column.getTableName())
                                && compareColumn.columnName.equals(column.columnName)) {

                            // Use other column
                            targetColumns.add(compareColumn);
                            column = compareColumn;
                            break;
                        }
                    }
                }

                if (!targetColumns.isEmpty()) {
                    QueryCondition.equateConditionsWithColumns(
                            aQueryTree.getConditionList(), targetColumns);
                }

                for (AttributeColumn column : aQueryNode.getRelationNode().getCondColumnList()) {
                    updateSegmentProjections(aQueryNode, column, aQueryTree);
                }
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Starting at the current QueryNode, adjust usage of the column up the
     * QueryTree
     *
     * @param aQueryNode currentNode
     * @param column AttributeColumn to adjust
     * @param aQueryTree QueryTree being adjusted
     * @throws org.postgresql.stado.exception.XDBServerException
     */
    private void updateSegmentProjections(QueryNode aQueryNode,
            AttributeColumn column, QueryTree aQueryTree)
            throws XDBServerException {
        final String method = "updateSegmentProjections";
        logger.entering(method);
        try {
            QueryNode currentNode;
            QueryNode anchorNode;


            currentNode = aQueryNode;
            anchorNode = aQueryNode;

            // traverse our way up to root node and adjust projections,
            // making sure that columns used in conditions are projected up
            currentNode = currentNode.getParent();

            while (currentNode != null) {
                for (QueryCondition aQueryCondition : currentNode.getConditionList()) {
                    for (AttributeColumn condColumn : aQueryCondition.getColumnList()) {

                        // See if we have a match, then we need to project up.
                        // We ignore case where it is a "tree-down" join
                        if (condColumn == column
                                && !currentNode.isPreserveSubtree()
                                && !(currentNode.isTreeDownJoin() && !currentNode.getRightNode()
                                .subtreeContains(anchorNode))) {
                            // we need to update nodes in between on tree
                            addColumnProjection(anchorNode, currentNode, column);

                            // note new "anchor"
                            anchorNode = currentNode;
                        }
                    }
                }

                // need to compare for correlated subqueries
                // this means checking the right tree
                if (currentNode.getRightNode() != null
                        && currentNode.getRightNode().isCorrelatedSubquery()) {

                    for (AttributeColumn condColumn :
                            currentNode.getRightNode().getRelationNode().getCorrelatedColumnList()) {

                        // If column matches, update projections,
                        // unless we are dealling with a preserve case (lookup)
                        // or the anchorNode is in a tree down join
                        if (condColumn == column
                                && !currentNode.isPreserveSubtree()
                                && !(currentNode.isTreeDownJoin() && !currentNode
                                .subtreeContains(anchorNode))) {
                            // we need to update nodes in between on tree
                            addColumnProjection(anchorNode, currentNode, column);

                            // note new "anchor"
                            anchorNode = currentNode;
                        }
                    }
                }

                if (currentNode == aQueryTree.getRootNode()) {
                    break;
                }
                // keep traversing up
                if (currentNode.getParent() == null) {
                    throw new XDBServerException(
                            ErrorMessageRepository.NOPARENTNODE_FOUND,
                            XDBServerException.SEVERITY_MEDIUM,
                            ErrorMessageRepository.NOPARENTNODE_FOUND_CODE);
                }
                currentNode = currentNode.getParent();
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Given two QueryNodes in a QueryTree, make sure that the specified column
     * is projected up the tree segment
     *
     * @param startNode the start QueryNode
     * @param endNode the end QueryNode
     * @param anAttributeColumn the column being used in the segment
     */
    private void addColumnProjection(QueryNode startNode, QueryNode endNode,
            AttributeColumn anAttributeColumn) {
        final String method = "addColumnProjection";
        logger.entering(method);
        try {
            SqlExpression aSqlExpression = new SqlExpression();

            aSqlExpression.setExprType(SqlExpression.SQLEX_COLUMN);
            aSqlExpression.setColumn(anAttributeColumn);
            // Make sure we quote and try and use alias first
            if (anAttributeColumn.getTableAlias() != null
                    && anAttributeColumn.getTableAlias().length() > 0) {
                aSqlExpression.setExprString(IdentifierHandler.quote(anAttributeColumn.getTableAlias()) + "."
                    + IdentifierHandler.quote(anAttributeColumn.columnName));               
            } else {
                aSqlExpression.setExprString(IdentifierHandler.quote(anAttributeColumn.getTableName()) + "."
                    + IdentifierHandler.quote(anAttributeColumn.columnName));
            }
            aSqlExpression.setExprDataType(anAttributeColumn.columnType);

            // new add this to projection list along the way
            addColumnProjection(startNode, endNode, aSqlExpression);
        } finally {
            logger.exiting(method);
        }

    }

    /**
     * Given two QueryNodes in a QueryTree, make sure that the specified column
     * is projected up the tree segment
     *
     * @param startNode the start QueryNode
     * @param endNode the end QueryNode
     * @param aSqlExpression the expression containg the column to add
     */
    private void addColumnProjection(QueryNode startNode, QueryNode endNode,
            SqlExpression aSqlExpression) {
        final String method = "addColumnProjection";
        logger.entering(method);
        try {

            QueryNode currentNode = startNode;
            boolean checkFlag;

            // traverse our way up to root node and adjust projections
            while (currentNode != endNode) {
                // only add it if it is not yet in proj list
                checkFlag = false;

                for (SqlExpression oldSqlExpression : currentNode.getProjectionList()) {
                    oldSqlExpression.rebuildString();
                    if (oldSqlExpression.getExprString()
                            .compareTo(aSqlExpression.getExprString()) == 0) {
                        checkFlag = true;
                        break;
                    }
                }

                // don't add if found
                if (!checkFlag) {
                    currentNode.getProjectionList().add(aSqlExpression);
                }
                currentNode = currentNode.getParent();
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * addHavingProjections
     *
     * Make sure we project up columns that are used in having projections
     *
     * @param aQueryTree
     */
    private void addHavingProjections(QueryTree aQueryTree) {

        for (QueryCondition aQueryCondition : aQueryTree.getHavingList()) {
            processHavingCondition(aQueryCondition, aQueryTree);
        }
    }

    /**
     * processHavingCondition
     *
     * Make sure we project up columns that are used in having projections
     *
     * @param aQueryCondition
     * @param aQueryTree
     */
    private void processHavingCondition(QueryCondition aQueryCondition,
            QueryTree aQueryTree) {
        if (aQueryCondition.getLeftCond() != null) {
            processHavingCondition(aQueryCondition.getLeftCond(), aQueryTree);
        }

        if (aQueryCondition.getRightCond() != null) {
            processHavingCondition(aQueryCondition.getRightCond(), aQueryTree);
        }

        if (aQueryCondition.getCondType() == QueryCondition.QC_SQLEXPR) {
            projectExpressions(aQueryCondition.getExpr(), aQueryTree);
        }
    }

    /**
     *
     * Look for having columns and make sure they are projected up
     *
     * @param aSqlExpression
     * @param aQueryTree
     */
    private void projectExpressions(SqlExpression aSqlExpression,
            QueryTree aQueryTree) {
        for (SqlExpression colExpr :
                SqlExpression.getNodes(aSqlExpression, SqlExpression.SQLEX_COLUMN)) {

            QueryNode currQueryNode = aQueryTree.getNodeById(
                    Integer.valueOf(colExpr.getColumn().relationNode.getNodeId()));

            addColumnProjection(currQueryNode, aQueryTree.getRootNode(), colExpr);
        }
    }
}
