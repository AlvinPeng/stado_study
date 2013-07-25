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
/****************************************************************************
 * QueryNode.java
 ****************************************************************************/

package org.postgresql.stado.optimizer;

import java.util.ArrayList;
import java.util.List;

// -------------------------------------------------------------
public class QueryNode {

    // nodeTypes
    public static final int JOIN = 1;

    public static final int RELATION = 2;

    private int nodeType;

    /** hint to optimizer, keep this (right) tree together */
    private boolean preserveSubtree = false; 

    private QueryNode rightNode;

    private QueryNode leftNode;

    private QueryNode parent;

    /** If we are not a join, the corresponding relation info
     * be it a table, or a subquery relation
     */
    private RelationNode relationNode;

    /** columns that projected out this Relation */
    private List<SqlExpression> projectionList; 

    /** Atomic conditions (non-joins) */
    private List<QueryCondition> conditionList;

    private List<Integer> joinList; 

    private List<SqlExpression> groupByList; 

    private List<RelationNode> uncorrelatedCondTreeList;

    /** row size of relation */
    private int rowsize; 

    /** total row size of projected columns */
    private int selectRowSize; 

    /** estimated cost for node */
    private long estCost; 

    /** estimated number of rows returned */
    private long estRowsReturned; 

    /** number of rows in relation */
    private long baseNumRows; 

    /** for debugging */
    private String label = ""; 

    /** for debugging */
    private int nodeId; 

    /** What tree this node is found in. */
    private QueryTree parentTree;

    /** for OUTER join handling (0 = no OUTER) */
    private short outerLevel = 0; 

    /** For grouping relations together that are in the same outer group */
    private short outerGroup = 0; 

    // This is only used for outer handling in right subtrees-
    // (for parent-child joins and lookup joins),
    // when looking at outers for joins on partitioned keys.
    // For non right subtrees, this is not used.
    private boolean isSubtreeOuter = false;

    // whether or not this is a special join type where we actually process
    // right side first and
    private boolean isTreeDownJoin = false;

    /**
     * For relation nodes, approximate amount by which non-join conditions
     * should reduce the number of rows returned
     */
    private double rowReductionFactor = 1;
    
    /**
     * Constructor
     */
    public QueryNode() {
        projectionList = new ArrayList<SqlExpression>();
        conditionList = new ArrayList<QueryCondition>();
        joinList = new ArrayList<Integer>();
        groupByList = new ArrayList<SqlExpression>();
        uncorrelatedCondTreeList = new ArrayList<RelationNode>();
        parent = null;

        nodeType = RELATION;
    }


    /**
     * Constructor for assigning RelationNode
     *
     * @param aRelationNode
     */

    public QueryNode(RelationNode aRelationNode) {
        this();
        relationNode = aRelationNode;
        // nodeId should already be unique in set.
        // This helps us in copying trees and nodes.
        nodeId = aRelationNode.getNodeId();
        projectionList = aRelationNode.getProjectionList();
        conditionList = aRelationNode.getConditionList();
        rowsize = aRelationNode.getRowsize();
        selectRowSize = rowsize;
        estCost = aRelationNode.getEstCost();
        estRowsReturned = aRelationNode.getEstRowsReturned();

        outerLevel = aRelationNode.getOuterLevel();

        // set costs for placeholders to 1; will be first anyway.
        if (aRelationNode.getNodeType() == RelationNode.SUBQUERY_CORRELATED_PH) {
            setRowsize(1);
            setSelectRowSize(1);
            setEstRowsReturned(1);
            setEstCost(1);
        }        
    }

    
    /**
     * Copies basic attributes (not rightNode, leftNode, parent)
     *
     * @return
     */

    public QueryNode baseCopy() {
        QueryNode newQueryNode = new QueryNode();

        newQueryNode.nodeType = this.nodeType;
        newQueryNode.preserveSubtree = this.preserveSubtree;
        newQueryNode.relationNode = this.relationNode;
        newQueryNode.projectionList = this.projectionList;
        newQueryNode.conditionList = this.conditionList;
        newQueryNode.uncorrelatedCondTreeList = this.uncorrelatedCondTreeList;

        newQueryNode.groupByList = this.groupByList;
        newQueryNode.rowsize = this.rowsize;
        newQueryNode.selectRowSize = this.selectRowSize;
        newQueryNode.estCost = this.estCost;
        newQueryNode.estRowsReturned = this.estRowsReturned;
        newQueryNode.baseNumRows = this.baseNumRows;
        newQueryNode.label = this.label;
        newQueryNode.nodeId = this.nodeId;

        newQueryNode.isSubtreeOuter = this.isSubtreeOuter;

        // copy joinList
        // we copy references to the Int object, but not the list itself
        newQueryNode.joinList.addAll(joinList);

        newQueryNode.outerLevel = this.outerLevel;
        newQueryNode.outerGroup = this.outerGroup;
        newQueryNode.rowReductionFactor = this.rowReductionFactor;

        newQueryNode.isTreeDownJoin = this.isTreeDownJoin;

        return newQueryNode;
    }

   
    /** 
     * See if subtree from this node contains a particular table
     *
     * @param searchTableName
     * @param searchAlias
     * @return
     */

    public QueryNode subtreeFind(String searchTableName, String searchAlias) {
        QueryNode foundNode = null;

        if (relationNode != null) {
            if (searchTableName.compareTo(relationNode.getTableName()) == 0
                    && searchAlias.compareTo(relationNode.getAlias()) == 0) {
                return this;
            } else {
                return null;
            }
        }

        if (leftNode != null) {
            foundNode = leftNode.subtreeFind(searchTableName, searchAlias);
        }

        if (foundNode == null) {
            if (rightNode != null) {
                foundNode = rightNode.subtreeFind(searchTableName, searchAlias);
            } else {
                foundNode = null;
            }
        }

        return foundNode;
    }

    // BUILD_CUT_START
    // for helping to debug
    /**
     *
     * @param prepend
     * @return
     */

    public String toString(String prepend) {
        StringBuffer sbNode = new StringBuffer();

        sbNode.append(prepend).append("--------------------------------------");
        sbNode.append('\n');
        sbNode.append(prepend).append(" label = ").append(label);
        sbNode.append('\n');
        sbNode.append(prepend).append(" nodeId = ").append(nodeId);
        sbNode.append('\n');
        sbNode.append(prepend).append(" nodeType =       ").append(nodeType);
        sbNode.append('\n');

        if (parent != null) {
            sbNode.append(prepend).append(" parent = ").append(parent.label);
            sbNode.append('\n');
        }

        switch (nodeType) {
        case JOIN:
            sbNode.append(prepend).append(" JOIN");
            sbNode.append('\n');
            break;

        case RELATION:
            sbNode.append(prepend).append(" RELATION");
            sbNode.append('\n');
            sbNode.append(prepend).append(" - Type: ").append(relationNode.getTypeString());
            sbNode.append('\n');
            break;
        }

        sbNode.append(prepend).append(" prserveSubTree = ").append(preserveSubtree);
        sbNode.append('\n');
        sbNode.append(prepend).append(" rowsize = ").append(rowsize);
        sbNode.append('\n');
        sbNode.append(prepend).append(" selectRowSize = ").append(selectRowSize);
        sbNode.append('\n');
        sbNode.append(prepend).append(" estCost = ").append(estCost);
        sbNode.append('\n');
        sbNode.append(prepend).append(" estRowsReturned = ").append(estRowsReturned);
        sbNode.append('\n');
        sbNode.append(prepend).append(" rowReductionFactor = ").append(rowReductionFactor);
        sbNode.append('\n');
        sbNode.append(prepend).append(" isSubtreeOuter = ").append(isSubtreeOuter);
        sbNode.append('\n');
        sbNode.append(prepend).append(" outerLevel = ").append(outerLevel);
        sbNode.append('\n');
        sbNode.append(prepend).append(" outerGroup = ").append(outerGroup);
        sbNode.append('\n');
        sbNode.append(prepend).append(" isTreeDownJoin = ").append(isTreeDownJoin);
        sbNode.append('\n');

        sbNode.append(prepend);
        sbNode.append('\n');
        sbNode.append(prepend).append(" ---Joins--- ");
        sbNode.append('\n');

        for (Integer joinNodeInt : joinList) {
            sbNode.append(prepend).append(" ").append(joinNodeInt.toString());
            sbNode.append('\n');
        }

        sbNode.append(prepend);
        sbNode.append('\n');
        sbNode.append(prepend).append(" ---Projections--- ");
        sbNode.append('\n');

        for (SqlExpression aSqlExpression : projectionList) {
            sbNode.append(aSqlExpression.toString(prepend));
            sbNode.append('\n');
        }

        sbNode.append(prepend);
        sbNode.append('\n');

        sbNode.append(prepend).append(" ---Conditions--- ");
        sbNode.append('\n');
        sbNode.append(prepend).append(" Condition count: ").append(conditionList.size());
        sbNode.append('\n');

        for (QueryCondition aCondition : conditionList) {
            sbNode.append(prepend).append(" ").append(aCondition.getCondString());
            sbNode.append('\n');
        }

        sbNode.append(prepend);
        sbNode.append('\n');
        sbNode.append(prepend).append(" ---Condition Columns--- ");
        sbNode.append('\n');

        return sbNode.toString();
    }

    // BUILD_CUT_ALT
    // public String toString (String prepend)
    // {
    // return null;
    // }
    // BUILD_CUT_END

    /**
     * Checks to see if specified QueryNode is in its subtree.
     * @param aQueryNode
     * @return
     */
    public boolean subtreeContains(QueryNode aQueryNode) {
        return this == aQueryNode
                || leftNode != null
                && (leftNode.subtreeContains(aQueryNode) || rightNode.subtreeContains(aQueryNode));
    }

    /**

     * Checks to see if specified RelationNode is in its subtree.
     * @param aRelationNode
     * @return
     */
    public boolean subtreeContains(RelationNode aRelationNode) {
        return relationNode == aRelationNode
        || leftNode != null
        && (leftNode.subtreeContains(aRelationNode) || rightNode.subtreeContains(aRelationNode));
    }

    /**
     * @return true if this is a correlated RelationNode
     */
    public boolean isCorrelatedSubquery () {
        if (relationNode != null) {
            return nodeType == RELATION && relationNode.isCorrelatedSubquery();
        }
        return false;
    }

    /**
     * @return true if this is a correlated RelationNode
     */
    public boolean isCorrelatedPlaceholder () {
        if (relationNode != null) {
            return nodeType == RELATION && relationNode.isCorrelatedPlaceholder();
        }
        return false;
    }

    /**
     * @return true if this is an uncorrelated RelationNode
     */
    public boolean isUncorrelatedSubquery() {
        if (relationNode != null) {
            return nodeType == RELATION && relationNode.isUncorrelatedSubquery();
        }
        return false;
    }

    /**
     * @return true if this is a scalar subquery
     */
    public boolean isScalarSubquery() {
        if (relationNode != null) {
            return nodeType == RELATION && relationNode.isScalarSubquery();
        }
        return false;
    }

    /**
     * @return true if this is a relation subquery
     */
    public boolean isRelationSubquery() {
        if (relationNode != null) {
            return nodeType == RELATION && relationNode.isRelationSubquery();
        }    
        return false;
    }

    /**
     * @return true if this is a relation subquery
     */
    public boolean isTable() {
        if (relationNode != null) {
            return nodeType == RELATION && relationNode.isTable();
        }
        return false;
    }   
        
    /**
     * @param baseNumRows the baseNumRows to set
     */
    public void setBaseNumRows(long baseNumRows) {
        this.baseNumRows = baseNumRows;
    }

    /**
     * @return the baseNumRows
     */
    public long getBaseNumRows() {
        return baseNumRows;
    }

    /**
     * @param conditionList the conditionList to set
     */
    public void setConditionList(List<QueryCondition> conditionList) {
        this.conditionList = conditionList;
    }

    /**
     * @return the conditionList
     */
    public List<QueryCondition> getConditionList() {
        return conditionList;
    }

    /**
     * @param estCost the estCost to set
     */
    public void setEstCost(long estCost) {
        this.estCost = estCost;
    }

    /**
     * @return the estCost
     */
    public long getEstCost() {
        return estCost;
    }

    /**
     * @param estRowsReturned the estRowsReturned to set
     */
    public void setEstRowsReturned(long estRowsReturned) {
        this.estRowsReturned = estRowsReturned;
    }

    /**
     * @return the estRowsReturned
     */
    public long getEstRowsReturned() {
        return estRowsReturned;
    }

    /**
     * @param groupByList the groupByList to set
     */
    public void setGroupByList(List<SqlExpression> groupByList) {
        this.groupByList = groupByList;
    }

    /**
     * @return the groupByList
     */
    public List<SqlExpression> getGroupByList() {
        return groupByList;
    }

    /**
     * @param isSubtreeOuter the isSubtreeOuter to set
     */
    public void setSubtreeOuter(boolean isSubtreeOuter) {
        this.isSubtreeOuter = isSubtreeOuter;
    }

    /**
     * @return the isSubtreeOuter
     */
    public boolean isSubtreeOuter() {
        return isSubtreeOuter;
    }

    /**
     * @param isTreeDownJoin the isTreeDownJoin to set
     */
    public void setTreeDownJoin(boolean isTreeDownJoin) {
        this.isTreeDownJoin = isTreeDownJoin;
    }

    /**
     * @return the isTreeDownJoin
     */
    public boolean isTreeDownJoin() {
        return isTreeDownJoin;
    }

    /**
     * @return the joinList
     */
    public List<Integer> getJoinList() {
        return joinList;
    }

    /**
     * @param label the label to set
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * @return the label
     */
    public String getLabel() {
        return label;
    }

    /**
     * @param leftNode the leftNode to set
     */
    public void setLeftNode(QueryNode leftNode) {
        this.leftNode = leftNode;
    }

    /**
     * @return the leftNode
     */
    public QueryNode getLeftNode() {
        return leftNode;
    }

    /**
     * @param nodeId the nodeId to set
     */
    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return the nodeId
     */
    public int getNodeId() {
        return nodeId;
    }

    /**
     * @param nodeType the nodeType to set
     */
    public void setNodeType(int nodeType) {
        this.nodeType = nodeType;
    }

    /**
     * @return the nodeType
     */
    public int getNodeType() {
        return nodeType;
    }

    /**
     * @param outerGroup the outerGroup to set
     */
    public void setOuterGroup(short outerGroup) {
        this.outerGroup = outerGroup;
    }

    /**
     * @return the outerGroup
     */
    public short getOuterGroup() {
        return outerGroup;
    }

    /**
     * @param outerLevel the outerLevel to set
     */
    public void setOuterLevel(short outerLevel) {
        this.outerLevel = outerLevel;
    }

    /**
     * @return the outerLevel
     */
    public short getOuterLevel() {
        return outerLevel;
    }

    /**
     * @param parent the parent to set
     */
    public void setParent(QueryNode parent) {
        this.parent = parent;
    }

    /**
     * @return the parent
     */
    public QueryNode getParent() {
        return parent;
    }

    /**
     * @param parentTree the parentTree to set
     */
    public void setParentTree(QueryTree parentTree) {
        this.parentTree = parentTree;
    }

    /**
     * @return the parentTree
     */
    public QueryTree getParentTree() {
        return parentTree;
    }

    /**
     * @param preserveSubtree the preserveSubtree to set
     */
    public void setPreserveSubtree(boolean preserveSubtree) {
        this.preserveSubtree = preserveSubtree;
    }

    /**
     * @return the preserveSubtree
     */
    public boolean isPreserveSubtree() {
        return preserveSubtree;
    }

    /**
     * @param projectionList the projectionList to set
     */
    public void setProjectionList(List<SqlExpression> projectionList) {
        this.projectionList = projectionList;
    }

    /**
     * @return the projectionList
     */
    public List<SqlExpression> getProjectionList() {
        return projectionList;
    }

    /**
     * @return the relationNode
     */
    public RelationNode getRelationNode() {
        return relationNode;
    }

    /**
     * @param rightNode the rightNode to set
     */
    public void setRightNode(QueryNode rightNode) {
        this.rightNode = rightNode;
    }

    /**
     * @return the rightNode
     */
    public QueryNode getRightNode() {
        return rightNode;
    }

    /**
     * @param rowReductionFactor the rowReductionFactor to set
     */
    public void setRowReductionFactor(double rowReductionFactor) {
        this.rowReductionFactor = rowReductionFactor;
    }

    /**
     * @return the rowReductionFactor
     */
    public double getRowReductionFactor() {
        return rowReductionFactor;
    }

    /**
     * @param rowsize the rowsize to set
     */
    public void setRowsize(int rowsize) {
        this.rowsize = rowsize;
    }

    /**
     * @return the rowsize
     */
    public int getRowsize() {
        return rowsize;
    }

    /**
     * @param selectRowSize the selectRowSize to set
     */
    public void setSelectRowSize(int selectRowSize) {
        this.selectRowSize = selectRowSize;
    }

    /**
     * @return the selectRowSize
     */
    public int getSelectRowSize() {
        return selectRowSize;
    }

    /**
     * @return the uncorrelatedCondTreeList
     */
    public List<RelationNode> getUncorrelatedCondTreeList() {
        return uncorrelatedCondTreeList;
    }
    
    /**
     * 
     * @return whether or not this node stems from a WITH clause
     */
    public boolean isWith() {
        if (getRelationNode() != null) {
            return getRelationNode().isWith();
        }
        return false;
    }
    
    /**
     * 
     * @return 
     */
    public boolean isUnusedTopWith() {
        return isWith() && !getRelationNode().isTopMostUsedWith();
    }
    
    /**
     * 
     * @return whether or not this relation has been derived
     * from a WITH statement. Note it does not mean that it
     * is a WITH statement (isWith()), just that it is a
     * wrapper node.
     */
    public boolean isWithDerived() {
        return relationNode.isWithDerived();
    }
}
