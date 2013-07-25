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
/******************************************************************************
 * QueryTree.java
 *
 *
 *****************************************************************************/
package org.postgresql.stado.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysTablespace;
import org.postgresql.stado.metadata.partitions.HashPartitionMap;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.metadata.partitions.ReplicatedPartitionMap;
import org.postgresql.stado.metadata.partitions.RobinPartitionMap;
import org.postgresql.stado.parser.SqlCreateTableColumn;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;


public class QueryTree implements IRebuildString {
    private static final XLogger logger = XLogger.getLogger(QueryTree.class);

    public static final int SCALAR_CORRELATED = 2;

    public static final int SCALAR_NONCORRELATED = 4;

    public static final int NONSCALAR_CORRELATED = 8;

    public static final int NONSCALAR_NONCORRELATED = 16;

    public static final int SCALAR = SCALAR_NONCORRELATED | SCALAR_CORRELATED;

    public static final int NONSCALAR = NONSCALAR_CORRELATED
            | NONSCALAR_NONCORRELATED;

    public static final int CORRELATED = SCALAR_CORRELATED | NONSCALAR_CORRELATED;

    public static final int NONCORRELATED = SCALAR_NONCORRELATED
            | NONSCALAR_NONCORRELATED;


    public static final int UNIONTYPE_NONE = 0;

    public static final int UNIONTYPE_UNION = 1;

    public static final int UNIONTYPE_UNIONALL = 2;

    //we need it to store relation info in case of left outer (+)
    private Map<String, RelationNode> relHandlerInfo =
                                    new HashMap<String, RelationNode>();

    private int queryType;

    private Map<QueryTree, RelationNode> placeHolderNodes = new HashMap<QueryTree, RelationNode>();

    private boolean isOuterJoin = false;

    // The conditions in the from clause when we have a outer . inner or a cross
    // join
    private List<QueryCondition> fromClauseConditions;

    private QueryTree parentQueryTree;
    
    // Reference to the top-most QueryTree
    // Helps with WITH clause
    private QueryTree topMostParentQueryTree;

    // This variable is only filled if this particular
    // tree is a sub query tree and has a co-relation with the
    // parent tree. This relation node there fore acts as a place holder
    // for that paricualr node.
    private RelationNode psuedoRelationNode;

    // This variable is used for storing the where condition root condition
    private QueryCondition whereRoot = null;

    // Contains a list of QueryTree for union with this Query Tree
    private List<QueryTree> unionQueryTreeList = new ArrayList<QueryTree>();

    private boolean hasUnion = false;

    private int unionType = UNIONTYPE_NONE;

    // Contains list of QueryConditions for this tree
    // where a condition does a scalar comparison based
    // on the results of a subquery that retains exactly one value
    // We make it a list of the class QueryCondition so that
    // we can simplify the condition and use a constant based on the result
    private List<SqlExpression> scalarSubqueryList; // of type SqlExpression

    // We keep 2 lists for noncorrelated queries,
    // since we treat it as a QueryNode in costing and joining,
    // but also as an expression for rewriting.
    private List<RelationNode> noncorSubqueryList;

    private List<RelationNode> correlatedSubqueryList;

    private List<RelationNode> relationSubqueryList;
    
    // For tracking WITH subqueries
    private List<RelationNode> topWithSubqueryList;

    private QueryNode rootNode; // main root node

    // This is the original top-level projection List
    private List<SqlExpression> projectionList;

    // This is a list which will be filled incase we have orphan columns
    private List<SqlExpression> hiddenProjectionList;

    private boolean isDistinct = false;

    // This is the modified, final top-level projection List
    private List<SqlExpression> finalProjList;

    // All conditions are in here, and are considered to be at the top "and"
    // level. This is done to make the job of the Optimizer easier

    // The Query Tree conditions are thoes which affect more than 1 condition
    // list
    private List<QueryCondition> conditionList; // QueryCondition

    // final group by list, of type SqlExpression
    private List<SqlExpression> groupByList;

    // final having list, of type QueryCondition
    private List<QueryCondition> havingList;

    // final order by list, of type SqlExpression
    private List<OrderByElement> orderByList;

    private List<RelationNode> relationNodeList;

    private List<Integer> unusedQueryNodeList;

    private Map<Integer, QueryNode> queryNodeTable;

    private boolean containsAggregates = false;

    private int currentNodeId = 0;

    private int currentRelationNodeId = 0;

    // Columns in the list which are orphan
    private List<SqlExpression> selectOrphans = new ArrayList<SqlExpression>();

    private List<SqlExpression> whereOrphans = new ArrayList<SqlExpression>();

    private List<SqlExpression> orderByOrphans = new ArrayList<SqlExpression>();

    private int lastOuterLevel = -1; // help with determining outers

    private boolean isCorrelatedSubtree = false;

    private boolean isPartOfExistClause = false;

    /**
     * whether first element in group by is a partitioned column
     */
    private boolean isPartitionedGroupBy = false;

    /** Support of LIMIT clause */
    private long limit = -1;

    /** Support of OFFSET clause */
    private long offset = -1;

    /*
     * SELECT INTO support
     */
    private boolean isInsertSelect = false;

    private String intoTableName;

    /** the actual name, whether or not it is a temp table */
    private String intoTableRefName;

    private boolean intoTempTable;

    private short partType = SysTable.PTYPE_DEFAULT;

    private PartitionMap partMap = null;

    private String partColumn = null;

    private List<SqlCreateTableColumn> colDefs;

    private SysTablespace tablespace;

    private SysTable intoTable;

    private boolean fetchCursor = false;
    private String cursorName;
    private int fetchCount = -1;
   

    public void setFetchCursor(boolean value) {
    	fetchCursor = value;
    }

    public boolean isFetchCursor() {
    	return fetchCursor;
    }

    public void setCursorName(String name) {
    	cursorName = name;
    }

    public String getCursorName() {
    	return cursorName;
    }

    public void setFetchCount(int value) {
    	fetchCount = value;
    }

    public int getFetchCount() {
    	return fetchCount;
    }


    /**
     * Assign target table information, for SELECT...INTO or CREATE TABLE AS
     * @param tableName
     * @param temporary
     */
    public void setIntoTable(String tableName, String referenceTableName,
            boolean temporary) {
    	intoTableName = tableName;
        intoTableRefName = referenceTableName;
    	intoTempTable = temporary;
    }

    /**
     * Assign target table partitioning information, for SELECT...INTO or
     * CREATE TABLE AS
     * @param partType
     * @param partColumn
     * @param partMap
     */
    public void setIntoTablePartitioning(short partType, String partColumn,
    		PartitionMap partMap) {
    	this.partType = partType;
    	this.partColumn = partColumn;
    	this.partMap = partMap;
    }

    /**
     * Assign information about target table's columns, for SELECT...INTO or
     * CREATE TABLE AS
     * @param columnNames
     */
    public void setIntoTableColumns(List<String> columnNames) {
    	if (columnNames == null) {
            return;
    	}
    	if (columnNames.size() > projectionList.size()) {
            throw new XDBServerException("CREATE TABLE AS specifies too many column names");
    	}
    	for (int i = 0; i < columnNames.size(); i++) {
            projectionList.get(i).setAlias(columnNames.get(i));
    	}
    }

    /**
     * Assign information about target table's tablespace, for SELECT...INTO or
     * CREATE TABLE AS
     * @param tablespace
     */
    public void setIntoTableSpace(SysTablespace tablespace) {
    	this.tablespace = tablespace;
    }

    /**
     * Get a Tablespace where target table should be created, for SELECT...INTO
     * or CREATE TABLE AS
     * @return
     */
    public SysTablespace getIntoTableSpace() {
    	return tablespace;
    }

    /**
     * Get name of the target table, for SELECT...INTO or CREATE TABLE AS
     * @return
     */
    public String getIntoTableName() {
    	return intoTableName;
    }

    /**
     * Get reference name of the target table, for SELECT...INTO or CREATE TABLE AS
     * @return
     */
    public String getIntoTableRefName() {
    	return intoTableRefName;
    }

    /**
     * Return if target table is temporary, for SELECT...INTO or CREATE TABLE AS
     * @return
     */
    public boolean isIntoTempTable() {
    	return intoTempTable;
    }

    /**
     * Get target columns as a list of SqlCreateTableColumn objects
     * @return
     */
    public List<SqlCreateTableColumn> getColumnDefinitions() {
    	if (colDefs == null) {
    		colDefs = new ArrayList<SqlCreateTableColumn>(projectionList.size());
    		for (SqlExpression colExpr : projectionList) {
    			colDefs.add(new SqlCreateTableColumn(colExpr));
    		}
    	}
    	return colDefs;
    }

    public void copyIntoTableInfo(QueryTree origin) {
        intoTable = origin.intoTable;
        intoTableName = origin.intoTableName;
        intoTableRefName = origin.intoTableRefName;
        intoTempTable = origin.intoTempTable;
        partType = origin.partType;
        partColumn = origin.partColumn;
        partMap = origin.partMap;
        tablespace = origin.tablespace;
    }

    /**
     * Create a SysTable for table that should be created by SELECT INTO or
     * CREATE TABLE AS statement. The created SysTable is neither inserted into
     * MetaData model nor persisted in the Metadata Database. Use
     * {@link org.postgresql.stado.metadata.SyncCreateTable} to do this
     * @param client
     * @return
     * @throws Exception
     */
    public SysTable createIntoTable(XDBSessionContext client) throws Exception {
        if (intoTable == null && intoTableName != null) {
            SysDatabase database = client.getSysDatabase();
            List<SqlCreateTableColumn> columnDefs = getColumnDefinitions();
            if (partType == SysTable.PTYPE_DEFAULT) {
                partType = SysTable.PTYPE_ROBIN;
                for (SqlCreateTableColumn colDef : columnDefs) {
                    if (colDef.canBePartitioningKey()) {
                        partType = SysTable.PTYPE_HASH;
                        partColumn = colDef.columnName;
                        break;
                    }
                }
            }

            intoTable = database.createSysTable(intoTableName, partType,
                    getPartitionMap(database), partColumn, null, null,
                    getColumnDefinitions(), tablespace, intoTempTable, client);
        }
        return intoTable;
    }

    private PartitionMap getPartitionMap(SysDatabase database) {
        if (partMap == null) {
            switch (partType) {
            case SysTable.PTYPE_HASH:
                partMap = new HashPartitionMap();
                break;
            case SysTable.PTYPE_LOOKUP:
            case SysTable.PTYPE_ONE:
                partMap = new ReplicatedPartitionMap();
                break;
            case SysTable.PTYPE_ROBIN:
                partMap = new RobinPartitionMap();
                break;
            default:
                return null;
            }
            Collection<DBNode> dbNodes = database.getDBNodeList();
            Collection<Integer> nodeNums = new ArrayList<Integer>(
                    dbNodes.size());
            for (DBNode dbNode : dbNodes) {
                nodeNums.add(dbNode.getNodeId());
            }
            partMap.generateDistribution(nodeNums);
        }
        return partMap;
    }

    /**
     * This method checks whether aliasToCheck is alias or table name
     * if it is alias then it returns its corresponding table name
     * @Parameter String aliasToCheck
     * @Return String tableName
     */
    public String getTableNameOfAlias(String aliasToCheck) {

        String tableAlias = null;

        for (RelationNode relNode : relHandlerInfo.values()) {

            //if token contains table alias, store the table name and return
            if((tableAlias = relNode.getTableAlis()) != null
                    && aliasToCheck.equalsIgnoreCase(tableAlias)) {
                return relNode.getTableName();
            }
        }

        return aliasToCheck;
    }

    /**
     *
     * @return
     */
    public int getQueryType() {
        return queryType;
    }

    /**
     *
     * @param queryType
     */
    public void setQueryType(int queryType) {
        this.queryType = queryType;
    }

    /**
     *
     * @return
     */
    public List<QueryCondition> getFromClauseConditions() {
        return fromClauseConditions;
    }

    /**
     *
     * @param value
     */
    public void setPartOfExistClause(boolean value) {
        isPartOfExistClause = value;
    }


    /**
     * Constructor
     *
     * @param commandToExecute
     */
    public QueryTree() {
        projectionList = new ArrayList<SqlExpression>();
        hiddenProjectionList = new ArrayList<SqlExpression>();
        finalProjList = new ArrayList<SqlExpression>();
        conditionList = new ArrayList<QueryCondition>();
        groupByList = new ArrayList<SqlExpression>();
        havingList = new ArrayList<QueryCondition>();
        orderByList = new ArrayList<OrderByElement>();
        relationSubqueryList = new ArrayList<RelationNode>();
        topWithSubqueryList = new ArrayList<RelationNode>();
        scalarSubqueryList = new ArrayList<SqlExpression>();
        noncorSubqueryList = new ArrayList<RelationNode>();
        correlatedSubqueryList = new ArrayList<RelationNode>();
        relationNodeList = new ArrayList<RelationNode>();
        unusedQueryNodeList = new ArrayList<Integer>();
        queryNodeTable = new HashMap<Integer, QueryNode>();
        fromClauseConditions = new ArrayList<QueryCondition>();
    }

    // Note: copies references to lists in some cases
    // This is used in generating multiple trees.
    /**
     *
     * @return
     */
    public QueryTree copy() {
        QueryNode newQueryNode;
        QueryTree newQueryTree = new QueryTree();

        newQueryTree.projectionList = this.projectionList;

        newQueryTree.hiddenProjectionList = this.hiddenProjectionList;
        newQueryTree.whereRoot = this.whereRoot;
        newQueryTree.conditionList = this.conditionList;
        newQueryTree.groupByList = this.groupByList;
        newQueryTree.havingList = this.havingList;
        newQueryTree.orderByList = this.orderByList;
        newQueryTree.containsAggregates = this.containsAggregates;
        newQueryTree.isDistinct = this.isDistinct;
        newQueryTree.currentNodeId = this.currentNodeId;
        newQueryTree.currentRelationNodeId = this.currentRelationNodeId;

        newQueryTree.lastOuterLevel = this.lastOuterLevel;

        newQueryTree.unionType = this.unionType;

        // ok to copy SqlExpression
        newQueryTree.scalarSubqueryList = this.scalarSubqueryList;

        // these are ok to copy; Vectors of type RelationNode
        newQueryTree.noncorSubqueryList = this.noncorSubqueryList;
        newQueryTree.correlatedSubqueryList = this.correlatedSubqueryList;
        newQueryTree.relationSubqueryList = this.relationSubqueryList;
        newQueryTree.topWithSubqueryList = this.topWithSubqueryList;

        // copy references to relationNodes
        newQueryTree.relationNodeList = this.relationNodeList;

        // First, only copy RELATION nodes
        for (QueryNode aQueryNode : queryNodeTable.values()) {
            if (aQueryNode.getNodeType() == QueryNode.RELATION) {
                newQueryNode = aQueryNode.baseCopy();

                newQueryTree.queryNodeTable.put(
                        Integer.valueOf(newQueryNode.getNodeId()), newQueryNode);
            }
        }
        // Now, do it again and handle any joins that we have created.
        for (QueryNode aQueryNode : queryNodeTable.values()) {
            if (aQueryNode.getNodeType() == QueryNode.JOIN
                    && aQueryNode.isPreserveSubtree()) {
                newQueryNode = copySubtreeNodes(aQueryNode, newQueryTree);

                newQueryTree.queryNodeTable.put(
                        Integer.valueOf(newQueryNode.getNodeId()), newQueryNode);
            }
        }

        // Copy root node and all its children
        // (Creates new JOIN nodes, adds references to nodes
        // already in our new queryNodeTable
        if (this.rootNode != null) {
            newQueryTree.rootNode = copyTreeNodeLinks(this.rootNode,
                    newQueryTree);
        }

        for (int i = 0; i < this.unusedQueryNodeList.size(); i++) {
            newQueryTree.unusedQueryNodeList.add(unusedQueryNodeList.get(i));
        }

        // Adding for parser - This makes a uplink to the query tree to which it
        // belongs
        // it is supposed to be null when there is no parent query tree.
        newQueryTree.parentQueryTree = this.parentQueryTree;

        // Adding the from clause conditons - This is a shallow copy.
        newQueryTree.fromClauseConditions = this.fromClauseConditions;

        newQueryTree.queryType = this.queryType;
        newQueryTree.isPartitionedGroupBy = this.isPartitionedGroupBy;
        newQueryTree.limit = this.limit;
        newQueryTree.offset = this.offset;

        newQueryTree.fetchCursor = this.fetchCursor;
        newQueryTree.cursorName = this.cursorName;
        newQueryTree.fetchCount = this.fetchCount;

        newQueryTree.copyIntoTableInfo(this);

        return newQueryTree;
    }

    /**
     *
     * @param aQueryNode
     * @param newQT
     * @return
     */
    public QueryNode copySubtreeNodes(QueryNode aQueryNode, QueryTree newQT) {
        QueryNode newQueryNode;

        if (aQueryNode.getNodeType() == QueryNode.JOIN) {
            // create a new join query node
            newQueryNode = aQueryNode.baseCopy();

            newQueryNode.setLeftNode(copySubtreeNodes(aQueryNode.getLeftNode(), newQT));
            newQueryNode.getLeftNode().setParent(newQueryNode);

            newQueryNode.setRightNode(copySubtreeNodes(aQueryNode.getRightNode(),
                    newQT));
            newQueryNode.getRightNode().setParent(newQueryNode);
        } else {
            // We already created it, so just look it up and reference it.
            newQueryNode = newQT.getNodeById(aQueryNode.getNodeId());
        }

        return newQueryNode;
    }

    /**
     * This is called when copying the actual structure tree
     * built up so far.
     *
     * @param aQueryNode
     * @param newQueryTree
     * @return QueryNode
     */
    public QueryNode copyTreeNodeLinks(QueryNode aQueryNode,
            QueryTree newQueryTree) {
        QueryNode newQueryNode;

        if (aQueryNode.getNodeType() == QueryNode.JOIN
                && !aQueryNode.isPreserveSubtree()) {
            // create a new query node
            newQueryNode = aQueryNode.baseCopy();

            newQueryNode.setLeftNode(copyTreeNodeLinks(aQueryNode.getLeftNode(),
                    newQueryTree));
            newQueryNode.getLeftNode().setParent(newQueryNode);

            newQueryNode.setRightNode(copyTreeNodeLinks(aQueryNode.getRightNode(),
                    newQueryTree));
            newQueryNode.getRightNode().setParent(newQueryNode);
        } else {
            newQueryNode = newQueryTree.getNodeById(aQueryNode.getNodeId());

            if (newQueryNode == null) {
                logger.debug("Could not find nodeId: " + aQueryNode.getNodeId());
            }
        }

        return newQueryNode;
    }

    /**
     * Get the next id.
     * We assign from the top most parent tree, making handling
     * WITH subqueries easier
     * @return 
     */
    public int getNextNodeId()
    {
        if (getTopMostParentQueryTree() != null 
                && this != getTopMostParentQueryTree()) {
            return getTopMostParentQueryTree().getNextNodeId();
        } else {
            return ++currentRelationNodeId; 
        }
    }
    
    /**
     * Get a new RelationNode.
     * This is the preferred technique for instantiating new RelationNodes.
     * It also adds it to the relationNodeList
     *
     * @return RelationNode
     */
    public RelationNode newRelationNode() {
        RelationNode aRelationNode = new RelationNode();

        aRelationNode.setNodeId(getNextNodeId());
        relationNodeList.add(aRelationNode);
        aRelationNode.setQueryTree(this);

        return aRelationNode;
    }



    /**
     * Get a new QueryNode
     *
     * @return QueryNode
     */
    public QueryNode newQueryNode() {
        QueryNode aQueryNode = new QueryNode();

        aQueryNode.setParentTree(this);
        aQueryNode.setNodeId(++currentNodeId);
        aQueryNode.setNodeType(QueryNode.JOIN);

        return aQueryNode;
    }


    // Get a new QueryNode - with a known RelationNode
    // This is the preferred technique for instantiating new nodes.
    // ------------------------------------------------------------------------
    /**
     *
     * @param aRelationNode
     * @return QueryNode
     */
    public QueryNode newQueryNode(RelationNode aRelationNode) {
        QueryNode aQueryNode = new QueryNode(aRelationNode);

        currentNodeId = aRelationNode.getNodeId();

        aQueryNode.setParentTree(this);
        //We increment it to be used later when creating join nodes.
        aQueryNode.setNodeId(currentNodeId++);
        aQueryNode.setNodeType(QueryNode.RELATION);

        return aQueryNode;
    }


    // for debugging
    // BUILD_CUT_START
    /**
     *
     * @return
     */
    @Override
    public String toString() {
        if (rootNode == null) {
            // NPE preventing
            return super.toString();
        }
        relabel();
        return queryNodeToString(rootNode, "L");
    }

    // BUILD_CUT_END

    /**
     * Convert the QueryNode to a string for debugging
     * @param aQueryNode
     * @param prepend
     * @return
     */
    private String queryNodeToString(QueryNode aQueryNode, String prepend) {
        // BUILD_CUT_START
        StringBuffer sbQueryNode = new StringBuffer();

        sbQueryNode.append(aQueryNode.toString(prepend));

        if (aQueryNode.getRightNode() != null) {
            sbQueryNode.append(queryNodeToString(aQueryNode.getRightNode(), prepend
                    + "R"));
        }

        if (aQueryNode.getLeftNode() != null) {
            sbQueryNode.append(queryNodeToString(aQueryNode.getLeftNode(), prepend
                    + "L"));
        }

        return sbQueryNode.toString();
        // BUILD_CUT_ALT
        // return null;
        // BUILD_CUT_END
    }


    // for debugging
    public void relabel() {
        relabelOne(rootNode, "L");
    }


    // recursively print out nodes
    /**
     *
     * @param aQueryNode
     * @param prepend
     */
    private void relabelOne(QueryNode aQueryNode, String prepend) {
        aQueryNode.setLabel(prepend);

        if (aQueryNode.getRightNode() != null) {
            relabelOne(aQueryNode.getRightNode(), prepend + "R");
        }

        if (aQueryNode.getLeftNode() != null) {
            relabelOne(aQueryNode.getLeftNode(), prepend + "L");
        }
    }

    // Get Set Methods--- for whereRoot
    /**
     *
     * @return
     */
    public QueryCondition getWhereRootCondition() {
        return whereRoot;
    }

    /**
     *
     * @param qc
     */
    public void setWhereRootCondition(QueryCondition qc) {
        whereRoot = qc;
    }

    /**
     *
     * @param nodeId
     * @return
     */

    public QueryNode getNodeById(int nodeId) {
        return queryNodeTable.get(Integer.valueOf(nodeId));
    }

    /**
     * @param the QueryNode to add to the tracking table
     */
    public void addToQueryNodeTable (QueryNode aQueryNode) {
        getQueryNodeTable().put(Integer.valueOf(aQueryNode.getNodeId()), aQueryNode);
    }



    /**
     * setLimit for QueryTree
     * @param limit
     */
    public void setLimit(long limit) {
        this.limit = limit;
    }


    /**
     * getLimit for QueryTree
     * @return
     */
    public long getLimit() {
        return limit;
    }


    /**
     * setOffset for QueryTree
     * @param offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }


    /**
     * getOffset for QueryTree
     * @return
     */
    public long getOffset() {
        return offset;
    }


    /**
     *
     * @return
     */
    public String rebuildString() {
        String selectString = " SELECT ";

        if (this.isDistinct) {
            selectString += " DISTINCT ";
        }
        // For each Sql Expression -

        // selectString += getListString(projectionList);
        selectString += getListStringOfProjections(projectionList);
        // from clause
        selectString += " From  ";
        selectString += getListString(relationNodeList);

        if (getWhereRootCondition() != null) {
            selectString += " WHERE ";
            this.getWhereRootCondition().rebuildCondString();
            selectString += getWhereRootCondition().getCondString();
        }

        int indexCond = 0;
        if(conditionList.size() != 0) {
            for(QueryCondition aQC : conditionList) {
                if (getWhereRootCondition() != null
                        && getWhereRootCondition().getAndedConditions().contains(aQC)) {
                    continue;
                }
                if (indexCond != 0 || getWhereRootCondition() != null) {
                    selectString += " AND ";
                } else {
                    selectString += " WHERE ";
                }
                indexCond++;
                aQC.rebuildCondString();
                selectString += aQC.getCondString();
            }
        }

        if (groupByList.size() > 0) {
            selectString += " GROUP BY ";
            selectString += getListString(groupByList) + " ";
        }
        if (getHavingList().size() > 0) {
            selectString += " HAVING ";
            QueryCondition having = havingList.get(0);
            having.rebuildCondString();
            selectString += " " + having.getCondString();
        }

        if (this.unionQueryTreeList != null) {
            for (QueryTree unionQueryTree : unionQueryTreeList) {
                if (unionQueryTree.unionType == UNIONTYPE_UNIONALL) {
                    selectString += " UNION ALL ";
                } else {
                    selectString += " UNION ";
                }
                selectString += unionQueryTree.rebuildString();
            }
        }

        if (orderByList.size() > 0) {
            selectString += " ORDER BY ";
            selectString += getListString(orderByList);
        }

        if (limit > 0) {
        	selectString += " LIMIT ";
        	selectString += limit;
        }

        return selectString;
    }

    /*
     * public String toString() { return selectString; }
     */

    /**
     *
     * @param list
     * @return
     */
    private String getListString(List<? extends IRebuildString> list) {
        boolean firstTime = true;
        String listString = "";
        for (IRebuildString rebuildNode : list) {
            // To allow for ","
            if (firstTime) {

                listString += rebuildNode.rebuildString();
                firstTime = false;
            } else {
                if (rebuildNode.rebuildString() == null
                        || rebuildNode.rebuildString().equals("")) {
                    continue;
                }
                listString += ", " + rebuildNode.rebuildString();
            }
        }
        return listString;
    }


    /**
     *
     * @param list
     * @return
     */

    private String getListStringOfProjections(List<? extends IRebuildString> list) {
        boolean firstTime = true;
        String listString = "";
        for (IRebuildString rebuildNode : list) {
            // To allow for ","
            if (firstTime) {

                listString += rebuildNode.rebuildString();
                if (((SqlExpression) rebuildNode).getAlias().length() > 0) {
                    listString += " as " + IdentifierHandler.quote(
                            ((SqlExpression) rebuildNode).getAlias());
                }
                firstTime = false;
            } else {
                if (rebuildNode.rebuildString() == null
                        || rebuildNode.rebuildString().equals("")) {
                    continue;
                }
                listString += ", " + rebuildNode.rebuildString();
                ;
                if (((SqlExpression) rebuildNode).getAlias().length() > 0) {
                    listString += " as " + IdentifierHandler.quote(
                            ((SqlExpression) rebuildNode).getAlias());
                }
            }
        }
        return listString;
    }


    /**
     *
     * @return
     */

    public int getOrphanCount() {
        return orderByOrphans.size() + selectOrphans.size() + whereOrphans.size();
    }


    /**
     * The only public function to deal with sub tree expression - It determines
     * the nature of the tree and then follows as fit.
     *
     * @param aQueryTreeTracker
     * @param subTreeExpression
     * @return
     */

    public QuerySubTreeHelper processSubTree(SqlExpression subTreeExpression,
            QueryTreeTracker aQueryTreeTracker) {
        QuerySubTreeHelper aQuerySubTreeHelper;

        QueryTree theParentTree = aQueryTreeTracker.GetCurrentTree();

        if (getOrphanCount() == 0) {
            setQueryType(queryType & QueryTree.NONCORRELATED);
        } else {
            setQueryType(queryType & QueryTree.CORRELATED);
        }

        // Check to make sure that the query is not scalar
        if ((subTreeExpression.getSubqueryTree().getQueryType() & QueryTree.SCALAR) == 0) {
            // Check to make sure whether the query is a co- realted query or a
            // non-
            // co- related query. The orphan count is the count of the columns
            // in the query
            // which were not assigned any query node from the query tree to in
            // which
            // it was being used. Incase the number is > 0 then we have a
            // co-related query
            if (getOrphanCount() > 0) {
                aQuerySubTreeHelper = handleNonScalarCorrelatedQuery(
                        theParentTree, subTreeExpression);
            } else {
                aQuerySubTreeHelper = handleNonScalarNonCorrelatedSubquery(
                        theParentTree, subTreeExpression);
            }

        } else {
            // Ok we know we have a scalar query -- This is the
            // only query type in which we will not have a projection
            // list or projected vectors.
            if (this.getOrphanCount() == 0) {
                aQuerySubTreeHelper = handleScalarNonCorrelatedSubQuery(
                        aQueryTreeTracker, subTreeExpression);
            } else {
                // We have reached a location where implementation is pending. @
                // present
                // treat it like a corelated sub query --
                // handleScalarCorrelatedQuery
                aQuerySubTreeHelper = handleNonScalarCorrelatedQuery(
                        theParentTree, subTreeExpression);
            }
        }
        return aQuerySubTreeHelper;
    }


    /**
     * This function handles the Non Scalar Correlated kind of queries Steps
     * followed are : 1. Create a Place Holder Node in the parent tree 2. Get
     * the Orphan List or thoes columns in the sub query tree which were not
     * found in this sub query tree , but were found in some other query tree.
     * TODO : At present I only recognize where orphans, orphans can be found in
     * other having group by's etc. 3. Add All these to the correlated Column
     * List 4. Also add them to the relation node, just created, implying that
     * this particular relation node join with the following relations in this
     * tree -- 5.We set the Container node , which is just a back pointer to the
     * relation node just generated 6. Set the parentCorrelatedExpr in the main
     * tree 7. create the helper object which will then be returned to the user
     *
     * @param theParentTree
     * @param subTreeExpression
     * @return
     */
    private QuerySubTreeHelper handleNonScalarCorrelatedQuery(
            QueryTree theParentTree, SqlExpression subTreeExpression) {
        QuerySubTreeHelper aQuerySubTreeHelper = new QuerySubTreeHelper();
        ArrayList<SqlExpression> projList = new ArrayList<SqlExpression>();
        // Get a new Node which will be a part of the main tree
        RelationNode aRelationNodeMainTree = theParentTree.newRelationNode();
        //
        aRelationNodeMainTree.setNodeType(RelationNode.SUBQUERY_CORRELATED);
        aRelationNodeMainTree.setSubqueryTree(this);
        aRelationNodeMainTree.setTableName("PlaceHolder");
        aRelationNodeMainTree.setAlias("PlaceHolder");

        // add all the columns which are orphans to the correlated column list
        // in the relation Node main tree -
        for (SqlExpression aSqlExpression : whereOrphans) {
            aRelationNodeMainTree.getCorrelatedColumnList()
                    .add(aSqlExpression.getColumn());

            // Now since we have the information regarding the columns which are
            // orphans.
            // we should add to the aRelationNodeMainTree.joinList there
            // respective relation
            // nodes
            if (!aRelationNodeMainTree.getJoinList()
                    .contains(aSqlExpression.getColumn().relationNode)) {
                aRelationNodeMainTree.getJoinList()
                        .add(aSqlExpression.getColumn().relationNode);
            }
        }
        subTreeExpression.setParentContainerNode(aRelationNodeMainTree);
        projList.addAll(whereOrphans);
        // Now create an expression
        SqlExpression aSubQueryTreeExpression = new SqlExpression();
        aSubQueryTreeExpression.setExprType(SqlExpression.SQLEX_SUBQUERY);
        aSubQueryTreeExpression.setSubqueryTree(this);
        aSubQueryTreeExpression.rebuildString();
        // Bottom up
        // --theParentTree.conditionList.addElement(); //add to main tree
        //
        aRelationNodeMainTree.setParentCorrelatedExpr(aSubQueryTreeExpression);
        // Add the relation node which holds the Main Tree to the
        theParentTree.correlatedSubqueryList.add(aRelationNodeMainTree);

        // assign the Sub Query Tree member variable the QueryTree we just
        // created
        aRelationNodeMainTree.setSubqueryTree(this);
        //
        aRelationNodeMainTree.setParentCorrelatedExpr(subTreeExpression);

        // Now in the end - we will add the place holder node and the subQuery
        // tree in a map
        // which will allows us to later on help us in the following things
        // 1. When we are analyzing an orphan node and trying to decide to which
        // subTree it belongs to and which place holder node was created for it.
        // I have decided to store this information in the parent tree rather
        // than
        // the sub Tree -- This information is not used at present
        theParentTree.addPlaceHolderNode(this, aRelationNodeMainTree);
        // fill the helper class
        aQuerySubTreeHelper.createdRelationNode = aRelationNodeMainTree;
        aQuerySubTreeHelper.projectedSqlExpression
                .addAll(generatePseudoColumnExpressions(projectionList,
                        aRelationNodeMainTree));
        aQuerySubTreeHelper.correlatedColumnExprList = projList;
        // As per new changes - we need to add to the projection list of the
        // relation node the expression involved in the condition
        return aQuerySubTreeHelper;
    }

    /**
     * This function is responsible for handling NON SCALAR - NON CORRELATED
     * Queries Steps are : 1. Create a Place Holder Node , that will represent
     * this tree in the Parent Tree 2.Set the Query Tree variable of this node
     * 3.Add the Tree to the non_cor_SubqueryList 4.Set the Container Node of
     * the sub tree , to have a back pointer to the relation Node 5. Create a
     * helper class object which is used to return the Projected Expression
     * Relation node Created projected expression from this node.
     *
     * @param theParentTree
     * @param subTreeExpression
     * @return
     */
    private QuerySubTreeHelper handleNonScalarNonCorrelatedSubquery(
            QueryTree theParentTree, SqlExpression subTreeExpression) {
        RelationNode aQueryNodeTree = theParentTree.newRelationNode();
        theParentTree.addPlaceHolderNode(this, aQueryNodeTree);
        // Name the Tree
        aQueryNodeTree.setTableName("SubQueryTreeNonCorrelatedPlaceHolder");
        aQueryNodeTree.setAlias("SubQueryTreeNonCorrelatedPlaceHolder");
        // Fill the QueryNode - assign the node the label of non-corelated query
        aQueryNodeTree.setNodeType(RelationNode.SUBQUERY_NONCORRELATED);
        // assign the subquerytree member variable the queryTree we just created
        aQueryNodeTree.setSubqueryTree(this);
        // Also set the expression - which is the same as contained in the
        // querycondition subTreeExpression condition.
        aQueryNodeTree.setParentNoncorExpr(subTreeExpression);
        // add the queryNode tree to the
        theParentTree.noncorSubqueryList.add(aQueryNodeTree);
        subTreeExpression.setParentContainerNode(aQueryNodeTree);
        QuerySubTreeHelper aQuerySubTreeHelper = new QuerySubTreeHelper();
        aQuerySubTreeHelper.createdRelationNode = aQueryNodeTree;
        // --Change
        aQuerySubTreeHelper.projectedSqlExpression
                .addAll(generatePseudoColumnExpressions(projectionList,
                        aQueryNodeTree));
        return aQuerySubTreeHelper;
    }

    /**
     * This function as the name suggests takes care of processing the SCALAR
     * NON CORRELATED QUERIES
     *
     * @param aQueryTreeTracker
     * @param subTreeExpression
     * @return
     */
    private QuerySubTreeHelper handleScalarNonCorrelatedSubQuery(
            QueryTreeTracker aQueryTreeTracker, SqlExpression subTreeExpression) {
        QuerySubTreeHelper aQuerySubTreeHelper = new QuerySubTreeHelper();
        if (!aQueryTreeTracker.GetCurrentTree().scalarSubqueryList
                .contains(subTreeExpression)) {
            aQueryTreeTracker.GetCurrentTree().scalarSubqueryList
                    .add(subTreeExpression);
        }
        return aQuerySubTreeHelper;
    }

    /**
     *
     * @param projectionList
     * @param aRelationNode
     * @return
     */
    private ArrayList<SqlExpression> generatePseudoColumnExpressions(
            List<SqlExpression> projectionList, RelationNode aRelationNode) {
        // Make a vector
        ArrayList<SqlExpression> psuedoColumnVector = new ArrayList<SqlExpression>();

        // Get the Enumerator from the vector being passed
        // For each element in the projection
        for (SqlExpression aSqlExpression : projectionList) {
            // Get the Sql Expression

            // The SqlExpression can be a simple column Expression or a
            // Complicated multi Sql
            // expression - Incase it is a simple expression
            if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {
                String tabColAlias = aSqlExpression.getColumn().getTableAlias();
                SqlExpression generatedColumnExpression = SqlExpression
                        .getSqlColumnExpression(
                                aSqlExpression.getColumn().columnName, tabColAlias,
                                tabColAlias, aSqlExpression.getColumn().columnAlias,
                                null);
                generatedColumnExpression.getColumn().relationNode = aRelationNode;
                generatedColumnExpression.setMapped(SqlExpression.EXTERNALMAPPING);
                generatedColumnExpression.setMappedExpression(aSqlExpression);
                generatedColumnExpression.setExprString(aSqlExpression
                        .rebuildString());
                generatedColumnExpression.getColumn().columnType = aSqlExpression.getColumn().columnType;
                generatedColumnExpression.setExprDataType(aSqlExpression.getExprDataType());
                generatedColumnExpression.setBelongsToTree(this);
                psuedoColumnVector.add(generatedColumnExpression);
            } else {
                // In this case we have to get hold of column expressions in
                // particular and
                // for a list of them and call this function recursively
                if (aSqlExpression.getExprType() == SqlExpression.SQLEX_FUNCTION) {
                    psuedoColumnVector.addAll(generatePseudoColumnExpressions(
                            aSqlExpression.getFunctionParams(), aRelationNode));
                } else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION) {
                    List<SqlExpression> vList = SqlExpression.getNodes(
                            aSqlExpression, SqlExpression.SQLEX_COLUMN);
                    psuedoColumnVector.addAll(generatePseudoColumnExpressions(
                            vList, aRelationNode));
                } else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_CONSTANT) {
                    // Incase we have a constant - we cannot do anything just
                    // let it go.

                } else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                    // Now if it is a subquery ie. The projection list has a
                    // subquery.Then we are sure that there should be a scalar
                    // Query
                    // It can be co-related or not co-related
                    // TODO
                } else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_CONDITION) {
                    // In case it is a condition expression-- The resultant of
                    // this would be true or false
                    // TODO
                } else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_CASE) {
                    // If this is a CASE expression all I need to do is get the
                    // columns involved in this case expression and then
                    // generate
                    // sql expressions for them
                    List<SqlExpression> vList = SqlExpression.getNodes(
                            aSqlExpression, SqlExpression.SQLEX_COLUMN);
                    ArrayList<SqlExpression> gList = generatePseudoColumnExpressions(
                            vList, aRelationNode);
                    psuedoColumnVector.addAll(gList);
                } else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_LIST) {
                    // We dont have to bother about this.Since the syntax will
                    // not
                    // allow a query with a column list.
                } else {
                    // throw new XDBServerException("Sorry");
                }
            }
        }
        return psuedoColumnVector;
    }

    // The place Holder / psuedo table structure starts here -- we have a
    // HashMap for place holder nodes
    // as they can be 1 to Many relationship with it and we have 1 - 1
    // relationship with psuedoRelationNode

    // Again : PlaceHolder Nodes is the representaion of the sub query tree in
    // the parent tree
    // psuedo tables relation node is the representaion of the parent tree in
    // the subtree

    /**
     * Note - Place Holder nodes are for created for : SCALAR NON_CORELATED
     * QUERIES
     * @param aSubQueryTree
     * @param aPlaceHolderNode
     */

    // This will add the place holder node to the HashMap and develop the
    // relationship
    // between the subtree for which this place holder node is created
    public void addPlaceHolderNode(QueryTree aSubQueryTree,
            RelationNode aPlaceHolderNode) {
        if (aPlaceHolderNode == null) {
            throw new XDBServerException("Place Holder Node cannot be Null");
        }
        placeHolderNodes.put(aSubQueryTree, aPlaceHolderNode);
    }

    // This function will get the place holder node that belongs to this
    // particular
    // sub query tree

    /**
     *
     * @param aSubQueryTree
     * @return
     */

    public RelationNode getPlaceHodlerNode(QueryTree aSubQueryTree) {
        return placeHolderNodes.get(aSubQueryTree);
    }


    /**
     * Get cost of tree. Obtained from cost of individual nodes
     * @return
     */
    public long getCost() {
        return getSubTreeTotalCost(rootNode);
    }


    /**
     *
     * @param aQueryNode
     * @return
     */
    private long getSubTreeTotalCost(QueryNode aQueryNode) {
        long cost = 0;

        if (aQueryNode.getLeftNode() != null) {
            cost += getSubTreeTotalCost(aQueryNode.getLeftNode());
        }

        if (aQueryNode.getRightNode() != null) {
            cost += getSubTreeTotalCost(aQueryNode.getRightNode());
        }

        cost += aQueryNode.getEstCost();

        return cost;
    }


    /**
     *
     * @return
     */

    public long getEstimatedCost() {

        if (rootNode != null) {
            return rootNode.getEstCost();
        } else {
            throw new XDBServerException(
                    "Root Node Of a Query Tree Cannot be NULL");
        }

    }


    /**
     * Determines whether or not the tree is a "partitioned" group by. That is,
     * the first group by element is a column on a table that is the
     * partitioning key, as well as it appears in the right subtree (in final
     * step).
     * @param database
     */
    public void determinePartitionedGroupBy(SysDatabase database) {

        isPartitionedGroupBy = false;

        if (groupByList.size() > 0) {
            SqlExpression firstGroupExpr = groupByList.get(0);

            if (firstGroupExpr.getExprType() == SqlExpression.SQLEX_COLUMN) {
                QueryNode groupTableNode;

                // We check to see if it is in the right Node off the
                // tree (our final join).
                // Also, see if we are only dealing with one node.
                if (rootNode.getRightNode() == null) {
                    groupTableNode = rootNode.subtreeFind(firstGroupExpr.getColumn()
                            .getTableName(), firstGroupExpr.getColumn()
                            .getTableAlias());
                } else {
                    groupTableNode = rootNode.getRightNode().subtreeFind(
                            firstGroupExpr.getColumn().getTableName(),
                            firstGroupExpr.getColumn().getTableAlias());
                }

                if (groupTableNode != null) {
                    // Got a candidate. See if it is the partitioning column
                    // for the table.
                    if (firstGroupExpr.getColumn().isPartitionColumn()) {
                        isPartitionedGroupBy = true;
                    }
                }
                // SysColumn groupSysColumn = firstGroupExpr.column;
            }
        }
    }


    /**
     *
     * @return
     */

    public boolean isPartitionedGroupBy() {
        return isPartitionedGroupBy;
    }


    /**
     * whether or not this just contains one table, a lookup.
     * @param database
     * @return
     */
    public boolean isSingleTableLookup() {
        if (this.rootNode.getRightNode() != null) {
            return false;
        }

        // for the table.
        RelationNode relNode = rootNode.getRelationNode();

        if (relNode.getNodeType() != RelationNode.TABLE) {
            return false;
        }

        return relNode.isLookup();
    }


    /**
     * Whether or not just a single db node is used for the table, whether
     * created on that one node, or a lookup.
     * @param database
     * @return
     */
    public boolean usesSingleDBNode() {
        if (this.rootNode.getRightNode() != null) {
            return false;
        }

        // for the table.
        RelationNode relNode = rootNode.getRelationNode();

        if (!relNode.isTable()) {
            return false;
        }

        if (relNode.getNodeList().size() == 1) {
            return true;
        }

        return false;
    }


    /**
     *
     * @param anExps
     */

    public void checkExpressionTypes(List<SqlExpression> anExps) {
        if (hasUnion) {
            for (int i = 0; i < unionQueryTreeList.size(); i++) {
                unionQueryTreeList.get(i)
                        .checkExpressionTypes(projectionList);
            }
        }
        if (anExps == null || projectionList == null
                || anExps.size() != projectionList.size()) {
            throw new XDBServerException(
                    "each UNION query must have the same number of columns.");

        }
        for (int i = 0; i < anExps.size(); i++) {
            if (!SqlExpression.checkCompatibilityForUnion(
                    anExps.get(i),
                    projectionList.get(i))) {
                throw new XDBServerException("UNION types "
                        + anExps.get(i).getExprDataType()
                                .getTypeString()
                        + " and "
                        + projectionList.get(i).getExprDataType()
                                .getTypeString() + " cannot be matched. ");
            }
        }
    }

    /**
     * Returns true if tree list only contains lookups (or is null)
     * @param queryTreeList
     * @return
     */
    private static boolean unionListContainsOnlyLookups(List<QueryTree> queryTreeList,
                SysDatabase database) {

        if (queryTreeList == null) {
            return true;
        }

        for (QueryTree aQueryTree : queryTreeList) {
            if (!aQueryTree.containsOnlyLookups(database)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns true if tree list only contains lookups (or is null)
     * @param subqueryList
     * @return
     */
    private static boolean listContainsOnlyLookups(List<RelationNode> subqueryList,
            SysDatabase database) {

        for (RelationNode aRelNode : subqueryList) {
            if (!aRelNode.getSubqueryTree().containsOnlyLookups(database)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Determines whether the subquery only contains lookups
     *
     * @param SysDatabase
     *
     * @return true if the subquery only contains lookup tables, otherwise
     *         false.
     */
    public boolean containsOnlyLookups(SysDatabase database) {
        final String method = "containsOnlyLookups";
        logger.entering(method);

        try {
            // See if it contains only lookups
            for (RelationNode aRelNode : getRelationNodeList()) {
                if (aRelNode.getNodeType() == RelationNode.SUBQUERY_CORRELATED_PH) {
                    continue;
                }

                // check subtree type
                if (!aRelNode.isLookup()) {
                    return false;
                }
            }

            // check subqueries, too.
            if (!listContainsOnlyLookups(getNoncorSubqueryList(),database)) {
                return false;
            }
            if (!listContainsOnlyLookups(getCorrelatedSubqueryList(),database)) {
                return false;
            }
            if (!listContainsOnlyLookups(getTopWithSubqueryList(),database)) {
                return false;
            }
            if (!listContainsOnlyLookups(getRelationSubqueryList(),database)) {
                return false;
            }
            if (!unionListContainsOnlyLookups(getUnionQueryTreeList(),database)) {
                return false;
            }

            return true;
        } finally {
            logger.exiting(method);
        }
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
     * @param containsAggregates the containsAggregates to set
     */
    public void setContainsAggregates(boolean containsAggregates) {
        this.containsAggregates = containsAggregates;
    }

    /**
     * @return the containsAggregates
     */
    public boolean isContainsAggregates() {
        return containsAggregates;
    }


    /**
     * @return the correlatedSubqueryList
     */
    public List<RelationNode> getCorrelatedSubqueryList() {
        return correlatedSubqueryList;
    }


    /**
     * @return the finalProjList
     */
    public List<SqlExpression> getFinalProjList() {
        return finalProjList;
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
     * @param hasUnion the hasUnion to set
     */
    public void setHasUnion(boolean hasUnion) {
        this.hasUnion = hasUnion;
    }

    /**
     * @return the hasUnion
     */
    public boolean isHasUnion() {
        return hasUnion;
    }


    /**
     * @param havingList the havingList to set
     */
    public void setHavingList(List<QueryCondition> havingList) {
        this.havingList = havingList;
    }

    /**
     * @return the havingList
     */
    public List<QueryCondition> getHavingList() {
        return havingList;
    }


    /**
     * @return the hiddenProjectionList
     */
    public List<SqlExpression> getHiddenProjectionList() {
        return hiddenProjectionList;
    }

    /**
     * @param isCorrelatedSubtree the isCorrelatedSubtree to set
     */
    public void setCorrelatedSubtree(boolean isCorrelatedSubtree) {
        this.isCorrelatedSubtree = isCorrelatedSubtree;
    }

    /**
     * @return the isCorrelatedSubtree
     */
    public boolean isCorrelatedSubtree() {
        return isCorrelatedSubtree;
    }

    /**
     * @param isDistinct the isDistinct to set
     */
    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    /**
     * @return the isDistinct
     */
    public boolean isDistinct() {
        return isDistinct;
    }

    /**
     * @param isOuterJoin the isOuterJoin to set
     */
    public void setOuterJoin(boolean isOuterJoin) {
        this.isOuterJoin = isOuterJoin;
    }

    /**
     * @return the isOuterJoin
     */
    public boolean isOuterJoin() {
        return isOuterJoin;
    }

    /**
     * @return the isPartOfExistClause
     */
    public boolean isPartOfExistClause() {
        return isPartOfExistClause;
    }

    /**
     * @param lastOuterLevel the lastOuterLevel to set
     */
    public void setLastOuterLevel(int lastOuterLevel) {
        this.lastOuterLevel = lastOuterLevel;
    }

    /**
     * @return the lastOuterLevel
     */
    public int getLastOuterLevel() {
        return lastOuterLevel;
    }


    /**
     * @return the noncorSubqueryList
     */
    public List<RelationNode> getNoncorSubqueryList() {
        return noncorSubqueryList;
    }

    /**
     * @param orderByList the orderByList to set
     */
    public void setOrderByList(List<OrderByElement> orderByList) {
        this.orderByList = orderByList;
    }

    /**
     * @return the orderByList
     */
    public List<OrderByElement> getOrderByList() {
        return orderByList;
    }

    /**
     * @return the orderByOrphans
     */
    public List<SqlExpression> getOrderByOrphans() {
        return orderByOrphans;
    }

    /**
     * @param parentQueryTree the parentQueryTree to set
     */
    public void setParentQueryTree(QueryTree parentQueryTree) {
        this.parentQueryTree = parentQueryTree;
    }
    
    /**
     * @return the parentQueryTree
     */
    public QueryTree getParentQueryTree() {
        return parentQueryTree;
    }

    /**
     * @param parentQueryTree the topMostParentQueryTree to set
     */
    public void setTopMostParentQueryTree(QueryTree queryTree) {
        this.topMostParentQueryTree = queryTree;
    }

    /**
     * @return the parentQueryTree
     */
    public QueryTree getTopMostParentQueryTree() {
        if (topMostParentQueryTree != null) {
            return topMostParentQueryTree;
        } else {
            // We must be the top-most query tree
            return this;
        }  
    }
    
    /**
     * Add to top most WITH 
     * 
     * @param aRelationNode 
     */
    public void addToTopWithList(RelationNode aRelationNode) {
        if (getTopMostParentQueryTree() == null) {
            throw new XDBServerException ("top most query tree is null");
        }            
        getTopMostParentQueryTree().topWithSubqueryList.add(aRelationNode);
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
     * @param psuedoRelationNode the psuedoRelationNode to set
     */
    public void setPseudoRelationNode(RelationNode psuedoRelationNode) {
        this.psuedoRelationNode = psuedoRelationNode;
    }


    /**
     * @return the psuedoRelationNode
     */
    public RelationNode getPseudoRelationNode() {
        return psuedoRelationNode;
    }

    /**
     * @return the queryNodeTable
     */
    public Map<Integer, QueryNode> getQueryNodeTable() {
        return queryNodeTable;
    }

    /**
     * @return the relationNodeList
     */
    public List<RelationNode> getRelationNodeList() {
        return relationNodeList;
    }

    /**
     * @return the relationSubqueryList
     */
    public List<RelationNode> getRelationSubqueryList() {
        return relationSubqueryList;
    }


    /**
     * @return the relHandlerInfo
     */
    public Map<String, RelationNode> getRelHandlerInfo() {
        return relHandlerInfo;
    }

    /**
     * @param rootNode the rootNode to set
     */
    public void setRootNode(QueryNode rootNode) {
        this.rootNode = rootNode;
    }

    /**
     * @return the rootNode
     */
    public QueryNode getRootNode() {
        return rootNode;
    }

    /**
     * @return the scalarSubqueryList
     */
    public List<SqlExpression> getScalarSubqueryList() {
        return scalarSubqueryList;
    }


    /**
     * @return the selectOrphans
     */
    public List<SqlExpression> getSelectOrphans() {
        return selectOrphans;
    }


    /**
     * @return the unionQueryTreeList
     */
    public List<QueryTree> getUnionQueryTreeList() {
        return unionQueryTreeList;
    }

    /**
     * @param unionType the unionType to set
     */
    public void setUnionType(int unionType) {
        this.unionType = unionType;
    }

    /**
     * @return the unionType
     */
    public int getUnionType() {
        return unionType;
    }


    /**
     * @return the unusedQueryNodeList
     */
    public List<Integer> getUnusedQueryNodeList() {
        return unusedQueryNodeList;
    }

    /**
     * whereOrphans
     */
    public void setWhereOrphans(List<SqlExpression> whereOrphans) {
        this.whereOrphans = whereOrphans;
    }

    /**
     * Set whether tree is for insert select
     */
    public void setIsInsertSelect (boolean value) {
        isInsertSelect = value;
    }

    /**
     * @return if this tree is for an INSERT SELECT
     * (to differentiate from SELECT INTO
     */
    public boolean isInsertSelect () {
        return isInsertSelect;
    }
    
    
    /**
     * This function pre process the order by list , The pre processing of the
     * order by list allows us to get the right expression from the select list
     * if we have a numeric number in the order list
     */
    public void preProcessOrderByList() {
        // The pre processing of the order by list allows us to get the right
        // expression from the select list if we have a numeric number in the
        // order list
        for (OrderByElement aOrderExpr : getOrderByList()) {
            // Check out if we have any numeric expressions
            // Get the SQL Expression from this orderExpression
            SqlExpression orderExpressionValue = aOrderExpr.orderExpression;
            // Check to see if we have a SqlExpression of type constant and
            // if it is numeric
            String exprString = orderExpressionValue.rebuildString();

            try {
                int parsedIntValue = Integer.parseInt(exprString);
                // Incase we get the parsed int value - we should replace this
                // particular
                // expression with the corresponding expression from the select
                // statement

                // The index that this element will access is therefore
                int indexToSelect = parsedIntValue - 1;
                // Check if we have a valid number
                if (indexToSelect >= getProjectionList().size()
                        || indexToSelect < 0) {

                    throw new XDBServerException(
                            ErrorMessageRepository.ORDERBY_CLAUSE_POINTS_TO_ILLEGAL_PROJ_COLUMN,
                            0,
                            ErrorMessageRepository.ORDERBY_CLAUSE_POINTS_TO_ILLEGAL_PROJ_COLUMN_CODE);
                    // throw new XDBSemanticException("Value in the order list
                    // is greater than the number of " +
                    // "projected expressions in the query OR is less than 1 ");
                } else {
                    SqlExpression aProjectedExpression = getProjectionList().get(indexToSelect);
                    aOrderExpr.orderExpression = new SqlExpression();
                    // Replace the expression in the order element by this
                    // expression
                    SqlExpression.copy(aProjectedExpression,
                            aOrderExpr.orderExpression);
                }
            } catch (NumberFormatException ex) {
                // This could be a column or an alias name we just let it pass
                // by and allow the
                // next step of finding the column in the used tables to take
                // place.
                continue;
            }
        }
    }      
    
    /**
     * 
     * @return subquery relations that are WITH statements
     */
    public List<RelationNode> getTopWithSubqueryList() {
        return topWithSubqueryList;
    }
}
