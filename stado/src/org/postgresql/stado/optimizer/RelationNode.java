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
package org.postgresql.stado.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ColumnNotFoundException;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.partitions.HashPartitionMap;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.parser.handler.IdentifierHandler;



//-------------------------------------------------------------
public class RelationNode implements IRebuildString {

    private static AtomicInteger nextCount = new AtomicInteger(1);

    // nodeTypes
    public static final int FAKE = 1;

    public static final int TABLE = 2;

    public static final int SUBQUERY_SCALAR = 3;

    public static final int SUBQUERY_RELATION = 4;

    public static final int SUBQUERY_NONCORRELATED = 5;

    public static final int SUBQUERY_CORRELATED = 6;

    public static final int SUBQUERY_CORRELATED_PH = 7; // placeholder in subq

    private int nodeId;

    private int nodeType;

    private List<SqlExpression> projectionList;

    private List<QueryCondition> conditionList;

    private List<RelationNode> joinList;

    private List<AttributeColumn> condColumnList;

    /** When SUBQUERY_CORRELATED, lists all columns that we get
     * from parent query.
     * We need this to make sure that we always project these up
     * the query tree
     */
    private List<AttributeColumn> correlatedColumnList;

    /** The table name that this node represents.
     * Note that if this node represents a subquery that is
     * part of a FROM clause, tableName is set to the AS relation name.
     */
    private String tableName;

    private String alias;

    /** to keep track for if this relation has own alias or alias assigned
     * by sub query.
     */
    private boolean hasNotOwnAlias = false;

    private int rowsize;

    private long estCost;

    private long estRowsReturned;

    private String relationString = "";

    private boolean isTemporaryTable;
    
    /* if ONLY is present with table */
    private boolean isOnly;

    private XDBSessionContext client;

    // This is used only in the case our QueryNode is a noncorrelated
    // subquery. It points back to the expression in the parent tree,
    // so it can be rewritten
    private SqlExpression parentNoncorExpr;

    private SqlExpression parentCorrelatedExpr;

    // for OUTER join handling
    private short outerLevel = 0; // for nested outers (0 = no OUTER)

    private List<RelationNode> childrenNodes = new ArrayList<RelationNode>();

    private List<RelationNode> parentNodes = new ArrayList<RelationNode>();

    private QueryTree queryTree;
    
    private QueryTree subqueryTree;

    // This allows us to track what temp table the columns involved in
    // the underlying table are currently in, when building up the plan
    private String currentTempTableName = "";
    
    // Is this relation from a WITH clause
    private boolean isWith = false;

    // If this represents a WITH, track if it is used in 
    // the top most tree. Important to avoid having
    // it added as an extra relation if it appears deeper
    // and feeds into other relations
    private boolean isTopMostUsedWith = false;
        
    // A WITH relation may be referenced more than once,
    // like any other. Since we try and use the existing 
    // code for FROM subquery handling, we track its usage
    public int withReferenceCount = 0; 

    // What underlying WITH relation this really is
    private RelationNode baseWithRelation;
    
    private SysTable sysTable = null;
    
    private boolean isLookupSet = false;
    private boolean isLookup = false;

    // For materialized relations (WITH)
    private PartitionMap matPartitionMap;
    
    /**
     * Constructor
     */
    public RelationNode() {
        projectionList = new ArrayList<SqlExpression>();
        conditionList = new ArrayList<QueryCondition>();
        joinList = new ArrayList<RelationNode>();
        condColumnList = new ArrayList<AttributeColumn>();
        correlatedColumnList = new ArrayList<AttributeColumn>();

        nodeType = TABLE;
    }

    /**
     * 
     * @param aQueryTree what query tree this belongs to
     */
    public void setQueryTree(QueryTree aQueryTree) {
        queryTree = aQueryTree;
    }

    /**
     * 
     * @return 
     */
    public QueryTree getQueryTree() {
        return queryTree;
    }
        
    /**
     *
     * @return
     */
    public boolean isSubquery() {
        return nodeType == SUBQUERY_SCALAR || nodeType == SUBQUERY_RELATION
        || nodeType == SUBQUERY_NONCORRELATED
        || nodeType == SUBQUERY_CORRELATED;
    }

    // BUILD_CUT_START
    // for helping to debug
    /**
     *
     * @param prepend
     */
    public void toString(String prepend) {
        StringBuffer sbNode = new StringBuffer(512);

        sbNode.append(prepend).append("--------------------------------------\n");
        sbNode.append(prepend).append(" nodeType =       ");
        sbNode.append(nodeType);
        sbNode.append('\n');

        sbNode.append(prepend).append(" ").append(getTypeString());
        sbNode.append('\n');
        sbNode.append(prepend).append(" rowsize = ").append(rowsize);
        sbNode.append('\n');
        sbNode.append(prepend).append(" estCost = ").append(estCost);
        sbNode.append('\n');
        sbNode.append(prepend).append(" estRowsReturned = ").append(estRowsReturned);
        sbNode.append('\n');

        sbNode.append(prepend);
        sbNode.append('\n');
        sbNode.append(prepend).append(" ---Joins--- ");
        sbNode.append('\n');

        for (RelationNode joinNode : joinList) {
            sbNode.append(prepend).append(" ").append(joinNode.tableName).append(joinNode.alias);
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

        for (AttributeColumn aColumn : condColumnList) {
            sbNode.append(prepend).append("  ").append(aColumn.columnName);
            sbNode.append('\n');
        }
    }

    // BUILD_CUT_ALT
    // public void toString (String prepend) { return; }
    // BUILD_CUT_END

    // BUILD_CUT_START
    // -----------------------------------------------------------------------
    /**
     *
     * @return
     */
    public String getTypeString() {
        String typeString = "";

        switch (nodeType) {
        case TABLE:
            typeString = "TABLE     Table & Alias =  " + tableName + " "
            + alias;
            break;

        case SUBQUERY_SCALAR:
            typeString = "SUBQUERY_SCALAR";
            break;

        case SUBQUERY_RELATION:
            typeString = "SUBQUERY_RELATION";
            break;

        case SUBQUERY_NONCORRELATED:
            typeString = "SUBQUERY_NONCORRELATED";
            break;

        case SUBQUERY_CORRELATED:
            typeString = "SUBQUERY_CORRELATED";
            break;

        case SUBQUERY_CORRELATED_PH:
            typeString = "SUBQUERY_CORRELATED_PH";
            break;
        }

        return typeString;
    }

    // BUILD_CUT_END

    /**
     * Given a column it tries to find the matching sql expression Test Cases :
     * column ColName AliasName TableName Alias Name * First check for colname -
     * as this should be the alias name of the column in the from expression.
     *
     * Incase we have the colname equals for more than one columns - check for
     * the alias last of the table. Check if the alias are the same -
     *
     * 
     * @return
     * @param column
     * @throws org.postgresql.stado.exception.ColumnNotFoundException
     */
    public SqlExpression getMatchingSqlExpression(AttributeColumn column)
    throws ColumnNotFoundException {
        /*
         * If the node type is a relation sub query -
         */
        if (nodeType == RelationNode.SUBQUERY_RELATION && subqueryTree != null) {
            // extract all the sql expressions from the projection list
            /*
             * For each sql expression
             */
            for (SqlExpression aSqlExpression : subqueryTree.getProjectionList()) {
                /*
                 * Check if the outerAlias is the same name as the
                 * columnName as a Rule : 1.OuterAlias = Alias = ColumnAlias =
                 * ColumnName if (Expression is a column)
                 *
                 */
                if (!aSqlExpression.getOuterAlias()
                        .equalsIgnoreCase(column.columnName)) {
                    /*
                     * If the Outer Alias Match fails we look for a weaker
                     * match - This implies that a user can use an aliasName
                     * even when the OuterAlias Name is specifed provided
                     * there is no ambiguity.
                     *
                     * select n_nationkey from (select n_nationkey ns from
                     * nation ) z(n_s) The user can use n_s , n_nationkey or
                     * ns as there is no ambiguity.
                     */
                    if(aSqlExpression.getAlias().equals("") &&
                            aSqlExpression.getProjectionLabel().equalsIgnoreCase(column.columnName)) {
                        return aSqlExpression;
                    }
                    if (!(aSqlExpression.getAlias()
                            .equalsIgnoreCase(column.columnName) &&
                            (tableName.equalsIgnoreCase(column.getTableName()) || alias.equalsIgnoreCase(column.getTableName())))) {
                        continue;
                    } else {
                        if(aSqlExpression.getOuterAlias().equals("")) {
                            aSqlExpression.setOuterAlias(aSqlExpression.getAlias());
                        }
                        return aSqlExpression;
                    }
                } else {
                    return aSqlExpression;
                }

            }
        } else if (nodeType == RelationNode.SUBQUERY_RELATION 
                && baseWithRelation != null) { //refers to WITH
            // extract all the sql expressions from the projection list
            /*
             * For each sql expression
             */
            for (SqlExpression aSqlExpression : getProjectionList()) {
                /*
                 * Check if the outerAlias is the same name as the
                 * columnName as a Rule : 1.OuterAlias = Alias = ColumnAlias =
                 * ColumnName if (Expression is a column)
                 *
                 */
                if (!aSqlExpression.getOuterAlias()
                        .equalsIgnoreCase(column.columnName)) {
                    /*
                     * If the Outer Alias Match fails we look for a weaker
                     * match - This implies that a user can use an aliasName
                     * even when the OuterAlias Name is specifed provided
                     * there is no ambiguity.
                     *
                     * select n_nationkey from (select n_nationkey ns from
                     * nation ) z(n_s) The user can use n_s , n_nationkey or
                     * ns as there is no ambiguity.
                     */
                    if((aSqlExpression.getAlias().equals("") 
                                || aSqlExpression.getAlias().equalsIgnoreCase(column.columnName))
                            && aSqlExpression.getProjectionLabel().equalsIgnoreCase(column.columnName)) {
                        return aSqlExpression;
                    }
                    if (!(aSqlExpression.getAlias()
                            .equalsIgnoreCase(column.columnName) &&
                            aSqlExpression.getColumn().getTableName().equalsIgnoreCase(column.getTableName()))) {
                        continue;
                    } else {
                        if(aSqlExpression.getOuterAlias().equals("")) {
                            aSqlExpression.setOuterAlias(aSqlExpression.getAlias());
                        }
                        return aSqlExpression;
                    }
                } else {
                    return aSqlExpression;
                }

            }
        }
            
        throw new ColumnNotFoundException("columns " + column.columnName,
                this.alias + "Table " + this.tableName);
    }

    public void handleAliasForSingleTableSubQueryNode() {
        if(this.nodeType == RelationNode.SUBQUERY_RELATION && subqueryTree != null
                && ( tableName.equals("") || tableName.equalsIgnoreCase(alias))) {
            if(subqueryTree.getRelationNodeList().size() == 1) {
                RelationNode rn = subqueryTree.getRelationNodeList().get(0);
                if(rn.nodeType == RelationNode.SUBQUERY_RELATION && subqueryTree != null) {
                    rn.handleAliasForSingleTableSubQueryNode();
                }
                if(rn.tableName == null && rn.alias == null) {
                    rn.tableName = this.alias;
                    rn.alias = this.alias;
                }
                if(rn.tableName.equalsIgnoreCase(rn.alias)) {
                    rn.alias = this.alias;
                    for(SqlExpression aSqlExp : rn.projectionList) {
                        aSqlExp.getColumn().setTableAlias(rn.alias);
                        aSqlExp.rebuildExpression();
                    }
                    for(SqlExpression aSqlExp : subqueryTree.getGroupByList()) {
                        aSqlExp.rebuildExpression();
                    }
                    for(SqlExpression aSqlExp : subqueryTree.getOrderByOrphans()) {
                        aSqlExp.rebuildExpression();
                    }
                    for(SqlExpression aSqlExp : subqueryTree.getScalarSubqueryList()) {
                        aSqlExp.rebuildExpression();
                    }
                    for(SqlExpression aSqlExp : subqueryTree.getSelectOrphans()) {
                        aSqlExp.rebuildExpression();
                    }
                    if(subqueryTree.getWhereRootCondition() != null) {
                        subqueryTree.getWhereRootCondition().rebuildCondString();
                    }
                }
            }
        }
    }

    /**
     *
     * @return
     */
    public static int getNextCount() {
        return nextCount.incrementAndGet();
    }

    /**
     * 
     */
    private String rebuildTableString() {
        
        if (isOnly) {
            relationString = " ONLY ";
        } else {
            relationString = "";
        }

        if (currentTempTableName.equals("")) {
            relationString += IdentifierHandler.quote(tableName);
            if (!tableName.equals(alias)) {
                relationString += " as " + IdentifierHandler.quote(alias);
            }
        } else {
            relationString += IdentifierHandler.quote(currentTempTableName);
        }
        return relationString;
    }
    
    /**
     *
     * @return
     */
    public String rebuildString() {
        switch (nodeType) {
        case TABLE:
            rebuildTableString();
            break;
        case RelationNode.SUBQUERY_RELATION:
            if (isWithDerived()) {
                if (baseWithRelation != null 
                        && currentTempTableName.isEmpty()
                        && !baseWithRelation.currentTempTableName.isEmpty()) {
                    // Make sure we have set temp table name if possible
                    currentTempTableName = baseWithRelation.currentTempTableName;
                }
                rebuildTableString();
            } else {
                relationString = "("
                    + subqueryTree.rebuildString()
                    + ") as "
                    + IdentifierHandler.quote(alias);
            }
            break;
            // TODO : All the various types of subqueries are to be handled later
        case RelationNode.SUBQUERY_CORRELATED_PH:
            break;
        case RelationNode.SUBQUERY_NONCORRELATED:
            break;
        case RelationNode.SUBQUERY_SCALAR:
            break;
        case RelationNode.SUBQUERY_CORRELATED:
            break;
        case RelationNode.FAKE:
        	break;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_RELATIONNODE_TYPE, 0,
                    ErrorMessageRepository.ILLEGAL_RELATIONNODE_TYPE_CODE);
        }
        return relationString;

    }

    /**
     *
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     *
     * @param tableName
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     *
     * @return
     */
    public String getTableAlis() {
        return alias;
    }

    /**
     *
     * @return
     */
    public boolean isTemporaryTable() {
        return isTemporaryTable;
    }

    /**
     *
     * @param temporaryTable
     */

    public void setTemporaryTable(boolean temporaryTable) {
        isTemporaryTable = temporaryTable;
    }

    /**
     *
     * @return
     */
    public boolean isOnly() {
        return isOnly;
    }

    /**
     *
     * @param isOnly
     */

    public void setOnly(boolean onlyTable) {
        isOnly = onlyTable;
    }
    
    /**
     *
     * @return
     */
    public XDBSessionContext getClient() {
        return client;
    }

    /**
     *
     * @param client
     */
    public void setClient(XDBSessionContext client) {
        this.client = client;
    }

    // For join handling
    /**
     *
     * @param parents
     * @param isOuter
     * @param newOuterLevel
     */
    public void addParentNodes(Collection<RelationNode> parents,
            boolean isOuter, short newOuterLevel) {
        parentNodes.addAll(parents);
        for (RelationNode parent : parents) {
            parent.childrenNodes.add(this);

            // This should probably not ever happen
            if (parent.outerLevel > outerLevel) {
                outerLevel = parent.outerLevel;
            }
            if (isOuter) {
                outerLevel = newOuterLevel;
            }
        }
    }

    // For inner join handling
    /**
     *
     * @param siblingNode
     */
    public void addSiblingJoin(RelationNode siblingNode) {
        // Set outer level to max of its siblings
        if (siblingNode.outerLevel > outerLevel) {
            outerLevel = siblingNode.outerLevel;
        }

        for (RelationNode parent : siblingNode.getParentNodes()) {
            parentNodes.add(parent);
            parent.childrenNodes.add(this);
        }
    }

    /**
     * @return true if this is a correlated RelationNode
     */
    public boolean isCorrelatedSubquery () {
        return nodeType == RelationNode.SUBQUERY_CORRELATED;
    }

    /**
     * @return true if this is a correlated RelationNode
     */
    public boolean isCorrelatedPlaceholder () {
        return nodeType == RelationNode.SUBQUERY_CORRELATED_PH;
    }

    /**
     * @return true if this is an uncorrelated RelationNode
     */
    public boolean isUncorrelatedSubquery() {
        return nodeType == RelationNode.SUBQUERY_NONCORRELATED;
    }

    /**
     * @return true if this is a scalar subquery
     */
    public boolean isScalarSubquery() {
        return nodeType == RelationNode.SUBQUERY_SCALAR;
    }

    /**
     * @return true if this is a relation subquery
     */
    public boolean isRelationSubquery() {
        return nodeType == RelationNode.SUBQUERY_RELATION;
    }

    /**
     * @return true if this is a relation subquery
     */
    public boolean isTable() {
        return nodeType == RelationNode.TABLE;
    }

    //
    /**
     *
     * @return
     */
    public List<RelationNode> getParentNodes() {
        return parentNodes;
    }

    //
    /**
     *
     * @param node
     * @param inserted
     */
    public void notifyOuterLevelInserted(RelationNode node, short inserted) {
        if (node != this && outerLevel >= inserted) {
            outerLevel++;
        }
    }

    /**
     * Get the SysTable for this
     *
     * @param database the database to look up for this column
     *
     * @return the SysTable that this column belongs to
     */
    public SysTable getSysTable ()
    throws IllegalArgumentException, XDBServerException {

        if (!isTable()) {
            return null;
        }
        if (sysTable == null) {
            if (client == null) {
                throw new XDBServerException("Internal Error: RelationNode's cleint not set");
            } else {
                SysDatabase database = client.getSysDatabase();
                sysTable = database.getSysTable(tableName);
            }
        }
        return sysTable;
    }

    /**
     * @return the outerLevel
     */
    public short getOuterLevel() {
        return outerLevel;
    }

    /**
     * @param alias the alias to set
     */
    public void setAlias(String alias) {
        this.alias = alias;
    }

    /**
     * @return the alias
     */
    public String getAlias() {
        return alias;
    }

    /**
     * @return the condColumnList
     */
    public List<AttributeColumn> getCondColumnList() {

        return condColumnList;
    }

    /**
     * @return the conditionList
     */
    public List<QueryCondition> getConditionList() {
        return conditionList;
    }

    /**
     * @return the correlatedColumnList
     */
    public List<AttributeColumn> getCorrelatedColumnList() {
        return correlatedColumnList;
    }

    /**
     * @param currentTempTableName the currentTempTableName to set
     */
    public void setCurrentTempTableName(String currentTempTableName) {

        if (currentTempTableName == null || currentTempTableName.isEmpty()) {
            throw new XDBServerException("Setting empty currentTempTableName");
        }
        this.currentTempTableName = currentTempTableName;
    }

    /**
     * @return the currentTempTableName
     */
    public String getCurrentTempTableName() {
        return currentTempTableName;
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
     * @return the hasNotOwnAlias
     */
    public boolean hasNotOwnAlias() {
        return hasNotOwnAlias;
    }

    /**
     * @param joinList the joinList to set
     */
    public void setJoinList(List<RelationNode> joinList) {
        this.joinList = joinList;
    }

    /**
     * @return the joinList
     */
    public List<RelationNode> getJoinList() {
        return joinList;
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
     * @param parentCorrelatedExpr the parentCorrelatedExpr to set
     */
    public void setParentCorrelatedExpr(SqlExpression parentCorrelatedExpr) {
        this.parentCorrelatedExpr = parentCorrelatedExpr;
    }

    /**
     * @return the parentCorrelatedExpr
     */
    public SqlExpression getParentCorrelatedExpr() {
        return parentCorrelatedExpr;
    }

    /**
     * @param parentNoncorExpr the parentNoncorExpr to set
     */
    public void setParentNoncorExpr(SqlExpression parentNoncorExpr) {
        this.parentNoncorExpr = parentNoncorExpr;
    }

    /**
     * @return the parentNoncorExpr
     */
    public SqlExpression getParentNoncorExpr() {
        return parentNoncorExpr;
    }

    /**
     * @return the projectionList
     */
    public List<SqlExpression> getProjectionList() {
        return projectionList;
    }

    /**
     * @return the relationString
     */
    public String getRelationString() {
        return relationString;
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
     * @param subqueryTree the subqueryTree to set
     */
    public void setSubqueryTree(QueryTree subqueryTree) {
        if (getQueryTree() == subqueryTree) {
            Exception e = new Exception("Cannot set subtree equal to tree a member of.");
            e.printStackTrace();
        }
        this.subqueryTree = subqueryTree;
    }

    /**
     * @return the subqueryTree
     */
    public QueryTree getSubqueryTree() {
        return subqueryTree;
    }
        
    public void setIsWith(boolean isWith) {
        this.isWith = isWith;
    }
    
    public boolean isWith() {
        return isWith;
    }
    
    public void setIsTopMostUsedWith(boolean isTopMostUsedWith) {
        this.isTopMostUsedWith = isTopMostUsedWith;
    }

    public boolean isTopMostUsedWith() {
        return isTopMostUsedWith;
    }
    
    /**
     * 
     * @param baseWithRelation to set
     */
    public void setBaseWithRelation(RelationNode baseWithRelation) {
        this.baseWithRelation = baseWithRelation;
    }
    /**
     * 
     * @return the base WITH relation that is actually being used 
     */
    public RelationNode getBaseWithRelation() {
        return baseWithRelation;
    }

    /**
     * 
     * @return whether or not this relation has been derived
     * from a WITH statement. Note it does not mean that it
     * is a WITH statement (isWith()), just that it is a
     * wrapper node.
     */
    public boolean isWithDerived() {
        return baseWithRelation != null;
    }
    
    /**
     * TODO: Expand for materialized relations
     * 
     * @return if this relation consists of just a lookup table.
     * 
     */
    public boolean isLookup() {
        
        if (!isLookupSet) {
            if (!isTable()) {
                isLookup = false;
            } else {
                isLookup = getSysTable().isLookup();
            }
            isLookupSet = true;
        }
        return isLookup;
    }
    
    /**
     *  the node list for the relation
     * @return the node list for the relation
     */
    public Collection<DBNode> getNodeList() {
        
        if (!isTable()) {
            // if not a table assume all until more intelligence
            // added
            return client.getSysDatabase().getDBNodeList();
        }
        return getSysTable().getNodeList();
    }
    
    /**
     * 
     * @return the column name the relation is partitioned on, or null
     *         if not partitioned
     */
    public String getPartitionColumnName() {
        
        if (isWithDerived()) {
            // WITHs currently always partition by the first column
            SqlExpression projExpr = projectionList.get(0);
            
            if (projExpr.getExprType() != SqlExpression.SQLEX_COLUMN) {
                return null;
            }
            
            return projExpr.getColumn().columnName;
            
        } else {
            SysColumn sysColumn = getPartitionedColumn();

            if (sysColumn == null) {
                return null;
            }
            return sysColumn.getColName();
        }
    }
    
    /**
     * 
     * @return the column the relation is partitioned on, or null
     *         if not partitioned
     */    
    private SysColumn getPartitionedColumn() {
        if (!isTable()) {
            return null;
        }
        return getSysTable().getPartitionedColumn();
    }
    
    /**
     * 
     * @return whether or not the relation is hashPArtitioned 
     */
    public boolean isHashPartitioned() {
        // If we are from a WITH, we assume that it is partitioned
        if (isWithDerived()) {
            return true;
        } else {
            return getSysTable().getPartitionScheme() == SysTable.PTYPE_HASH;
        }
    }
    
    //public boolean onSameNodes(RelationNode otherRelationNode) {
    //    return getSysTable().onSameNodes(otherRelationNode.getSysTable(database))); 
    //}
    
    /** 
     * @return if the relation exists only on a single node
     */
    public boolean isOnSingleNode() {
        return getSysTable().getPartitionScheme() == SysTable.PTYPE_ONE;
    }
    
    /**
     * 
     */
    private void initMaterializedPartitionMap() {

        if (client == null || client.getSysDatabase() == null) {
            return;
        }
        matPartitionMap = new HashPartitionMap();
        
        Collection<DBNode> dbNodeList = client.getSysDatabase().getDBNodeList();
        ArrayList<Integer> nodeIdList = new ArrayList<Integer>(dbNodeList.size());
        for (DBNode aDBNode : dbNodeList) {
            nodeIdList.add(aDBNode.getNodeId());
        }
        matPartitionMap.generateDistribution(nodeIdList);
    }
    /**
     * 
     * @return the partition map for the relation 
     */
    public PartitionMap getPartitionMap() {
        
        if (!isTable()) {
            if (isWithDerived()) {
                if (matPartitionMap == null) {
                    initMaterializedPartitionMap();
                }
                return matPartitionMap;
            } else {
                return null;
            }
        }
            
        return getSysTable().getPartitionMap();
    }
    
    public boolean onSameNodes(RelationNode otherRelationNode) {
        /*
        Collection myPartitions = getNodeList();
        Collection otherPartitions = otherTable.getNodeList();

        if (myPartitions.size() != otherPartitions.size()) {
            // trivial
            return false;
        }
        */

        return getPartitionMap().equals(otherRelationNode.getPartitionMap());
    }
    
}
