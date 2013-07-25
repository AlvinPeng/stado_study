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
 * Leaf.java
 *
 */

package org.postgresql.stado.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.FromRelation;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.handler.IdentifierHandler;

 
/* This could be cleaned up more, such as using getter()/setters() */

/**
 * Each leaf contains information about the current step to execute, the
 * current table to join. It is a bit of a misnomer, as each Leaf is
 * really a step.
 *
*/
public class Leaf {

    /** uniquely identify each leaf in plan */
    private int leafStepNo = 0;

    /** Table name at this leaf */
    private String tableName = "";

    /** Target table name (fake temp table) */
    private String targetTableName;

    public static final int REGULAR = 1;

    public static final int SUBQUERY_JOIN = 2;

    /** indicates a subplan for correlated subqueries */
    public static final int SUBQUERY_DATA_DOWN = 3;

    private int leafType = REGULAR;

    /** The select statement for this step */
    private String selectStatement;

    /** The statement to execute without the select, which allows for rewriting. */
    private String nonProjectionPart;

    /** The expression to project (not necessarily just columns) */
    public ArrayList<Projection> selectColumns;

    /** A map for seeing if we already added the projection.
     * This is used separately from selectColumns, because
     * selectColumns may have the same projection appear twice
     */
    public LinkedHashMap<String,Projection> selectColumnMap;

    /** whether or not DISTINCT should be applied here. */
    private boolean distinct = false;

    /**
     * A String to be concatenated with
     * "CREATE TABLE x ( " + sbCreateTableColumns.toString() + ")"
     * It is used to determine the the schema of the temp table results
     * of this step.
     */
    private StringBuilder sbCreateTableColumns = new StringBuilder(128);

    /** This is used for creating an extra table for correlated subqueries.
     * and determines the schema of the table that is passed down.
     */
    private String createCorrelatedTableString = "";

    /** correlated select string */
    private String correlatedSelectString = "";

    private List<Join> joinConditions;

    /** The "temp" table to join with on this step. */
    private String joinTableName;

    /** All tables used by this step */
    private List<String> tableList;

    protected List<FromRelation> fromRelationList;

    /** Atomic conditions, e.g.,  col LIKE 'A%' */
    private List<String> conditions;

    /** Group by expressions. */
    protected List<Projection> groupByColumns;

    /** Having conditions. */
    private List<String> havingConditions;

    /** Whether or not this is an extra step for combining aggregates */
    private boolean combinerStep = false;

    /** for count(distinct x) */
    private int addedGroupCount = 0;

    /** Dealing just with temp tables- see if it is ok just to combine
     * on combiner node.
     */
    private boolean combineOnMain = false;

    /** Flag that we created an extra step here for special handling.
     * Needed for some cases of OUTER joins
     */
    private boolean extraStep = false;

    /** This is a list of temp tables that can be dropped after completing
     * this step.
     */
    protected List<String> tempTableDropList;

    /** for correlated subqueries */
    protected QueryPlan subplan;

    /** extra steps for some outer joins */
    protected QueryPlan outerSubplan;

    /** uncorrelated subplans.
     * These should be executed first on this step.
     */
    protected List<QueryPlan> uncorrelatedSubplanList;

    /** Aids with column name generation */
    private static int unnamedColCount = 0;

    /** We want to already store the node list to use when executing
     * the query
     */
    protected List<DBNode> queryNodeList;

    /**
     * Give a hint to Executor if we can execute the correlated join down at the
     * nodes.
     */
    private boolean correlatedHashable = false;

    /** Which expression to use if correlated when hashing. */
    protected SqlExpression correlatedParentHashableExpression;

    /** Which expression to use if correlated when hashing. */
    protected SqlExpression correlatedChildHashableExpression;

    /** correlated join table. */
    protected String correlatedJoinTableName;

    /** This is used to store correlated columns, to allow us
     * to create an index on it.
     */
    private List<String> correlatedColumnList;

    /**
     * Used for uncorrelated IN clause, to flag that we can take advantage of
     * partitioning of the left hand side condition, to execute more
     * efficiently. This is set in the last leaf in the plan.
     */
    protected String finalInClausePartitioningTable = null;

    /**
     * This is set when we are forced to use slow aggregation. We do not group
     * by as we normally would on the first aggregation step.
     */
    private boolean suppressedGroupBy = false;

    /**
     * Use for indicating that a query only needs to go to one node
     */
    private String hashTableName = null;

    /** which column to hash on for sending results */
    private String hashColumn = null;

    /** Indicates that we are dealing with a single step query only involving
     * lookups. We can pick one single node to execute it on.
     */
    private boolean lookupStep = false;

    /** If it is a simple correlated query. */
    private boolean singleStepCorrelated = false;

    /** what to hash on for a hashable single correlated step. */
    private String singleCorrelatedHash = null;

    /** Tracks what new aliases to assign at the end of the step. */
    private HashMap<AttributeColumn,String> newAliasTable =
            new HashMap<AttributeColumn,String>();

    /** For subqueries with LIMIT */
    private long limit = -1;

    /** For subqueries with OFFSET */
    private long offset = -1;

    /** For outer joins, we may add unique identifier. */
    public boolean usesOid = false;

    // this is used only for outer joins
    protected List<SqlExpression> outerExprList;

    // For outer joins
    protected List<SqlExpression> innerExprList;

    /** For outer join, indicates column from that specifies source node. */
    private String outerNodeIdColumn = null;

    /** Used for tracking where we are in outer handling */
    protected int lastOuterLevel = 0;

    /** For uniquely identifying rows in outer joins.
     * It is a Vector in case we have multiple levels of outers.
     */
    private List<Leaf.OuterIdColumn> outerIdColumnList = new ArrayList<Leaf.OuterIdColumn>();

    /** For outer handling to uniquely identify rows, indicates position
     * of generated serial column.
     */
    private short serialColumnPosition = -1;

    /** The where clause for this step. */
    private String whereClause;

    /** If not null, the SqlExpression to base partitioning info on,
     * for PreparedStatements.
     */
    private SqlExpression partitionParameterExpression = null;

    /** Condition for getting outer'ed rows */
    private String outerTestString;

    /************************************************************************/
    /**
     * Projections used in this step
     */
    public class Projection {
        /** What is being selected. */
        public String projectString;

        /** Only used when this appears in a group by;
         * Determines where in select clause this appears.
         * This is needed by the multi-step aggregator.
         */
        public int groupByPosition = 0;

        /** the name of the column to select */
        private String createString;

        /** whether or not to force quoting on a group*/
        public boolean forceGroupQuote = false;
        
        /**
         *
         * @param projection - what to project
         */
        public Projection(String projection) {
            projectString = projection;
        }

        /**
         * Set the creat column name of the projection.
         *

         * @param createColName

         */

        public void setCreateColumnName(final String createColName) {
            createString = createColName;
        }

        /**
         *
         * @return the create column name of the projection
         */

        public String getCreateColumnName() {
            return createString;
        }

    }

    /************************************************************************/
        /**
     * Joins used in this step.
         */
    public class Join {
        /** the join string. */
        private String joinString = "";

        /** the condition from which the join string was derived. */
        public QueryCondition aQueryCondition;

        /**
         *
         * @param aQueryCondition
         * @param joinString
         */

        public Join(QueryCondition aQueryCondition, String joinString) {
            this.aQueryCondition = aQueryCondition;
            this.joinString = joinString;
        }

        /**
         *
         * @return the join String
         */
        public String getJoinString() {
            return joinString;
        }

        /**
         *
         * @return the QueryCondition this was based on
         */
        public QueryCondition getQueryCondition() {
            return aQueryCondition;
        }
    }

    /************************************************************************/
    /** This is used to support outer joins, where we need to uniquely
     * identify a row
     */
    public class OuterIdColumn {

        /** the source column for the outer */
        private String sourceColumnName;

        /** how to select the column in other steps */
        private String selectColumnName;

        /** the column type */
        private ExpressionType columnExprType;

        /** whether or not it is a pseudo serial */
        private boolean isPseudoSerial;

        /**
         * Constructor
         *
         * @param sourceColumnName - the source column for the outer
         * @param selectColumnName - how to select the column in other steps
         * @param columnExprType - the column type
         * @param isPseudoSerial - whether or not it is a pseudo serial
         */

        public OuterIdColumn(String sourceColumnName, String selectColumnName,
                ExpressionType columnExprType, boolean isPseudoSerial) {
            //this.sourceColumnName = sourceColumnName;
            this.selectColumnName = selectColumnName;
            this.columnExprType = columnExprType;
            this.isPseudoSerial = isPseudoSerial;
        }

        /**

         *
         * @return the string to use in a SELECT
         */

        public String getSelectColumnName() {
            return selectColumnName;
        }
    }

    /**
     *
     */
    public Leaf() {
        selectColumnMap = new LinkedHashMap<String,Projection>();
        selectColumns = new ArrayList<Projection>();
        tableList = new ArrayList<String>();
        fromRelationList = new ArrayList<FromRelation>();
        tempTableDropList = new ArrayList<String>();
        joinConditions = new ArrayList<Join>();
        conditions = new ArrayList<String>();
        groupByColumns = new ArrayList<Projection>();
        havingConditions = new ArrayList<String>();
        queryNodeList = new ArrayList<DBNode>();
        uncorrelatedSubplanList = new ArrayList<QueryPlan>();
    }

    /**

     *
     * @return the select statement to execute for this step
     */
    public String getSelect()
    {
        return getSelectStatement();
    }

    /**
     *
     * @return the CREATE TABLE command to execute for intermediate results
     * for this step.
     */

    public String getTempTargetCreateStmt() {
        StringBuilder sbCreate = new StringBuilder(128);

        if (Props.XDB_USE_LOAD_FOR_STEP) {
            sbCreate.append(Props.XDB_SQLCOMMAND_CREATEGLOBALTEMPTABLE_START)
                    .append(" ").append(IdentifierHandler.quote(targetTableName))
                    .append(" (")
                    .append(sbCreateTableColumns.toString())
                    .append(") ")
                    .append(Props.XDB_SQLCOMMAND_CREATEGLOBALTEMPTABLE_SUFFIX);

        } else {
            sbCreate.append(Props.XDB_SQLCOMMAND_CREATETEMPTABLE_START)
                    .append(" ")
                    .append(IdentifierHandler.quote(targetTableName))
                    .append(" (")
                    .append(sbCreateTableColumns.toString())
                    .append(") ")
                    .append(Props.XDB_SQLCOMMAND_CREATETEMPTABLE_SUFFIX);
        }
        return sbCreate.toString();
    }

    /**
     * Adds projection to selection list and CREATE TABLE statement.
     *
     * @param aSqlExpression - the expression to select in this step
     * @param isFinalStep - whether or not this is the final step
     *
     * @return the column name to use in the temp table.
     */
    public void appendProjection(
            final String columnName,
            final String exprString,
            final ExpressionType exprType,
            final boolean isFinalStep) {

        // Also determine column name to add for the CREATE TABLE statement
        if (sbCreateTableColumns.length() != 0) {
            // don't add any spaces here (for QueryPlan)
            sbCreateTableColumns.append(",");
        }

        // the create column name
        Projection aColumn = this.new Projection(exprString);


        // Add column to the selection list
        aColumn.createString = columnName; // Also note createColumnName
        selectColumns.add(aColumn);
        selectColumnMap.put(aColumn.projectString, aColumn);            

        // Unknown data type is not a problem on the final step - the temp table is not created  
        if (!isFinalStep 
        		&& (exprType == null || exprType.getTypeString() == null || "".equals(exprType.getTypeString()))) {
            throw new XDBServerException(ErrorMessageRepository.ERROR_GETTING_TYPE_INFO + exprString);
        }
        // keep leading space here. Code in QueryPlan depends on it
        sbCreateTableColumns.append(" ");
        sbCreateTableColumns.append(IdentifierHandler.quote(columnName));            
        sbCreateTableColumns.append(" ");
        // When all else fails, fallback to VARCHAR
        if (exprType == null)
            sbCreateTableColumns.append("VARCHAR");    
        else
            sbCreateTableColumns.append(exprType.getTypeString());
    }

    /**
     *
     * @param aSqlExpression - projection to add to this step.
     * @param isFinalStep - determines if it is the final step in a plan
     * or subplan, in which case we want to use the alias in the create
     * table statement.
     *
     * @return - the column name to use in the temp table
     */
    public String appendProjectionFromExpr(final SqlExpression aSqlExpression,
            final boolean isFinalStep) {
    	String columnName;
    	
        aSqlExpression.rebuildString();       
            
        // Handle aggregates
        if (aSqlExpression.isTempExpr()) {
            columnName = aSqlExpression.getAlias();
        } else {
            if (isFinalStep && aSqlExpression.getAlias().length() > 0) {
                columnName = aSqlExpression.getAlias();

                // See if we already have a column with this name here
                if (isCreateColumn(columnName)) {
                    // generate a new name
                    columnName = generateColumnName();

                    // Add mapping for this expression to the generated name
                    newAliasTable.put(aSqlExpression.getColumn(), columnName);
                }
            } else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {
                if (aSqlExpression.getColumn().tempColumnAlias.length() > 0) {
                    columnName = aSqlExpression.getColumn().tempColumnAlias;
                } else if (aSqlExpression.getColumn().columnAlias.length() > 0) {
                    columnName = aSqlExpression.getColumn().columnAlias;
                } else {
                    columnName = aSqlExpression.getColumn().columnName;
                }

                // See if we already have a column with this name here
                if (isCreateColumn(columnName)) {
                    // generate a new name
                    columnName = generateColumnName();
                    
                    if (aSqlExpression.getColumn().columnAlias.length() == 0) {
                        aSqlExpression.setAlias(columnName);
                    }
                    // Add mapping for this expression to the generated name
                    newAliasTable.put(aSqlExpression.getColumn(), columnName);
                }
            } else if (isFinalStep && aSqlExpression.getAggAlias().length() > 0) {
                // for unions with aggregates
                columnName = aSqlExpression.getAggAlias();
            } else {
                if (aSqlExpression.getAlias().length() > 0) {
                    columnName = aSqlExpression.getAlias();

                    // See if we already have a column with this name here
                    if (isCreateColumn(columnName)) {
                        // generate a new name
                        columnName = generateColumnName();

                        // Add mapping for this expression to the generated name
                        newAliasTable.put(aSqlExpression.getColumn(), columnName);
                    }

                } else {
                    columnName = generateColumnName();
                }
            }
        }

        appendProjection(columnName, aSqlExpression.getExprString(), 
        		aSqlExpression.getExprDataType(), isFinalStep);
        return columnName; 
    }

    /**
     * Checks to see if we already have the projection in our select list
     *
     * @param columnString - the projection to check
     * @return

     */
    public boolean isProjection(final String projectionString) {
        // See if it already is in Leaf.
        if (projectionString == null) {
            return false;
        }        
        return selectColumnMap.get(projectionString) != null;
    }


    /**
     * Checks to see if we already have the column name (not its origin)
     * in our select list
     *
     * @param columnString
     *
     * @return

     */
    public boolean isCreateColumn(final String columnString) {

        // See if it already is in aLeaf.
        for (Projection aProjection : selectColumns) {          
            if (columnString.compareToIgnoreCase(aProjection.createString) == 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks to see if we already have the column in our group by list
     *

     * @param columnString
     *
     * @return whether or not expression is present in group by clause
     */
    public boolean isGroupByColumn(final String columnString) {

        // See if it already is in aLeaf.
        for (Projection aColumn : groupByColumns) {
            if (columnString.compareToIgnoreCase(aColumn.projectString) == 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * Determines the select clause
     *
     * @return the select clause for this leaf
     */
    private String getSelectClause() {
        StringBuilder selectClause = new StringBuilder(64);
        selectClause.append("SELECT ");

        if (distinct) {
            selectClause.append(" distinct ");
        }

        int i = 0;
        for (Projection aColumn : selectColumns) {
            if (i++ > 0) {
                selectClause.append(",");
            }
            selectClause.append(aColumn.projectString);

            String colAlias = aColumn.getCreateColumnName();

            if (colAlias != null && colAlias.length() > 0) {
                selectClause.append(" AS ").append(IdentifierHandler.quote(colAlias));
            }
        }

        return selectClause.toString();
    }

    /**
     * Determine where clause
     *
     * @param Vector<String> join condition list
     *
     * @return the where clause as a string
     */
    private String determineWhereClause (List<String> joinCondList) {

        StringBuilder sbWhereClause = new StringBuilder(128);

        // If we mis-classified some joins for some reason, put them here
        for (int i = 0; i < joinCondList.size(); i++) {
            if (sbWhereClause.length() > 0) {
                sbWhereClause.append(" AND ");
            }

            sbWhereClause.append(joinCondList.get(i));
        }

        // Get other (atomic) conditions
        for (int i = 0; i < conditions.size(); i++) {

            if (sbWhereClause.length() > 0) {
                sbWhereClause.append(" AND ");
            }
            sbWhereClause.append(conditions.get(i));
        }

        if (sbWhereClause.length() > 0)
        {
            return " WHERE " + sbWhereClause.toString();
        } else {
            return "";
        }
    }

    /**
     * Determine final select statement
     *
     * @param aQueryPlan   the QueryPlan this Leaf belongs to
     */
    public void determineSelectStatement(QueryPlan aQueryPlan) {
        
        // TODO: this is based on old prototype code and should be reworked,
        // in particular the String based-code; just use associations
        // similar to how Optimizer and QueryPlan works.
        
        // Track outer levels
        int tableOuterLevel = 0;

        String startTable = ""; // for handling first Leaf
        String sOut;

        sOut = getSelectClause();

        // start building up FROM list
        // start with "join" table, and track it
        if (joinTableName != null && joinTableName.length() > 0) {
            tableList.add(joinTableName);
                aQueryPlan.trackTable(joinTableName, this);
            }

        // Get join conditions

        // This has been reworked to use the INNER JOIN and LEFT OUTER JOIN
        // syntax. I ended up rechecking the join conditions so I can add
        // them to the appropriate ON clause that follows the join.
        // It is not too pretty...
        StringBuilder sbFromOn = new StringBuilder(128);
        if (joinTableName != null) {
            sbFromOn.append(IdentifierHandler.quote(joinTableName));
        }

        StringBuilder sCurrentFrom = new StringBuilder(64);

        ArrayList<String> joinCondList = new ArrayList<String>();
        ArrayList<FromRelation> redoRelationList = new ArrayList<FromRelation>();
        
        for (int i = 0; i < joinConditions.size(); i++) {
            joinCondList.add(joinConditions.get(i).getJoinString());
        }

        for (FromRelation srcRelation : fromRelationList) {

            redoRelationList.add(srcRelation);
            
            // Only add real alias if it has not already been folded
            // into a temp table.

            // amart: I had an error when tried to query temp table using
            // explicit alias.
            // if (sTableName.indexOf (this.tempTablePrefix) == 0)
            // {
            // srcRelation.setAlias("");
            // }
        }       

        int lastSize = 0;

        // Re do in case subtrees (parent-child, lookup) done in wrong order
        // for INNER JOIN, LEFT OUTER JOIN syntax
        while (redoRelationList.size() > 0 && lastSize != redoRelationList.size()) {
            
            lastSize = redoRelationList.size();

            //for (FromRelation redoRelation : redoRelationList) {
            for (int i=0; i < redoRelationList.size(); i ++) {
                
                FromRelation redoRelation = redoRelationList.get(i);
                
                String sTableName = redoRelation.getTableName();
                String sAlias = redoRelation.getAlias();               
                boolean isOnly = redoRelation.getIsOnly();

                // See if we are doing the first step, just add table
                if (sbFromOn.length() == 0) {
                    if (isOnly) {
                        sCurrentFrom.append("ONLY ");
                    }
                    sCurrentFrom.append(IdentifierHandler.quote(sTableName));
                    startTable = IdentifierHandler.quote(sTableName);

                    // Make sure we really have an alias
                    // that is different from original name
                    if (sAlias.length() > 0 && sAlias.compareTo(sTableName) != 0) {
                        sCurrentFrom.append(" ").append(IdentifierHandler.quote(sAlias));
                    }
                    sbFromOn = new StringBuilder(sCurrentFrom);

                    redoRelationList.remove(redoRelation);
                    continue;
                } else {
                    tableOuterLevel = redoRelation.getOuterLevel();

                    // TODO: RIGHT OUTER JOIN
                    // Note that we purposely use != here and not >
                    // because the outer level could theoretically
                    // decrease. The important thing is, when we have a new
                    // level, we must always execute those on the same level
                    // (the inner joins) first.
                    sCurrentFrom = new StringBuilder(32);                    
                    if (tableOuterLevel != lastOuterLevel) {
                        sCurrentFrom.append(" LEFT OUTER JOIN ");
                    } else {
                        sCurrentFrom.append(" INNER JOIN ");
                    }
                    if (isOnly) {
                        sCurrentFrom.append("ONLY ");
                    }
                    sCurrentFrom.append(IdentifierHandler.quote(sTableName));

                    if (sAlias.length() > 0
                            && sAlias.compareTo(sTableName) != 0) {
                        sCurrentFrom.append(" ")
                                .append(IdentifierHandler.quote(sAlias));
                    }
                }

                StringBuilder sOn = new StringBuilder(32);

                for (int k = 0; k < joinCondList.size(); k++) {
                    String joinStr = joinCondList.get(k);

                    // See if it appears in join string
                    // Take into account tables and aliases that are
                    // substring names of others
                    // There may be multiple conditions, and multiple
                    // tables being added on the same step.
                    // This could be a little cleaner.
                    String quotedTableName = IdentifierHandler.quote(sTableName);
                    String quotedAlias = IdentifierHandler.quote(sAlias);
                    
                    if (joinStr.indexOf(" " + quotedTableName + ".") > 0
                            || joinStr.indexOf("(" + quotedTableName + ".") >= 0
                            || joinStr.indexOf(quotedTableName + ".") == 0
                            || joinStr.indexOf(" " + quotedAlias + ".") > 0
                            || joinStr.indexOf("(" + quotedAlias + ".") >= 0
                            || joinStr.indexOf(quotedAlias + ".") == 0) {
                        boolean isUsedLater = false;

                        // see if it also contains an unused table
                        for (FromRelation testRelation : redoRelationList) {
                            
                            String testTable = IdentifierHandler.quote(testRelation.getTableName());

                            // ignore if it is the same one or are start
                            // table. If not, see if we found the join table.
                            if (!(testTable.compareTo(quotedTableName) == 0 || testTable
                                    .compareTo(startTable) == 0)) {
                                if (joinStr.indexOf(testTable + ".") >= 0) {
                                    isUsedLater = true;
                                }
                            }

                            String testAlias = IdentifierHandler.quote(testRelation.getAlias());

                            if (!(testAlias.compareTo(quotedAlias) == 0 || testAlias
                                    .compareTo(startTable) == 0)) {
                                if (joinStr.indexOf(testAlias + ".") >= 0) {
                                    isUsedLater = true;
                                }
                            }
                        }

                        if (!isUsedLater) {
                            if (sOn.length() == 0) {
                                sOn.append("ON");
                            } else {
                                sOn.append(" AND");
                            }

                            sOn.append(" ").append(joinStr);
                            joinCondList.remove(k);
                            k--;
                        }
                    }
                }

                if (sOn.length() > 0 || joinCondList.size() == 0) {
                    sbFromOn.append(" ").append(sCurrentFrom)
                            .append(" ").append(sOn.toString());

                    // Remove from list
                    redoRelationList.remove(i);
                    i--;

                    // Save table outer level
                    lastOuterLevel = redoRelation.getOuterLevel();
                    //tableOuterLevel;
                }
            }
        }

        // If no join condition specified for table,
        // replace INNER with CROSS
        if (sbFromOn.indexOf(" ON ") < 0) {
            if (Props.XDB_SQL_USECROSSJOIN) {
                sbFromOn = new StringBuilder(
                        sbFromOn.toString().replaceAll(" INNER ", " CROSS "));
            } else {
                sbFromOn = new StringBuilder(
                        sbFromOn.toString().replaceAll(" INNER ", ","));
                sbFromOn = new StringBuilder(
                        sbFromOn.toString().replaceAll("JOIN", " "));
            }
        }

        // Check for other cartesian products,
        // such as joining with joined lookups w/o a condition
        for (FromRelation testRelation : redoRelationList) {

            String sTableName = testRelation.getTableName();
            String sAlias = testRelation.getAlias();

            sCurrentFrom.setLength(0);
            
            // See if we are doing the first step, just add table
            if (sbFromOn.length() == 0) {
                sCurrentFrom.append(IdentifierHandler.quote(sTableName));
                startTable = IdentifierHandler.quote(sTableName);

                // Make sure we really have an alias
                // that is different from original name
                if (sAlias.length() > 0 && sAlias.compareTo(sTableName) != 0) {
                    sCurrentFrom.append(" ").append(IdentifierHandler.quote(sAlias));
                }
                sbFromOn = new StringBuilder(sCurrentFrom);

                redoRelationList.remove(testRelation);
                continue;
            }

            if (sbFromOn.length() > 0) {
                if (Props.XDB_SQL_USECROSSJOIN) {
                    sCurrentFrom.append(" CROSS JOIN ")
                            .append(IdentifierHandler.quote(sTableName));
                } else {
                    sCurrentFrom.append(" ,").append(IdentifierHandler.quote(sTableName));
                }

                if (sAlias.length() > 0 && sAlias.compareTo(sTableName) != 0) {
                    sCurrentFrom.append(" ").append(IdentifierHandler.quote(sAlias));
                }
            }
            sbFromOn.append(" ").append(sCurrentFrom);
        }

        // we need to use ALL condition Strings from all nodes
        whereClause = determineWhereClause(joinCondList);

        nonProjectionPart = sbFromOn.length() == 0 ? whereClause : "FROM "
                + sbFromOn.toString() + " " + whereClause
                + determineGroupByClause() + determineHavingClause();

        if (limit > 0) {
            nonProjectionPart += " LIMIT " + limit;
        } else {
            // Uncomment to help in debugging while avoid huge results.
            // nonProjectionPart += " LIMIT 100";
        }
 
        if (this.offset > 0) {
            nonProjectionPart += " OFFSET " + offset;
        }
        // save it
        setSelectStatement(sOut + " " + nonProjectionPart);

        // TODO LIMIT and OFFSET in subquery
    }

    /**
     * Determine group by clause
     *
     * @return the group by clause as a string
     */
    private String determineGroupByClause() {

        // Don't display group by if we supressed it
        if (isSuppressedGroupBy()) {
            return null;
        } else {
            StringBuilder sbGroup = new StringBuilder(64);

            for (Projection groupProjection : groupByColumns) {
                if (sbGroup.length() == 0) {
                    sbGroup.append(" group by ");
                } else {
                    sbGroup.append(", ");
                }
                if (groupProjection.forceGroupQuote) {
                    sbGroup.append(IdentifierHandler.quote(
                            groupProjection.projectString));
                } else {
                    sbGroup.append(groupProjection.projectString);
                }
        }
            return sbGroup.toString();
            }
        }

    /**
     * Determine the HAVING clause
     *
     * @return the having clause as a string
     */
    private String determineHavingClause() {

        StringBuilder sbHaving = new StringBuilder(64);

        for (String havingCondition : havingConditions) {
            if (sbHaving.length() == 0) {
                sbHaving.append(" having ");
            } else {
                sbHaving.append(" and ");
            }
            sbHaving.append(havingCondition);
        }
        return sbHaving.toString();
    }

    /**

     * Returns the SELECT statement with everything at the FROM clause and
     * later.
     *
     * @return the SELECT statement with everything at the FROM clause and
     * later.
     */
    protected String getNonProjectionSelectPart() {
        return nonProjectionPart;
    }

    /**
     * Sets the non projection select part of the statement.
     *
     * We need this for outer handling.
     *
     * @param value

     */
    protected void setNonProjectionSelectPart(final String value) {
        nonProjectionPart = value;
    }

    /**

     * Adds correlated column to allow us to index by it later.
     *
     * @param sqlExpression - the correlated expression
     */
    protected void addCorrelatedColumn(SqlExpression sqlExpression) {
        if (correlatedColumnList == null) {
            correlatedColumnList = new ArrayList<String>();
        }

        if (sqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {
            String columnName;
            if (sqlExpression.getColumn().columnAlias != null
                    && sqlExpression.getColumn().columnAlias.length() > 0) {
                columnName = sqlExpression.getColumn().columnAlias;
            } else {
                columnName = sqlExpression.getColumn().columnName;
            }

            // See if we already have
            for (String compareCol : correlatedColumnList) {
                    if (compareCol.equals(columnName)) {
                        return;
                    }
                }
            correlatedColumnList.add(columnName);
        }
    }

    /**

     *
     * @return the correlated column list
     */
    protected List<String> getCorrelatedColumns() {
        return correlatedColumnList;
    }

    /**
     * This should be called when finished with creating the leaf, for
     * updating aliases to use in the next step.
     */
    public void updateTempAliases() {

        for (AttributeColumn anAC : newAliasTable.keySet()) {
            if (anAC != null) {
                String newAlias = newAliasTable.get(anAC);
                anAC.tempColumnAlias = newAlias;
            }
        }
        newAliasTable.clear();
    }

    /**

     * Adds a serial column to the current Leaf. This is used in outer join

     * handling.
     *
     * @param aQueryPlan

     */
    private void addOuterIdColumns(QueryPlan aQueryPlan) {
        usesOid = true;

        SysDatabase database = aQueryPlan.getSysDatabase();

        int outerCounter = aQueryPlan.getCurrentOuterCounter();

        Projection aColumn = null;

        for (FromRelation fromRelation : fromRelationList) {
            

            SysTable aSysTable = database.getSysTable(fromRelation.getTableName());

            List<SysColumn> columnList = aSysTable.getRowID();

            /*
             * We want to avoid using all the columns to ensure uniqueness. It
             * is also possible that the table does not have many rows and has
             * duplicates. Instead, we see if it is ok to use OID, which may be
             * ok for smaller data sets. If not, we throw an Exception and tell
             * them they need a unique row identifier.
             */
            if (!aSysTable.isTrueRowID()) {
                if (Props.XDB_USE_OID_IN_OUTER) {
                    addOid(aQueryPlan);
                } else {
                    throw new XDBServerException(
                            "Cannot perform outer join for this version unless"
                                    + " the outer most table has unique row identifying"
                                    + " information, either a primary key, unique index or XROWID,"
                                    + "or, if user allows OID usage with xdb.use_oid_in_outer.");
                }
            } else {
                // we found a unique key for the table
                int i = 0;

                for (SysColumn aSysColumn : columnList) {

                    ExpressionType anET = new ExpressionType();
                    anET.type = aSysColumn.getColType();
                    anET.length = aSysColumn.getColLength();
                    anET.precision = aSysColumn.getColPrecision();
                    anET.scale = aSysColumn.getColScale();

                    Leaf.OuterIdColumn outerColumn = new Leaf.OuterIdColumn(
                            aSysColumn.getColName(), "XOID" + outerCounter
                                    + "_" + ++i, anET, false);

                    outerIdColumnList.add(outerColumn);

                    aColumn = new Projection(
                            IdentifierHandler.quote(fromRelationList.get(0).getTableName())
                            + "." + IdentifierHandler.quote(aSysColumn.getColName()));

                    selectColumns.add(aColumn);
                    selectColumnMap.put(aColumn.projectString, aColumn);

                    sbCreateTableColumns.append(", ")
                            .append(IdentifierHandler.quote(outerColumn.selectColumnName))
                            .append(" ")
                            .append(anET.getTypeString());
                }

                break;
            }
        }
    }

    /**

     * Adds a serial column to the current Leaf. This is used in outer join

     * handling.
     *
     * @param aQueryPlan
     *
     * @return the outer OID column created.
     */
    public String addOid(QueryPlan aQueryPlan) {
        usesOid = true;

        int outerCounter = aQueryPlan.getCurrentOuterCounter();
        String outerOIdCol = "XOID" + outerCounter;

        Projection aColumn = null;

        if (joinTableName == null) {
            // We may have multiple tables, use the first one for a unique
            // identifier
            aColumn = new Projection(IdentifierHandler.quote(fromRelationList.get(0).getTableName())
                    + ".OID");
        } else {
            aColumn = new Projection(IdentifierHandler.quote(joinTableName) + ".OID");
        }
        selectColumns.add(aColumn);
        selectColumnMap.put(aColumn.projectString, aColumn);

        ExpressionType anET = new ExpressionType();
        anET.setExpressionType(ExpressionType.BIGINT_TYPE, 0, 0, 0);

        Leaf.OuterIdColumn outerIdColumn = new Leaf.OuterIdColumn(
                aColumn.projectString, outerOIdCol, anET, false);

        outerIdColumnList.add(outerIdColumn);

        // sbCreateTableColumns += ", xserialid " +
        // Properties.XDB_SERIAL_INTERNAL_TEMPLATE;
        sbCreateTableColumns.append(", ").append(outerOIdCol).append(" BIGINT");

        return outerOIdCol;
    }

    /**

     * For outer join handling, use serial
     *
     * @param columnName - the name for the new column
     */
    public void addOuterIdToColumnList(String columnName) {

        ExpressionType anET = new ExpressionType();
        anET.setExpressionType(ExpressionType.BIGINT_TYPE, 0, 0, 0);

        Leaf.OuterIdColumn outerIdColumn = new Leaf.OuterIdColumn(columnName,
                columnName, anET, true);

        outerIdColumnList.add(outerIdColumn);
    }

    /**

     * For outer join handling, use serial
     *
     * @param aQueryPlan - the current QueryPlan
     *
     * @return - the serial id column created.
     */
    public String addOuterIdSerial(QueryPlan aQueryPlan) {

        int outerCounter = aQueryPlan.getCurrentOuterCounter();
        String outerOIdCol = "XSERIALID" + outerCounter;

        sbCreateTableColumns.append(", ")
                .append(IdentifierHandler.quote(outerOIdCol))
                .append(" BIGINT");

//        Leaf.Projection aColumn = new Leaf.Projection("0");
//        aColumn.alias = outerOIdCol;
//        selectColumns.add(aColumn);
//        selectColumnMap.put(aColumn.projectString, aColumn);

        addOuterIdToColumnList(outerOIdCol);

        return outerOIdCol;
    }

    /**

     * Adds a XNODEID column to the current Leaf. This is used in outer join
     * handling for tracking the source
     *
     * @param aQueryPlan
     *
     * @return

     */
    public String addXnodeid(QueryPlan aQueryPlan) {

        int outerCounter = aQueryPlan.getCurrentOuterCounter();
        String outerNodeIdCol = "XONODEID" + outerCounter;

        // XNODEID should be substituted in MultinodeExecutor
        Projection aColumn = new Projection(outerNodeIdCol);
        selectColumns.add(aColumn);
        selectColumnMap.put(aColumn.projectString, aColumn);

        sbCreateTableColumns.append(", ")
                .append(IdentifierHandler.quote(outerNodeIdCol)).append(" INT");

        return outerNodeIdCol;
    }

    /**

     * Updates the select for outer.

     *

     * Rebuild select statement due to adding additional info for outer

     *
     * @param aQueryPlan - the Plan this Leaf belongs to
     * @param addKeyColumns - whether or not to add as the identifying columns
     */
    public void updateSelectForOuter(QueryPlan aQueryPlan, boolean addKeyColumns) {
        // If first step, use primary key to get unique rows
        if (addKeyColumns) {
            addOuterIdColumns(aQueryPlan);
        }

        outerNodeIdColumn = addXnodeid(aQueryPlan);
        aQueryPlan.incrementOuterCounter();

        // If this is not null we have consecutive outers.
        // The select statement will have a UNION, so we need to append this
        // in both parts of the select
        if (getSelectStatement() != null) {
            String outerSource = getOuterColumnSourceString();

            if (outerSource.length() > 0) {
                outerSource += ", ";
            }

            setSelectStatement(getSelectStatement().replaceAll("FROM "
                    + IdentifierHandler.quote(joinTableName), ", " 
                    + outerSource + outerNodeIdColumn
                    + " FROM " + IdentifierHandler.quote(joinTableName)));
        }
    }

    /**
     * Returns the outer column select string.
     *
     * @return - the outer column select string
     */
    protected String getOuterColumnSelectString() {
        StringBuilder sbOuter = new StringBuilder(64);

        for (Leaf.OuterIdColumn outerIdColumn : outerIdColumnList) {
            if (outerIdColumn.selectColumnName != null) {
                if (sbOuter.length() > 0) {
                    sbOuter.append(",");
                }
                sbOuter.append(IdentifierHandler.quote(outerIdColumn.getSelectColumnName()));
            }
        }

        return sbOuter.toString();
    }

    /**
     * Outer columns, for UNION handling.
     *
     * @return - the required outer columns
     */
    protected String getOuterColumnSourceString() {
        StringBuilder sbOuter = new StringBuilder(64);

        for (Leaf.OuterIdColumn outerIdColumn : outerIdColumnList) {
            if (outerIdColumn.sourceColumnName != null) {
                if (sbOuter.length() > 0) {
                    sbOuter.append(",");
                }
                if (outerIdColumn.isPseudoSerial) {
                    sbOuter.append("0 AS ");
                }
                sbOuter.append(outerIdColumn.sourceColumnName);
            }
        }

        return sbOuter.toString();
    }

    /**
     * Gets the outer node id column
     *
     * @return outer node id column
     */
    protected String getOuterNodeIdColumn() {
        return outerNodeIdColumn;
    }

    /**
     * @return the WHERE clause used for this step.
     */
    protected String getWhereClause() {
        return whereClause;
    }

    /**
     * Resets the expressions that are going to be selected on this step.
     */
    protected void resetProjections() {
        selectColumns = new ArrayList<Projection>();
        selectColumnMap = new LinkedHashMap<String,Projection>();
        sbCreateTableColumns = new StringBuilder(128);
    }

    /**
     * Generates a column for the target temp table.
     *
     * @return a column name
     */
    private String generateColumnName() {
        return "EXPRESSION_" + this.leafStepNo + "_" + ++unnamedColCount;
    }

    /**
     * Adds condition to the join list for this step.
     *
     * @param aQueryCondition
     */
    protected void addJoin (QueryCondition aQueryCondition, String joinString) {

        Join aJoin = new Join(aQueryCondition, joinString);
        joinConditions.add(aJoin);
    }

    /**
     * Creates a list of outer Id columns from this step to be used for unique
     * row identification in a later step.
     *
     * @param tableName the table name to assign the new column to
     *
     * @return a List of SqlExpressions representing the new columns
     */
    protected List<SqlExpression> createSqlExprFromOuterIdColumns (final String newTableName) {

        ArrayList<SqlExpression> columnSEList = new ArrayList<SqlExpression>();

        for (Leaf.OuterIdColumn outerIdColumn: outerIdColumnList) {

            // We also need to add the unique row identifiers of the previous
            // step to the new step
            AttributeColumn anAC = new AttributeColumn();
            anAC.setColumnName(outerIdColumn.getSelectColumnName());
            anAC.setTableName(newTableName);
            anAC.setTableAlias(newTableName);
            anAC.columnType = outerIdColumn.columnExprType;

            SqlExpression aSqlExpr = new SqlExpression();
            aSqlExpr.setExprType(SqlExpression.SQLEX_COLUMN);
            aSqlExpr.setColumn(anAC);
            aSqlExpr.setExprDataType(anAC.columnType);

            columnSEList.add(aSqlExpr);
        }

        return columnSEList;
    }

    /**
     * Creates a new Leaf based on the this one, for creating a new
     * outer step
     *
     * @return a new outer-ready Leaf
     */
    protected Leaf createDerivedOuterLeaf () {
        Leaf outerLeaf = new Leaf();

        outerLeaf.queryNodeList = queryNodeList;
        outerLeaf.distinct = distinct;

        outerLeaf.joinTableName = joinTableName;
        outerLeaf.targetTableName = targetTableName + "_o";
        outerLeaf.tableName = tableName;

        outerLeaf.fromRelationList.addAll(fromRelationList);
        outerLeaf.joinConditions.addAll(joinConditions);

        // Also, add all of the joining expressions,
        // while building up outer test expression
        String outerTest = "";

        Iterator<SqlExpression> itOuterExpr = outerExprList.iterator();
        Iterator<SqlExpression> itInnerExpr = innerExprList.iterator();

        while (itOuterExpr.hasNext()) {
            SqlExpression outExpr = itOuterExpr.next();
            SqlExpression inExpr = itInnerExpr.next();

            // Add projection to new step
            outerLeaf.appendProjectionFromExpr(outExpr, false);

            if (outerTest.length() > 0) {
                outerTest += " AND ";
            }

            outerTest += inExpr.getExprString() + " IS NULL";
        }
        outerLeaf.setOuterTestString(outerTest);

        return outerLeaf;
    }

    /**
     * Checks if we have projections assigned.
     *
     * @return whether or not this step has defined projections
     */
    public boolean hasProjections() {
        return this.selectColumns != null && this.selectColumns.size() > 0;
    }

    /**
     * Adds the specified table to the list of tables on this step.
     *
     * @param usedTableName  the table to add to the list of selected tables
     *                       on this step
     */
    public void addUsedTable (String usedTableName) {
        tableList.add(usedTableName);
    }

    /**
     * Adds the specified condition to this step.
     *
     * @param usedTableName  the table to add to the list of conditions
     *                       for this step
     */
    public void addCondition (String condition) {
        conditions.add(condition);
    }

    /**
     * Adds the specified having condition to this step.
     *
     * @param usedTableName  the table to add to the list of having conditions
     *                       for this step
     */
    public void addHavingCondition (String condition) {
        havingConditions.add(condition);
    }

    /**
     * strip out quotes and parens to get to the actual hash column name
     *
     * @param
     */
    public static String normalizeHashColumnName (String hashColumnName) {

        String columnName = new String(hashColumnName);
        
        while (columnName.lastIndexOf('.') > 0 && columnName.length() > 1) {                    
            columnName = columnName.substring(columnName.lastIndexOf('.') + 1);
        }
        if (columnName.endsWith(")") && columnName.length() > 1) {
            columnName = columnName.substring(0, columnName.length() - 1);
        }        
        return IdentifierHandler.stripQuotes(columnName);                
    }
    
    /**
     * Assigns Hash table and column info
     *
     * @param
     */
    public void setHashInfo (String hashTableName, String hashColumnName) {
        this.hashTableName = hashTableName;
        this.hashColumn = normalizeHashColumnName(hashColumnName);      
    }

    /**
     * Checks if the table is contained in the leaf
     *
     * @param tableName table name to search for
     */
    public boolean containsTable (String tableName) {
        for (FromRelation fromRelation : fromRelationList) {
            if (fromRelation.getTableName().equalsIgnoreCase(tableName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Increments the added group count counter
     */
    public void incrementAddedGroupCount() {
        this.addedGroupCount++;
    }

    /**
     *
     */
    public String getOuterTestString() {
        return outerTestString;
    }

    /**
     *
     */
    public void setOuterTestString(String outerTestString) {
        this.outerTestString = outerTestString;
    }

    /**
     *
     * @return
     */
    protected int getLeafStepNo() {
        return leafStepNo;
    }

    /**
     *
     * @param leafStepNo
     */
    protected void setLeafStepNo(int leafStepNo) {
        this.leafStepNo = leafStepNo;
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
    public String getTargetTableName() {
        return targetTableName;
    }

    /**
     *
     * @param targetTableName
     */
    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    /**
     *
     * @return
     */
    public int getLeafType() {
        return leafType;
    }

    /**
     *
     * @param leafType
     */
    public void setLeafType(int leafType) {
        this.leafType = leafType;
    }

    /**
     *
     * @return
     */
    public String getSelectStatement() {
        return selectStatement;
    }

    /**
     *
     * @param selectStatement
     */
    public void setSelectStatement(String selectStatement) {
        this.selectStatement = selectStatement;
    }

    /**
     *
     * @return
     */
    public boolean isDistinct() {
        return distinct;
    }

    /**
     *
     * @param distinct
     */
    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    /**
     *
     * @return
     */
    public String getCreateTableColumns() {
        return sbCreateTableColumns.toString();
    }

    /**
     *
     * @return
     */
    public String getCreateCorrelatedTableString() {
        return createCorrelatedTableString;
    }

    /**
     *
     * @param createCorrelatedTableString
     */
    public void setCreateCorrelatedTableString(String createCorrelatedTableString) {
        this.createCorrelatedTableString = createCorrelatedTableString;
    }

    /**
     *
     * @return
     */
    public String getCorrelatedSelectString() {
        return correlatedSelectString;
    }

    /**
     *
     * @param correlatedSelectString
     */
    public void setCorrelatedSelectString(String correlatedSelectString) {
        this.correlatedSelectString = correlatedSelectString;
    }

    /**
     *
     * @return
     */
    public String getJoinTableName() {
        return joinTableName;
        }

    /**
     *
     * @param joinTableName
     */
    public void setJoinTableName(String joinTableName) {
        this.joinTableName = joinTableName;
    }

    /**
     *
     * @return
     */
    public boolean isCombinerStep() {
        return combinerStep;
    }

    /**
     *
     * @param combinerStep
     */
    public void setCombinerStep(boolean combinerStep) {
        this.combinerStep = combinerStep;
    }

    /**
     *
     * @return
     */
    public boolean isCombineOnMain() {
        return combineOnMain;
    }

    /**
     *
     * @param combineOnMain
     */
    public void setCombineOnMain(boolean combineOnMain) {
        this.combineOnMain = combineOnMain;
    }

    /**
     *
     * @return
     */
    public boolean isExtraStep() {
        return extraStep;
    }

    /**
     *
     * @param extraStep
     */
    public void setExtraStep(boolean extraStep) {
        this.extraStep = extraStep;
    }

    /**
     *
     * @return
     */
    public boolean isCorrelatedHashable() {
        return correlatedHashable;
    }

    /**
     *
     * @param correlatedHashable
     */
    public void setCorrelatedHashable(boolean correlatedHashable) {
        this.correlatedHashable = correlatedHashable;
    }

    /**
     *
     * @return
     */
    public boolean isSuppressedGroupBy() {
        return suppressedGroupBy;
                }

    /**
     *
     * @param suppressedGroupBy
     */
    public void setSuppressedGroupBy(boolean suppressedGroupBy) {
        this.suppressedGroupBy = suppressedGroupBy;
            }

    /**
     *
     * @return
     */
    public String getHashTableName() {
        return hashTableName;
        }

    /**
     *
     * @return
     */
    public String getHashColumn() {
        return hashColumn;
    }

    /**
     *
     * @return
     */
    public boolean isLookupStep() {
        return lookupStep;
    }

    /**
     *
     * @param lookupStep
     */
    public void setLookupStep(boolean lookupStep) {
        this.lookupStep = lookupStep;
    }

    /**
     *
     * @return
     */
    public boolean isSingleStepCorrelated() {
        return singleStepCorrelated;
    }

    /**
     *
     * @param singleStepCorrelated
     */
    public void setSingleStepCorrelated(boolean singleStepCorrelated) {
        this.singleStepCorrelated = singleStepCorrelated;
    }

    /**
     *
     * @return
     */
    public String getSingleCorrelatedHash() {
        return singleCorrelatedHash;
    }

    /**
     *
     * @param singleCorrelatedHash
     */
    public void setSingleCorrelatedHash(String singleCorrelatedHash) {
        this.singleCorrelatedHash = singleCorrelatedHash;
    }

    /**
     * Gets LIMIT clause amount
     *
     * @return
     */
    public long getLimit() {
        return limit;
    }

    /**
     * Sets LIMIT clause amount
     *
     * @param limit
     */
    public void setLimit(long limit) {
        this.limit = limit;
    }

    /**
     * Gets the offset if limit is used.
     *
     * @return
     */
    public long getOffset() {
        return offset;
                }

    /**
     * Sets the offset for this statement
     *
     * @param offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
                }

    /**
     * Gets the ordinaal serial column position
     *
     * @return
     */
    public short getSerialColumnPosition() {
        return serialColumnPosition;
            }

    /**
     * Sets the ordinal serial column position
     *
     * @param serialColumnPosition
     */
    public void setSerialColumnPosition(short serialColumnPosition) {
        this.serialColumnPosition = serialColumnPosition;
        }

    /**
     *
     * @return the partition SqlExpression
     */
    public SqlExpression getPartitionParameterExpression() {
        return partitionParameterExpression;
    }

    /**
     * Set the partition parameter expression
     *
     * @param partitionParameterExpression
     */
    public void setPartitionParameterExpression(SqlExpression partitionParameterExpression) {
        this.partitionParameterExpression = partitionParameterExpression;
    }

    /**
     *
     * @return the added group count
     */
    public int getAddedGroupCount() {
        return addedGroupCount;
    }
   
}
