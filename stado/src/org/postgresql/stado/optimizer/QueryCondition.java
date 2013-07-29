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
import java.util.Collections;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ColumnNotFoundException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.parser.Command;


/**
 * QueryCondition is for handling conditions like
 * col1 > 10. A QueryCondition can also represent a compound 
 * condition, or conditions like x in (subquery).
 */
public class QueryCondition implements IRebuildString {

    // allow for complex conditions
    // These constants set the type of condition.
    // It will either be made up of boolean comparison and/or compound
    // conditions, or just a base SQL Expression

    /** 1001 - INCLAUSE,BETWEEN etc. */    
    public static final int QC_COMPOSITE = 2; 

    /** 1000- AND OR */    
    public static final int QC_CONDITION = 8; 

    /** 0100 - Any SqlExpression */
    public static final int QC_SQLEXPR = 4; 

    /** 10000 - (<, =, >,>=,<=) */    
    public static final int QC_RELOP = 16; 

    /** 1010 - Represents both COND and COMPOSITE  */
    public static final int QC_COND_COMPOSITE = QC_CONDITION | QC_COMPOSITE;

    public static final int QC_RELOP_COMPOSITE = QC_RELOP | QC_COMPOSITE;

    public static final int QC_ALL = QC_COMPOSITE | QC_CONDITION | QC_SQLEXPR
            | QC_RELOP;

    /*
     * The above is a binary method of grouping : For eg. QC_COND_COMPOSITE = 2 |
     * 8 ;
     *
     * when we AND QC_COMPOSITE with QC_COND_COMPOSITE - it will result in a
     * positive value similarly for QC_COND .For all other we will get a zero
     * value and we can therefore know that the element does not belong to the
     * group. To add to the group we just need to OR the elements
     */
    private int condType;

    private QueryCondition leftCond;

    private QueryCondition rightCond;

    /** AND, OR, =, >, etc */    
    private String operator; 

    /** condition expression */    
    private SqlExpression expr; 

    // these can be checked at the top level
    
    /** does this represent a join condition? */
    private boolean isJoin = false; 

    /**
     * This is used when costing out queries, and is set when
     * a column is compared against an expression that evaluates to a
     * constant/single value, eg, col1 = 'fred'
     */
    private boolean isAtomic = false;

    /**
     * This was created to simplify some of the complexity with condition
     * checking in QueryPlan.java. Due to outer join checking, and a
     * misunderstanding of when isAtomic should be set in the Parser, sometimes
     * conditions are added twice to the query.
     */
    private boolean isInPlan = false;

    /** this will contain the original condition at the top level,
     * helpful for debugging
     */
    private String condString = "";

    /** List of RelationNodes involved- only set at top level condition
     * Helps with optimization
     */
    private List<RelationNode> relationNodeList;

    /** This is only to be filled out by Optimizer, leave blank when parsing */
    private List<Integer> nodeIdList;

    /** List of columns used in condition (only set at top level)
     * This is used by the optimizer
     */
    private List<AttributeColumn> columnList;

    /** The variable contains the between clause
     * The variable contains the inclause
     */
    private CompositeCondition aCompositeClause;

    private boolean anyAllFlag = false;

    private String anyAllString = null;

    private List<SqlExpression> projectedColumns = new ArrayList<SqlExpression>();

    private List<AttributeColumn> correlatedColumns;

    /** This variable signifies whether the condition has to be negated in the
     * end or does it have
     * to be positive- in most of the cases we have this as positive.
     */
    private boolean isPositive = true;

    /** Parent of this query condition */
    private QueryCondition parentQueryCondition;

    /** This points to the query tree to which this particular
     * query condition belongs. */
    private QueryTree parentQueryTree;

    /** Constructor */
    public QueryCondition() {

        relationNodeList = new ArrayList<RelationNode>();
        columnList = new ArrayList<AttributeColumn>();
        nodeIdList = new ArrayList<Integer>();

    }
   
    /**
     * Rebuild condString
     * This is useful for the having clause when we
     * do aggregates and update column names and expressions.
     *
     * @param aQueryCondition
     * @return
     */
    public void rebuildCondString() {
        this.condString = rebuildCondition(this);
    }

    /**
     * Use recursion to get all of the elements
     *
     * @param aQueryCondition
     * @return
     */
    private String rebuildCondition(QueryCondition aQueryCondition) {
        String newCondString = "";

        if (aQueryCondition.isPositive == false) {
            newCondString = " NOT ";
        }

        if (aQueryCondition.anyAllFlag == true) {
            newCondString = " " + aQueryCondition.anyAllString;
        }

        if (aQueryCondition.condType == QC_COMPOSITE) {
            newCondString += aQueryCondition.aCompositeClause.rebuildString();
            return newCondString;
        }
        if (aQueryCondition.condType == QC_SQLEXPR) {
            // Make sure we rebuild the condition
            aQueryCondition.expr.rebuildExpression();

            if (aQueryCondition.expr.getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                newCondString += "(" + aQueryCondition.expr.getExprString() + " ) ";
            } else {
                newCondString += aQueryCondition.expr.getExprString();
            }
        } else {
            String leftCondString = "";
            String rightCondString = "";

            // Always do left side first
            if (aQueryCondition.getLeftCond() != null) {
                // do recursion
                leftCondString = rebuildCondition(aQueryCondition.getLeftCond());
            }

            if (aQueryCondition.rightCond != null) {
                rightCondString = rebuildCondition(aQueryCondition.rightCond);

                // Put it together
                newCondString += "(" + leftCondString + " "
                        + aQueryCondition.operator + " " + rightCondString
                        + ")";
                aQueryCondition.condString = newCondString;
            }
        }

        return newCondString;
    }

    /**
     * return all the nodes of a particular type
     *
     * @param qc
     * @param nodetype
     * @return
     */
    public static Vector<QueryCondition> getNodes(QueryCondition qc,
            int nodetype) {
        /* Get all Nodes of a particular type  */
        Vector<QueryCondition> conditions = new Vector<QueryCondition>();
        QueryCondition qcr = qc.rightCond;
        QueryCondition qcl = qc.getLeftCond();

        /* Check if this node is of type we are expecting */
        if ((qc.condType & nodetype) > 0) {
            conditions.add(qc);
        }

        if ((qc.condType & QueryCondition.QC_COMPOSITE) > 0) {
            conditions.addAll(qc.aCompositeClause.getNodes(qc, nodetype));
            return conditions;
        }
        /* Get all the nodes from the right */
        if (qcr != null) {
            conditions.addAll(getNodes(qcr, nodetype));
        }
        /* Get all the nodes from the left */
        if (qcl != null) {
            conditions.addAll(getNodes(qcl, nodetype));
        }

        return conditions;
    }

    /**
     *
     * @return
     */
    public String rebuildString() {
        this.rebuildCondString();
        return condString;
    }

    /**
     *
     * @param aParentCondition
     */
    public void setParent(QueryCondition aParentCondition) { // Set the
        // parent
        // condition
        this.parentQueryCondition = aParentCondition;
        if (this.condType == QC_COMPOSITE) {
            aCompositeClause.setParent(this);
        }
        if (this.rightCond != null) {
            rightCond.setParent(this);

        }
        if (this.getLeftCond() != null) {
            getLeftCond().setParent(this);

        }
    }


    /**
     * Each query tree can have at most one place holder node
     * we there fore ask the query tree to get the relation node
     *
     * @param aAttributeColumn
     * @throws org.postgresql.stado.exception.ColumnNotFoundException
     * @return
     */
    public RelationNode getPseudoRelationNode(AttributeColumn aAttributeColumn)
            throws ColumnNotFoundException {

        // Find the query condition to which this column belongs
        QueryCondition aQueryCondition = findCondition(aAttributeColumn);
        if (aQueryCondition == null) {
            throw new ColumnNotFoundException(aAttributeColumn.columnAlias,
                    aAttributeColumn.getTableAlias());
        }
        return aQueryCondition.parentQueryTree.getPseudoRelationNode();
    }

    /**
     *
     * @param aAttributeColumn
     * @return
     */
    private QueryCondition findCondition(AttributeColumn aAttributeColumn) {
        if (this.rightCond != null) {
            QueryCondition found = rightCond.findCondition(aAttributeColumn);
            if (found != null) {
                return found;
            }
        }

        if (this.getLeftCond() != null) {
            QueryCondition found = getLeftCond().findCondition(aAttributeColumn);
            if (found != null) {
                return found;
            }
        }

        for (QueryCondition sqlExprCondition : QueryCondition.getNodes(this,
                QueryCondition.QC_SQLEXPR)) {
            SqlExpression aSqlExpression = sqlExprCondition.expr;
            if (aSqlExpression.getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                QueryCondition found = aSqlExpression.getSubqueryTree().getWhereRootCondition().findCondition(aAttributeColumn);
                if (found != null) {
                    return found;
                }
            }
            if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {
                if (aAttributeColumn == aSqlExpression.getColumn()) {
                    return this;
                }
            }

            if (aSqlExpression.getExprType() == SqlExpression.SQLEX_FUNCTION) {
                for (SqlExpression element : aSqlExpression.getFunctionParams()) {
                    if (element.getColumn() == aAttributeColumn) {
                        // return element.column.ParentQueryCondition;
                        return this;
                    }

                }
                if (aAttributeColumn == aSqlExpression.getColumn()) {
                    return this;
                }
            }

            if (aSqlExpression.getLeftExpr() != null) {
                if (aSqlExpression.getLeftExpr().containsColumn(aAttributeColumn)) {
                    return this;
                }
            }
            if (aSqlExpression.getRightExpr() != null) {
                if (aSqlExpression.getRightExpr().containsColumn(aAttributeColumn)) {
                    return this;
                }
            }
        }
        return null;
    }

    /**
     *
     * @param right
     * @param left
     * @param Operator
     */
    public QueryCondition(SqlExpression right, SqlExpression left,
            String Operator) {

        QueryCondition aRightExpressionCondition = buildQCFromExpression(right);
        QueryCondition leftQueryCondition = buildQCFromExpression(left);
        this.operator = Operator;
        this.rightCond = aRightExpressionCondition;
        this.setLeftCond(leftQueryCondition);
        this.condType = QueryCondition.QC_CONDITION;
    }

    /**
     *
     * @param positive
     */
    public void setPositive(boolean positive) {
        isPositive = positive;
    }

    /**
     *
     * @param startValue
     * @return
     */
    protected QueryCondition buildQCFromExpression(SqlExpression startValue) {
        QueryCondition aStartValueCond = new QueryCondition();
        aStartValueCond.condType = QC_SQLEXPR;
        startValue.rebuildExpression();
        aStartValueCond.condString = startValue.getExprString();
        aStartValueCond.expr = startValue;
        return aStartValueCond;
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return condString;
    }

    /**
     * Checks to see if the specified SqlExpression is found anywhere in the
     * condition.
     * @param aSqlExpression
     * @return
     */
    public boolean containsSqlExpression(SqlExpression aSqlExpression) {
        boolean check = false;

        if (condType == QueryCondition.QC_SQLEXPR) {
            return expr.contains(aSqlExpression);
        }

        if (this.getLeftCond() != null) {
            check = this.getLeftCond().containsSqlExpression(aSqlExpression);
        }

        if (!check && this.rightCond != null) {
            return this.rightCond.containsSqlExpression(aSqlExpression);
        }

        return check;
    }


    /**
     *
     * @return
     */
    public boolean isSimple() {
        if (condType == QC_CONDITION) {
            return (getLeftCond() == null || getLeftCond().isSimple())
                    && (rightCond == null || rightCond.isSimple());
        } else if (condType == QC_RELOP) {
            if (getLeftCond() != null && getLeftCond().expr != null
                    && getLeftCond().expr.hasSubQuery()) {
                return false;
            }
            if (rightCond != null && rightCond.expr != null
                    && rightCond.expr.hasSubQuery()) {
                return false;
            }
        } else if (condType == QC_SQLEXPR) {
            return !expr.hasSubQuery();
        } else if (condType == QC_COMPOSITE) {
            if (aCompositeClause.compareExpressionQueryCondition != null
                    && !aCompositeClause.compareExpressionQueryCondition.isSimple()) {
                return false;
            }
            if (aCompositeClause.expressionConditionList != null) {
                for (QueryCondition condition : aCompositeClause.expressionConditionList) {
                    if (!condition.isSimple()) {
                        return false;
                    }
                }
            }
            if (aCompositeClause instanceof CompositeTreeCondition) {
                CompositeTreeCondition compositeTree = (CompositeTreeCondition) aCompositeClause;
                if (compositeTree.subTreeExpressionCondition != null
                        && !compositeTree.subTreeExpressionCondition.isSimple()) {
                    return false;
                }
            } else if (aCompositeClause instanceof BetweenClause) {
                BetweenClause betweenClause = (BetweenClause) aCompositeClause;
                if (betweenClause.startValueQueryCondition != null
                        && !betweenClause.startValueQueryCondition.isSimple()) {
                    return false;
                }
                if (betweenClause.endValueQueryCondition != null
                        && !betweenClause.endValueQueryCondition.isSimple()) {
                    return false;
                }
            } else if (aCompositeClause instanceof CLikeClause) {
                CLikeClause cLikeClause = (CLikeClause) aCompositeClause;
                if (cLikeClause.compToExpression != null
                        && !cLikeClause.compToExpression.isSimple()) {
                    return false;
                }
                if (cLikeClause.escapeExpression != null
                        && cLikeClause.escapeExpression.hasSubQuery()) {
                    return false;
                }
            }
        }
        return true;
    }


    /**
     * Checks if the specified table and string is in a "simple" condition. That
     * is, table.column = table2.column2, and not (table.column + 2.5) =
     * some_expression. No ORs allowed; must be top-level. This is useful in
     * checking partitioning
     * @return
     */
    public boolean isSimpleTableJoin() {

        if (condType == QueryCondition.QC_SQLEXPR
                && expr.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION
                && "=".equals(expr.getOperator())
                && expr.getLeftExpr().getExprType() == SqlExpression.SQLEX_COLUMN
                && expr.getRightExpr().getExprType() == SqlExpression.SQLEX_COLUMN
                && expr.getLeftExpr().getMappedExpression() == null) {
            return true;
        }

        return false;
    }

    /**
     * Checks if we have a join with two relations, whether simple or subquery.
     *
     * @return
     */
    public boolean isTwoRelationJoin() {    
        return isJoin && getRelationNodeList().size() == 2;
    }

    /**
     * Steps through conditions in conditionList, and replaces "equivalent"
     * columns in columnList that it finds. This is done to reduce duplicate
     * instances.
     *
     * @param conditionList
     *            list of QueryConditions to search
     * @param columnList
     *            list of desired target replacement columns
     */
    static public void equateConditionsWithColumns(List<QueryCondition> conditionList,
            List<AttributeColumn> columnList) {
        // Get all AC's in QueryConditions
        for (QueryCondition aQC : conditionList) {
            // Get all SqlExpressions in the QueryCondition
            for (QueryCondition compQC : QueryCondition.getNodes(aQC,
                    QueryCondition.QC_SQLEXPR)) {
                compQC.expr.replaceColumnInExpression(columnList);
            }
        }
    }


    /**
     * Return the condition as a set of implicitly ANDed simple conditions, if
     * the condition may be represented so. If not return a singleton.
     * 
     * @return
     */
    public List<QueryCondition> getAndedConditions() {
        // We can reorganize only AND and NOT OR, quickly return if that can
        // not be the case before performing more heavy checks
        if (condType != QueryCondition.QC_CONDITION) {
            return Collections.singletonList(this);
        } else {
            // Apply the rule: not (X or Y) ~ (not X) and (not Y)
            // That may help to process left and right conditions recursively
            // If they are already not positive
            if (!isPositive && "OR".equals(operator)) {
                isPositive = true;
                leftCond.setPositive(!leftCond.isPositive);
                rightCond.setPositive(!rightCond.isPositive);
                operator = "AND";
            }
            if (isPositive && "AND".equals(operator)) {
                ArrayList<QueryCondition> conditions = new ArrayList<QueryCondition>();
                conditions.addAll(leftCond.getAndedConditions());
                leftCond.isJoin = true;
                conditions.addAll(rightCond.getAndedConditions());
                rightCond.isJoin = true;
                return conditions;
            } else {
                return Collections.singletonList(this);
            }
        }
    }


    /**
     * Checks if this condition can be used to determine a single node to
     * execute on, and if so, returns the DBNode otherwise null.
     * @param database
     * @return
     */
    public Collection<DBNode> getPartitionedNode(XDBSessionContext client) {
        SqlExpression columnExpr;
        SqlExpression valueExpr;

        if (isJoin) {
            return null;
        }

        // For update and delete, we want to handle case like
        // col1 = val1 AND col2 = val2.
        // We therefore loosen the criteria. This is safe, won't return ORs.
        if (condType == QC_CONDITION && operator == "AND") {
            Collection<DBNode> vNodes;
            if (getLeftCond() != null) {
                vNodes = getLeftCond().getPartitionedNode(client);
                if (vNodes != null) {
                    return vNodes;
                }
            }
            if (rightCond != null) {
                vNodes = rightCond.getPartitionedNode(client);
                if (vNodes != null) {
                    return vNodes;
                }
            }
            return null;
        }

        if (relationNodeList.size() != 1
                || !(condType == QueryCondition.QC_SQLEXPR
                        && expr.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION 
                        && expr.getOperator().equals("="))) {
            return null;
        }

        // Check to see if we have a condition on the hash column
        if (expr.getLeftExpr().getExprType() == SqlExpression.SQLEX_COLUMN
                && expr.getLeftExpr().getMappedExpression() == null) {
            columnExpr = expr.getLeftExpr();
            valueExpr = expr.getRightExpr();
        } else if (expr.getRightExpr().getExprType() == SqlExpression.SQLEX_COLUMN
                && expr.getRightExpr().getMappedExpression() == null) {
            columnExpr = expr.getRightExpr();
            valueExpr = expr.getLeftExpr();
        } else {
            return null;
        }

        // Now check if it is a partitioned column
        AttributeColumn anAC = columnExpr.getColumn();

        if (anAC.isPartitionColumn()) {
            // check if right side is an expression
            if (valueExpr.getExprType() == SqlExpression.SQLEX_CONSTANT) {
                // Make sure value type is the same as column type
                if (columnExpr.getExprDataType() == null) {
                    columnExpr.setExprDataType(columnExpr.getColumn()
                            .getColumnType(
                                    client.getSysDatabase()));
                }
                valueExpr.setExprDataType(columnExpr.getExprDataType());
                Collection<DBNode> vNodes = anAC.getSysTable().getNode(valueExpr
                        .getNormalizedValue());
                return vNodes == null || vNodes.size() == 0 ? null : vNodes;
            }
        }
        return null;
    }


    /**
     * Finds if the condition contains a parameterized condition comparing
     * against a partitioned column, so that we can be sure to update the
     * destination each time the parameter value changes.
     *
     * @param database - the current database being used.
     *
     * @return a SqlExpression containing column and parameter information.
     */
    public SqlExpression getPartitionParameterExpression(SysDatabase database) {
        SqlExpression columnExpr;
        SqlExpression valueExpr;

        if (isJoin) {
            return null;
        }

        // We want to handle case like col1 = val1 AND col2 = val2.
        if (condType == QC_CONDITION && operator == "AND") {
            if (getLeftCond() != null) {
                SqlExpression sqlExpressionParam = getLeftCond().getPartitionParameterExpression(database);
                if (sqlExpressionParam != null) {
                    return sqlExpressionParam;
                }
            }
            if (rightCond != null) {
                SqlExpression sqlExpressionParam = rightCond.getPartitionParameterExpression(database);
                if (sqlExpressionParam != null) {
                    return sqlExpressionParam;
                }
            }
            return null;
        }

        if (relationNodeList.size() != 1
                || !(condType == QueryCondition.QC_SQLEXPR
                        && expr.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION && expr
                        .getOperator() == "=")) {
            return null;
        }

        // Check to see if we have a condition on the hash column
        if (expr.getLeftExpr().getExprType() == SqlExpression.SQLEX_COLUMN
                && expr.getLeftExpr().getMappedExpression() == null) {
            columnExpr = expr.getLeftExpr();
            valueExpr = expr.getRightExpr();
        } else if (expr.getRightExpr().getExprType() == SqlExpression.SQLEX_COLUMN
                && expr.getRightExpr().getMappedExpression() == null) {
            columnExpr = expr.getRightExpr();
            valueExpr = expr.getLeftExpr();
        } else {
            return null;
        }

        // Check if it is a parameter
        if (valueExpr.getExprType() != SqlExpression.SQLEX_PARAMETER) {
            return null;
        }

        // Now check if it is a partitioned column
        AttributeColumn anAC = columnExpr.getColumn();

        if (anAC.isPartitionColumn()) {
            //consolidate all the info we need for planning,
            //both the parameter number and
            columnExpr.setParamNumber(valueExpr.getParamNumber());
            return columnExpr;
        }

        return null;
    }


    /**
     * @param aCompositeClause the aCompositeClause to set
     */
    public void setACompositeClause(CompositeCondition aCompositeClause) {
        this.aCompositeClause = aCompositeClause;
    }


    /**
     * @return the aCompositeClause
     */
    public CompositeCondition getACompositeClause() {
        return aCompositeClause;
    }


    /**
     * @param anyAllFlag the anyAllFlag to set
     */
    public void setAnyAllFlag(boolean anyAllFlag) {
        this.anyAllFlag = anyAllFlag;
    }


    /**
     * @return the anyAllFlag
     */
    public boolean isAnyAllFlag() {
        return anyAllFlag;
    }


    /**
     * @param anyAllString the anyAllString to set
     */
    public void setAnyAllString(String anyAllString) {
        this.anyAllString = anyAllString;
    }


    /**
     * @return the anyAllString
     */
    public String getAnyAllString() {
        return anyAllString;
    }


    /**
     * @param columnList the columnList to set
     */
    public void setColumnList(List<AttributeColumn> columnList) {
        this.columnList = columnList;
    }


    /**
     * @return the columnList
     */
    public List<AttributeColumn> getColumnList() {
        return columnList;
    }


    /**
     * @param condString the condString to set
     */
    public void setCondString(String condString) {
        this.condString = condString;
    }


    /**
     * @return the condString
     */
    public String getCondString() {
        return condString;
    }


    /**
     * @param condType the condType to set
     */
    public void setCondType(int condType) {
        this.condType = condType;
    }


    /**
     * @return the condType
     */
    public int getCondType() {
        return condType;
    }


    /**
     * @param correlatedColumns the correlatedColumns to set
     */
    public void setCorrelatedColumns(List<AttributeColumn> correlatedColumns) {
        this.correlatedColumns = correlatedColumns;
    }


    /**
     * @return the correlatedColumns
     */
    public List<AttributeColumn> getCorrelatedColumns() {
        if (correlatedColumns == null) {
            correlatedColumns = new ArrayList<AttributeColumn>();
        }
        return correlatedColumns;
    }


    /**
     * @param expr the expr to set
     */
    public void setExpr(SqlExpression expr) {
        this.expr = expr;
    }


    /**
     * @return the expr
     */
    public SqlExpression getExpr() {
        return expr;
    }


    /**
     * @param isAtomic the isAtomic to set
     */
    public void setAtomic(boolean isAtomic) {
        this.isAtomic = isAtomic;
    }

    /**
     * @return the isAtomic
     */
    public boolean isAtomic() {
        return isAtomic;
    }

    /**
     * @param isInPlan the isInPlan to set
     */
    public void setInPlan(boolean isInPlan) {
        this.isInPlan = isInPlan;
    }

    /**
     * @return the isInPlan
     */
    public boolean isInPlan() {
        return isInPlan;
    }

    /**
     * @param isJoin the isJoin to set
     */
    public void setJoin(boolean isJoin) {
        this.isJoin = isJoin;
    }

    /**
     * @return the isJoin
     */
    public boolean isJoin() {
        return isJoin;
    }

    /**
     * @return the isPositive
     */
    public boolean isPositive() {
        return isPositive;
    }

    /**
     * @param leftCond the leftCond to set
     */
    public void setLeftCond(QueryCondition leftCond) {
        this.leftCond = leftCond;
    }

    /**
     * @return the leftCond
     */
    public QueryCondition getLeftCond() {
        return leftCond;
    }

    /**
     * @param rightCond the rightCond to set
     */
    public void setRightCond(QueryCondition rightCond) {
        this.rightCond = rightCond;
    }

    /**
     * @return the rightCond
     */
    public QueryCondition getRightCond() {
        return rightCond;
    }

    /**
     * @param nodeIdList the nodeIdList to set
     */
    public void setNodeIdList(List<Integer> nodeIdList) {
        this.nodeIdList = nodeIdList;
    }

    /**
     * @return the nodeIdList
     */
    public List<Integer> getNodeIdList() {
        return nodeIdList;
    }

    /**
     * @param operator the operator to set
     */
    public void setOperator(String operator) {
        this.operator = operator;
    }

    /**
     * @return the operator
     */
    public String getOperator() {
        return operator;
    }

    /**
     * @param parentQueryCondition the parentQueryCondition to set
     */
    public void setParentQueryCondition(QueryCondition parentQueryCondition) {
        this.parentQueryCondition = parentQueryCondition;
    }

    /**
     * @return the parentQueryCondition
     */
    public QueryCondition getParentQueryCondition() {
        return parentQueryCondition;
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
     * @param projectedColumns the projectedColumns to set
     */
    public void setProjectedColumns(List<SqlExpression> projectedColumns) {
        this.projectedColumns = projectedColumns;
    }

    /**
     * @return the projectedColumns
     */
    public List<SqlExpression> getProjectedColumns() {
        return projectedColumns;
    }

    /**
     * @param relationNodeList the relationNodeList to set
     */
    public void setRelationNodeList(List<RelationNode> relationNodeList) {
        this.relationNodeList = relationNodeList;
    }

    /**
     * @return the relationNodeList
     */
    public List<RelationNode> getRelationNodeList() {
        return relationNodeList;
    }
    /**
     * This is an abstract base class for all the composite elements All
     * composite elements have to extend this class and implement the required
     * abstract functions.
     */
    abstract public class CompositeCondition implements IRebuildString {
        // This will tell us which sql command uses this
        // condition
        Command commandToExecute;

        boolean isPositive = true;

        String condString;

        QueryCondition compareExpressionQueryCondition;

        List<QueryCondition> expressionConditionList = new Vector<QueryCondition>();

        public CompositeCondition(Command commandToExecute) {
            this.commandToExecute = commandToExecute;
            setCondType(QueryCondition.QC_COMPOSITE);
        }

        /**
         * This function is used to set the parents of all the conditions
         * @param aQueryCondition
         */
        public void setParent(QueryCondition aQueryCondition) {

        }

        /**
         * Each Composite clause has a unique way of bulding its string from the
         * basic information provided to it.
         *
         * @return
         */
        abstract public String rebuildString();

        /**
         * The Get Nodes Function is responsible for returning back the nodes of
         * a particular type which are embedded in this composite clause
         *
         * @param qc
         * @param nodeType
         * @return
         */
        abstract public List<QueryCondition> getNodes(QueryCondition qc,
                int nodeType);

        /**
         *
         * @return
         */
        public List<QueryCondition> getExpressionConditionList() {
            return expressionConditionList;
        }

        /**
         *
         * @return
         */
        public QueryCondition getCompareExpressionQueryCondition() {
            return compareExpressionQueryCondition;
        }

        /**
         *
         * @return
         */
        public boolean leadsToJoinCondition() {
            return false;
        }

        /**
         * @param oldExpr
         * @param newExpr
         * @return
         */
        abstract public CompositeCondition changeExpression(
                SqlExpression oldExpr, SqlExpression newExpr);
    }

    /**
     * This class represents the InClauseList - for eg.
     *
     * select * from nation where n_nationkey in (1,2,3,4);
     */
    public class InClauseList extends CompositeCondition {
        /**
         *
         * @param SqlExprList
         * @param compareExpression
         * @param isPositive
         * @param commandToExecute
         */
        public InClauseList(List<SqlExpression> sqlExprList,
                SqlExpression compareExpression, boolean isPositive, Command commandToExecute) {
            super(commandToExecute);
            this.isPositive = isPositive;
            compareExpression.rebuildExpression();
            compareExpressionQueryCondition = buildQCFromExpression(compareExpression);
            for (SqlExpression aSqlExpression : sqlExprList) {
                QueryCondition aNewQueryCondition = buildQCFromExpression(aSqlExpression);
                expressionConditionList.add(aNewQueryCondition);
            }
        }

        /**
         *
         * @return
         */
        @Override
        public String rebuildString() {
            compareExpressionQueryCondition.getExpr().rebuildString();
            String condString = compareExpressionQueryCondition.getExpr().getExprString()
                    + (isPositive ? "" : " NOT")
                    + " IN  ( ";
            boolean firstTime = true;
            for (QueryCondition aQueryCondition : expressionConditionList) {
                aQueryCondition.rebuildCondString();
                if (firstTime) {
                    condString += aQueryCondition.getCondString();
                    firstTime = false;
                } else {
                    condString += " , " + aQueryCondition.getCondString() + " ";
                }
            }
            return condString + " )";
        }

        /**
         *
         * @param qc
         * @param nodeType
         * @return
         */
        @Override
        public List<QueryCondition> getNodes(QueryCondition qc, int nodeType) {
            Vector<QueryCondition> vecNodes = new Vector<QueryCondition>();
            for (QueryCondition aQueryCondition : expressionConditionList) {
                vecNodes.addAll(QueryCondition.getNodes(aQueryCondition,
                        nodeType));
            }
            vecNodes.addAll(QueryCondition.getNodes(
                    compareExpressionQueryCondition, nodeType));
            return vecNodes;
        }

        /**
         *
         * @param oldExpr
         * @param newExpr
         * @return
         */
        @Override
        public CompositeCondition changeExpression(SqlExpression oldExpr,
                SqlExpression newExpr) {
            Vector<SqlExpression> exprList = new Vector<SqlExpression>();
            SqlExpression compExpression = compareExpressionQueryCondition.getExpr();
            for (QueryCondition element : expressionConditionList) {
                if (element.getExpr() == oldExpr) {
                    exprList.add(newExpr);
                } else {
                    exprList.add(element.getExpr());
                }
            }
            if (compareExpressionQueryCondition.getExpr() == oldExpr) {
                compExpression = newExpr;
            }
            InClauseList result = new InClauseList(exprList, compExpression,
                    isPositive, commandToExecute);
            return result;
        }

    }

    abstract class CompositeTreeCondition extends CompositeCondition {

        QueryCondition subTreeExpressionCondition;

        public CompositeTreeCondition(Command commandToExecute) {
            super(commandToExecute);
        }

        /**
         *
         * @param aQueryTree
         * @param compareExpression
         * @param isPositive
         */
        protected void processSubtree(QueryTree aQueryTree,
                SqlExpression compareExpression, boolean isPositive) {
            processSubtree(aQueryTree, isPositive);
            // The compare expression is the expression on the right hand side
            // of the
            // expression.The value it contains can be either a SqlExpression or
            // a Subtree
            // We will not do any processing on it as we should have already
            // done the required
            // processing while processing the expression.Here we are just
            // trying to have a
            // sematically coherent class.
            compareExpressionQueryCondition = buildQCFromExpression(compareExpression);
        }


        /**
         *
         * The subtree we get here can be a non scalar query . 
         * @param aQueryTree
         * @param isPositive
         */
        public void processSubtree(QueryTree aQueryTree, boolean isPositive) {
            // Create a new SqlExpression from the subtree passed to the
            // function
            SqlExpression subTreeExpression = new SqlExpression(aQueryTree);
            // The make a query condition of this expression - This is nothing
            // but wrapping the expression into a query condition
            subTreeExpressionCondition = buildQCFromExpression(subTreeExpression);
            // Set the isPositive value - If this is negative we will have a NOT
            // in front of the condition
            this.isPositive = isPositive;
            // Set the query tree type to NONSCALAR as we can expect to have a
            // SCALAR or
            // a NONSCALAR on the right hand side.
            aQueryTree.setQueryType(QueryTree.NONSCALAR);
            // Ask the query Tree to do subtree processing.This is a special
            // handling
            // done for subtrees.Depending on the tree type different processing
            // is
            // done.Therefore ever processSubTree function should be preceded by
            // setting
            // the type of the subquery tree.
            QuerySubTreeHelper aQuerySubTreeHelper = aQueryTree.processSubTree(
                    subTreeExpression,
                    commandToExecute.getaQueryTreeTracker());
            // Now check if we have generated a node
            if (aQuerySubTreeHelper.projectedSqlExpression != null) {
                getProjectedColumns().addAll(aQuerySubTreeHelper.projectedSqlExpression);
            }

            if (aQuerySubTreeHelper.correlatedColumnExprList != null) {
                setCorrelatedColumns(aQuerySubTreeHelper.getCorrelatedColumnAttributed());
            }
        }

        /**
         *
         * @return
         */
        @Override
        public boolean leadsToJoinCondition() {
            return subTreeExpressionCondition.getExpr().getSubqueryTree() != null;
        }

        /**
         *
         * @param aQuerySubTreeHelper
         */
        public void extractQueryTreeInformation(
                QuerySubTreeHelper aQuerySubTreeHelper) {
            if (aQuerySubTreeHelper.createdRelationNode != null) {
                getRelationNodeList().add(aQuerySubTreeHelper.createdRelationNode);
            }
            if (aQuerySubTreeHelper.projectedSqlExpression != null) {
                getProjectedColumns().addAll(aQuerySubTreeHelper.projectedSqlExpression);
            }
            if (aQuerySubTreeHelper.correlatedColumnExprList != null) {
                setCorrelatedColumns(aQuerySubTreeHelper.getCorrelatedColumnAttributed());
            }
        }

    }

    /**
     * This is InClause which represents a Tree - select * from nation where
     * n_nationkey in (select r_nationkey from region);
     *
     * The Inclause tree extends Composite Tree and uses the general
     * processSubTree function.
     */

    public class InClauseTree extends CompositeTreeCondition {

        /**
         *
         * @param aQueryTree
         * @param compareExpression
         * @param isPositive
         * @param commandToExecute
         */
        public InClauseTree(QueryTree aQueryTree,
                SqlExpression compareExpression, boolean isPositive,
                Command commandToExecute) {
            super(commandToExecute);
            // This call leads to doing the processing of the subtree. This
            // subtree is the right
            // expression in the in clause.The left can be a SCALAR query or it
            // can be a Simple expression
            // BUT this can be a NONSCALAR query.The processSubTree is actually
            // the function of composite
            // TreeCondition
            processSubtree(aQueryTree, compareExpression, isPositive);
            // After doing th required processing we now convert the Tree into a
            // tree which the
            // Optimizer expects- All it does is set the right expression and
            // the left expression
            transformToTraditionalTree();

        }

        /**
         * This function changes the Tree to conform to what the Optimizer
         * expects 1. Set the Operator of the current query Condition to "IN" 2.
         * Set the left and the right condition
         */
        private void transformToTraditionalTree() {

            setOperator("IN");
            // assign the query condition - compareExpressionQueryCondition
            // into the left expression
            setLeftCond(compareExpressionQueryCondition);

            // assign the queryCondition subTreeExpression Condition to the
            // right
            setRightCond(subTreeExpressionCondition);

        }

        /**
         * This function rebuilds the string for this particular IN Clause
         *
         * @return
         */
        @Override
        public String rebuildString() {
            String condString = "";
            compareExpressionQueryCondition.rebuildCondString();
            if (this.isPositive == false) {
                condString += " NOT ";
            }
            condString += compareExpressionQueryCondition.getCondString() + " IN ";
            subTreeExpressionCondition.getExpr().rebuildString();
            String selectString = subTreeExpressionCondition.getExpr().getExprString();
            //  don't double add (, some databases don't like it
            if (selectString.trim().startsWith("(")) {
                condString += selectString + " ";
            } else {
                condString += "(" + selectString + ") ";
            }

            return condString;
        }

        /**
         *
         * @param qc
         * @param nodeType
         * @return
         */
        @Override
        public Vector<QueryCondition> getNodes(QueryCondition qc, int nodeType) {
            Vector<QueryCondition> vec = new Vector<QueryCondition>();

            vec.addAll(QueryCondition.getNodes(compareExpressionQueryCondition,
                    nodeType));
            vec.addAll(QueryCondition.getNodes(subTreeExpressionCondition,
                    nodeType));

            return vec;
        }

        /**
         *
         * @param oldExpr
         * @param newExpr
         * @return
         */
        @Override
        public CompositeCondition changeExpression(SqlExpression oldExpr,
                SqlExpression newExpr) {
            return this;
        }
    }

    // -----------------------------------------------------------------------------------------
    /* Inner Class ExistClause */
    // -----------------------------------------------------------------------------------------
    /**
     * This class is responsible for taking care of the exists clause of SQL
     */
    public class ExistClause extends CompositeTreeCondition {

        /**
         * The Constructor accepts a query tree which is the subtree to which
         * the exist clause is applied and it also has a boolean flag which
         * tells us whether we need to have a NOT EXISIT or just a EXISTS
         *
         * @param commandToExecute
         * @param aQueryTree
         * @param isPositive
         */
        public ExistClause(QueryTree aQueryTree, boolean isPositive,
                Command commandToExecute) {
            super(commandToExecute);
            // Create a new SQLExpression - The SqlExpression just acts as a
            // wrapper around the subquery tree
            SqlExpression subTreeExpression = new SqlExpression(aQueryTree);
            // Wrap it up with the Query Condition
            subTreeExpressionCondition = buildQCFromExpression(subTreeExpression);
            // Set the positive flag
            this.isPositive = isPositive;
            // Set the query tree to the non scalar as we know there can be
            // more than one value
            aQueryTree.setQueryType(QueryTree.NONSCALAR);
            // process the subtree
            processSubtree(aQueryTree, isPositive);
            // Convert the tree to the way the optimizer wants it.
            transformToTraditionalTree();
        }

        /**
         * This function converts the tree to the way the optimizer wants it
         */

        private void transformToTraditionalTree() {
            // Note that we put it to the right tree.
            setRightCond(subTreeExpressionCondition);
            setOperator("EXISTS");
        }

        /**
         *
         * @param qc
         * @param nodeType
         * @return The value it returns is a vector of nodes that we have asked
         *         for
         */
        @Override
        public Vector<QueryCondition> getNodes(QueryCondition qc, int nodeType) {
            Vector<QueryCondition> vec = new Vector<QueryCondition>();
            vec.addAll(QueryCondition.getNodes(subTreeExpressionCondition,
                    nodeType));
            return vec;
        }

        /**
         *
         * @return
         */
        @Override
        public String rebuildString() {
            this.condString = "";
            if (this.isPositive == false) {
                this.condString += " NOT";
            }
            this.condString += " EXISTS ";

            subTreeExpressionCondition.getExpr().rebuildString();

            String selectString = subTreeExpressionCondition.getExpr().getExprString();

            // don't double add (
            if (selectString.trim().startsWith("(")) {
                this.condString += selectString + " ";
            } else {
                this.condString += "(" + selectString + ") ";
            }

            return this.condString;
        }

        /**
         *
         * @param aQueryCondition
         */
        @Override
        public void setParent(QueryCondition aQueryCondition) {
            subTreeExpressionCondition.setParentQueryCondition(aQueryCondition);
        }

        /**
         *
         * @return
         */
        @Override
        public boolean leadsToJoinCondition() {
            if (subTreeExpressionCondition.getExpr().getSubqueryTree().getOrphanCount() > 0) {
                return true;
            } else {
                return false;
            }
        }

        /**
         *
         * @param oldExpr
         * @param newExpr
         * @return
         */
        @Override
        public CompositeCondition changeExpression(SqlExpression oldExpr,
                SqlExpression newExpr) {
            return this;
        }

    }

    /*
     * ******************** Exist Clause ENDS*****************************
     */

    /*
     * ******************** Between Clause Starts*************************
     */

    /*
     * The below represents the Between Clause select * from nation where
     * n_nationkey between 1 and 4;
     */
    public class BetweenClause extends CompositeCondition {

        QueryCondition startValueQueryCondition;

        QueryCondition endValueQueryCondition;

        public static final String BETWEEN_KEYWORD = "BETWEEN";

        public static final String AND_KEYWORD = "AND";

        public static final String NOT_KEYWORD = "NOT";

        /**
         *
         * @param aParentCondition
         */
        @Override
        public void setParent(QueryCondition aParentCondition) {
            startValueQueryCondition.setParentQueryCondition(aParentCondition);
            endValueQueryCondition.setParentQueryCondition(aParentCondition);
            compareExpressionQueryCondition.setParentQueryCondition(aParentCondition);
        }

        /**
         *
         * @param startValue
         * @param endValue
         * @param compareExpression
         * @param isPositive
         * @param commandToExecute
         */
        public BetweenClause(SqlExpression startValue, SqlExpression endValue,
                SqlExpression compareExpression, boolean isPositive, Command commandToExecute) {
            super(commandToExecute);
            startValueQueryCondition = buildQCFromExpression(startValue);

            if (startValueQueryCondition.getExpr() != null
                    && startValueQueryCondition.getExpr().getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                // We are sure that this should be a scalar query and therefore
                // we will set the
                // subquery type to SCALAR

                // Deeming the above statement true - I think that Any-All-In
                // and Exist will need to have
                // processSubtree() where as Clike and Check should not have
                // check clause
                // QueryTreeTracker aQueryTreeTracker =
                // QueryTreeTracker.GetQueryTreeTracker();
                if (startValueQueryCondition.getExpr().getSubqueryTree().getOrphanCount() > 0) {
                    startValueQueryCondition.getExpr().getSubqueryTree().setQueryType(QueryTree.SCALAR_CORRELATED);
                } else {
                    startValueQueryCondition.getExpr().getSubqueryTree().setQueryType(QueryTree.SCALAR_NONCORRELATED);
                }
                // Only if the command is a select command do we have to
                // do the sub tree processing
                if (commandToExecute.getCommandToExecute() == Command.SELECT) {
                    QuerySubTreeHelper aQuerySubTreeHelper = startValue.getSubqueryTree().processSubTree(
                            startValue, commandToExecute.getaQueryTreeTracker());

                    // Now check if we have generated a node
                    if (aQuerySubTreeHelper.createdRelationNode != null) {
                        getRelationNodeList().add(aQuerySubTreeHelper.createdRelationNode);
                    }

                    if (aQuerySubTreeHelper.projectedSqlExpression != null) {
                        getProjectedColumns().addAll(aQuerySubTreeHelper.projectedSqlExpression);
                    }
                }
            }

            endValueQueryCondition = buildQCFromExpression(endValue);

            if (endValueQueryCondition.getExpr() != null
                    && endValueQueryCondition.getExpr().getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                // We are sure that this should be a scalar query
                // QueryTreeTracker aQueryTreeTracker = .GetQueryTreeTracker();
                // add to the subquery list
                if (endValueQueryCondition.getExpr().getSubqueryTree().getOrphanCount() > 0) {
                    endValueQueryCondition.getExpr().getSubqueryTree().setQueryType(QueryTree.SCALAR_CORRELATED);
                } else {
                    endValueQueryCondition.getExpr().getSubqueryTree().setQueryType(QueryTree.SCALAR_NONCORRELATED);
                }
                if (commandToExecute.getCommandToExecute() == Command.SELECT) {
                    QuerySubTreeHelper aQuerySubTreeHelper = endValue.getSubqueryTree().processSubTree(
                            endValue, commandToExecute.getaQueryTreeTracker());
                    // Now check if we have generated a node
                    if (aQuerySubTreeHelper.createdRelationNode != null) {
                        getRelationNodeList().add(aQuerySubTreeHelper.createdRelationNode);
                    }

                    if (aQuerySubTreeHelper.projectedSqlExpression != null) {
                        getProjectedColumns().addAll(aQuerySubTreeHelper.projectedSqlExpression);
                    }
                }
            }

            this.isPositive = isPositive;

            // TODO : check if this is really required -The compare
            // Expression should already have been processed 
            compareExpressionQueryCondition = buildQCFromExpression(compareExpression);

            if (compareExpressionQueryCondition.getExpr() != null
                    && compareExpressionQueryCondition.getExpr().getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                // We are sure that this should be a scalar query
                // QueryTreeTracker aQueryTreeTracker =
                // QueryTreeTracker.GetQueryTreeTracker();
                if (compareExpressionQueryCondition.getExpr().getSubqueryTree().getOrphanCount() > 0) {
                    compareExpressionQueryCondition.getExpr().getSubqueryTree().setQueryType(QueryTree.SCALAR_CORRELATED);
                } else {
                    compareExpressionQueryCondition.getExpr().getSubqueryTree().setQueryType(QueryTree.SCALAR_NONCORRELATED);
                }

                QuerySubTreeHelper aQuerySubTreeHelper = compareExpression.getSubqueryTree().processSubTree(
                        startValue, commandToExecute.getaQueryTreeTracker());

                if (aQuerySubTreeHelper.createdRelationNode != null) {
                    getRelationNodeList().add(aQuerySubTreeHelper.createdRelationNode);
                }
                if (aQuerySubTreeHelper.projectedSqlExpression != null) {
                    getProjectedColumns().addAll(aQuerySubTreeHelper.projectedSqlExpression);
                }
            }

            this.condString = this.rebuildString();
            transformToTraditionalTree();
        }

        /**
         *
         * Rebuild the expression
         *
         * @return
         */
        @Override
        public String rebuildString() {
            this.condString = "";
            this.compareExpressionQueryCondition.getExpr().rebuildExpression();
            this.condString += this.condString
                    + this.compareExpressionQueryCondition.getExpr().getExprString();
            if (this.isPositive == false) {
                this.condString += " NOT " + " ";
            }
            this.condString += " " + BETWEEN_KEYWORD + " ";
            startValueQueryCondition.getExpr().rebuildExpression();
            this.condString += startValueQueryCondition.getExpr().getExprString() + " ";
            this.condString += AND_KEYWORD + " ";
            endValueQueryCondition.getExpr().rebuildExpression();
            this.condString += endValueQueryCondition.getExpr().getExprString();
            return this.condString;
        }

        /**
         *
         * @param qc
         * @param nodeType
         * @return
         */
        @Override
        public Vector<QueryCondition> getNodes(QueryCondition qc, int nodeType) {
            Vector<QueryCondition> nodesList = new Vector<QueryCondition>();
            Vector<QueryCondition> vNodes = QueryCondition.getNodes(
                    compareExpressionQueryCondition, nodeType);
            nodesList.addAll(vNodes);
            vNodes = QueryCondition.getNodes(startValueQueryCondition, nodeType);
            nodesList.addAll(vNodes);
            vNodes = QueryCondition.getNodes(endValueQueryCondition, nodeType);
            nodesList.addAll(vNodes);
            return nodesList;
        }

        /*
         * TODO : This function is not complete - In order to have query
         * condition to work correctly TODO: we have to have a expression which
         * can contain 2 - sql expressions rather than one.
         */
        private void transformToTraditionalTree() {

            setOperator("BETWEEN");
            // assign the query condition - compareExpressionQueryCondition
            // into the left expression
            setLeftCond(compareExpressionQueryCondition);
            // assign the queryCondition subTreeExpression Condition to the
            // right
            setRightCond(endValueQueryCondition);

        }

        /**
         *
         * @param oldExpr
         * @param newExpr
         * @return
         */
        @Override
        public CompositeCondition changeExpression(SqlExpression oldExpr,
                SqlExpression newExpr) {
            SqlExpression startValue = startValueQueryCondition.getExpr();
            SqlExpression endValue = endValueQueryCondition.getExpr();
            SqlExpression compareExpression = compareExpressionQueryCondition.getExpr();
            if (startValueQueryCondition.getExpr() == oldExpr) {
                startValue = newExpr;
            }
            if (endValueQueryCondition.getExpr() == oldExpr) {
                endValue = newExpr;
            }
            if (compareExpressionQueryCondition.getExpr() == oldExpr) {
                compareExpression = newExpr;
            }

            BetweenClause clause = new BetweenClause(startValue, endValue,
                    compareExpression, isPositive, commandToExecute);
            return clause;
        }

    }

    /*
     * ********************Between Ends*********************************
     */

    /*
     * *******************Check Starct**********************************
     */
    public class CheckNullClause extends CompositeCondition {

        /**
         *
         * @param CompSqlExpression
         * @param isPositive
         * @param commandToExecute
         */
        public CheckNullClause(SqlExpression CompSqlExpression,
                boolean isPositive, Command commandToExecute) {
            super(commandToExecute);
            this.isPositive = isPositive;
            compareExpressionQueryCondition = buildQCFromExpression(CompSqlExpression);

            if (compareExpressionQueryCondition.getExpr().getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                if (commandToExecute.getCommandToExecute() == Command.SELECT) {
                    QuerySubTreeHelper aQuerySubTreeHelper = compareExpressionQueryCondition.getExpr().getSubqueryTree().processSubTree(
                            compareExpressionQueryCondition.getExpr(),
                            commandToExecute.getaQueryTreeTracker());
                    if (aQuerySubTreeHelper.createdRelationNode != null) {
                        getRelationNodeList().add(aQuerySubTreeHelper.createdRelationNode);
                    }
                    if (aQuerySubTreeHelper.projectedSqlExpression != null) {
                        getProjectedColumns().addAll(aQuerySubTreeHelper.projectedSqlExpression);
                    }
                    if (aQuerySubTreeHelper.correlatedColumnExprList != null) {
                        setCorrelatedColumns(aQuerySubTreeHelper.getCorrelatedColumnAttributed());
                    }
                }
            }
        }

        /**
         *
         * @param aParentCondition
         */
        @Override
        public void setParent(QueryCondition aParentCondition) {
            compareExpressionQueryCondition.setParentQueryCondition(aParentCondition);
        }

        /**
         *
         * @return
         */
        @Override
        public String rebuildString() {
            String rebuiltString = " (";
            compareExpressionQueryCondition.getExpr().rebuildExpression();
            rebuiltString += compareExpressionQueryCondition.getExpr().getExprString();
            rebuiltString += " " + "IS";
            if (this.isPositive == false) {
                rebuiltString += " " + "NOT";
            }
            rebuiltString += " " + " NULL";
            rebuiltString += " )";
            return rebuiltString;
        }

        /**
         *
         * @param qc
         * @param nodeType
         * @return
         */
        @Override
        public Vector<QueryCondition> getNodes(QueryCondition qc, int nodeType) {
            Vector<QueryCondition> nodesInvolved = new Vector<QueryCondition>();
            nodesInvolved.addAll(QueryCondition.getNodes(
                    compareExpressionQueryCondition, nodeType));
            return nodesInvolved;
        }

        /**
         *
         * @param oldExpr
         * @param newExpr
         * @return
         */
        @Override
        public CompositeCondition changeExpression(SqlExpression oldExpr,
                SqlExpression newExpr) {
            Vector<SqlExpression> exprList = new Vector<SqlExpression>();
            SqlExpression compExpression = compareExpressionQueryCondition.getExpr();
            for (Object element2 : expressionConditionList) {
                QueryCondition element = (QueryCondition) element2;
                if (element.getExpr() == oldExpr) {
                    exprList.add(newExpr);
                } else {
                    exprList.add(element.getExpr());
                }
            }
            if (compareExpressionQueryCondition.getExpr() == oldExpr) {
                compExpression = newExpr;
            }
            CheckNullClause result = new CheckNullClause(compExpression,
                    isPositive, commandToExecute);
            return result;
        }
    }
    
    public class CheckBooleanClause extends CompositeCondition {

        boolean value = true;

        /**
         *
         * @param CompSqlExpression
         * @param isPositive
         * @param value
         * @param commandToExecute
         */
        public CheckBooleanClause(SqlExpression CompSqlExpression,
                boolean isPositive, boolean value, Command commandToExecute) {
            super(commandToExecute);
            this.isPositive = isPositive;
            this.value = value;
            compareExpressionQueryCondition = buildQCFromExpression(CompSqlExpression);

            if (compareExpressionQueryCondition.getExpr().getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                if (commandToExecute.getCommandToExecute() == Command.SELECT) {
                    QuerySubTreeHelper aQuerySubTreeHelper = compareExpressionQueryCondition.getExpr().getSubqueryTree().processSubTree(
                            compareExpressionQueryCondition.expr,
                            commandToExecute.getaQueryTreeTracker());
                    if (aQuerySubTreeHelper.createdRelationNode != null) {
                        relationNodeList.add(aQuerySubTreeHelper.createdRelationNode);
                    }
                    if (aQuerySubTreeHelper.projectedSqlExpression != null) {
                        projectedColumns.addAll(aQuerySubTreeHelper.projectedSqlExpression);
                    }
                    if (aQuerySubTreeHelper.correlatedColumnExprList != null) {
                        correlatedColumns = aQuerySubTreeHelper.getCorrelatedColumnAttributed();
                    }
                }
            }
        }

        /**
         *
         * @param aParentCondition
         */
        @Override
        public void setParent(QueryCondition aParentCondition) {
            compareExpressionQueryCondition.parentQueryCondition = aParentCondition;
        }

        /**
         *
         * @return
         */
        @Override
        public String rebuildString() {
            String rebuiltString = " (";
            compareExpressionQueryCondition.expr.rebuildExpression();
            rebuiltString += compareExpressionQueryCondition.getExpr().getExprString();
            rebuiltString += " " + "IS";
            if (this.isPositive == false) {
                rebuiltString += " " + "NOT";
            }
            rebuiltString += " " + ( value ? "TRUE" : "FALSE" );
            rebuiltString += " )";
            return rebuiltString;
        }

        /**
         *
         * @param qc
         * @param nodeType
         * @return
         */
        @Override
        public Vector<QueryCondition> getNodes(QueryCondition qc, int nodeType) {
            Vector<QueryCondition> nodesInvolved = new Vector<QueryCondition>();
            nodesInvolved.addAll(QueryCondition.getNodes(
                    compareExpressionQueryCondition, nodeType));
            return nodesInvolved;
        }

        /**
         *
         * @param oldExpr
         * @param newExpr
         * @return
         */
        @Override
        public CompositeCondition changeExpression(SqlExpression oldExpr,
                SqlExpression newExpr) {
            Vector<SqlExpression> exprList = new Vector<SqlExpression>();
            SqlExpression compExpression = compareExpressionQueryCondition.expr;
            for (Object element2 : expressionConditionList) {
                QueryCondition element = (QueryCondition) element2;
                if (element.expr == oldExpr) {
                    exprList.add(newExpr);
                } else {
                    exprList.add(element.expr);
                }
            }
            if (compareExpressionQueryCondition.expr == oldExpr) {
                compExpression = newExpr;
            }
            CheckBooleanClause result = new CheckBooleanClause(compExpression,
                    isPositive, value, commandToExecute);
            return result;
        }
    }

    /*
     * ***************** Check End***********************************
     */

    /*
     * ******************Clike Clause Starts*************************
     */
    public class CLikeClause extends CompositeCondition {

        private String likeString;

        public QueryCondition compToExpression;

        public SqlExpression escapeExpression;

        /**
         *
         * @param compSqlExpression
         * @param toCompSqlExpression
         * @param isPositive
         * @param likeString
         * @param commandToExecute
         * @param anEscapeExpression
         */
        public CLikeClause(SqlExpression compSqlExpression,
                SqlExpression toCompSqlExpression, boolean isPositive,
                String likeString, Command commandToExecute,
                SqlExpression anEscapeExpression) {
            super(commandToExecute);
            this.escapeExpression = anEscapeExpression;
            this.isPositive = isPositive;
            this.likeString = likeString;
            compareExpressionQueryCondition = buildQCFromExpression(compSqlExpression);
            compToExpression = buildQCFromExpression(toCompSqlExpression);
            if (compToExpression.getExpr().getExprType() == SqlExpression.SQLEX_SUBQUERY) {

                if (compToExpression.getExpr().getSubqueryTree().getOrphanCount() == 0) {
                    compToExpression.getExpr().getSubqueryTree().setQueryType(QueryTree.SCALAR_NONCORRELATED);
                } else {
                    compToExpression.getExpr().getSubqueryTree().setQueryType(QueryTree.SCALAR_CORRELATED);
                }

                if (commandToExecute.getCommandToExecute() == Command.SELECT) {
                    if (toCompSqlExpression.getSubqueryTree() != null) {
                        QuerySubTreeHelper aQuerySubTreeHelper = toCompSqlExpression.getSubqueryTree().processSubTree(
                                toCompSqlExpression,
                                commandToExecute.getaQueryTreeTracker());

                        // Now check if we have generated a node
                        if (aQuerySubTreeHelper.createdRelationNode != null) {
                            getRelationNodeList().add(aQuerySubTreeHelper.createdRelationNode);
                        }

                        if (aQuerySubTreeHelper.projectedSqlExpression != null) {

                            getProjectedColumns().addAll(aQuerySubTreeHelper.projectedSqlExpression);
                        }
                        if (aQuerySubTreeHelper.correlatedColumnExprList != null) {
                            setCorrelatedColumns(aQuerySubTreeHelper.getCorrelatedColumnAttributed());
                        }

                    }
                }
            }
        }

        /**
         *
         * @return
         */
        @Override
        public String rebuildString() {
            compareExpressionQueryCondition.rebuildCondString();

            String rebuiltString = " (";
            rebuiltString += compareExpressionQueryCondition.getCondString() + " ";
            if (this.isPositive == false) {
                rebuiltString += " " + "NOT";
            }
            rebuiltString += " " + likeString + " ";
            compToExpression.getExpr().rebuildExpression();
            rebuiltString += compToExpression.getExpr().getExprString();

            if (escapeExpression != null) {
                rebuiltString += " ESCAPE ";
                rebuiltString += escapeExpression.rebuildString();
            }
            rebuiltString += " )";
            return rebuiltString;
        }

        public void transformToTraditionalTree() {
            setRightCond(compareExpressionQueryCondition);
            setLeftCond(compToExpression);
            setOperator("LIKE");
        }

        /**
         *
         * @param aParentCondition
         */
        @Override
        public void setParent(QueryCondition aParentCondition) {
            compareExpressionQueryCondition.setParentQueryCondition(aParentCondition);
            compToExpression.setParentQueryCondition(aParentCondition);
        }

        /**
         *
         * @param qc
         * @param nodeType
         * @return
         */
        @Override
        public Vector<QueryCondition> getNodes(QueryCondition qc, int nodeType) {

            Vector<QueryCondition> nodesInvolved = new Vector<QueryCondition>();
            nodesInvolved.addAll(QueryCondition.getNodes(
                    compareExpressionQueryCondition, nodeType));
            nodesInvolved.addAll(QueryCondition.getNodes(compToExpression,
                    nodeType));
            return nodesInvolved;
        }

        /**
         *
         * @param oldExpr
         * @param newExpr
         * @return
         */
        @Override
        public CompositeCondition changeExpression(SqlExpression oldExpr,
                SqlExpression newExpr) {
            SqlExpression compExpression = compareExpressionQueryCondition.getExpr();
            SqlExpression toCompExpression = compToExpression.getExpr();
            if (compareExpressionQueryCondition.getExpr() == oldExpr) {
                compExpression = newExpr;
            }
            if (compToExpression.getExpr() == oldExpr) {
                toCompExpression = newExpr;
            }
            CLikeClause result = new CLikeClause(compExpression,
                    toCompExpression, isPositive, "LIKE", commandToExecute,
                    escapeExpression);
            return result;
        }
    }

}
