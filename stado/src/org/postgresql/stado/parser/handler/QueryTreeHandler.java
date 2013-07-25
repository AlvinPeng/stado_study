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
package org.postgresql.stado.parser.handler;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.ColumnNotFoundException;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.OrderByElement;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.SqlCreateTableColumn;
import org.postgresql.stado.parser.core.CSQLParserConstants;
import org.postgresql.stado.parser.core.syntaxtree.IntoClause;
import org.postgresql.stado.parser.core.syntaxtree.LimitClause;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.OffsetClause;
import org.postgresql.stado.parser.core.syntaxtree.SelectWithoutOrder;
import org.postgresql.stado.parser.core.syntaxtree.SelectWithoutOrderAndSet;
import org.postgresql.stado.parser.core.syntaxtree.UnionSpec;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;

/**
 * This class transforms the complete query tree prepared by the Parser to
 * the one required by the Optimizer.
 */
public class QueryTreeHandler extends DepthFirstRetArguVisitor {
    private static final XLogger logger = XLogger
            .getLogger(QueryTreeHandler.class);

    private Command commandToExecute;

    /**
     * This class is just an information class and is there to help the function
     * return more than 1 parameter.
     */
    public static class CAnalyzeAndCompleteColInfoAndNodeInfo {
        public CAnalyzeAndCompleteColInfoAndNodeInfo() {
            nodeListInExpression = new Vector();
            orphanExpressionVector = new Vector();
        }

        Vector nodeListInExpression;

        /**
         * Columns in the list which are orphan
         */
        public List<SqlExpression> orphanExpressionVector;
    }

    public static final int PROJECTION = 1;

    public static final int CONDITION = 2;

    public static final int HAVING = 3;

    public static final int ORDERBY = 4;

    public static final int GROUPBY = 5;

    private SysDatabase database = null;

    private static int genCount = 0;

    /**
     * Variable for handling Unions
     */
    int unionType = 0;

    /***************************************************************************
     * Constuctor
     */
    public QueryTreeHandler(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
        database = commandToExecute.getClientContext().getSysDatabase();
    }

    /**
     *
     * This function will just check and expand wild cards. for eg, select *
     * from tab will be expanded to the columns in table tab. It will also
     * assign the appropriate colums to the right tables.
     *
     * @param vProjList
     *            SqlExpressions in the projction list
     * @param vRelationNodeList
     *            A list of Tables associated with this query
     */
    public static void checkAndExpand(List<SqlExpression> vProjList,
            List<RelationNode> vRelationNodeList, SysDatabase database,
            Command commandToExecute) {
        // 0. Create a Vector into which you will like to collect new
        // SqlExpressions

        ArrayList<SqlExpression> vSqlExpression = new ArrayList<SqlExpression>();
        List<SqlExpression> vExpandedExpr = new ArrayList<SqlExpression>();

        // 1. Get The expressions to expand.
        // For each expression in the list
        for (SqlExpression sqlExpr : vProjList) {
            // Check to see if we have any expressions like * or TabName.*
            // The SQLEX_COLUMNLIST - - is set in ProjectionListHandler.java
            // This is an indicator that we have to expand this particular
            // expression
            if (sqlExpr.getExprType() == SqlExpression.SQLEX_COLUMNLIST) {

                vSqlExpression.add(sqlExpr);
            }

            // If the expression type is function then we need to check
            // the function params

            if (sqlExpr.getExprType() == SqlExpression.SQLEX_FUNCTION) {
                // Then we also need to check the parameters for expansion
                checkAndExpand(sqlExpr.getFunctionParams(), vRelationNodeList,
                        database, commandToExecute);
                if (sqlExpr.getAlias() == null || sqlExpr.getAlias().equals("")) {
                    sqlExpr.setOuterAlias("EXPRESSION" + ++genCount);
                    sqlExpr.setAlias(sqlExpr.getOuterAlias());

                    // Label projection PostgreSQL-style
                    if (sqlExpr.getExprType() == SqlExpression.SQLEX_FUNCTION) {
                        sqlExpr.setProjectionLabel(sqlExpr.getFunctionName());
                    } else if (sqlExpr.getExprType() == SqlExpression.SQLEX_CASE) {
                        sqlExpr.setProjectionLabel("case");
                    }
                }
            }
            if (sqlExpr.getExprType() == SqlExpression.SQLEX_CONSTANT) {
                if (sqlExpr.getAlias() == null || sqlExpr.getAlias().equals("")) {
                    sqlExpr.setOuterAlias("EXPRESSION" + ++genCount);
                    sqlExpr.setAlias(sqlExpr.getOuterAlias());
                }
            }

            // Incase we have a column - Just make sure that the expression is
            // that tableName and tableAlias are not null or ""

            if (sqlExpr.getExprType() == SqlExpression.SQLEX_COLUMN) {
                if (sqlExpr.getColumn().getTableName() == null
                        || sqlExpr.getColumn().getTableName().equals("")
                          && !sqlExpr.getColumn().getTableAlias().equals("")) { 
                    vSqlExpression.add(sqlExpr);
                    if (sqlExpr.getAlias() != null && sqlExpr.getAlias().length() > 0) {
                        sqlExpr.setProjectionLabel(sqlExpr.getAlias());
                    } else {
                        sqlExpr.setProjectionLabel(sqlExpr.getColumn().getColumnName());
                    }
                }
            }
        }

        // Upto this point we have collected all the expressions - Now we will
        // expand them

        // It is important to do it in two seprate parts 1st finding out and
        // then doing it again for the
        // SqlExpressions which are to be expanded -- as we are using the same
        // list to return the expanded
        // SqlExpressions.

        // Get the expressions which we have filtered to be expanded
        for (SqlExpression aSqlExpression : vSqlExpression) {
            // Expand-- Find it the expression is * or Tab.*
            if (aSqlExpression.getExprString().equals("*")) {
                // Incase we have a * - we will have to find out all the
                // relations in the from clause and create a SqlExpression
                // for each of them
                vExpandedExpr.clear();
                for (RelationNode qn : vRelationNodeList) {
                    // Populate the projection list
                    
                    // Do not include top level unused WITH relations
                    if (qn.getQueryTree().getParentQueryTree() == null
                            && qn.isWith() 
                            && !qn.isTopMostUsedWith())
                    {
                        continue;
                    }
                    // There seeems to be a problem with * with respect to
                    // WITH, so repopulate here
                    if (qn.isWith()) {
                        qn.getProjectionList().clear();
                        populateSubRelationProjectionList(qn, database);                       
                    } else if (qn.getProjectionList().isEmpty()) {
                        populateProjectionList(qn, database);
                    }
                                                
                    vExpandedExpr.addAll(qn.getProjectionList());
                }
                vProjList.addAll(vProjList.indexOf(aSqlExpression),
                        vExpandedExpr);
                vProjList.remove(aSqlExpression);
            } else {
                // First get hold of the Table - The user can use a.* which
                // implies we need to find a table
                // which has the name "a" as the alias.
                String identifier = "";
                if (aSqlExpression.getExprType() != SqlExpression.SQLEX_COLUMN) {
                    int id = aSqlExpression.getExprString().lastIndexOf(".");

                    if (id < 0) {
                        throw new XDBServerException(
                                ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                                ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
                    } else {
                        identifier = aSqlExpression.getExprString().substring(0, id);
                    }
                } else {
                    identifier = aSqlExpression.getColumn().getTableAlias();
                }

                /**
                 * Now Determine if it is a TableName or an Alias
                 * The tableName.* can be a real table or it could be an aliased
                 * table -- There fore we get all the nodes in the relationList
                 */
                List<RelationNode>  vMatchesFound = new ArrayList<RelationNode>();
                for (RelationNode qn : vRelationNodeList) {
                    // Check the name specified with the alias - Incase an alias
                    // is not specifed the alias is automatically set to the table
                    // name Therefore this should not give any problems.
                    String toCheckWith = identifier;
                    if (qn.getAlias() != null && qn.getAlias().equals(qn.getTableName())) {
                        if (qn.getAlias() != null
                                && qn.getAlias().equalsIgnoreCase(toCheckWith)) {
                            vMatchesFound.add(qn);
                        } 
                    } else {
                        // Incase they are not equal --implying that the query
                        // node had an alias - check with the alias
                        if (identifier.equalsIgnoreCase(qn.getAlias())) {
                            vMatchesFound.add(qn);
                        }
                    }
                }
                // After all the processing is over --
                if (vMatchesFound.size() < 1) {

                    if (identifier == null || identifier.length() == 0) {
                        // We could be dealing with nesting FROM subqueries
                        // Search via traversal
                        QueryTree traverseTree = commandToExecute.getaQueryTreeTracker().GetCurrentTree();

                        boolean found = false;

                        while (traverseTree != null && !found) {
                            for (RelationNode relNode : traverseTree.getRelationSubqueryList()) {
                                if (relNode.getSubqueryTree() != null) {
                                    for (SqlExpression compareExpr : relNode.getSubqueryTree().getProjectionList()) {
                                        if (compareExpr.getAlias().equalsIgnoreCase(aSqlExpression.getAlias())) {
                                            if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {
                                                // We need to get the alias for this formulated relation,
                                                aSqlExpression.getColumn().setTableName(relNode.getAlias());
                                                aSqlExpression.getColumn().setTableAlias(relNode.getAlias());
                                                aSqlExpression.getColumn().relationNode = relNode;
                                                found = true;
                                                break;
                                            } 
                                        }
                                    }
                                    if (found) { 
                                        break; 
                                    }
                                }
                            }

                            traverseTree = traverseTree.getParentQueryTree();
                        }
                        if (found) {
                            continue;
                        }
                        // Now look for it in WITH statements
                        for (RelationNode subRelationNode : commandToExecute.getaQueryTreeTracker().GetCurrentTree().getTopMostParentQueryTree().getTopWithSubqueryList()) {

                            if (subRelationNode.getSubqueryTree() != null) {
                                for (SqlExpression compareExpr : subRelationNode.getSubqueryTree().getProjectionList()) {
                                    if (compareExpr.getAlias().equalsIgnoreCase(aSqlExpression.getAlias())) {
                                        if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {
                                            // We need to get the alias for this formulated relation,
                                            aSqlExpression.getColumn().setTableName(subRelationNode.getAlias());
                                            aSqlExpression.getColumn().setTableAlias(subRelationNode.getAlias());
                                            aSqlExpression.getColumn().relationNode = subRelationNode;
                                            found = true;
                                            break;
                                        } 
                                    }
                                }
                                if (found) { 
                                    break; 
                                }
                            }
                        }
                        if (found) {
                            continue;
                        }
                    }
                   
                    throw new XDBServerException(
                            ErrorMessageRepository.TABLE_ALIAS_NOT_FOUND_FROM_CLAUSE
                                    + "(" + aSqlExpression.getColumn().getTableAlias() + ")",
                            0,
                            ErrorMessageRepository.TABLE_ALIAS_NOT_FOUND_FROM_CLAUSE_CODE);                  
                    
                } else if (vMatchesFound.size() > 1) {
                    throw new XDBServerException(
                            ErrorMessageRepository.TABLE_ALIAS_FOUND_AMBIGOUS
                                    + "( " + aSqlExpression.getExprString() + " )",
                            0,
                            ErrorMessageRepository.TABLE_ALIAS_FOUND_AMBIGOUS_CODE);
                } else {
                    // Now that we have found our one and only Match -- we will
                    // get all the colmns of this
                    // table and add the SqlExpressions to the ProjectList
                    RelationNode qn = vMatchesFound.get(0);
                    if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMNLIST) {
                        vExpandedExpr = populateProjectionList(qn, database);
                        vProjList.addAll(vProjList.indexOf(aSqlExpression),
                                vExpandedExpr);
                        vProjList.remove(aSqlExpression);
                    } else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {
                        aSqlExpression.getColumn().setTableName(qn.getTableName());
                        aSqlExpression.getColumn().setTableAlias(qn.getAlias());
                        aSqlExpression.getColumn().relationNode = qn;
                    }
                }

            }
        }// else continue
        // finally - Remove the orignal elements
        Iterator<SqlExpression> enRemove = vProjList.iterator();

        while (enRemove.hasNext()) {
            SqlExpression aSqlExpression = enRemove.next();
            if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMNLIST) {
                enRemove.remove();
            }
        }

        // We are done with expansions
    }

        /**
     * 
     * @param qn
     * @param database 
     */
    public static void populateSubRelationProjection(SqlExpression aSqlExpression,
            RelationNode relNode, SysDatabase database) {
        
        SqlExpression aSqlColExpression = new SqlExpression();
        if (System.getProperty("TrackSQLExpression") != null
                && System.getProperty("TrackSQLExpression").equals("1")) {
        }
        // Make it of type column
        aSqlColExpression.setExprType(SqlExpression.SQLEX_COLUMN);
        // Make the mapped to External Mapping - This indicates that
        // the column is mapped to a SqlExpression
        aSqlColExpression.setMapped(SqlExpression.EXTERNALMAPPING);
        // Assign the Sql xpression to which this column is mapped
        aSqlColExpression.setMappedExpression(aSqlExpression);
        // Create an attribute Column
        aSqlColExpression.setColumn(new AttributeColumn());
        // Assign it as being mapped
        aSqlColExpression.getColumn().columnGenre |= AttributeColumn.MAPPED;
        // Set the Relation node to this new node
        aSqlColExpression.getColumn().relationNode = relNode;

        // now fill the Column expression - this basically fills
        // the alias, name for both the column and table
        fillColumnExpression(relNode, aSqlColExpression, aSqlExpression);        
        
        aSqlColExpression.setProjectionLabel(aSqlExpression.getProjectionLabel());

        if (aSqlColExpression.getProjectionLabel() == null) {
            aSqlColExpression.setProjectionLabel(aSqlExpression.getAlias());
        }
        // In case we are dealing with WITH, copy previously determined
        // data type information
        if (aSqlExpression.getColumn() != null) {
            aSqlColExpression.getColumn().columnType = aSqlExpression.getColumn().columnType;            
        }
        aSqlColExpression.setExprDataType(aSqlExpression.getExprDataType());

        
        // Finally add the SqlExpression to the projection list
        relNode.getProjectionList().add(aSqlColExpression);
    }
    
    /**
     * For WITH subquery handling, populate projections
     * @param qn
     * @param database 
     */
    public static void populateSubRelationProjectionList(RelationNode relNode,
            SysDatabase database) {
        
        if (relNode == null) {
            throw new XDBServerException("Internal error: null relation for subrelation");
        }
        
        if (relNode.getSubqueryTree() != null) {
            for (SqlExpression aSqlExpression : relNode.getSubqueryTree().getProjectionList()) {
                // Create a new expression
                populateSubRelationProjection(aSqlExpression, relNode, database);
            }            
        } else {
            // See if we have a base relation
            if (relNode.isWithDerived()) {

                if (relNode.getBaseWithRelation().getSubqueryTree() != null) {
                    // Need to set outside viewable projections for a WITH
                   for (SqlExpression aSqlExpression : relNode.getBaseWithRelation().getProjectionList()) {
                       // Create a new expression
                       // Note we are taking the projections from the subtree and 
                       // creating here
                       populateSubRelationProjection(aSqlExpression, relNode, database);
                       //
                       //if (aSqlColExpression.getProjectionLabel() == null) {
                       //    aSqlColExpression.setProjectionLabel(aSqlExpression.getProjectionLabel());
                       //}
                       // Set labels
                    }    
                } else {
                    throw new XDBServerException("Internal error: null subquery tree for base WITH relation");                    
                }
            } else {
                throw new XDBServerException("Internal error: null subquery tree for subrelation with no base WITH relation");
            }
        }

    }
                
    /**
     * This function will take in a Query Node and populate the query nodes projection list 
     * (SELECT *...)
     * @param qn
     * @throws IllegalArgumentException
     */
    private static List<SqlExpression> populateProjectionList(RelationNode qn,
            SysDatabase database) {
        /**
         * Check out what we are getting in terms of query node. There are
         * different types of Nodes - TABLE , SUBQUERY. For this function only
         * two of them are relvant - TABLE and SUBQUERYRELATION Since we use it
         * requires us to have a Table:which is represented by RelationNode
         */
        List<SqlExpression> result = new ArrayList<SqlExpression>();
        if (qn.getNodeType() == RelationNode.TABLE) {
            // Incase it is a table - get the sys table from the meta-data
            SysTable aSysTable = database.getSysTable(qn.getTableName());
            // If we are able to find the sys table we will proceed
            if (aSysTable == null) {

                throw new XDBServerException(
                        ErrorMessageRepository.NO_TABLES_FOUND + " ( "
                                + qn.getTableName() + "  ) ", 0,
                        ErrorMessageRepository.NO_TABLE_FOUND_CODE);
            }
            // Now!! Get the columns from the systable
            for (SysColumn syscol : aSysTable.getColumns()) {
                // Create a New Sql Expression
                SqlExpression sql;
                String columnAlias = null;
                String operator = null;
                // Incase we are dealing with a row id column and the
                // user has not specified it explicitly( in which case the
                // executoin will not come here) ignore it and move forward
                if (syscol.getColName().equalsIgnoreCase(
                        SqlCreateTableColumn.XROWID_NAME)) {
                    continue;
                }
                // This function is a SqlExpression Factory and will create
                // SqlExpressions as per function signature
                sql = SqlExpression.getSqlColumnExpression(syscol.getColName(),
                        aSysTable.getTableName(), qn.getAlias(), columnAlias,
                        operator);
                // Set the query Node
                sql.getColumn().relationNode = qn;
                sql.setProjectionLabel(syscol.getColName());
                // Add this Sql expression to the projection list
                result.add(sql);
                // And to the Projection List of the Query Node to which it
                // belongs
                // The below expression will be a column expression
                qn.getProjectionList().add(sql);
            }
        } else if (qn.getNodeType() == RelationNode.SUBQUERY_RELATION) {
            // populate the projection list for this column - In this case
            // we might also have expressions, rather than just the columns
            // Therefore we have to have a mapping scheme - which basically
            // says which column Expression in the projection list points
            // to which SqlExpression in the underlying SUBQUERY_RELATION

            // For each Sql Expression that we found in the projection list
            // of the Subqery
            if (qn.getProjectionList().size() == 0) {
                populateSubRelationProjectionList(qn, database);
            }
            result.addAll(qn.getProjectionList());
        }
        return result;
    }

    /**
     * This function encapsulates the functionality of handling the columnName
     * ,Table Name , Column Alias ,Table Alias for all the SqlColumn Expressions
     *
     * @param qn
     * @param aSqlColExpression
     * @param aSqlExpression
     */
    private static void fillColumnExpression(RelationNode qn,
            SqlExpression aSqlColExpression, SqlExpression aSqlExpression) {
        // The SqlColExpression needs to be modified as per the information

        // we do get mapped column expressions in the
        // order by and group by list - Incase these are columns they should
        // have proper relation node. Incase they are not columns but alias. The
        // relationnode will already be null

        // If the column is internall Mapped , implying that it belongs to the
        // same query tree
        if (aSqlColExpression.getMapped() != SqlExpression.INTERNALMAPPING) {
            // Then we will set the alias and the tableName to the alias of the
            // query node
            // The example we have is
            /**
             * select n_nationkey
             */
            if (qn.getAlias() != null && qn.getAlias().equals("") == false) {
                if (aSqlExpression.getColumn() != null) {
                    aSqlColExpression.getColumn()
                            .setTableAlias(aSqlExpression.getColumn()
                                    .getTableAlias());
                    // Set only if not set already and we have an alias to assign from
                    // (WITH handling)
                    if ((aSqlColExpression.getColumn().getTableName() == null 
                            || aSqlColExpression.getColumn().getTableName().length() == 0)
                            && aSqlColExpression.getColumn().getTableAlias() != null
                            && aSqlColExpression.getColumn().getTableAlias().length() > 0) {
                        
                        aSqlColExpression.getColumn().setTableName(aSqlExpression.getColumn()
                            .getTableAlias());
                    }
                } else {
                    aSqlColExpression.getColumn().setTableAlias(qn.getAlias());
                    aSqlColExpression.getColumn().setTableName(qn.getAlias());
                }
            }
        } else {
            aSqlColExpression.setExprDataType(aSqlExpression.getExprDataType());
            aSqlColExpression.setExprString(aSqlExpression.getExprString());
            aSqlColExpression.setAlias(aSqlExpression.getOuterAlias());

            if (aSqlExpression.getColumn() != null
                    && (aSqlExpression.getColumn().columnAlias == null
                            || aSqlExpression.getColumn().columnAlias.equals("") || aSqlExpression.getColumn().columnName
                            .equals(aSqlExpression.getColumn().columnAlias))) {
                aSqlColExpression.setColumn(aSqlExpression.getColumn());
            }

        }

        if (aSqlExpression.getOuterAlias() == null || aSqlExpression.getOuterAlias()
                .equals("")
                || aSqlColExpression.getMapped() != SqlExpression.EXTERNALMAPPING) {
            if (aSqlExpression.getAlias() == null || aSqlExpression.getAlias().equals("")) {
                if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {
                    if (aSqlExpression.getColumn().columnAlias != null
                            && aSqlExpression.getColumn().columnAlias.equals("") == false) {
                        aSqlColExpression.getColumn().columnName = aSqlExpression.getColumn().columnAlias;
                        aSqlColExpression.getColumn().columnAlias = aSqlColExpression.getColumn().columnName;
                        aSqlColExpression.setAlias(aSqlExpression.getColumn().columnAlias);
                    } else {
                        aSqlColExpression.getColumn().columnName = aSqlExpression.getColumn().columnName;
                        aSqlColExpression.setAlias(aSqlExpression.getColumn().columnName);
                        aSqlColExpression.getColumn().columnAlias = aSqlColExpression.getColumn().columnName;
                    }
                } else {
                    aSqlColExpression.setOuterAlias("EXPRESSION" + ++genCount);
                    if (aSqlExpression.getOuterAlias() != null
                            && !aSqlExpression.getOuterAlias().equals("")) {
                        aSqlColExpression.setAlias(aSqlExpression.getOuterAlias());
                        aSqlColExpression.getColumn().columnName = aSqlExpression.getOuterAlias();
                    } else {
                        aSqlColExpression.setAlias(aSqlColExpression.getOuterAlias());
                        aSqlColExpression.getColumn().columnName = aSqlColExpression.getOuterAlias();
                    }
                    aSqlColExpression.getColumn().columnAlias = aSqlColExpression.getColumn().columnName;
                }
            } else {

                aSqlColExpression.setAlias(aSqlExpression.getAlias());
                if (aSqlExpression.getColumn() != null) {
                    aSqlColExpression.getColumn().columnName = aSqlExpression.getColumn().columnName;
                    aSqlColExpression.getColumn().columnAlias = aSqlExpression.getColumn().columnAlias;
                }
            }
        } else {
            if (aSqlColExpression.getAlias() == null
                    || aSqlColExpression.getAlias().equals("") == true) {
                aSqlColExpression.setAlias(aSqlExpression.getOuterAlias());
            }
            aSqlColExpression.getColumn().columnName = aSqlExpression.getOuterAlias();
            aSqlColExpression.getColumn().columnAlias = aSqlColExpression.getColumn().columnName;
        }
    }

    /**
     * Grammar production: f0 -> <LIMIT_> f1 -> ( <DECIMAL_LITERAL> | <ALL_> )
     */
    @Override
    public Object visit(LimitClause n, Object argu) {
        QueryTree aQueryTree = (QueryTree) argu;

        if (n.f1.which == 0) {
            aQueryTree.setLimit(new Integer(
                    ((NodeToken) n.f1.choice).tokenImage).longValue());
        }
        return null;
    }

    /**
     * Grammar production:
	 * f0 -> <INTO_>
	 * f1 -> [ <TEMPORARY_> | <TEMP_> ]
	 * f2 -> [ <TABLE_> ]
	 * f3 -> TableName(prn)
     */
    @Override
    public Object visit(IntoClause n, Object argu) {
        QueryTree aQueryTree = (QueryTree) argu;
        TableNameHandler tnh = new TableNameHandler(commandToExecute.getClientContext());
        n.f3.accept(tnh, argu);
        aQueryTree.setIntoTable(tnh.getTableName(), tnh.getTableName(), n.f1.present());
        return null;
    }

    /**
     * Grammar production: f0 -> <OFFSET_> f1 -> <DECIMAL_LITERAL>
     */

    @Override
    public Object visit(OffsetClause n, Object argu) {
        QueryTree aQueryTree = (QueryTree) argu;
        aQueryTree.setOffset(new Integer(n.f1.tokenImage).longValue());
        return null;
    }

    /**
     * Grammar production: f0 -> SelectWithoutOrderAndSetWithParenthesis(prn) f1 -> (
     * <UNION_> [ <ALL_> ] ( SelectWithoutOrderAndSet(prn) | UnionSpec(prn) ) )*
     */

    @Override
    public Object visit(SelectWithoutOrder n, Object argu) {
        QueryTree aQueryTree = (QueryTree) argu; // This variable will be set as the public variable.
        n.f0.accept(this, aQueryTree);
        if (n.f1.present()) {
            aQueryTree.setHasUnion(true);
            for (Iterator it = n.f1.nodes.iterator(); it.hasNext();) {
                NodeSequence noS = (NodeSequence) it.next();
                NodeOptional nOp = (NodeOptional) noS.nodes.get(1);
                NodeChoice ch = (NodeChoice) noS.nodes.get(2);
                QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                        commandToExecute);

                if (nOp.present()) {
                    // Set the all to true
                    unionType = QueryTree.UNIONTYPE_UNIONALL;
                } else {
                    unionType = QueryTree.UNIONTYPE_UNION;
                }
                QueryTree aQueryTreeToAdd = new QueryTree();

                switch (ch.which) {
                case 0: // select
                    aQueryTreeToAdd.setUnionType(unionType);
                    aQueryTreeToAdd.setTopMostParentQueryTree(aQueryTree.getTopMostParentQueryTree());
                    ch.accept(aQueryTreeHandler, aQueryTreeToAdd);
                    aQueryTree.getUnionQueryTreeList().add(aQueryTreeToAdd);
                    break;
                case 1: // union
                    aQueryTreeToAdd.setUnionType(unionType);
                    ch.accept(aQueryTreeHandler, aQueryTreeToAdd);
                    aQueryTree.getUnionQueryTreeList().add(aQueryTreeToAdd);
                    break;

                default:
                    break;
                }
            }
        } else {
            unionType = QueryTree.UNIONTYPE_NONE;
        }
        return null;
    }

    /**
     * Grammar production: f0 -> "(" f1 -> SelectWithoutOrder(prn) f2 -> ")"
     */

    @Override
    public Object visit(UnionSpec n, Object argu) {
        n.f1.accept(this, argu);
        return null;
    }

    // 2. Check if it is * or qualified tab.*

    // 2.1 If unqualified -- get all the columns listed
    // and create a seprate SQLExpression for each of them
    // add them to the projection list of the Tree as well as
    // the Node.

    // 2.2 Incase we find that the name is Qualified -
    // 2.2.1 Find the Node to which the qualified * belongs
    // We generate SqlExpression only for the Qualified Table
    /*
     * This is the heart of this UNIT-- All other functions are called from this
     * point
     *
     */

    /**
     * f0 -> <SELECT_> f1 -> [ <ALL_> | <DISTINCT_> | <UNIQUE_> ] f2 ->
     * SelectList(prn) f3 -> [ IntoClause(prn) ] f4 -> FromClause(prn) f5 -> [
     * WhereClause(prn) ] f6 -> [ GroupByClause(prn) ]
     */
    /**
     * This is a main function which is responsible for creating QueryTree and
     * then filling them up
     *
     * @param n
     * @param argu
     * @return
     */

    @Override
    public Object visit(SelectWithoutOrderAndSet n, Object argu) {
        // Register the current query tree, we register a query tree that we are
        // currently working on
        // and de register it when we are done with it at the end of the
        // function.This allows for easy accesss to the query tree.
        QueryTreeTracker aQueryTreeTracker = commandToExecute
                .getaQueryTreeTracker();
        QueryTree aQueryTree = (QueryTree) argu;
        aQueryTreeTracker.registerTree(aQueryTree);
        // Step : 1

        // In order to start building the query tree we will start with checking
        // if it is a all| distinct | unique
        // The default is all, and distinct and unique mean the same.
        if (n.f1.present()) {
            switch (((NodeChoice) n.f1.node).which) {
            case 0:
                aQueryTree.setDistinct(false);
                break;
            case 1:
                aQueryTree.setDistinct(true);
                break;
            case 2:
                aQueryTree.setDistinct(true);
                break;

            }

        }

        // Step 2
        // We now proceed to find what is to be projected
        /*
         * This will allow the projection list to be ready
         *
         * The Projection List Handler adds the list of SQLExpression from the
         * SELECT ----------FROM Clause The SQL expressions thus generated are
         * not complete - They lack Column Table Information - which can be had
         * only after "from" clause has been processed
         *
         * The Projection List Can Contain a. SqlExpression There are 9 types of
         * SQLExpression SQLEX_SUBQUERY : Not Supported As Yet Scalar Correlated :
         * UnCorelated : NonScalar Not Allowed SQLEX_UNARY_EXPRESSION :
         * SQLEX_CASE : SQLEX_OPERATOR_EXPRESSION : SQLEX_FUNCTION :
         * SQLEX_CONSTANT : SQLEX_CONDITION : SQLEX_COLUMNLIST : SQLEX_COLUMN :
         * and all can be a part of the projection list.
         *
         * The basic functionalty of this step is to make sure that we have the
         * tables with the right columns to project.
         *
         * Exceptional Conditions :
         *
         * Incase we are dealing with a subtree and this subtree is a part of a
         * exist clause in order to provide for optimization we replace it with
         * a constant value of (1). Just to ignore all the columns selected by
         * the user
         */
        if (aQueryTree.isPartOfExistClause() == true) {
            // Destroy the projection list
            aQueryTree.getProjectionList().clear();
            // and add a SqlExpression which is a constant
            SqlExpression aSqlExpression = new SqlExpression();
            aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
            aSqlExpression.setConstantValue("1");
            aQueryTree.getProjectionList().add(aSqlExpression);

        } else {

            /**
             * This step just collects all the SQLExpressions in the query and
             * places them in the projection list. The elements are raw at this
             * point in time and will be analyzed in the next step.
             *
             */
            ProjectionListHandler aProjectionListHandler = new ProjectionListHandler(
                    commandToExecute);
            n.f2.accept(aProjectionListHandler, aQueryTree);
        }
        // Step 3
        /*
         * This will delegate the begining of the Into Clause -- Not Implemented
         * as yet
         */
        n.f3.accept(this, aQueryTree);
        // Step 4
        /*
         * This will delegate the responisiblty to the From Clause - After this
         * the Query Tree will have information regarding 1. The tables used --
         * !!!This generates RelationNodes.!!!
         *
         * Now we can have 2 Different kinds of relations nodes here
         *
         * a.Normal Table Name
         *
         * b.SubQueries Relation
         *
         * The subquery relation node can have a simple scalar or non scalar
         * query.It should not have a correlated query.
         *
         * Also queries which have a relation subquery cannot have corelated
         * subquery
         *
         * Some DBs do not support queries like select * from nation where
         * n_nationkey in (select c_nationkey from (select * from customer)
         * where c_nationkey = n_nationkey)
         */
        if (n.f4.present()) {
            FromClauseHandler aFromClauseHandler = new FromClauseHandler(
                    commandToExecute);
            n.f4.accept(aFromClauseHandler, aQueryTree);
        } else {
            RelationNode aFakeNode = aQueryTree.newRelationNode();
            aFakeNode.setNodeType(RelationNode.FAKE);
            aFakeNode.setClient(commandToExecute.getClientContext());
        }

        // We could have some conditions on the from clause which are
        // for outer , cross and inner joins. In this case we treat
        // them similar to the where condition

        // There are 2 approaches I can take here
        // a. Merge the condition with where clause
        // b. To process each condition individually
        for (QueryCondition aFromQueryCondition : aQueryTree.getFromClauseConditions()) {
            processWhereCondition(aFromQueryCondition, aQueryTree,
                    commandToExecute, true);
        }
        // Step 4.1
        /*
         * After doing the From clause we are now in a position to expand SQL
         * expressions like "*" and "tableName.*"
         */
        checkAndExpand(aQueryTree.getProjectionList(), aQueryTree.getRelationNodeList(),
                database, commandToExecute);

        /*
         * Once we have done the analysis of the from clause and the projection
         * list We have to now check if the projection list is valid and also
         * set the Attribute Column's - Table Name.
         *
         * This function will modify the QueryTree - by filling the Attribute
         * Columns with the node information. -- This function calls
         * analyzeAndCompleteColInfoAndNodeInfo() which when run in PROJECTION
         * MODE -- will lead to
         *
         * a) assiging of tablename to all the attribute columns b) Assiging of
         * Aliases etc. c) It adds the SQLExpression into the matching
         * RelationNode's d) It returns thoes columns for which it was not able
         * to find any relation node.
         *
         *
         * TODO - Handling co-related subqueries in projectionlist
         */

        /*
         * We now set the QueryTree for each SQL expression to the query tree we
         * are working with.The idea is to develop a mesh where we can access
         * information easily in later steps
         */
        for (SqlExpression aSqlExpression : aQueryTree.getProjectionList()) {
            SetBelongsToTree(aSqlExpression, aQueryTree);
        }

        List<SqlExpression> projectionOrphans = checkAndFillTableNames(
                aQueryTree.getProjectionList(), aQueryTree.getRelationNodeList(),
                new Vector(), PROJECTION, commandToExecute);

        aQueryTree.getSelectOrphans().addAll(projectionOrphans);

        /*
         * Handle possible subqueries in projection list where we could
         * not yet resolve which relations they belonged to.
         *
        for (SqlExpression aSqlExpression : aQueryTree.getProjectionList()) {
            if (aSqlExpression.getExprType() == SqlExpression.SQLEX_SUBQUERY)
            {
                SQLExpressionHandler aSQLExpressionHandler =
                        new SQLExpressionHandler(commandToExecute);
                aSQLExpressionHandler.finishSubQueryAnalysis(aSqlExpression);
            }
        }

        // Step 5
        /*
         * Now we are ready to go to where clause. This will delegate the
         * respon. to Where Clause
         *
         * The output of this is a complex Query condition, which is called the
         * Root Query Condition.
         *
         * How ever like Projection List the SQLexpressions are not complete -
         * we lack information about the column- RelationNode mapping
         */
        WhereClauseHandler aWhereClauseHandler = new WhereClauseHandler(
                commandToExecute);
        n.f5.accept(aWhereClauseHandler, aQueryTree);

        // After the analysis -- If we dont have a where Condition
        // Other wise we need to do semantic analysis of the Where Clause
        // condition

        QueryCondition aQueryCondition = aQueryTree.getWhereRootCondition();

        if (aQueryTree.getWhereRootCondition() != null) {
            ProcessWhereCondition(aQueryCondition, aQueryTree, commandToExecute);
        // Step 6
        /*
         * This will delegate the respons. to GroupByClause handler
         */
        }

        GroupByClauseHandler aGroupByClauseHandler = new GroupByClauseHandler(
                commandToExecute);
        n.f6.accept(aGroupByClauseHandler, aQueryTree);
        n.f7.accept(aGroupByClauseHandler, aQueryTree);
        aQueryTree.setGroupByList(aGroupByClauseHandler.expressionList);
        for (SqlExpression aSqlExpression : aQueryTree.getGroupByList()) {
            SetBelongsToTree(aSqlExpression, aQueryTree);
        }

        aQueryTree.setHavingList(aGroupByClauseHandler.havingList);

        /*
         * We will now have to fill up the table name etc . information
         */

        checkAndExpand(aQueryTree.getGroupByList(), aQueryTree.getRelationNodeList(),
                database, commandToExecute);

        /* See if we are dealing with ordinal group by elements, and handle. */
        aQueryTree.setGroupByList(replaceExpressionsFromList(
                aQueryTree.getProjectionList(), aQueryTree.getGroupByList()));

        /*
         * Fill table names
         */
        checkAndFillTableNames(aQueryTree.getGroupByList(),
                aQueryTree.getRelationNodeList(), aQueryTree.getProjectionList(),
                GROUPBY, commandToExecute);
        // Step 7
        // To fill information in query conditions
        if (aQueryTree.getHavingList().size() > 0) {
            QueryCondition aHavingCondition = aQueryTree.getHavingList()
                    .get(0);
            // This is to mark all the conditions and expression in the having
            // clause
            setBelongsToTree(aHavingCondition, aQueryTree);
            checkAndFillQueryConditionColumns(
                    aQueryTree.getHavingList().get(0),
                    aQueryTree.getRelationNodeList(), aQueryTree.getProjectionList(),
                    HAVING, commandToExecute);
        }
        // After all the information is collected -
        // Step 8
        /*
         * Before returning the tree we should get all the SQLExpressions and
         * fill there Final Data Types
         */
        FillAllExprDataTypes(aQueryTree, commandToExecute);
        // Step 9 -- Commenting this Step of making the databasea accept Data
        // format
        /*
         * Now that we have all the expression and there data types we will like
         * to do some sematic analysis and do some changes
         */
        // --SemanticAnalysis(aQueryTree);
        // Check if the query tree contains aggregate
        aQueryTree.setContainsAggregates(isAggregateQuery(aQueryTree));

        /*
         * In order to have a complete tree -- Just rebuild the expressions-
         * This function will take all the expressions in the projection list
         * and make them again
         */

        rebuildAllExpressions(aQueryTree);

        // We need to do some special handling for relation query tree
        manageFromSubQuery(aQueryTree);
        // This function will set the owner ship member feild for all the
        // columns in the expression - Special hanling for
        // sub correlated queries

        setOwnerShipColumns(aQueryTree);
        // The End - De Register the query tree.
        aQueryTreeTracker.deRegisterCurrentTree();
        return aQueryTree;
    }

    /**
     * Looks to see if expression contains a column not in an aggregate.
     * This is useful for detecting a missing GROUP BY.
     *
     * @param projectionExpr the expression to check
     *
     * @return String representing found column
     */
    private String findNonAggregatedColumn(SqlExpression projectionExpr) {
        String result = null;
        if (projectionExpr.getExprType() == SqlExpression.SQLEX_COLUMN) {
            result = projectionExpr.getColumn().columnName;
        } else if (projectionExpr.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION) {
            if (projectionExpr.getLeftExpr() != null) {
                result = findNonAggregatedColumn(projectionExpr.getLeftExpr());
            }
            if (result == null && projectionExpr.getRightExpr() != null) {
                result = findNonAggregatedColumn(projectionExpr.getRightExpr());
            }
        } else if (projectionExpr.getExprType() == SqlExpression.SQLEX_CASE) {
            for (SqlExpression aSqlExpression : projectionExpr.getCaseConstruct().getSQLExpressions()) {
                result = findNonAggregatedColumn(aSqlExpression);
                if (result != null) {
                    break;
                }
            }
        } else if (projectionExpr.getExprType() == SqlExpression.SQLEX_FUNCTION
                        && !projectionExpr.isAggregateExpression()) {
            for (SqlExpression aSqlExpression : projectionExpr.getFunctionParams()) {
                result = findNonAggregatedColumn(aSqlExpression);
                if (result != null) {
                    break;
                }
            }
        }
        return result;
    }

    /**
     * This allows for replacing ordinal position markers with the
     * corresponding expression in a source list.
     * This is useful for cases like GROUP BY 1,2
     *
     * @param referenceList- the list to refer to
     * @param changeList the list to change
     *
     * @return the new list, with replacements
     */
    private List<SqlExpression> replaceExpressionsFromList (
            List<SqlExpression> referenceList,
            List<SqlExpression> changeList)
            throws XDBServerException
    {
        if (changeList == null) {
            return null;
        }
        // FB10309
        if (changeList.isEmpty()) {
            boolean hasAggregates = false;
            String nonAggregatedColumn = null;
            for (SqlExpression projectionExpr : referenceList) {
                if (!hasAggregates && projectionExpr.containsAggregates()) {
                    hasAggregates = true;
                }
                if (nonAggregatedColumn == null) {
                    nonAggregatedColumn = findNonAggregatedColumn(projectionExpr);
                }
            }
            if (hasAggregates && nonAggregatedColumn != null) {
                throw new XDBServerException ("column "
                                + nonAggregatedColumn
                                + " must appear in the GROUP BY clause or be used in an aggregate function");
            }
        }
        /* See if we are dealing with ordinal group by elements, and handle. */
        List<SqlExpression> newList = new ArrayList<SqlExpression>();

        for (SqlExpression aGroupExpr : changeList)
        {
            Integer ordinal;
            if (aGroupExpr.isConstantExpr()) {
                try {
                    ordinal = new Integer(aGroupExpr.getConstantValue());
                } catch (Exception se) {
                    throw new XDBServerException ("ERROR: non-integer constant in list.");
                }
                if (ordinal < 0 || ordinal > referenceList.size()) {
                    throw new XDBServerException ("ERROR: position " + ordinal
                            + " is not in select list");
                }
                // replace expression from projection list
                newList.add(referenceList.get(ordinal - 1));
            } else {
                // preserve original
                newList.add(aGroupExpr);
            }
        }
        return newList;
    }

    /**
     * This function is incomplete and we dont do anything here-
     *
     * @param aQueryTree
     */
    public static void setOwnerShipColumns(QueryTree aQueryTree) {
    }

    public static void ProcessWhereCondition(QueryCondition aQueryCondition,
            QueryTree aQueryTree, Command commandToExecute) {
        processWhereCondition(aQueryCondition, aQueryTree, commandToExecute, false);
    }

    /**
     * This function does the processing of the where condition.
     *
     * @param aQueryCondition
     *            The QueryCondition to process , the root condition is sent in
     *            along with the Query Tree of which it is a member of
     * @param aQueryTree
     *            The query tree which we are trying to analyze
     */
    private static void processWhereCondition(QueryCondition aQueryCondition,
            QueryTree aQueryTree, Command commandToExecute, boolean isFromCondition) {
        // before we start to work will call the function setParent() which will
        // set parent for each
        aQueryCondition.setParent(null);
        aQueryCondition.rebuildString();
        setBelongsToTree(aQueryCondition, aQueryTree);

        /*
         * Once we have all the conditions that we want -- Time to find all the
         * columns involved with each of the RelationNode -
         *
         */

        /*
         * We will now only send the root condition -- This function finds the
         * Mapping between RelationNode and Column.
         */

        CAnalyzeAndCompleteColInfoAndNodeInfo colinfo = checkAndFillQueryConditionColumns(
                aQueryCondition, aQueryTree.getRelationNodeList(),
                aQueryTree.getProjectionList(), CONDITION, commandToExecute);
        aQueryTree.setWhereOrphans(colinfo.orphanExpressionVector);
        // This will provide us with a list of elements which were not mapped to
        // the same query, but were mapped to
        // a parent query
        // In order to handle the co-related case we need to make a new query
        // node which will be a place
        // holder for the original node - How ever the columns which have been
        // mapped to parent nodes will have
        // the original tables as there relationNode and also there alias and
        // tableName will be the same

        // The place holder node will now contain a condition List - which will
        // be not be populated
        // and we will populate the Sub Trees conditionColumnList only.As it is
        // sure that this will be
        // treated as a join.

        // The joinList of the node will be populated with the nodes to which it
        // is going to get joined

        // Now an important point is that we are not going to have the cross
        // refernce with the column but will
        // have the cross refernce with the psuedo node.

        // The place holder node will now act as the real node and participate
        // in all the join and non-join operations
        // respectively. It is important to realize
        if (colinfo.orphanExpressionVector.size() > 0) {
            // Create a place holder for the node - Since we already know which
            // node the column really belongs we should try to create a pseudo
            // column for each unique node.
            RelationNode aPlaceHolderNode = aQueryTree.newRelationNode();
            aPlaceHolderNode.setTableName("PsuedoTable");
            aPlaceHolderNode.setAlias("PsuedoTable");
            aPlaceHolderNode.setNodeType(RelationNode.SUBQUERY_CORRELATED_PH);

            aQueryTree.setPseudoRelationNode(aPlaceHolderNode);
            // Add all the columns from the orphan list to the condition column
            // list of the newly created place holder nodes.
            for (SqlExpression aColumnExpression : colinfo.orphanExpressionVector) {
                aPlaceHolderNode.getCondColumnList().add(aColumnExpression.getColumn());

                // Also I need to add the columns to the projection list which
                // are related to the orphan column
                SqlExpression joinsWith = null;
                SqlExpression thisExpr = null;
                QueryCondition aParentCondition = aColumnExpression.getColumn()
                        .getParentQueryCondition();

                // Corrected a bug here with comparing.
                // it won't find t1.col1 + 95, since it != t1.col.
                // I corrected, but we will still have a problem with
                // expressions like "t1.col1 - t2.col2 = 0"
                if (aParentCondition.getExpr().getLeftExpr()
                        .containsColumn(aColumnExpression.getColumn())) {
                    joinsWith = aParentCondition.getExpr().getRightExpr();
                    thisExpr = aParentCondition.getExpr().getLeftExpr();
                } else {
                    joinsWith = aParentCondition.getExpr().getLeftExpr();
                    thisExpr = aParentCondition.getExpr().getRightExpr();
                }
                // Now we will start to think about what we should do to
                // handle different expression types
                if (joinsWith.getExprType() == SqlExpression.SQLEX_COLUMN) {
                    joinsWith.getColumn().relationNode.getProjectionList().add(
                            joinsWith);
                    aQueryTree.getHiddenProjectionList().add(joinsWith);
                } else if (joinsWith.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION
                        || joinsWith.getExprType() == SqlExpression.SQLEX_FUNCTION
                        || joinsWith.getExprType() == SqlExpression.SQLEX_CASE
                        || joinsWith.getExprType() == SqlExpression.SQLEX_SUBQUERY
                        || joinsWith.getExprType() == SqlExpression.SQLEX_CONDITION) {
                    // old comments in the code led me to believe that this
                    // should be done as well. Still, this could cause problems.
                    Iterator<SqlExpression> itColumns = SqlExpression.getNodes(
                            joinsWith, SqlExpression.SQLEX_COLUMN).iterator();

                    while (itColumns.hasNext()) {
                        SqlExpression aSqlEx = itColumns.next();
                        aSqlEx.getColumn().relationNode.getProjectionList().add(aSqlEx);
                        aQueryTree.getHiddenProjectionList().add(aSqlEx);
                    }
                } else if (joinsWith.getExprType() == SqlExpression.SQLEX_CONSTANT) {
                    // Dont do anything -- except mark the query condition

                } else if (joinsWith.getExprType() == SqlExpression.SQLEX_COLUMNLIST) {
                    // Get the columns in the column list and add them
                    // to the relation node projection list each one of them
                    // belongs to
                    // mds - I don't think this should apply
                } else if (joinsWith.getExprType() == SqlExpression.SQLEX_LIST) {
                    // Dont do any thing
                }

                // We also need to handle cases like t1.col1 - t2.col2 = 0,
                // where they are on the same side of the epxression!

                if (thisExpr.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION
                        || thisExpr.getExprType() == SqlExpression.SQLEX_FUNCTION
                        || thisExpr.getExprType() == SqlExpression.SQLEX_CASE
                        || thisExpr.getExprType() == SqlExpression.SQLEX_SUBQUERY
                        || thisExpr.getExprType() == SqlExpression.SQLEX_CONDITION) {
                    // Check to see if another table's columns appear on same
                    // side of expression
                    Iterator<SqlExpression> itColumns = SqlExpression.getNodes(
                            thisExpr, SqlExpression.SQLEX_COLUMN).iterator();

                    while (itColumns.hasNext()) {
                        SqlExpression aSqlEx = itColumns.next();
                        if (!aColumnExpression.getColumn().getTableName()
                                .equalsIgnoreCase(aSqlEx.getColumn().getTableName())) {
                            aSqlEx.getColumn().relationNode.getProjectionList()
                                    .add(aSqlEx);
                            aQueryTree.getHiddenProjectionList().add(aSqlEx);
                        }
                    }
                }

            }

        }

        // Now find the conditions which have these outside columns -
        // as we are going to progress we are going to cris cross the nodes
        // for each condition -- but in this case we will not insert the orignal
        // node but will instead insert the place holder node
        /**
         * It is good to do a rebuild of conditions
         *
         */
        // QueryCondition toRebuildQueryCondition = aQueryCondition;
        // toRebuildQueryCondition.rebuildCondString();
        /*
         * Once the columns inside the Query Tree are filled -- we now have to
         * find the top level query conditions.
         *
         * Top level condition s defined as and AND condition node -- which does
         * not have a parent OR condition. Or if there is just one condition
         * then that will be the top level condition
         */
        List<QueryCondition> topLevelQueryConditions;
        if (isFromCondition && aQueryTree.isOuterJoin()) {
            topLevelQueryConditions = Collections
                    .singletonList(aQueryCondition);
        } else {
            topLevelQueryConditions = aQueryCondition.getAndedConditions();
        }
        /*
         * We also need to set the top level condition RelationNodelist
         */
        for (QueryCondition qc : topLevelQueryConditions) {
            /**
             * Each of the Query Condition will be analyzed
             *
             * In prespective of SubQuery Node
             *
             * If the expression does not belong to the same query tree then we
             * must allow for join.
             *
             */
            qc.setRelationNodeList(getRelationNodes(qc));
            QCHelperColumnList aQcHelperColumnList = getQueryColumnList(qc);
            // A hack - We should make changes to getQueryColumnList which
            // actually returns
            // the list of columns in a particular query condition.Here we will
            // check out
            // thoes columns which donot belong to this tree. There fore they
            // may belong to
            // a subQuery Tree.
            Enumeration columns = aQcHelperColumnList.getColumnExprListThisTree()
                    .elements();
            while (columns.hasMoreElements()) {
                AttributeColumn aColumn = (AttributeColumn) columns
                        .nextElement();

                if (aColumn.getMyParentTree() != qc.getParentQueryTree()) {
                    // knocking off the columns which dont belong to
                    // this query tree and are borrowed from a parent
                    // query tree

                    // OR

                    // This Column could be coming from a
                    // scalar subquery.

                    continue;
                }

                qc.getColumnList().add(aColumn);
            }
        }

        /*
         *
         * The Next step will be to analyze the top level query condition Will
         * be to segregate into join and non join conditions
         *
         */

        SegregateJoinHelper aSegregateJoinhelper = segregateIntoSimpleAndJoinConditions(topLevelQueryConditions);

        /*
         * Now that we have Join and Non- Join Query Conditions seperated -- we
         * will move forward -- Add all the Join Conditions to the condition
         * list in the Query Tree. --We will also have to get all the top level
         * conditions
         *  -- And add all the njoin conditions to the nodes.
         *
         */
        if (aSegregateJoinhelper.joinQConditionVector != null) {
            aQueryTree.getConditionList()
                    .addAll(aSegregateJoinhelper.joinQConditionVector);
        }

        if (aSegregateJoinhelper.joinQConditionVector != null
                && aSegregateJoinhelper.joinQConditionVector.size() > 0) {
            populateJoinExpressionCrossReference(aSegregateJoinhelper.joinQConditionVector);
            /*
             * The join expression will now get us the columns -- these columns
             * will already have the nodes set.
             *
             * 1. get the SqlExpression 2. Get all the columns involved in the
             * SqlExpression 3. Segregate them by nodes 4. Now get the
             * RelationNodes 5. in a for loop ï¿½ skip self.
             *  -- This function will also set the join flag in the expression.
             */
            /*
             * Once we have populated the RelationNode variable - joinList - we
             * will process the simple conditions.
             */
        }
        List<QueryCondition> simpleCondVector = null;

        if (aSegregateJoinhelper.nonjoinQConditionVector != null
                && aSegregateJoinhelper.nonjoinQConditionVector.size() > 0) {
            simpleCondVector = aSegregateJoinhelper.nonjoinQConditionVector;

            /*
             * Get the SQL expressions from the -- Simple Query Condition
             *
             */

            List<QueryCondition> simpleExprForQueryTree = getSimpleExprForQueryTree(simpleCondVector);
            aQueryTree.getConditionList().addAll(simpleExprForQueryTree);

            /*
             * We will now have to add all the other expressions to the Nodes --
             * the above function will have removed the QueryTree expressions
             * from the simpleCondVector- The rest of them have to be added to
             * the Nodes.
             */

            /*
             * Remove the conditions from the list
             */
            simpleCondVector.removeAll(simpleExprForQueryTree);

            addToNodesSimpleQueryCondition(simpleCondVector, aQueryTree);
        }

        /*
         *
         * The condition list contains all the QueryConditions. But we need to
         * divide the Query Conditions between the Query Tree and the Query
         * Nodes.
         *
         * The following is a basic algroithm. After Filling the Column
         * Attributes with the node information. We Get all the QueryConditions
         * which are Primary Query Conditions
         *
         * A Primary Query Condition is one which is a AND and has No OR
         * condition.
         *
         * After getting this list we Perform a Join check. A Join check is
         * nothing but a check where we see if the condition "QueryCondition"
         * has a Condition which involves a
         *
         * SQLexpression with columns from 2 different tables or A Condition
         * which requires reference of 2 different tables. If we term this as a
         * join it will be added to the query tree other wise it will be added
         * to the query node condition list.
         */
    }

    /**
     * This function sets a cross reference to between all the query conditions
     * and the tree to which they belong
     *
     * @param aQueryCondition
     * @param aQueryTree
     */
    public static void setBelongsToTree(QueryCondition aQueryCondition,
            QueryTree aQueryTree) {
        Vector<QueryCondition> allQueryCondtions = QueryCondition.getNodes(
                aQueryCondition, QueryCondition.QC_ALL);

        for (QueryCondition qc : allQueryCondtions) {
            qc.setParentQueryTree(aQueryTree);
            if (qc.getCondType() == QueryCondition.QC_SQLEXPR) {
                SetBelongsToTree(qc.getExpr(), aQueryTree);
            }
        }

        // determine and set the owner of columns which are
        // in the condition. we might like to have this as a universal
        // property where every SqlExpression knows to which tree it
        // belongs and so does each column
        for (QueryCondition exprCondition : QueryCondition.getNodes(
                aQueryCondition, QueryCondition.QC_SQLEXPR)) {
            for (SqlExpression expr : SqlExpression.getNodes(
                    exprCondition.getExpr(), SqlExpression.SQLEX_COLUMN)) {
                expr.getColumn().setMyParentTree(aQueryTree);
                expr.getColumn().setParentQueryCondition(exprCondition);
            }
        }
    }

    /**
     * This is for SQLExpression- It sets the belongs to tree in SqlExpression
     *
     * @param aSqlExpression
     * @param aTreeToSet
     */
    public static void SetBelongsToTree(SqlExpression aSqlExpression,
            QueryTree aTreeToSet) {
        for (SqlExpression aSqlExpr : SqlExpression.getNodes(aSqlExpression,
                SqlExpression.SQLEX_ALL)) {
            aSqlExpr.setBelongsToTree(aTreeToSet);
        }

    }

    /*
     * This function will get all the expression in the expression string and
     * call the rebuild expression string on it -- similary it will call the
     * rebuild on condition strings Rebuilds the query tree using the
     * information it has @param aQueryTree
     */

    void rebuildAllExpressions(QueryTree aQueryTree) {
        for (SqlExpression sqlExpr : aQueryTree.getProjectionList()) {
            sqlExpr.rebuildExpression();
        }

        // Group by
        for (SqlExpression sqlExpr : aQueryTree.getGroupByList()) {
            sqlExpr.rebuildExpression();
        }

        // Having
        if (aQueryTree.getHavingList().size() > 0) {
            QueryCondition havingConditon = aQueryTree.getHavingList()
                    .get(0);
            Vector havingExprConditions = QueryCondition.getNodes(
                    havingConditon, QueryCondition.QC_SQLEXPR);
            Enumeration havingExprEnumerationEnumeration = havingExprConditions
                    .elements();
            while (havingExprEnumerationEnumeration.hasMoreElements()) {
                QueryCondition qc = (QueryCondition) havingExprEnumerationEnumeration
                        .nextElement();
                SqlExpression sqlExpr = qc.getExpr();
                sqlExpr.rebuildExpression();
            }
        }
    }

    /*
     * This function will determine if there is any function in the query which
     * is an aggregate function @param aQueryTree @return true if it is a
     * aggregate query
     */
    public static boolean isAggregateQuery(QueryTree aQueryTree) {
        // Get all the sqlexpressions from the projection list which contain
        // functions

        if (aQueryTree.getGroupByList().size() > 0) {
            return true;
        }

        for (SqlExpression aSqlExpression : aQueryTree.getProjectionList()) {
            for (SqlExpression aExpr : SqlExpression.getNodes(aSqlExpression,
                    SqlExpression.SQLEX_FUNCTION)) {
                if (aExpr.containsAggregates()) {
                    return true;

                }

            }
        }
        return false;
    }

    /**
     * This function will fill all the Data types for all Expressions
     *
     * @param aQueryTree
     *            all the SQLExpressions are removed and
     * @throws ColumnNotFoundException
     */
    public static void FillAllExprDataTypes(QueryTree aQueryTree,
            Command commandToExecute) {
        SysDatabase database = commandToExecute.getClientContext().getSysDatabase();

        /*
         * First we handle the projection list
         */
        // The projection list will contain all the Sql Expression Roots
        // which are to be projected
        SysTable sysTable = null;
        SysColumn col = null;
        for (SqlExpression aSqlExpression : aQueryTree.getProjectionList()) {
            aSqlExpression.setIsProjection(true);
            aSqlExpression.setExprDataType(SqlExpression
                    .setExpressionResultType(aSqlExpression, commandToExecute));
            if(aSqlExpression.getColumn() != null) {
                if ((sysTable = database.checkForSysTable(aSqlExpression.getColumn().getTableName())) != null
                        && (col = sysTable.getSysColumn(aSqlExpression.getColumn().getColumnName())) != null) {
                    if(col.isWithTimeZone) {
                        aSqlExpression.getExprDataType().isWithTimeZone = true;
                    }
                }
            }
        }
        /**
         * Then Group by list
         */
        for (SqlExpression aSqlExpression : aQueryTree.getGroupByList()) {
            aSqlExpression.setExprDataType(SqlExpression
                    .setExpressionResultType(aSqlExpression, commandToExecute));
        }
        /**
         * Then we come to conditions
         */
        for (QueryCondition havingCondition : aQueryTree.getHavingList()) {
            for (QueryCondition exprCondition  : QueryCondition.getNodes(
                    havingCondition, QueryCondition.QC_SQLEXPR)) {
                SqlExpression aSqlExpression = exprCondition.getExpr();
                aSqlExpression.setExprDataType(SqlExpression
                        .setExpressionResultType(aSqlExpression, commandToExecute));
            }
        }
        /**
         * We also need to take care of condtions in the from clause due to
         * inner and outer clauses
         */
        for (QueryCondition fromCondition : aQueryTree.getFromClauseConditions()) {
            for (QueryCondition aQueryCondition : QueryCondition.getNodes(
                    fromCondition, QueryCondition.QC_SQLEXPR)) {
                SqlExpression aSqlExpression = aQueryCondition.getExpr();
                aSqlExpression.setExprDataType(SqlExpression.setExpressionResultType(
                        aSqlExpression, commandToExecute));
            }
        }

        /**
         * The Where Clause Condition
         */
        if (aQueryTree.getWhereRootCondition() != null) {
            for (QueryCondition qCond : QueryCondition.getNodes(aQueryTree
                    .getWhereRootCondition(), QueryCondition.QC_SQLEXPR)) {
                SqlExpression aSqlExpression = qCond.getExpr();
                // TODO: review the querycondition changes;
                // not sure if this makes a whole lot of sense
                // it returns conditions, but is checking for columns here
                if (aSqlExpression.getColumn() != null) {
                    // Set the column type from project list
                    aSqlExpression.getColumn().columnType = aSqlExpression.getColumn()
                            .getColumnType(database);
                    //
                    if (aSqlExpression.getColumn().columnType.type == ExpressionType.DATE_TYPE
                            || aSqlExpression.getColumn().columnType.type == ExpressionType.TIME_TYPE
                            || aSqlExpression.getColumn().columnType.type == ExpressionType.TIMESTAMP_TYPE) {
                        aSqlExpression.getColumn()
                                .setParentQueryCondition(normalizeExpr(
                                        aSqlExpression.getColumn()
                                                .getParentQueryCondition(),
                                        aSqlExpression.getColumn().columnType.type));
                    }

                }
                aSqlExpression.setExprDataType(SqlExpression
                        .setExpressionResultType(aSqlExpression, commandToExecute));
            }
        }
    }

    /**
     * PreConditions For this Function
     *
     * 1. NOT NULL- aQuery Tree, projection List, aQueryTree.RelationNodeList
     * 1.1 Query Node List Only contains Tables.
     *
     * 2. Cases Considered : No Attribute Columns in the Query
     *
     * 3. Exceptions Thrown- NullPointerException -- When aQueryTree ,
     * ProjectionList is NULL
     */
    /**
     * This function is not in use
     *
     * @param aQueryTree
     * @throws NullPointerException
     * @throws IllegalArgumentException
     * @throws ColumnNotFoundException
     */
    protected void analyzeFromAndProjectionList(QueryTree aQueryTree) {
            // Get the Projection List -- On each SqlExpression call getNodes(--
            // Type Attribute--)
            List<RelationNode> vTableNodes = aQueryTree.getRelationNodeList();

            if (aQueryTree.getProjectionList() == null || vTableNodes == null) {
                throw new XDBServerException(
                        ErrorMessageRepository.QUERY_PROJ_LIST_EMPTY, 0,
                        ErrorMessageRepository.QUERY_PROJ_LIST_EMPTY_CODE);
            }

            // For each element in the list
            for (SqlExpression sqlExpr : aQueryTree.getProjectionList()) {
                // Get the Columns in the expression
                Vector vColsInExpr = SqlExpression.getNodes(sqlExpr,
                        SqlExpression.SQLEX_COLUMN);
                // Check to see if we have any Columns in this Sql Expression
                if (vColsInExpr == null || vColsInExpr.size() <= 0) {
                    continue;
                } else {
                    // If we have some columns in the expression -
                    Enumeration enAttribCols = vColsInExpr.elements();

                    while (enAttribCols.hasMoreElements()) {
                        // We get the expressions of the type columns - from the
                        // call to getNodes()
                        SqlExpression sqlExprCols = (SqlExpression) enAttribCols
                                .nextElement();
                        // Get the Atrribute Column and check if it is a valid
                        // column
                        AttributeColumn aAttribColumnInProjList = sqlExprCols.getColumn();
                        //
                        if (aAttribColumnInProjList.columnName == null) {
                            throw new XDBServerException(
                                    ErrorMessageRepository.NULL_COLUMN_NAME,
                                    0,
                                    ErrorMessageRepository.NULL_COLUMN_NAME_CODE);
                        }
                        // get the table node to which it belongs
                        RelationNode qTableNode = getTableRelationNode(
                                sqlExprCols, vTableNodes, database);

                        if (qTableNode != null) {
                            qTableNode.getProjectionList().add(sqlExprCols);
                            // Once we Have the qTableNode - We can get the name
                            // and set the Table Name of the
                            // AttributeColumn.
                            aAttribColumnInProjList.setTableName(qTableNode
                                    .getTableName());
                        }
                    }
                }
            }
        }

    /*
     * This Takes care of F1 and F0 - Tokens
     */
    @Override
    public Object visit(NodeToken n, Object argu) {
        QueryTree aQueryTree = (QueryTree) argu;
        switch (n.kind) {
        // All implies all tuples have to be selected
        case CSQLParserConstants.ALL_:
            aQueryTree.setDistinct(false);
            break;
        // Unique is a synonym of Distinct
        case CSQLParserConstants.UNIQUE_:
            aQueryTree.setDistinct(true);
            break;
        // The Distinct clause is to allow the resulttant queries to have only -
        // unique tuples
        case CSQLParserConstants.DISTINCT_:
            aQueryTree.setDistinct(true);
            break;
        // We will get this when we have a few SQL expessions from select to
        // from clause
        default: // Do nothing
            break;
        }
        return null;
    }

    /**
     * This function will take in the SQL expressions collected from the SQL
     * Experssions and find out all the expressions which have columns
     * associated with it.
     *  -- The Columns in these expressions are not complete as they still need
     * to know which tables they belong.
     *
     * We have 3 Fields -- 1.Table 2.TableAlias 3.Column
     *
     * The input for making attributes are as follows - <identifier> . <
     * identifer> -- This is broken in to TableAlias.Column name or
     * <identifierName> - This is identified as ColumnName
     *
     * Leaving the alias and Table name blank. We will have to search the
     * metadata information for finding out the appropriate table name and the
     * alias( which will be same as the table name)
     *
     * @param checkList
     *            Contains the expressions which have to be checked and filled
     * @param nodeList
     *            Contains the relation nodes
     * @param projList
     *            Contains the projection list from the tree
     * @param mode
     *            PROJECION/GROUP BY
     * @return Vector of orphan columns - or columns which could not be found in
     *         the same tree
     * @throws IllegalArgumentException
     */
    public static List<SqlExpression> checkAndFillTableNames(List<SqlExpression> checkList,
            List<RelationNode> nodeList, List<SqlExpression> projList, int mode,
            Command commandToExecute) {
        List<SqlExpression> orphanColumns = new ArrayList<SqlExpression>();
        Iterator<SqlExpression> itCheckList = checkList.iterator();
        /*
         * While there are expressions in the Projection List
         */
        while (itCheckList.hasNext()) {
            CAnalyzeAndCompleteColInfoAndNodeInfo analysisInfo = analyzeAndCompleteColInfoAndNodeInfo(
                    itCheckList, nodeList, projList, mode, commandToExecute);
            orphanColumns.addAll(analysisInfo.orphanExpressionVector);
        }
        return orphanColumns;
    }

    /**
     * This function run in two modes a) -- When analyzing Projections -- While
     * this is a list of SQLExpression b) -- When analyzing Condions.-- c) The
     * returned vector is a list of nodeList that are involved in the expression
     * List -
     *
     * @param enSQLExprList
     * @param nodeList :
     *            contains the relation nodes from where various sql expression
     *            columns can be derived
     * @param projectionList :
     *            contains expressions in the projection list
     * @param mode -
     *            PROJECTION / CONDITION
     * @return CAnalyzeAndCompleteColInfoAndNodeInfo object which contains
     *         useful information when run in CONDITION MODE.
     * @throws ColumnNotFoundException
     */
    public static CAnalyzeAndCompleteColInfoAndNodeInfo analyzeAndCompleteColInfoAndNodeInfo(
            Iterator<SqlExpression> itSQLExprList, List<RelationNode> nodeList, List<SqlExpression> projectionList,
            int mode, Command commandToExecute) {
        SysDatabase database = commandToExecute.getClientContext().getSysDatabase();
        commandToExecute.getaQueryTreeTracker();
        CAnalyzeAndCompleteColInfoAndNodeInfo columnInformation = new CAnalyzeAndCompleteColInfoAndNodeInfo();
        /* This will contain a list of Query Nodes involved with this expression */

        /* For each expression in the expression List */
        while (itSQLExprList.hasNext()) {
            SqlExpression sql = itSQLExprList.next();
            /*
             * - Get the column nodes from the Sql Expression. This will return
             * us the list of column nodes which belong to this Sql Expression
             */
            Vector vColNodes = SqlExpression.getNodes(sql, SqlExpression.SQLEX_COLUMN);

            /* After getting all the Columns involved in the expression */
            Enumeration enColNodes = vColNodes.elements();

            /*
             * For each of the Columns in the expression we try to find the
             * table to which it belongs.
             */
            while (enColNodes.hasMoreElements()) {

                /* Get the SQL Expression -- */
                SqlExpression sqlcols = (SqlExpression) enColNodes
                        .nextElement();

                if (sqlcols.getColumn() == null) {
                    throw new XDBServerException(
                            ErrorMessageRepository.NULL_COLUMN_NAME, 0,
                            ErrorMessageRepository.NULL_COLUMN_NAME_CODE);
                }

                /* Extract the Attribute Column from it */
                AttributeColumn colToCheck = sqlcols.getColumn();

                if (colToCheck.columnName == null
                        || colToCheck.columnName.equals("")) {
                    throw new XDBServerException(
                            ErrorMessageRepository.NULL_COLUMN_NAME, 0,
                            ErrorMessageRepository.NULL_COLUMN_NAME_CODE);
                }

                try {

                    // Before going to the database we must see that if the mode
                    // is
                    // Having, Order by or Group by and if the column could be found
                    // in the
                    // alias of the projectionlist

                    // In case of having we have to make
                    // sure that the sqlexpression
                    // in the having clause if present in the select clause is
                    // aggregated or is present in the
                    // group by clause
                    if (mode == ORDERBY || mode == GROUPBY || mode == HAVING) {
                        // First check if the column is mapped to a
                        // SqlExpression
                        Vector dupList = new Vector();
                        for (SqlExpression aSqlExpression : projectionList) {
                            AttributeColumn aAttrCol = aSqlExpression.getColumn();
                            if (aAttrCol == colToCheck) {
                                // we already set up equivalency
                                dupList.clear();
                                break;
                            }
                            if (aSqlExpression.getAlias()
                                    .equals(colToCheck.columnName) ||
                                aAttrCol != null &&
                                  aAttrCol.columnName
                                    .equalsIgnoreCase(colToCheck.columnName) &&
                                  aAttrCol.getTableName()
                                    .equalsIgnoreCase(colToCheck.getTableName())) {
                                dupList.add(aSqlExpression);
                            }
                        }
                        if (dupList.size() > 1) {

                            // We can have duplicate names here it really does
                            // not matter-
                            // the condition is that both the duplicates
                            // should
                            // have the same underling table

                            /**
                             * It appears that this code checks each possible
                             * duplicate to see it the table matches the previous
                             * candidate dupe, and throws an Exception if it
                             * thinks it found one.
                             */

                            Enumeration dupMatches = dupList.elements();
                            RelationNode toMatchWith = null;
                            Boolean aliasMatchFound = false;

                            while (dupMatches.hasMoreElements()) {
                                SqlExpression aSqlExpression = (SqlExpression) dupMatches
                                        .nextElement();

                                if (aSqlExpression.getExprType() == SqlExpression.SQLEX_COLUMN) {

                                    // We will only consider this case
                                    if (toMatchWith == null) {
                                        toMatchWith = aSqlExpression.getColumn().relationNode;
                                        if (aSqlExpression.getAlias().equalsIgnoreCase(colToCheck.columnAlias)) {
                                            aliasMatchFound = true;
                                        }
                                    } else {

                                        if (!toMatchWith.getTableAlis().equals(
                                                aSqlExpression.getColumn()
                                                .relationNode.getTableAlis())) {
                                            // it appears to be unique, continue
                                            continue;
                                        } else if (!toMatchWith
                                                .getTableName()
                                                .equals(
                                                        aSqlExpression.getColumn().relationNode
                                                                .getTableName())) {
                                            // We know that we are going to
                                            // order on the same column though
                                            // it
                                            // has multiple representations in
                                            // the projection list
                                            // So we check if there is a next
                                            // one to take care of
                                            continue;

                                        } else {
                                            if (!aliasMatchFound &&
                                                aSqlExpression.getAlias().equalsIgnoreCase(colToCheck.columnAlias)) {
                                                // We know that earlier we did not found alias, but the actual column
                                                // Now, we found the alias
                                                toMatchWith = aSqlExpression.getColumn().relationNode;
                                                aliasMatchFound = true;
                                                continue;
                                            } else if (aliasMatchFound &&
                                                    !aSqlExpression.getAlias().equalsIgnoreCase(colToCheck.columnAlias)) {
                                                // We already found the alias first, now we found column with
                                                // same name. Hence, ignore it.
                                                continue;
                                            }

                                            throw new XDBServerException(
                                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF
                                                            + "( "
                                                            + colToCheck.columnName
                                                            + " )",
                                                    0,
                                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF_CODE);
                                        }
                                    }
                                }
                            }
                            if(toMatchWith == null) {
                                SqlExpression prevMatchedExpression = null;
                                dupMatches = dupList.elements();

                                while (dupMatches.hasMoreElements()) {
                                    SqlExpression aSqlExpression = (SqlExpression) dupMatches
                                            .nextElement();

                                    if (aSqlExpression.getExprType() == SqlExpression.SQLEX_FUNCTION) {

                                        // We will only consider this case
                                        if (prevMatchedExpression == null && aSqlExpression != sqlcols) {
                                            prevMatchedExpression = aSqlExpression;
                                            if (!aSqlExpression.isAlliasSameAsColumnNameInFunction(colToCheck)) {
                                                colToCheck.columnType = aSqlExpression.getExprDataType();
                                                colToCheck.columnGenre |= AttributeColumn.MAPPED;
                                                // Copy everything from expression 1 to Expression 2
                                                SqlExpression.copy(aSqlExpression, sqlcols);
                                                //
                                                sqlcols.setMapped(SqlExpression.INTERNALMAPPING);
                                                sqlcols.setMappedExpression(aSqlExpression);
                                            }
                                        }
                                        else if(prevMatchedExpression.isSameFunction(aSqlExpression)) {
                                            continue;
                                        }
                                        else {
                                            throw new XDBServerException(
                                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF
                                                            + "( "
                                                            + colToCheck.columnName
                                                            + " )",
                                                    0,
                                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF_CODE);
                                        }
                                    }
                                }
                            }
                        } else if (dupList.size() == 1) {
                            SqlExpression aSqlExpression = (SqlExpression) dupList
                                    .elementAt(0);
                            // Related to regression #8661 - take alias for order by
                            if ((!aSqlExpression
                                    .isAlliasSameAsColumnNameInFunction(colToCheck)
                                        || mode == ORDERBY)
                                    && aSqlExpression != sqlcols) {
                                colToCheck.columnType = aSqlExpression.getExprDataType();
                                colToCheck.columnGenre |= AttributeColumn.MAPPED;
                                // Copy everything from expression 1 to Expression 2
                                SqlExpression.copy(aSqlExpression, sqlcols);
                                //
                                sqlcols.setMapped(SqlExpression.INTERNALMAPPING);
                                sqlcols.setMappedExpression(aSqlExpression);
                            }
                        }
                    }
                    // If it is mapped
                    if ((colToCheck.columnGenre & AttributeColumn.MAPPED) == 0) {
                        // Get The Node to which the column belongs - This
                        // function will internally
                        // make a call into the DB and therefore can be
                        // expensive
                        RelationNode aTableRelationNode = getTableRelationNode(
                                sqlcols, nodeList, database);
                        if (aTableRelationNode != null) {
                            if (columnInformation.nodeListInExpression
                                    .contains(aTableRelationNode) == false) {
                                columnInformation.nodeListInExpression
                                        .add(aTableRelationNode);
                            }
                            /*
                             * In case we are working in the projection mode we
                             * will also add the SQL expression to the Query
                             * Node for which we found the match - the sql is
                             * the sqlexpression from which we extracted the
                             * columns
                             */
                            if (PROJECTION == mode || ORDERBY == mode) {
                                if (aTableRelationNode.getProjectionList()
                                        .contains(sql) == false) {
                                    // if it is a column and is mapped, ignore
                                    if (sql.getExprType() == SqlExpression.SQLEX_COLUMN
                                            && (sql.getColumn().columnGenre & AttributeColumn.MAPPED) == AttributeColumn.MAPPED) {
                                        // ignore
                                    } else {
                                        sqlcols.rebuildExpression();
                                        // Only Add unique columns
                                        // Note that equivalencies are assigned during planning,
                                        // but we do some here already to avoid extra projections
                                        // being added
                                        boolean found = false;
                                        
                                        for (SqlExpression colExpr : aTableRelationNode.getProjectionList())
                                        {
                                            colExpr.rebuildExpression();
                                            if (sqlcols.getExprString().equalsIgnoreCase(colExpr.getExprString())) {
                                                found = true;
                                                break;
                                            }
                                        }
                                        if (!found) {
                                            aTableRelationNode.getProjectionList()
                                                    .add(sqlcols);                                            
                                        }
                                        /*
                                        if (aTableRelationNode.getProjectionList()
                                                .contains(sqlcols) == false) {
                                            aTableRelationNode.getProjectionList()
                                                    .add(sqlcols);
                                        }
                                        */
                                    }
                                }
                            }
                            /*
                             * In condition mode we have to add the column to
                             * the condition column list
                             */
                            else if (CONDITION == mode) {
                                if (aTableRelationNode.getCondColumnList()
                                        .contains(colToCheck) == false) {
                                    // We cannot add to the node list --
                                    // directly - Instead
                                    // we will need to check if the
                                    if (aTableRelationNode.getCondColumnList()
                                            .contains(colToCheck) == false) {
                                        aTableRelationNode.getCondColumnList()
                                                .add(colToCheck);
                                    }
                                }
                            }
                            /*
                             * TODO
                             */
                            else if (HAVING == mode || ORDERBY == mode
                                    || GROUPBY == mode) {

                                // Do nothing
                            }

                            colToCheck.setTableName(aTableRelationNode
                                    .getTableName());

                            // In case we have a TableName alias as null -
                            // implying that the
                            // user had entered ID and NOT ID.ID we assign the
                            // table name alias as
                            // table itself.-- Note - The coulmnName Alias Will
                            // be taken care in the
                            // AliasSpec() call.--

                            if (colToCheck.getTableAlias() == null
                                    || colToCheck.getTableAlias().equals("")) {
                                if (aTableRelationNode.getAlias() == null
                                        || aTableRelationNode.getAlias().equals("")) {
                                    colToCheck.setTableAlias(aTableRelationNode
                                            .getTableName());
                                } else {
                                    colToCheck
                                            .setTableAlias(aTableRelationNode.getAlias());
                                }
                            }

                            // Set the Query Node to which the column belongs
                            colToCheck.relationNode = aTableRelationNode;
                            sqlcols.rebuildExpression();
                        } else if (ORDERBY == mode || mode == PROJECTION
                                || mode == CONDITION || mode == GROUPBY) {
                            throw new XDBServerException(
                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF
                                            + "( " + colToCheck.columnName
                                            + " )",
                                    0,
                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF_CODE);

                        } else {
                            throw new XDBServerException(
                                    "The Column could not be found "
                                            + colToCheck.columnName);
                        }

                    }
                } catch (ColumnNotFoundException colNotFoundException) {
                    SqlExpression aColumnExpression = colNotFoundException
                            .getColumnExpression();
                    // This will naturally set the column info to - ORPHAN as
                    // the
                    // relation node of the column has not been filled.
                    aColumnExpression.getColumn().setColumnGenere();
                    columnInformation.orphanExpressionVector.add(aColumnExpression);

                    QueryTree parentTree = aColumnExpression.getBelongsToTree();
                    if (parentTree != null)
                    {
                        parentTree = parentTree.getParentQueryTree();
                        aColumnExpression.setBelongsToTree(parentTree);
                    }
                    if (parentTree != null) {
                        List<SqlExpression> vColVector = new ArrayList<SqlExpression>();
                        vColVector.add(
                                colNotFoundException.getColumnExpression());
                        analyzeAndCompleteColInfoAndNodeInfo(
                                vColVector.iterator(),
                                parentTree.getRelationNodeList(),
                                parentTree.getProjectionList(),
                                mode, commandToExecute);

                        // Incase we find the columns relationnode we mark the
                        // column as - Orphan - This
                        // will later help us in deciding which node to use when
                        // we do the criss cross
                        SqlExpression aOrphanColumnExpression = vColVector.get(0);
                        SqlExpression.setExpressionResultType(
                                aOrphanColumnExpression, commandToExecute);
                    } else {
                        throw colNotFoundException;
                    }
                } catch (Exception ex) {
                    throw new XDBServerException(ex.getMessage(), ex);
                }
            }
        }
        return columnInformation;
    }

    /*
     * This function is used to find the column table relationship i.e. Given a
     * list of tables and one column - determine to which table does the column
     * belong to
     *
     * @param Columna Name - colName @ param listofRelationNodes - After
     * analyzing the From Clause The function does not have the functionalty for
     * checking if the column belongs to a temp table or a select clause.
     * @throws IllegalArgumentException @throws ColumnNotFoundException
     */
    protected static RelationNode getTableRelationNode(
            SqlExpression columnExpression, List<RelationNode> listOfRelationNodes,
            SysDatabase database) throws IllegalArgumentException,
            ColumnNotFoundException {
        SqlExpression DUMMY = new SqlExpression();

        if (System.getProperty("TrackSQLExpression") != null
                && System.getProperty("TrackSQLExpression").equals("1")) {
            logger.debug("QueryTreeHandler.java  2047  : " + DUMMY.toString());
        }
        // 1. Check to see if the expression passed in is of type column
        if (columnExpression.getExprType() != SqlExpression.SQLEX_COLUMN) {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }

        // 2. Incase yes then extract the column out of the expression
        AttributeColumn column = columnExpression.getColumn();
        // Incase we already know the relation node - just return that
        if (column.relationNode != null) {
            return column.relationNode;
        }
        // 3. initialize the relation node
        RelationNode matchedNode = null;
        // The hashtable is used to store all the matches
        Hashtable dupTableList = new Hashtable();

        // Get the list of relation nodes
        // For each relation 
        for (RelationNode qTableNode : listOfRelationNodes) {
            // Get the column information about this table
            // make sure it is a real table
            if (qTableNode.getNodeType() == RelationNode.TABLE) {
                try {
                    // Get the syscolumn matching this column
                    SysColumn syscol = database.getSysTable(
                            qTableNode.getTableName()).getSysColumn(
                            column.columnName);
                    // If we find the SysColumn add it to the DUP List else move
                    // unitl
                    // there are no more tables
                    if (syscol == null) {
                        continue;
                    } else {
                        dupTableList.put(qTableNode, DUMMY);
                    }

                } catch (Exception ex) {
                    throw new XDBServerException("Table " + qTableNode.getTableName() +
                            " has not been found in database " + database.getDbname());
                }
            } else if (qTableNode.getNodeType() == RelationNode.SUBQUERY_RELATION) {
                try {
                    SqlExpression aSqlExpression = qTableNode
                            .getMatchingSqlExpression(column);
                    columnExpression.setMapped(SqlExpression.EXTERNALMAPPING);
                    columnExpression.setMappedExpression(aSqlExpression);
                    if (aSqlExpression != null) {
                        dupTableList.put(qTableNode, aSqlExpression);
                    }
                } catch (ColumnNotFoundException ex) {
                    // We ignore this since we are going to
                    // try to find the right node for expression
                }
            }
        }
        /*
         * If we have more than one nodes -- inthe dupTableList - we should
         * check whether the alias of the table matches -- select a.x,b.x from A
         * a,B b; --
         */

        Enumeration aEnumeration = dupTableList.keys();

        int elementCount = 0;
        while (aEnumeration.hasMoreElements()) {
            elementCount++;
            aEnumeration.nextElement();
        }

        // Incase we have an ambiguity
        if (elementCount > 1) {
            Enumeration endups = dupTableList.keys();

            while (endups.hasMoreElements()) {
                RelationNode tabNode = (RelationNode) endups.nextElement();
                // Start paste
                if (tabNode.getAlias().equals(tabNode.getTableName())) {
                    if (tabNode.isTemporaryTable() == true) {

                        if (matchedNode == null) {
                            matchedNode = tabNode;

                        } else {
                            throw new XDBServerException(
                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF
                                            + "( " + column.columnName + " )",
                                    0,
                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF_CODE);
                        }
                    } else {
                        if (columnExpression.getColumn().getTableAlias()
                                .equalsIgnoreCase(tabNode.getAlias()) == false) {
                            continue;
                        } else {
                            if (matchedNode == null) {
                                matchedNode = tabNode;
                                // In this case only in this case, where we know
                                // that the user has not
                                // specified any alias name for the relation
                                // node.
                                columnExpression.getColumn()
                                        .setTableAlias(tabNode.getAlias());

                            } else {
                                throw new XDBServerException(
                                        ErrorMessageRepository.AMBIGUOUS_COLUMN_REF
                                                + "( "
                                                + column.columnName
                                                + " )",
                                        0,
                                        ErrorMessageRepository.AMBIGUOUS_COLUMN_REF_CODE);
                            }

                        }
                    }
                } else {
                    if (columnExpression.getColumn().getTableAlias()
                            .equalsIgnoreCase(tabNode.getAlias()) == false) {
                        continue;
                    } else {
                        if (matchedNode == null) {
                            matchedNode = tabNode;

                        } else {
                            throw new XDBServerException(
                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF
                                            + "( " + column.columnName + " )",
                                    0,
                                    ErrorMessageRepository.AMBIGUOUS_COLUMN_REF_CODE);
                        }
                    }

                }
                // end Paste

            }
        } else if (elementCount == 0) {
            throw new ColumnNotFoundException(columnExpression);
        } else {
            Enumeration aRelNodeEnumeration = dupTableList.keys();
            while (aRelNodeEnumeration.hasMoreElements()) {
                RelationNode aRelationNode = (RelationNode) aRelNodeEnumeration
                        .nextElement();
                /**
                 * Else if we have just one match we still need to check if the
                 * tableAlias Match ie. a query like SELECT c_name, count(*)
                 * FROM customer c, orders WHERE c_custkey = o_custkey AND
                 * customer.c_custkey IN (SELECT c_custkey FROM customer WHERE
                 * c.c_name like '%1')
                 *
                 * We should be able to find that the variable c_name doesnt
                 * match the inner table but rather matches the outer table and
                 * is there fore a co-related query.
                 *
                 * If this check is not made c.c_name will match up with
                 * customer and we will not be able to find a co relation.
                 */

                /**
                 * This indicates that the column had some alias - Incase we
                 * dont have an alias we are not worried as then we have a open
                 * choice.
                 */

                if (columnExpression.getColumn() != null
                        && columnExpression.getColumn().getTableAlias().equals("") == false) {
                    // Check this alias with table alias of the relation node --
                    // Implying
                    // the user did not set a alias for this table
                    if (aRelationNode.getAlias()
                            .equals(aRelationNode.getTableName())) {
                        if (aRelationNode.isTemporaryTable() == true) {

                            // In this case only in this case, where we know
                            // that the user has not
                            // specified any alias name for the relation node.
                            columnExpression.getColumn()
                                    .setTableAlias(aRelationNode.getAlias());

                        } else {
                            if (columnExpression.getColumn().getTableAlias()
                                    .equalsIgnoreCase(
                                            aRelationNode.getAlias()) == false) {
                                throw new ColumnNotFoundException(
                                        columnExpression);
                            }
                        }
                    } else {

                        // If the user has specified an alias for this
                        // particular table then it does not matter whether we
                        // have
                        // a temporary table @ hand or a perm. table. We just
                        // have to do the same thing.
                        if (columnExpression.getColumn().getTableAlias()
                                .equalsIgnoreCase(
                                        aRelationNode.getAlias()) == false) {
                            throw new ColumnNotFoundException(columnExpression);
                        }
                    }
                } else {
                    columnExpression.getColumn().setTableAlias(aRelationNode.getAlias());
                }
                SqlExpression aSqlExpression = (SqlExpression) dupTableList
                        .get(aRelationNode);
                if (aSqlExpression != DUMMY) {
                    fillColumnExpression(aRelationNode, columnExpression,
                            aSqlExpression);
                }

                return aRelationNode;

            }
        }
        return matchedNode;
    }

    /**
     * PreCondition for calling this function : The nodeList and the
     * conditionList should not be empty or Null
     *
     * @param aRootCondition :
     *            The Query Condition which we want to analyze
     * @param nodeList :
     *            The Relation nodes where we will be able to find the columns
     *            associated with the nodes
     * @param projectionList :
     *            The SQLexpressions in the projection list
     * @param mode :
     *            Mode can be HAVING / CONDITION (where)
     * @return CAnalyzeAndCompleteColInfoAndNodeInfo This is a helper class
     *         which allows us transfer more than one object from a function
     */
    public static CAnalyzeAndCompleteColInfoAndNodeInfo checkAndFillQueryConditionColumns(
            QueryCondition aRootCondition, List<RelationNode> nodeList,
            List<SqlExpression> projectionList, int mode,
            Command commandToExecute) {
        // For each SQLExpression in the Query Condition
        QueryCondition qc = aRootCondition;
        // Get the SqlExpression--
        Vector sqlExprCondList = QueryCondition.getNodes(qc,
                QueryCondition.QC_SQLEXPR);
        // After we get a list of SQLExpressions contained in the QueryCondition
        // We need to extract them out --
        // Note that SQLExpressions are embedded in the query condition
        Enumeration enSqlExprCondList = sqlExprCondList.elements();
        List<SqlExpression> sqlExprList = new ArrayList<SqlExpression>();
        while (enSqlExprCondList.hasMoreElements()) {
            QueryCondition qcexpr = (QueryCondition) enSqlExprCondList
                    .nextElement();
            sqlExprList.add(qcexpr.getExpr());
        }

        // Before sending in the node list we should filter out all other
        // relation nodes except the
        // relation nodes which are SUBQUERY_RELATION or TABLE
        Vector newNodeListAfterFilter = new Vector();
        for (RelationNode aRelationNode : nodeList) {
            if (aRelationNode.getNodeType() == RelationNode.TABLE
                    || aRelationNode.getNodeType() == RelationNode.SUBQUERY_RELATION) {
                newNodeListAfterFilter.add(aRelationNode);
            }
        }

        // Once we have extracted the expressions - we submit them for analysis
        // -- they get updated there and the
        // the function return a list of nodes which were involved in the Query
        // Nodes, involved in the Query Condition
        // (Analyze -- and Fill the columns -- and add the Columns to the
        // conditionList of
        // of the Node.)
        CAnalyzeAndCompleteColInfoAndNodeInfo colInfo = analyzeAndCompleteColInfoAndNodeInfo(
                sqlExprList.iterator(), newNodeListAfterFilter, projectionList,
                mode, commandToExecute);
        for (Object element2 : colInfo.orphanExpressionVector) {
         SqlExpression element = (SqlExpression) element2;
         if (element.getColumn().getParentQueryCondition() == null) {
        element.getColumn().setParentQueryCondition(aRootCondition);
         }
      }
        return colInfo;
    }

    /*
     * This function will decide whether there is a join condition involved in
     * this particular Query condition.
     *
     * A Query Condition is called as a Join condition if any query condition
     * under it has a SQLEXPRESSION involving a column from two tables.
     *
     * @param aQueryCondition @return :True if the condition is join
     */
    public static boolean isJoinQueryCondition(QueryCondition aQueryCondition)
            throws ColumnNotFoundException {
        if (aQueryCondition.getCondType() == QueryCondition.QC_COMPOSITE) {
            if (aQueryCondition.getACompositeClause().leadsToJoinCondition()) {
                return true;
            }
        }

        // Get all the conditions
        List<QueryCondition> queryExprConds = QueryCondition.getNodes(
                aQueryCondition, QueryCondition.QC_ALL);
        // Iterate thru the result - the relation op will have > , = etc.
        for (QueryCondition queryCond : queryExprConds) {
            // Ask for the relation nodes involved
            List<RelationNode> RelationNodeList = getRelationNodes(queryCond);
            // If the count is more than 1 - we are sure that the condition
            // invloves join.
            if (RelationNodeList.size() > 1) {
                return true;
            } else {
                continue;
            }
        }
        // If nothing comes out of it we return false.
        return false;
    }


    public static class SegregateJoinHelper {
        List<QueryCondition> joinQConditionVector;

        List<QueryCondition> nonjoinQConditionVector;
    }

    public static SegregateJoinHelper segregateIntoSimpleAndJoinConditions(
            List<QueryCondition> qConditions) {
        // A plcae holder for data to be returned
        SegregateJoinHelper aSegregateJoinhelper = new SegregateJoinHelper();
        // This will be populated and we will add it to place holder -
        // aSegregateJoinHelper
        List<QueryCondition> joinVector = new ArrayList<QueryCondition>();
        // after the segregation we will populate the below vector with non-join
        // conditions.
        List<QueryCondition> nonjVector = new ArrayList<QueryCondition>();

        /**
         * The fun begins now
         */

        // For each condition
        for (QueryCondition qc : qConditions) {
            // and check if it is a join query condition - and add to the vector
            // where it belongs
            if (isJoinQueryCondition(qc)) {
                joinVector.add(qc);
            } else {
                nonjVector.add(qc);
            }
        }
        // Set the resultant vectors in the place holder and return the place
        // holder
        aSegregateJoinhelper.joinQConditionVector = joinVector;
        aSegregateJoinhelper.nonjoinQConditionVector = nonjVector;
        return aSegregateJoinhelper;
    }

    /*
     * This function will provide a map of sql expression which are joins.
     *
     * These SQL expressions are later used to create a crossreference between
     * various nodes.
     *
     * For eg. if we have a condition a.x = b.x and c.x > 10
     *
     * We will return a Map where a.x - b.x will be mapped.
     *
     */
    public static void populateJoinExpressionCrossReference(
            List<QueryCondition> joinQConditionVector)
            throws ColumnNotFoundException {
        // For each Query Condition
        for (QueryCondition qc : joinQConditionVector) {
            qc.setJoin(true);

            // Get the nodes with RELOP i.e. with the operator > , = etc.
            for (QueryCondition qcRelopNode : QueryCondition.getNodes(qc,
                    QueryCondition.QC_RELOP_COMPOSITE)) {
                List<RelationNode> RelationNodelist = getRelationNodes(qcRelopNode);
                // Check if the size is greater than - 1 implying that it is a
                // join node

                if (RelationNodelist.size() > 1) {
                    populateJoinCrossReference(qcRelopNode);
                } else {

                    // We know that we have a join condition --This could mean
                    // that
                    // we have a situation where exist clause/ any / all clause
                    // has
                    // a corelated join.We have to take the incoming relation
                    // node
                    // and add to it the node which it joins with in the query
                    // condition

                    // Only Complex Conditons will land here
                    if (qcRelopNode.getACompositeClause() != null) {
                        // The reason why we say that a particular composite
                        // clause
                        // is a join query is because we have a column in one of
                        // its
                        // conditions which is co-related.

                    } else {
                        // This is a not a error condition, we can have
                        // or condition land here. for eg
                        /*
                         * (p_partkey = l_partkey and p_brand = 'Brand#33') or
                         * (p_partkey = l_partkey and p_brand = 'Brand#43') so
                         * in this case even though we have a query condition
                         * which is a join we are going to process it in
                         * conjuction with the other non - join conditions
                         *
                         *
                         */

                    }
                }
            }
            // Look at base query conditions, too.
            for (QueryCondition qcRelopNode : QueryCondition.getNodes(qc,
                    QueryCondition.QC_SQLEXPR)) {
                List<RelationNode> RelationNodelist = getRelationNodes(qcRelopNode);
                // Check if the size is greater than - 1 implying that it is a
                // join node
                if (RelationNodelist.size() > 1) {
                    populateJoinCrossReference(qcRelopNode);
                }
            }
        }
    }

    // -RSS -We must make a check here if the node actually belong to this tree
    // or it belongs to a seprate
    // tree. -- Incase it belongs to seprate tree we will replace it with the
    // psuedo Table Node- So that the
    // Criss cross is done well
    public static void populateJoinCrossReference(
            QueryCondition joinRelOpQueryCondition)
            throws ColumnNotFoundException {
        List<RelationNode> aRelationNodeVector = getRelationNodes(joinRelOpQueryCondition);
        for (RelationNode aRelationNode : aRelationNodeVector) {
            if (aRelationNode.getNodeType() == RelationNode.SUBQUERY_CORRELATED) {
                continue;
            }

            for (RelationNode aRelationNodeInner : aRelationNodeVector) {
                if (aRelationNode != aRelationNodeInner) {
                    aRelationNodeInner.getJoinList().add(aRelationNode);
                }
            }
        }
    }

    /*
     * This function is responsible for finding if this SqlExpression is a Join
     * Expression
     *
     * The following algorithm is used - Get the Attribute columns - If the
     * number of Query node is more than 1 then we return true else false.
     */
    public static boolean isJoinExpression(SqlExpression aSqlExpression) {

        Vector SqlExpressionVector = SqlExpression.getNodes(aSqlExpression,
                SqlExpression.SQLEX_CONDITION);

        Vector RelationNodeVector = new Vector();
        Enumeration enSqlExprEnumeration = SqlExpressionVector.elements();
        while (enSqlExprEnumeration.hasMoreElements()) {
            SqlExpression sqlExprRelOp = (SqlExpression) enSqlExprEnumeration
                    .nextElement();
            Vector sqlcolVector = SqlExpression.getNodes(sqlExprRelOp,
                    SqlExpression.SQLEX_COLUMN);

            Enumeration sqlcolEnumeration = sqlcolVector.elements();
            while (sqlcolEnumeration.hasMoreElements()) {
                SqlExpression sqlcol = (SqlExpression) sqlcolEnumeration
                        .nextElement();
                AttributeColumn aAttributeColumn = sqlcol.getColumn();

                if (RelationNodeVector.contains(aAttributeColumn.relationNode) == false) {
                    RelationNodeVector.add(aAttributeColumn.relationNode);
                    if (RelationNodeVector.size() > 1) {
                        return true;
                    }
                }
            }
        }

        // before giving back
        return false;
    }

    /*
     * We do not expect any query condition to be a join condition here -- All
     * we need to check here is that this QueryCondition does not have more than
     * 1 SQLExpression
     */

    public static List<QueryCondition> getSimpleExprForQueryTree(
            List<QueryCondition> simpleConds) throws ColumnNotFoundException {
        List<QueryCondition> QueryCondVector = new ArrayList<QueryCondition>();
        for (QueryCondition qc : simpleConds) {
            List<RelationNode> RelationNodeVector = getRelationNodes(qc);

            if (RelationNodeVector.size() > 1) {

                QueryCondVector.add(qc);
                // If we have a OR query condition we add it as it is
            }
        }
        return QueryCondVector;
    }

    /**
     * Handles :
     *
     * @param qc
     * @return
     * @throws ColumnNotFoundException
     *
     * This function returns the relation nodes involved in this particular
     * query condition
     *
     * For normal conditions like n_nationkey = 1 it will return nation
     *
     * For conditions like n_nationkey = c_nationkey it will return nation,
     * customer
     *
     * For conditons where nation belongs to a parent tree and customer belongs
     * to a sub tree when we are analyzing the subtree we will get the
     * (PsuedoRelationNode) and customer
     *
     * For conditions where we have an exist clause and we are analysing
     *
     * Example:
     *
     * select * from nation where exists (select * from customer where
     * n_nationkey = c_nationkey) the function will return nation and
     * PlaceHolderNode.The placeholder node is a relation node which represents
     * the subtree in parent tree.The psuedoRelationNode is the counter part of
     * this.
     *
     *
     */

    public static List<RelationNode> getRelationNodes(QueryCondition qc)
            throws ColumnNotFoundException {
        // If this has already been determined, just return it.
        // This is called several times, and there is no need to keep
        // redoing the same work.
        if (qc.getRelationNodeList().size() > 0) {
            return qc.getRelationNodeList();
        }

        // Get the columns involved in this query
        QCHelperColumnList aQcHelperColumnList = getQueryColumnList(qc);

        // Incase we have a subquery tree this column list will be with the
        // columns
        // in the co-related expressions in the query.

        // Combine the columns both projected and co-related columns
        // and then find the relation nodes invloved.
        Vector colList = aQcHelperColumnList.getColumnExprListThisTree();
        colList.addAll(aQcHelperColumnList.getColumnExprListParentTree());

        Enumeration enColumnEnumeration = colList.elements();
        Vector nodeList = new Vector();
        while (enColumnEnumeration.hasMoreElements()) {
            AttributeColumn attribcol = (AttributeColumn) enColumnEnumeration
                    .nextElement();

            RelationNode aBelongsToRelationNode = attribcol.relationNode;

            // This will -- create problems when we do a recusrsive select
            // We should write a function which will check the GrandParent
            // for this function.

            // The logic here is to check if the column involved in the
            // condition belongs to this particular query tree, if it does
            // then we just add it to the nodeList which is then returned

            // Incase it does not belong to this query tree. Implying that
            // we have an orphan column. Then depending on if we are looking
            // at that particular condition from the Parent Tree or the
            // subtree we return the PlaceHolder Node or PsuedoTable Node.
            if (attribcol.getMyParentTree() == qc.getParentQueryTree()) {
                // If it is orphan
                if ((attribcol.columnGenre & AttributeColumn.ORPHAN) == AttributeColumn.ORPHAN) {
                    aBelongsToRelationNode = qc
                            .getPseudoRelationNode(attribcol);
                }
            } else {
                // If it is orphan and we are seeing the column from a
                // parent tree, then we get the place holder node
                if ((attribcol.columnGenre & AttributeColumn.ORPHAN) == AttributeColumn.ORPHAN) {
                    // Incase we are checking out a co-related query
                    // we will reach here -- The logic is as follows:
                    // Each column has the field ,
                    aBelongsToRelationNode = qc.getParentQueryTree()
                            .getPlaceHodlerNode(attribcol.getMyParentTree());
                    // Only add the relation if it is not present in the
                    // nodeList.
                    if (nodeList.contains(attribcol.relationNode) == false) {
                        nodeList.add(attribcol.relationNode);
                    }

                } else {
                    // but what about columns which are not orphans but we are
                    // lookin
                    // at them from a the parent. for eg. in the below query
                    // select n_nationkey from nation where n_nationkey in
                    // (select c_nationkey from customer);
                    // while analyzing n_nationkey in (select c_nationkey from
                    // customer), c_nationkey will come in
                    // this category. We will in this case get the place holder
                    // node and add it.
                    aBelongsToRelationNode = qc.getParentQueryTree()
                            .getPlaceHodlerNode(attribcol.getMyParentTree());
                }

            }
            if (nodeList.contains(aBelongsToRelationNode) == false) {
                nodeList.add(aBelongsToRelationNode);
            }

        }

        // Added extra handling for correlated subqueries
        // If parent has a correlated subquery, see if it is involved in
        // this QueryCondition.

        if (qc.getParentQueryTree().getCorrelatedSubqueryList().size() > 0) {
            // Just add the ones we don't yet have
            for (RelationNode phNode : getCorrelatedPlaceholder(qc,
                    qc.getParentQueryTree().getCorrelatedSubqueryList())) {
                if (!nodeList.contains(phNode)) {
                    nodeList.add(phNode);
                }
            }
        }

        return nodeList;
    }

    /**
     * see if this expression involves correlated subquery, and return a list of
     * all Placeholders if it does.
     */

    private static List<RelationNode> getCorrelatedPlaceholder(
            QueryCondition aQueryCondition, List<RelationNode> correlatedList) {
        List<RelationNode> placeHolders = new ArrayList<RelationNode>();

        if (aQueryCondition.getLeftCond() != null) {
            placeHolders.addAll(getCorrelatedPlaceholder(
                    aQueryCondition.getLeftCond(), correlatedList));
        }
        if (aQueryCondition.getRightCond() != null) {
            placeHolders.addAll(getCorrelatedPlaceholder(
                    aQueryCondition.getRightCond(), correlatedList));
        }

        if (aQueryCondition.getCondType() == QueryCondition.QC_SQLEXPR) {
            placeHolders.addAll(getCorrelatedPlaceholder(aQueryCondition.getExpr(),
                    correlatedList));
        }

        return placeHolders;
    }

    /**
     *
     */
    private static List<RelationNode> getCorrelatedPlaceholder(
            SqlExpression aSqlExpression, List<RelationNode> correlatedList) {
        Vector placeHolders = new Vector();

        if (aSqlExpression.getLeftExpr() != null) {
            placeHolders.addAll(getCorrelatedPlaceholder(
                    aSqlExpression.getLeftExpr(), correlatedList));
        }
        if (aSqlExpression.getRightExpr() != null) {
            placeHolders.addAll(getCorrelatedPlaceholder(
                    aSqlExpression.getRightExpr(), correlatedList));
        }

        if (aSqlExpression.getExprType() == SqlExpression.SQLEX_SUBQUERY) {
            // check for correlated
            for (RelationNode compNode : correlatedList) {
                if (aSqlExpression.getSubqueryTree() == compNode.getSubqueryTree()) {
                    // Got one!
                    placeHolders.add(compNode);
                }
            }
        }

        return placeHolders;
    }

    /**
     * This function return all the query columns in the query condition There
     * is some special handling here incase of an expression being a queryTree
     *
     * Insuch cases we try to get the projected expression by checking out the
     *
     * "projectedVector" a variable in queryTree
     *
     * This is filled only in processSubTree function, also a function in
     * QueryTree.java. For details on how this variable is filled please see:
     * QueryTree.java (ProcessSubTree)
     *
     *
     *
     * New logic : We will have a QCHelper object to help transfer information
     * about a query condition 1. ColumnsInvolved in this query condition which
     * belong to this particular tree 2.ColumnsInvolved in query condition which
     * belong to some other tree
     *
     * @param qc
     * @return
     */

    public static QCHelperColumnList getQueryColumnList(QueryCondition qc) {
        // This will return all the SQL expressions present in this Condition
        // Exceptions:
        // Incase of the SQLExpression being a QueryTree, the query tree is
        // returned
        // rather than going deep into getting the sql expression of the tree.
        Vector sqlExprList = QueryCondition.getNodes(qc,
                QueryCondition.QC_SQLEXPR);

        // For each expression
        Enumeration enSqlList = sqlExprList.elements();
        Vector ColumnList = new Vector();
        Vector correlatedColumnVector = new Vector();

        while (enSqlList.hasMoreElements()) {
            // Get the query condition , the expressions are embedded in the
            // query conditions therefore we have to extract them out
            QueryCondition aQueryCond = (QueryCondition) enSqlList
                    .nextElement();
            SqlExpression aSqlExpr = aQueryCond.getExpr();
            Vector columnExprVector = new Vector();

            // Incase we have a query tree as a sql expression then we will need
            // to find out the projected columns by this query

            // We can get a SqlExpression when we have a relation operator on
            // one end and a Sql query on the other end.
            if (aSqlExpr.getExprType() == SqlExpression.SQLEX_SUBQUERY) {

                QueryCondition aQueryCondition = aQueryCond.getParentQueryCondition();
                Vector projectedVector = new Vector();
                // Just make a call to get the correlated columns
                // Just make another call to get the projected columns
                if (aQueryCondition.getCondType() == QueryCondition.QC_COMPOSITE) {
                    projectedVector.addAll(aQueryCondition.getProjectedColumns());
                    // keep a track of these expression which are
                    // co-related
                    correlatedColumnVector.addAll(aQueryCondition
                            .getCorrelatedColumns());
                } else {
                    // Incase we dont have a composite condition - Then it
                    // implies that
                    // we will be have a SQL Subquery attached to a rel op.
                    // Example part
                    // ps_supplycost =
                    // (select min (ps_supplycost)
                    // from partsupp ps
                    // where ps_partkey = p_partkey)"

                    // What we need to do in this scenario is to allow only
                    // other than
                    // non scalar correlated query to get the projected vector
                    projectedVector.addAll(aQueryCond.getProjectedColumns());
                    if ((aQueryCond.getExpr().getSubqueryTree().getQueryType() & QueryTree.SCALAR_NONCORRELATED) == 0) {
                        correlatedColumnVector.addAll(aQueryCond
                                .getCorrelatedColumns());
                    }
                }

                // TODO: verify correctness incase we have a sub query tree in the
                // where condition, the query condition can only compare itself
                // with one - need to determine what to do with the other.
                if (projectedVector.size() >= 1) {
                    Enumeration listProjectedEnumeration = projectedVector
                            .elements();

                    while (listProjectedEnumeration.hasMoreElements()) {
                        SqlExpression aSqlExpression = (SqlExpression) listProjectedEnumeration
                                .nextElement();
                        columnExprVector.addAll(SqlExpression.getNodes(
                                aSqlExpression, SqlExpression.SQLEX_COLUMN));
                    }
                }

                // if condition not met, we don�t have any projected expression
                // A subquery tree only projects thoes expressions which are
                // involved in co-related relation with the parent query
            } else {
                // Incase we have a expression we just get all the columns
                // involved in the expression
                // and add it to the vector which will be returned
                columnExprVector.addAll(SqlExpression.getNodes(aSqlExpr,
                        SqlExpression.SQLEX_COLUMN));
            }

            Enumeration colExprEnumeration = columnExprVector.elements();

            // Now in the final step we get all the columns out of each
            // expression and add it to the column list.

            while (colExprEnumeration.hasMoreElements()) {
                SqlExpression sqlexpr = (SqlExpression) colExprEnumeration
                        .nextElement();
                AttributeColumn column = sqlexpr.getColumn();
                ColumnList.add(column);
            }
        }
        QCHelperColumnList aQCHelperColumnList = new QCHelperColumnList(
                ColumnList, correlatedColumnVector);
        return aQCHelperColumnList;
    }

    /**
     * This takes care of adding to the relation nodes to the query condition's
     * relation node list and the query condition to the relationNodes
     * "ConditionList"
     * 
     * @param simpleConds
     *            - The query conditions which has to be analyzed
     * @param aQueryTree
     *            - The Query Tree which holds this Query Condition
     */
    public static void addToNodesSimpleQueryCondition(
            List<QueryCondition> simpleConds, QueryTree aQueryTree)
            throws ColumnNotFoundException {
        for (QueryCondition qc : simpleConds) {
            List<RelationNode> RelationNodeVector = getRelationNodes(qc);

            if (RelationNodeVector.size() > 1) {
                throw new XDBServerException(
                        ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                        ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
            } else if (RelationNodeVector.size() == 1) {
                for (RelationNode aRelationNode : RelationNodeVector) {
                    if (qc.getRelationNodeList().contains(aRelationNode) == false) {
                        qc.getRelationNodeList().add(aRelationNode);
                    }
                    if (aRelationNode.getConditionList().contains(qc) == false) {
                        aRelationNode.getConditionList().add(qc);
                    }
                }
            } else if (RelationNodeVector.size() < 1) {
                aQueryTree.getConditionList().add(qc);
            }

        }
    }

    /**
     *
     * @param aQueryTree
     *
     *
     */
    public void manageFromSubQuery(QueryTree aQueryTree) {
        // For each Sql expression get all the SQL Column Expressions
        for (SqlExpression sqlExpr : aQueryTree.getProjectionList()) {
            Vector colVec = SqlExpression.getNodes(sqlExpr,
                    SqlExpression.SQLEX_COLUMN);

            // We now have the ColumnExpressions in each of the projected
            // expressions.

            Enumeration colVecEnumeration = colVec.elements();

            while (colVecEnumeration.hasMoreElements()) {
                // For each column
                SqlExpression sqlColumnExpr = (SqlExpression) colVecEnumeration
                        .nextElement();

                // Do not get if it is a With statement
                if (sqlColumnExpr.getColumn().relationNode.getNodeType() == RelationNode.SUBQUERY_RELATION
                        && !sqlColumnExpr.getColumn().relationNode.isWith()) {
                    try {
                        // Get the matching Sql Expression - This implies that
                        // if we have a query like
                        // select ns from (select n_nationkey from nation (ns));
                        // we want to execute it.
                        // so we will want to find the match from (ns -->
                        // n_nationkey) and create an
                        // expression

                        SqlExpression aSqlExpression = sqlColumnExpr.getColumn().relationNode
                                .getMatchingSqlExpression(sqlColumnExpr.getColumn());

                        // Check if we already have added sqlColumnExpr to the
                        // projection list of the query node
                        if (sqlColumnExpr.getColumn().relationNode.getProjectionList()
                                .contains(aSqlExpression) == false) {
                            SqlExpression aColumSqlExpression = new SqlExpression();

                            if (System.getProperty("TrackSQLExpression") != null
                                    && System.getProperty("TrackSQLExpression")
                                            .equals("1")) {
                                logger.debug("QueryTreeHandler.java  3361  : "
                                        + aColumSqlExpression.toString());
                            }
                            /**
                             * The Outer alias is the alias for columns
                             * projected from a particular table for eg. (select
                             * n_name as n from nation) n_nation(nname)
                             *
                             * column n_name alias = n column n_name outer alias =
                             * nname
                             *
                             * So with in the query it is mapped to n, where
                             * outside the query it is mapped to nname
                             *
                             * as a rule 1. outeralias = alias = columnName ( if
                             * the projection is a column) | expressionName
                             */
                            aColumSqlExpression = SqlExpression
                                    .getSqlColumnExpression(
                                            aSqlExpression.getOuterAlias(),// columName
                                            sqlColumnExpr.getColumn().relationNode.getAlias(),// TableName
                                            sqlColumnExpr.getColumn().relationNode.getAlias(),// TableAlias
                                            aSqlExpression.getOuterAlias(),// ColumnAlias
                                            aSqlExpression.getOperator());// Operator

                            aColumSqlExpression.getColumn().relationNode = sqlColumnExpr.getColumn().relationNode;
                            aColumSqlExpression.setExprType(sqlColumnExpr.getExprType());
                            aColumSqlExpression.setExprDataType(sqlColumnExpr.getExprDataType());

                            aColumSqlExpression.rebuildExpression();
                            sqlColumnExpr.getColumn().relationNode.getProjectionList()
                                    .add(aColumSqlExpression);
                        }
                    } catch (Exception ex) {
                        throw new XDBServerException(
                                ErrorMessageRepository.SUBQUERY_MANAGE_ERROR,
                                ex,
                                ErrorMessageRepository.SUBQUERY_MANAGE_ERROR_CODE);

                    }
                }

            }

        }
    }
    
    public void setWithProjectionTypes(QueryTree aQueryTree) {
            
        // For each Sql expression get all the SQL Column Expressions
        for (SqlExpression sqlExpr : aQueryTree.getProjectionList()) {
            Vector colVec = SqlExpression.getNodes(sqlExpr,
                    SqlExpression.SQLEX_COLUMN);

            // We now have the ColumnExpressions in each of the projected
            // expressions.

            Enumeration colVecEnumeration = colVec.elements();

            while (colVecEnumeration.hasMoreElements()) {
                // For each column
                SqlExpression sqlColumnExpr = (SqlExpression) colVecEnumeration
                        .nextElement();

                if (sqlColumnExpr.getColumn().relationNode.getNodeType() == RelationNode.SUBQUERY_RELATION) {
                    try {
                        // Get the matching Sql Expression - This implies that
                        // if we have a query like
                        // select ns from (select n_nationkey from nation (ns));
                        // we want to execute it.
                        // so we will want to find the match from (ns -->
                        // n_nationkey) and create an
                        // expression

                        SqlExpression aSqlExpression = sqlColumnExpr.getColumn().relationNode
                                .getMatchingSqlExpression(sqlColumnExpr.getColumn());

                        // Check if we already have added sqlColumnExpr to the
                        // projection list of the query node
                        if (sqlColumnExpr.getColumn().relationNode.getProjectionList()
                                .contains(aSqlExpression) == false) {
                            SqlExpression aColumSqlExpression = new SqlExpression();

                            if (System.getProperty("TrackSQLExpression") != null
                                    && System.getProperty("TrackSQLExpression")
                                            .equals("1")) {
                                logger.debug("QueryTreeHandler.java  3361  : "
                                        + aColumSqlExpression.toString());
                            }
                            /**
                             * The Outer alias is the alias for columns
                             * projected from a particular table for eg. (select
                             * n_name as n from nation) n_nation(nname)
                             *
                             * column n_name alias = n column n_name outer alias =
                             * nname
                             *
                             * So with in the query it is mapped to n, where
                             * outside the query it is mapped to nname
                             *
                             * as a rule 1. outeralias = alias = columnName ( if
                             * the projection is a column) | expressionName
                             */
                            aColumSqlExpression = SqlExpression
                                    .getSqlColumnExpression(
                                            aSqlExpression.getOuterAlias(),// columName
                                            sqlColumnExpr.getColumn().relationNode.getAlias(),// TableName
                                            sqlColumnExpr.getColumn().relationNode.getAlias(),// TableAlias
                                            aSqlExpression.getOuterAlias(),// ColumnAlias
                                            aSqlExpression.getOperator());// Operator

                            aColumSqlExpression.getColumn().relationNode = sqlColumnExpr.getColumn().relationNode;
                            aColumSqlExpression.setExprType(sqlColumnExpr.getExprType());
                            aColumSqlExpression.setExprDataType(sqlColumnExpr.getExprDataType());

                            aColumSqlExpression.rebuildExpression();
                            sqlColumnExpr.getColumn().relationNode.getProjectionList()
                                    .add(aColumSqlExpression);
                        }
                    } catch (Exception ex) {
                        throw new XDBServerException(
                                ErrorMessageRepository.SUBQUERY_MANAGE_ERROR,
                                ex,
                                ErrorMessageRepository.SUBQUERY_MANAGE_ERROR_CODE);

                    }
                }

            }

        }
    }

    private static QueryCondition normalizeExpr(
            QueryCondition anInputCondition, int aType) {
        if (anInputCondition == null
                || anInputCondition.getCondType() != QueryCondition.QC_RELOP) {
            return anInputCondition;
        }
        SqlExpression theResult = anInputCondition.getLeftCond().getExpr();
        if (theResult == null || theResult.getExprType() != SqlExpression.SQLEX_CONSTANT) {
            theResult = anInputCondition.getRightCond().getExpr();
            if (theResult == null || theResult.getExprType() != SqlExpression.SQLEX_CONSTANT) {
                theResult = null;
            }
        }
        if (theResult != null) {
            try {
                if (aType == Types.DATE) {
                    theResult.setConstantValue(SqlExpression.normalizeDate(theResult.getConstantValue()));
                } else if (aType == Types.TIME) {
                    theResult.setConstantValue(SqlExpression.normalizeTime(theResult.getConstantValue()));
                }
            } catch (Exception ex) {
                // fallback
                aType = Types.TIMESTAMP;
            }
            if (aType == Types.TIMESTAMP) {
                theResult.setConstantValue(SqlExpression.normalizeTimeStamp(theResult.getConstantValue()));
            }
        }
        return anInputCondition;
    }
    /**
     * This function finds the expression type of all the SQL Expressions in the
     * order by clause
     *
     * @param orderByList
     *                This variable contains the SqlExpression in the order by
     *                list
     * @throws ColumnNotFoundException
     *                 The exception is thrown if we have a column name which
     *                 could not be found in the database
     */
    public static void FillSQLExpressionInformation(QueryTree aQueryTree, 
            List<OrderByElement> orderByList,
            Command commandToExecute)
            throws ColumnNotFoundException {
        Vector<SqlExpression> expressionList = new Vector<SqlExpression>();
        for (OrderByElement aOrderByElement : orderByList) {
            expressionList.add(aOrderByElement.orderExpression);
        }
        QueryTreeHandler.checkAndExpand(expressionList,
                aQueryTree.getRelationNodeList(), 
                commandToExecute.getClientContext().getSysDatabase(), 
                commandToExecute);

        List<SqlExpression> orderByOrpans = QueryTreeHandler.checkAndFillTableNames(
                expressionList, aQueryTree.getRelationNodeList(),
                aQueryTree.getProjectionList(), QueryTreeHandler.ORDERBY,
                commandToExecute);
        aQueryTree.getOrderByOrphans().addAll(orderByOrpans);

        Enumeration<SqlExpression> exprList = expressionList.elements();
        while (exprList.hasMoreElements()) {

            SqlExpression aSqlExpression = exprList.nextElement();
            aSqlExpression.rebuildExpression();
            try {
                SqlExpression.setExpressionResultType(aSqlExpression,
                        commandToExecute);
            } catch (NullPointerException nullex) {
                throw nullex;
            }
        }
    }   
}
