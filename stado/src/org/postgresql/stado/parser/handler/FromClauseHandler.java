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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysView;
import org.postgresql.stado.metadata.SysViewColumns;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.Parser;
import org.postgresql.stado.parser.SqlCreateTableColumn;
import org.postgresql.stado.parser.SqlSelect;
import org.postgresql.stado.parser.core.ParseException;
import org.postgresql.stado.parser.core.syntaxtree.ColumnNameList;
import org.postgresql.stado.parser.core.syntaxtree.FromTableSpec;
import org.postgresql.stado.parser.core.syntaxtree.INode;
import org.postgresql.stado.parser.core.syntaxtree.JoinSpec;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.SelectAliasSpec;
import org.postgresql.stado.parser.core.syntaxtree.TableName;
import org.postgresql.stado.parser.core.syntaxtree.TableSpec;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * This class provides the parser-level handling for the FROM part of the SQL query.
 */
public class FromClauseHandler extends DepthFirstRetArguVisitor {
    Command commandToExecute;

    /**
     * We increment the outerCounter each time we encounter an OUTER,
     * to ensure unique outer levels, even amongst multiple "outer branches"
     * from the same base table.
     */
    short outerCounter = 0;

    /**
     * Class constructor
     * @param commandToExecute
     */
    public FromClauseHandler(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
    }

    /**
     * The from clause handler - will give us the information on the Query Nodes
     * Case: 1. Simple From Clause - From TableA, TableB 2. Complex From Clause -
     * From ( select * from a) as Tablea, Tableb f0 -> TableSpec(prn) f1 -> (
     * <CROSS_> <JOIN_> TableSpec(prn) | ( [ <INNER_> ] <JOIN_> TableSpec(prn)
     * JoinSpec(prn) | ( <LEFT_> | <RIGHT_> | <FULL_> ) [ <OUTER_> ] <JOIN_>
     * TableSpec(prn) JoinSpec(prn) ) | <NATURAL_> ( [ <INNER_> ] <JOIN_>
     * TableSpec(prn) | ( <LEFT_> | <RIGHT_> | <FULL_> ) [ <OUTER_> ] <JOIN_>
     * TableSpec(prn) ) )*
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(FromTableSpec n, Object argu) {
        QueryTree aQueryTree = (QueryTree) argu;
        QueryCondition aQueryCondition = null;
        RelationNode prevRelation = null;
        List<SqlExpression> vCondExpr = null;
        RelationNode lastRelation = (RelationNode) n.f0.accept(this, argu);
        lastRelation.handleAliasForSingleTableSubQueryNode();
        if (n.f1.present()) {
        	Iterator<INode> aListOfJoins = n.f1.elements();
            while (aListOfJoins.hasNext()) {
                NodeChoice aChoice = (NodeChoice) aListOfJoins.next();
                prevRelation = lastRelation;
                NodeChoice aCh = null;
                switch (aChoice.which) {
                case 0:
                    // Cross Join
                    NodeSequence aOuterNode = (NodeSequence) aChoice.choice;
                    TableSpec ts = (TableSpec) aOuterNode.nodes.get(2);
                    lastRelation = (RelationNode) ts.accept(this, argu);
                    lastRelation.handleAliasForSingleTableSubQueryNode();
                    lastRelation.addSiblingJoin(prevRelation);
                    break;
                case 1: // ORDINARY (not natural) Join
                    aCh = (NodeChoice) aChoice.choice;
                    switch (aCh.which) {
                    case 0:
                        // Inner Join [ <INNER_> ] <JOIN_> TableSpec(prn)
                        // JoinSpec(prn)
                        aOuterNode = (NodeSequence) aCh.choice;
                        ts = (TableSpec) aOuterNode.nodes.get(2);
                        lastRelation = (RelationNode) ts.accept(this, argu);

                        lastRelation.handleAliasForSingleTableSubQueryNode();
                        JoinSpec js = (JoinSpec) aOuterNode.nodes.get(3);
                        QueryCondition qc = (QueryCondition) js.accept(this,
                                argu);
                        Vector<QueryCondition> expressions = QueryCondition
                                .getNodes(qc, QueryCondition.QC_SQLEXPR);

                        // We need a collection just containing the
                        // expressoins, not QCs

                        vCondExpr = new ArrayList<SqlExpression>();
                        for (QueryCondition qcExpr : expressions) {
                            vCondExpr.add(qcExpr.getExpr());
                        }
                        QueryTreeHandler.analyzeAndCompleteColInfoAndNodeInfo(
                                vCondExpr.iterator(),
                                aQueryTree.getRelationNodeList(), null,
                                QueryTreeHandler.CONDITION, commandToExecute);

                        HashSet<RelationNode> parents = new HashSet<RelationNode>();
                        for (RelationNode relNode : aQueryTree.getRelationNodeList()) {
                            if (relNode == lastRelation) {
                                continue;
                            }
                            for (QueryCondition qcExpr : expressions) {
                                if (qcExpr.getExpr().contains(relNode)) {
                                    lastRelation.addSiblingJoin(relNode);
                                }
                            }
                        }
                        break;
                    case 1:
                        // outer ( <LEFT_> | <RIGHT_> | <FULL_>) [ <OUTER_> ]
                        // <JOIN_> TableSpec(prn) JoinSpec(prn)
                        NodeSequence aCh1 = (NodeSequence) aCh.choice;
                        aQueryTree.setOuterJoin(true);
                        switch (((NodeChoice) aCh1.elementAt(0)).which) {
                        case 0: // <LEFT_>
                            lastRelation = (RelationNode) aCh1.elementAt(3)
                                    .accept(this, argu);
                            lastRelation.handleAliasForSingleTableSubQueryNode();
                            qc = (QueryCondition) aCh1.elementAt(4).accept(
                                    this, argu);

                            expressions = QueryCondition.getNodes(qc,
                                    QueryCondition.QC_SQLEXPR);

                            // We need a collection just containing the
                            // expressoins, not QCs
                            vCondExpr = new ArrayList<SqlExpression>();
                            for (QueryCondition qcExpr : expressions) {
                                vCondExpr.add(qcExpr.getExpr());
                            }
                            QueryTreeHandler
                                    .analyzeAndCompleteColInfoAndNodeInfo(
                                            vCondExpr.iterator(),
                                            aQueryTree.getRelationNodeList(), null,
                                            QueryTreeHandler.CONDITION,
                                            commandToExecute);

                            parents = new HashSet<RelationNode>();
                            for (RelationNode relNode : aQueryTree.getRelationNodeList()) {
                                if (relNode == lastRelation) {
                                    continue;
                                }
                                for (QueryCondition qcExpr : expressions) {
                                    if (qcExpr.getExpr().contains(relNode)) {
                                        parents.add(relNode);
                                        break;
                                    }
                                }
                            }
                            lastRelation.addParentNodes(parents, true,
                                    ++outerCounter);

                            break;
                        case 1: // <RIGHT_>
                            throw new XDBServerException(
                                    "Not implemented yet (right outer join).");
                        case 2: // <FULL_>
                            throw new XDBServerException(
                                    "Not implemented yet (full join).");
                        }
                    }

                    break;
                case 2: // natural Join
                    NodeSequence aSCh = (NodeSequence) aChoice.choice;
                    aCh = (NodeChoice) aSCh.elementAt(1);
                    switch (aCh.which) {
                    case 0:
                        // Inner Join [ <INNER_> ] <JOIN_> TableSpec(prn)
                        lastRelation = (RelationNode) ((NodeSequence) aCh.choice)
                                .elementAt(2).accept(this, argu);
                        lastRelation.handleAliasForSingleTableSubQueryNode();
                        aQueryCondition = createQueryConditionForNaturalJoin(aQueryTree);
                        aQueryTree.getFromClauseConditions().add(
                                aQueryCondition);
                        lastRelation.addSiblingJoin(prevRelation);
                        break;
                    case 1:
                        // outer ( <LEFT_> | <RIGHT_> | <FULL_> ) [ <OUTER_> ]
                        // <JOIN_> TableSpec(prn)
                        NodeSequence aCh1 = (NodeSequence) aCh.choice;
                        aQueryTree.setOuterJoin(true);
                        switch (((NodeChoice) aCh1.elementAt(0)).which) {
                        case 0: // <LEFT_>
                            lastRelation = (RelationNode) ((NodeSequence) aCh.choice)
                                    .elementAt(3).accept(this, argu);
                            lastRelation.handleAliasForSingleTableSubQueryNode();
                            aQueryCondition = createQueryConditionForNaturalJoin(aQueryTree);
                            aQueryTree.getFromClauseConditions().add(
                                    aQueryCondition);
                            lastRelation.addParentNodes(Collections
                                    .singleton(prevRelation), true,
                                    ++outerCounter);

                            break;
                        case 1: // <RIGHT_>
                            throw new XDBServerException(
                                    "Not implemented yet (natural right join).");
                        case 2: // <FULL_>
                            throw new XDBServerException(
                                    "Not implemented yet (natural full join).");
                        }
                        break;

                    }
                    break;
                }
            }
        }

        return null;
    }

    /**
     * f0 -> "ON" SQLComplexExpression(prn) | <USING_> "(" ColumnNameList(prn)
     * ")"
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(JoinSpec n, Object argu) {
        QueryTree aQueryTree = (QueryTree) argu;
        QueryCondition aQueryCondition = null;
        switch (n.f0.which) {
        case 0:
            NodeSequence aNodeSequence = (NodeSequence) n.f0.choice;
            INode aNodeComplexExpression = aNodeSequence.elementAt(1);
            QueryConditionHandler aQueryConditionHandler = new QueryConditionHandler(
                    commandToExecute);
            aNodeComplexExpression.accept(aQueryConditionHandler, argu);
            aQueryCondition = aQueryConditionHandler.aRootCondition;
            aQueryTree.getFromClauseConditions().add(aQueryCondition);
            break;
        case 1:
            NodeSequence aNodeSequence1 = (NodeSequence) n.f0.choice;
            INode aNodeColumnList = aNodeSequence1.elementAt(2);
            ColumnNameListHandler aColListHandler = new ColumnNameListHandler();
            aNodeColumnList.accept(aColListHandler, argu);
            List<String> columnNameList = aColListHandler.getColumnNameList();
            List<RelationNode> theRelNodes = aQueryTree.getRelationNodeList();
            ArrayList<QueryCondition> aQueryConditionList = new ArrayList<QueryCondition>();
                for (String col : columnNameList) {
                    RelationNode theNode1 = theRelNodes
                            .get(theRelNodes.size() - 1);
                    RelationNode theNode2 = theRelNodes
                            .get(theRelNodes.size() - 2);
                    SqlExpression exp1 = SqlExpression.getSqlColumnExpression(
                            col, theNode1.getTableName(), theNode1.getAlias(),
                            col, "+");
                    SqlExpression exp2 = SqlExpression.getSqlColumnExpression(
                            col, theNode2.getTableName(), theNode2.getAlias(),
                            col, "+");
                    SqlExpression exp = new SqlExpression();
                    exp.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
                    exp.setOperator("=");
                    exp.setLeftExpr(exp1);
                    exp.setRightExpr(exp2);
                    QueryCondition cqc = new QueryCondition();
                    cqc.setCondType(QueryCondition.QC_SQLEXPR);
                    cqc.setExpr(exp);
                    aQueryConditionList.add(cqc);
                }
            aQueryCondition = createQueryConditionFromQueryConditionList(aQueryConditionList);
            aQueryTree.getFromClauseConditions().add(aQueryCondition);
            break;
        }
        return aQueryCondition;
    }

    /**
     * f0 -> [ONLY_] TableName(prn) [ SelectAliasSpec(prn) ] | "("
     * SelectWithoutOrderAndSet(prn) ")" [SelectAliasSpec(prn)]["("
     * ColumnNameList(prn) ")"]
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(TableSpec n, Object argu) {
        QueryTree aQueryTree = (QueryTree) argu;
        RelationNode aRelationNode = null;
        
        switch (n.f0.which) {
        case 0:
            NodeSequence ns = (NodeSequence) n.f0.choice;

            NodeOptional nOp = (NodeOptional) ns.elementAt(0);
            
            TableName nt = (TableName) ns.elementAt(1);
            TableNameHandler aTableNameHandler = new TableNameHandler(
                    commandToExecute.getClientContext());
            nt.accept(aTableNameHandler, null);

            /* Handle ONLY */
            if (nOp.present())
            {
                aTableNameHandler.setOnly(true);
            }
            

            nOp = (NodeOptional) ns.elementAt(2);

       
            if (commandToExecute.getClientContext().getSysDatabase()
                    .isViewExists(aTableNameHandler.getTableName())) {
                
                aRelationNode = aQueryTree.newRelationNode();
                            
                String cmd = this.commandToExecute.getClientContext()
                        .getSysDatabase().getSysView(
                                aTableNameHandler.getTableName()).getViewText();

                Parser parser = new Parser(commandToExecute.getClientContext());
                try {
                    parser.parseStatement(cmd);
                } catch (ParseException e) {
                    throw new XDBServerException(
                            "Failed to parse view statement: " + cmd);
                }
                aRelationNode.setNodeType(RelationNode.SUBQUERY_RELATION);

                // The SubQuery Tree will have the query tree which will
                // be filled up with the select statement information
                SqlSelect select = (SqlSelect) parser.getSqlObject();
                QueryTree aSubQueryTree = select.aQueryTree;
                SysView aView = this.commandToExecute.getClientContext()
                        .getSysDatabase().getSysView(
                                aTableNameHandler.getTableName());
                if (aView.getViewColumns() != null
                        && aView.getViewColumns().size() > 0) {
                    int i = 0;
                    for (SysViewColumns col : aView.getViewColumns()) {
                        aSubQueryTree.getProjectionList()
                            .get(i).setAlias(col.getViewColumn());
                        aSubQueryTree.getProjectionList()
                            .get(i).setOuterAlias(col.getViewColumn());
                        i++;
                    }
                }
                aSubQueryTree.setParentQueryTree(aQueryTree);
                aSubQueryTree.setTopMostParentQueryTree(aQueryTree.getTopMostParentQueryTree());                
                aRelationNode.setAlias(aTableNameHandler.getTableName());
                NodeOptional aliasNode = (NodeOptional)ns.elementAt(2);
                if(aliasNode.present()) {
                    aRelationNode.setAlias(((SelectAliasSpec)aliasNode.node).f1.f0.f0.choice.toString());
                }
                aRelationNode.setSubqueryTree(aSubQueryTree);
                if (aRelationNode.getAlias() == null
                        || aRelationNode.getAlias().equals("")) {
                    // Generate an aliasName
                    aRelationNode.setAlias("TempTableGenerated"
                            + RelationNode.getNextCount());
                    aRelationNode.setTableName(aRelationNode.getAlias());
                } else {
                    // For subqueries type relation the alias and the
                    // tableName will be the same
                    aRelationNode.setTableName(aRelationNode.getAlias());
                }
                aQueryTree.getRelationSubqueryList().add(aRelationNode);

            } else {
                
                for (RelationNode subRelationNode : aQueryTree.getTopMostParentQueryTree().getTopWithSubqueryList()) {
                    
                    if (subRelationNode.getAlias().equalsIgnoreCase(aTableNameHandler.getTableName())) {

                        // We create a new relation node for this, but we also
                        // just want to refer to the original WITH so that we 
                        // avoid executing more than once while processing.
                        aRelationNode = aQueryTree.newRelationNode();
                        aRelationNode.setNodeType(RelationNode.SUBQUERY_RELATION);
                        aRelationNode.setTableName(aTableNameHandler.getTableName());
                        aRelationNode.setOnly(aTableNameHandler.isOnly());
                        aRelationNode.setClient(commandToExecute.getClientContext());
                        aRelationNode.setAlias(aTableNameHandler.getReferenceName());

                        // Set the base relation that we are using
                        aRelationNode.setBaseWithRelation(subRelationNode);

                        /* Set up projection info */
                        QueryTreeHandler.populateSubRelationProjectionList(aRelationNode, 
                                commandToExecute.getClientContext().getSysDatabase());
                        
                        argu = aRelationNode;
                        HandleAlias(nOp, argu, aRelationNode, aRelationNode
                                .getTableName());                        
                        
                        // If we are the top query tree, track that the WITH 
                        // relation was used here. Ones that are not used
                        // we want to avoid trying to use when optimizing
                        if (aQueryTree.getTopMostParentQueryTree() == aQueryTree) {
                            subRelationNode.setIsTopMostUsedWith(true);
                        }
                        
                        //no subRelationNode.getSubqueryTree().getRelationSubqueryList().remove(subRelationNode);
                        subRelationNode.withReferenceCount++;
                        
                        return aRelationNode;
                    }
                }                 

                aRelationNode = aQueryTree.newRelationNode();
                aRelationNode.setNodeType(RelationNode.TABLE);
                aRelationNode.setTableName(aTableNameHandler.getTableName());

                aRelationNode
                        .setTemporaryTable(aTableNameHandler.isTemporary());
                aRelationNode.setOnly(aTableNameHandler.isOnly());
                aRelationNode.setClient(commandToExecute.getClientContext());
                aRelationNode.setAlias(aTableNameHandler.getReferenceName());

                /* This call is no of much use -- */
                n.f0.accept(this, argu);

                /*
                 * Pass the RelationNode into f1 to get set the alias -- If it
                 * is there
                 */

                argu = aRelationNode;
                HandleAlias(nOp, argu, aRelationNode, aRelationNode
                        .getTableName());
                // aQueryTree.queryNodeList.add(argu); -- No need for this
                // now
            }
            break;
        case 1:
            /*
             * In case we come across a this case we know that this is a query
             * which has a relation in the form clause For eg. select * from
             * (select* from nation)
             */
            aRelationNode = aQueryTree.newRelationNode();
            aRelationNode.setNodeType(RelationNode.SUBQUERY_RELATION);

            // The SubQuery Tree will have the query tree which will
            // be filled up with the select statement information
            QueryTree aSubQueryTree = new QueryTree();

            NodeSequence ns2 = (NodeSequence) n.f0.choice;

            INode nSelectWithoutOrderAndSet = ns2.elementAt(1);

            // We will now try to get the Query SubTree

            QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                    commandToExecute);
            nSelectWithoutOrderAndSet.accept(aQueryTreeHandler, aSubQueryTree);
            aRelationNode.setSubqueryTree(aSubQueryTree);

            // I havent added an alias spec which can also be possible-
            INode nAlias = ns2.elementAt(3);

            HandleAlias((NodeOptional) nAlias, argu, aRelationNode, "");

            // For realtion tables these two parameters are always same
            if (aRelationNode.getAlias() == null || aRelationNode.getAlias().equals("")) {
                // Generate an aliasName
                aRelationNode.setAlias("TempTableGenerated"
                        + RelationNode.getNextCount());
                aRelationNode.setTableName(aRelationNode.getAlias());
            } else {
                // For subqueries type relation the alias and the
                // tableName will be the same
                aRelationNode.setTableName(aRelationNode.getAlias());
            }

            // Now we will take care of the columns that are projected from
            // the above
            // We will create the columns and will
            INode nColumnList = ns2.elementAt(4);
            HandleColumnList((NodeOptional) nColumnList, aSubQueryTree);
            aQueryTree.getRelationSubqueryList().add(aRelationNode);
            // n.f0.choice.accept(this,argu);
            break;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }
        //will be needed in case of LeftOuter (+) handling
        if (aRelationNode != null) {
            aQueryTree.getRelHandlerInfo().put(aRelationNode.getTableName(), aRelationNode);
        }
        return aRelationNode;
    }

    /**
     * This function handles the Table list associated with the from clause
     *
     * @param nOp
     * @param aSubQueryTree
     */
    private void HandleColumnList(NodeOptional nOp, QueryTree aSubQueryTree) {
        if (nOp.present()) {
            class columnNameListHandler extends DepthFirstVoidArguVisitor {
                public Vector<String> nameList = new Vector<String>();

                /**
                 * f0 -> <IDENTIFIER_NAME> f1 -> ( "," <IDENTIFIER_NAME> )*
                 */
                @Override
                public void visit(ColumnNameList n, Object argu) {
                    n.f0.accept(this, argu);
                    n.f1.accept(this, argu);
                }

                @Override
                public void visit(NodeToken n, Object argu) {
                    if (!(n.tokenImage.equals(",") || n.tokenImage.equals(")")
                            || n.tokenImage.equals("("))) {
                        nameList.add(n.tokenImage);
                    }
                }
            }

            columnNameListHandler aColumnNameListHandler = new columnNameListHandler();
            nOp.accept(aColumnNameListHandler, aSubQueryTree);
            Vector<String> nameList = aColumnNameListHandler.nameList;

            if (nameList.size() != aSubQueryTree.getProjectionList().size()) {
                throw new XDBServerException(
                        ErrorMessageRepository.ALIAS_COUNT_UNEQUAL_SUBQUERY_PROJ
                                + aSubQueryTree.rebuildString(),
                        0,
                        ErrorMessageRepository.ALIAS_COUNT_UNEQUAL_SUBQUERY_PROJ_CODE);
            }
            int count = 0;
            for (SqlExpression aSqLExpression : aSubQueryTree.getProjectionList()) {
                aSqLExpression.setOuterAlias(nameList.get(count));
                aSqLExpression.setAlias(aSqLExpression.getOuterAlias());
                count++;
            }

        }

    }

    /**
     * This function handles the alias specs associated with any particular
     * table
     *
     * @param nOp
     * @param argu
     * @param aRelationNode
     * @param tableName
     */
    private void HandleAlias(NodeOptional nOp, Object argu,
            RelationNode aRelationNode, String tableName) {
        if (nOp.present()) {
            // The alias Specifcatioin is taken care of by AliasSpecHandler--
            AliasSpecHandler aliasHandler = new AliasSpecHandler();
            // The agru contains the Query Tree -- but it is better that
            nOp.accept(aliasHandler, argu);
            // When it returns -- it will have the information on the alias name
            // of this Node.
            aRelationNode.setAlias(aliasHandler.getAliasName());
        } else {
            aRelationNode.setAlias(tableName);
        }
    }

    private QueryCondition createQueryConditionForNaturalJoin(
            QueryTree aQueryTree) {

        List<RelationNode> listNodes = aQueryTree.getRelationNodeList();
        RelationNode theNode = listNodes.get(listNodes.size() - 1);
        Vector<SqlExpression> theListOfColumn = GetListOfColumnsForNode(theNode);
        Vector<SqlExpression> theCommonListOfColumns = new Vector<SqlExpression>();
        ArrayList<QueryCondition> qcs = new ArrayList<QueryCondition>();

        for (int i = 0; i < listNodes.size() - 1; i++) {
            for (SqlExpression element : GetListOfColumnsForNode(listNodes
                    .get(i))) {
                for (SqlExpression el1 : theListOfColumn) {
                    if (el1.getAlias().toUpperCase().equals(element.getAlias())) {
                        theCommonListOfColumns.add(el1);

                        SqlExpression exp = new SqlExpression();
                        exp.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
                        exp.setOperator("=");
                        exp.setLeftExpr(el1);
                        exp.setRightExpr(element);
                        QueryCondition cqc = new QueryCondition();
                        cqc.setCondType(QueryCondition.QC_SQLEXPR);
                        cqc.setExpr(exp);
                        qcs.add(cqc);
                    }
                }
            }
        }

        if (qcs.size() == 0) {
            throw new XDBServerException("Tables do not have same columns.");
        }
        return createQueryConditionFromQueryConditionList(qcs);

    }

    private QueryCondition createQueryConditionFromQueryConditionList(
            ArrayList<QueryCondition> aQueryConditionList) {
        int size = aQueryConditionList.size();
        if (size == 1) {
            return (QueryCondition) aQueryConditionList.get(0);
        } else {
            QueryCondition current = new QueryCondition();
            current.setLeftCond((QueryCondition) aQueryConditionList
                    .get(size - 2));
            current.setRightCond((QueryCondition) aQueryConditionList
                    .get(size - 1));
            current.setCondType(QueryCondition.QC_CONDITION);
            current.setOperator("AND");
            for (int i = size - 3; i >= 0; i--) {
                QueryCondition newCurrent = new QueryCondition();
                newCurrent.setLeftCond((QueryCondition) aQueryConditionList
                        .get(i));
                newCurrent.setRightCond(current);
                newCurrent.setCondType(QueryCondition.QC_CONDITION);
                newCurrent.setOperator("AND");
                newCurrent.rebuildCondString();
                current = newCurrent;
            }
            return current;
        }
    }

    /**
     * @param theNode
     * @return
     */
    private Vector<SqlExpression> GetListOfColumnsForNode(RelationNode theNode) {
        Vector<SqlExpression> theResult = new Vector<SqlExpression>();
        if (theNode.getNodeType() == RelationNode.TABLE) {
            for (SysColumn el1 : commandToExecute.getClientContext()
                    .getSysDatabase().getSysTable(
                            theNode.getTableName()).getColumns()) {
                if (!el1.getColName().equalsIgnoreCase(
                        SqlCreateTableColumn.XROWID_NAME)) {
                    SqlExpression exp = new SqlExpression();
                    exp.setExprType(SqlExpression.SQLEX_COLUMN);
                    exp.setAlias(el1.getColName().toUpperCase());
                    AttributeColumn col = new AttributeColumn();
                    col.columnAlias = el1.getColName();
                    col.columnName = el1.getColName();
                    col.setTableAlias(theNode.getAlias());
                    col.setTableName(theNode.getTableName());
                    col.rebuildString();
                    exp.setColumn(col);
                    exp.rebuildExpression();
                    theResult.add(exp);
                }
            }
        } else {
             theResult.addAll(theNode.getSubqueryTree().getProjectionList());
        }
        return theResult;
    }
}
