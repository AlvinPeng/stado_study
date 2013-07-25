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

import java.util.Observable;
import java.util.Stack;
import java.util.Vector;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QuerySubTreeHelper;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.core.CSQLParserConstants;
import org.postgresql.stado.parser.core.syntaxtree.ExistsClause;
import org.postgresql.stado.parser.core.syntaxtree.IsBooleanClause;
import org.postgresql.stado.parser.core.syntaxtree.IsNullClause;
import org.postgresql.stado.parser.core.syntaxtree.INode;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.Relop;
import org.postgresql.stado.parser.core.syntaxtree.SQLAndExp;
import org.postgresql.stado.parser.core.syntaxtree.SQLAndExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLBetweenClause;
import org.postgresql.stado.parser.core.syntaxtree.SQLComplexExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLCondResult;
import org.postgresql.stado.parser.core.syntaxtree.SQLInClause;
import org.postgresql.stado.parser.core.syntaxtree.SQLLikeClause;
import org.postgresql.stado.parser.core.syntaxtree.SQLORExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLRelationalExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLRelationalOperatorExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLUnaryLogicalExpression;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;

/**
 * This class is responsible to Handle a Query Condition
 */
public class QueryConditionHandler extends DepthFirstRetArguVisitor {
    private static final XLogger logger = XLogger.getLogger(QueryConditionHandler.class);

    public QueryCondition aRootCondition;

    private Command commandToExecute;

    private boolean handleLeftOuterNeeded;

    private boolean handleInnerJoinNeeded;

    /**
     * List containing query conditions associated with left outer(+)
     */
    public Vector<QueryCondition> queryCondList = new Vector<QueryCondition>();

    /**
     * List containing query conditions associated with inner join within left outer(+)
     */
    public Vector<QueryCondition> innerQueryCondList = new Vector<QueryCondition>();

    /**
     * approach rebuild string
     */
    private Stack<QueryCondition> sCurrent = new Stack<QueryCondition>();

    public QueryConditionHandler(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
    }

    public boolean isHandleLeftOuterNeeded() {
        return handleLeftOuterNeeded;
    }

    public void setHandleLeftOuterNeeded(boolean handleLeftOuterNeeded) {
        this.handleLeftOuterNeeded = handleLeftOuterNeeded;
    }

    public boolean isHandleInnerJoinNeeded() {
        return handleInnerJoinNeeded;
    }

    public void setHandleInnerJoinNeeded(boolean handleInnerJoinNeeded) {
        this.handleInnerJoinNeeded = handleInnerJoinNeeded;
    }

    /*
     * The following functions will be declared here for proper handling This
     * handler will yeild one single query condition
     *  -- The Query Condition itself is nothing but the expansion of SQLComplex
     * Expression We however dont have to catch the SQLComplex expression
     * instead we have to catch -SQLAndExpression -SQLORExpression
     *
     */
    /**
     * f0 -> SQLUnaryLogicalExpression(prn) f1 -> ( SQLAndExp(prn) )*
     */

    /**
     * f0 -> SQLAndExpression(prn) f1 -> ( SQLORExpression(prn) )*
     */
    @Override
    public Object visit(SQLComplexExpression n, Object argu) {
        /*
         * Protocol - We will follow GIVE TO GET Therefore it is the
         * responsibilty of the calling function to provide the callee with all
         * the arguments
         */
        /*
         * Set the starting Query Condition - the function also adds the Query
         * Condition to the stack. The stack basically helps th QueryCondition
         * handler to keep track of the current query condition that it is
         * working on.-- I have used it to fill condString Variable
         */
        QueryCondition qc = createQueryCondition();

        sCurrent.push(qc);
        // -- Stack Logic

        /* Create the helper object for this object -- */
        QueryConditionHelper qh = new QueryConditionHelper(qc);

        /* The qc- blank is now set in the Query CionditionHelper */

        /* First Call to SQLAndExpression -- Might lead to multiple calls to it */
        n.f0.accept(this, qh);
        /* First Call to SQLOR */
        n.f1.accept(this, qh);

        /*
         * We Should Now have the complete Tree-- So set it in the aRoot which
         * can now be picked by the calling function
         */
        aRootCondition = (QueryCondition) qh.getArgument();
        // Pop out the last Query Condition
        sCurrent.pop();
        return null;

    }

    /**
         * This function will get called from 2 Places a) SQLComplex Expressions
         * b) SQLORExpression
         *
         * PreCondition -- It is the responsibilty of the calling function to
         * provide a QueryCondition Object embedded in the QueryConditionHelper
     * Object.
         *
     * f0 -> SQLUnaryLogicalExpression(prn)
     * f1 -> [ <LEFTOUTER_> ]
     * f2 -> ( SQLAndExp(prn) )*
         */
    @Override
    public Object visit(SQLAndExpression n, Object argu) {
        QueryConditionHelper qh = (QueryConditionHelper) argu;

        n.f0.accept(this, qh);
        // This is a call to unary logical expression -- The query Condtion that
        // was set earlier in
        // SQLComplexExpression will be used by UnaryLogical Expression to fill
        // and return.
        // The unary logical may change the query condition object and will
        // provide with the
        // root.
        if (n.f1.present()) {
            handleLeftOuterNeeded = true;
            queryCondList.add((QueryCondition) qh.getArgument());
        } else {
            if (((QueryCondition) qh.getArgument()).getRightCond() != null
                    && ((QueryCondition) qh.getArgument()).getRightCond().getExpr() != null
                    && ((QueryCondition) qh.getArgument()).getRightCond().getExpr().getExprType() == SqlExpression.SQLEX_COLUMN) {
                handleInnerJoinNeeded = true;
                innerQueryCondList.add((QueryCondition) qh.getArgument());
            }
        }
        n.f2.accept(this, qh);

        // The root of the expression returned by unary logical is sent to
        // SQLAndExp -- where it juggles
        // and sets the "AND" as the root and returns.

        return null;
    }

    /**
     * f0 -> <AND_> f1 -> SQLUnaryLogicalExpression(prn) f2 -> [ <LEFTOUTER_> ]
     */
    @Override
    public Object visit(SQLAndExp n, Object argu) {
        QueryConditionHelper aqueryhandler = (QueryConditionHelper) argu;

        // -- This is the Query Condition of type condition
        // When we encounter another QueryCondtion of this type we remove any
        // other element
        // in the stack and make it the fore most
        QueryCondition stactop = sCurrent.pop();

        QueryCondition qc = createQueryCondition();

        qc.setCondString(qc.getCondString() + stactop.getCondString());

        // The following fields are to be filled -- I will move this into the
        // query condition
        // class to increase localization of code
        qc.setOperator("AND");
        qc.setCondType(QueryCondition.QC_CONDITION);
        qc.setLeftCond((QueryCondition) aqueryhandler.getArgument());
        // --- Add the Query Condition to the stack
        sCurrent.push(qc);

        // Before calling

        // Dummy Call--We can get away with this but just to keep the quorum
        n.f0.accept(this, argu);

        // Now we work for the right branch
        QueryCondition qcr = createQueryCondition();

        sCurrent.push(qcr);

        QueryConditionHelper qhr = new QueryConditionHelper(qcr);

        n.f1.accept(this, qhr);

        qc.setRightCond((QueryCondition) qhr.getArgument());
        if (n.f2.present()) {
            if (!handleLeftOuterNeeded) {
                handleLeftOuterNeeded = true;
            }
            queryCondList.add(qc.getRightCond());
        } else {
            if (qc.getRightCond().getRightCond() != null
                    && qc.getRightCond().getRightCond().getExpr() != null
                    && qc.getRightCond().getRightCond().getExpr().getExprType() == SqlExpression.SQLEX_COLUMN) {
                if (!handleInnerJoinNeeded) {
                    handleInnerJoinNeeded = true;
                }
                innerQueryCondList.add(qc.getRightCond());
            }
        }
        // Set the node in the handler to the new ly formed operator node
        ((QueryConditionHelper) argu).setArgument(qc);

        qc.setCondString(qc.getCondString()
                + ((QueryCondition) qhr.getArgument()).getCondString());

        // pop out the Right Side
        sCurrent.pop();

        return null;
    }

    /**
     * f0 -> <OR_> f1 -> SQLAndExpression(prn)
     */
    @Override
    public Object visit(SQLORExpression n, Object argu) {
        QueryConditionHelper aqueryhandler = (QueryConditionHelper) argu;

        QueryCondition qc = createQueryCondition();
        QueryCondition stackTop = sCurrent.pop();
        qc.setCondString(stackTop.getCondString() + qc.getCondString());
        sCurrent.push(qc);
        // The following fileds are to be filled -- I will move this into the
        // query condition
        // class to increase localization of code

        qc.setOperator("OR");
        qc.setCondType(QueryCondition.QC_CONDITION);
        qc.setLeftCond((QueryCondition) aqueryhandler.getArgument());

        n.f0.accept(this, argu);

        // Now we work for the right branch

        QueryCondition qcr = createQueryCondition();
        sCurrent.push(qcr);

        QueryConditionHelper qhr = new QueryConditionHelper(qcr);
        n.f1.accept(this, qhr);
        qc.setRightCond((QueryCondition) qhr.getArgument());

        // Set the node in the handler to the newly formed operator node
        ((QueryConditionHelper) argu).setArgument(qc);

        qc.setCondString(qc.getCondString() + qcr.getCondString());
        sCurrent.pop();
        return null;
    }

    /**
     *
     * f0 -> ( ExistsClause(prn) | [ <NOT_> ] SQLCondResult(prn) | ( [ <NOT_> ]
     * SQLRelationalExpression(prn) ) )
     *
     * The argu actually has a unfilled QueryCondition object- coming down from
     * SQLAndExpression-- and the only reason why are catch ing this is becasue
     * we are expecting -- NOT token to be caught here.
     *
     * TODO: In case we do catch NOT -- we would like to set it in the incoming
     * query condition
     *
     */
    @Override
    public Object visit(SQLUnaryLogicalExpression n, Object argu) {

        // TODO: I am not taking care of exists clause -- at this moment -
        // the reason is that we dont have a mechanism to look into this.
        // So the call will lead to a call to SQLRelationalExpression
        n.f0.accept(this, argu);
        // Handle optional NOT after handling of the remaining expression:
        // some handlers are replacing the QueryCondition in the helper class
        switch (n.f0.which) {
        case 0:
            break;
        case 1:
        case 2:
            NodeSequence aNodeSequence = (NodeSequence) n.f0.choice;
            NodeOptional aNodeOptional = (NodeOptional) aNodeSequence.elementAt(0);
            if (aNodeOptional.present()) {
                QueryConditionHelper qh = (QueryConditionHelper) argu;
                QueryCondition aQueryCondition = (QueryCondition) qh.getArgument();
                aQueryCondition.setPositive(false);
            }
            break;
        }

        return null;
    }

    @Override
    public Object visit(SQLCondResult n, Object argu) {
        // we will explicitly create a new query condition for this case
        // When it is true we will have 1 = 1 and if it is false we will
        // have 1 <> 1

        QueryConditionHelper qh = (QueryConditionHelper) argu;
        QueryCondition left = (QueryCondition) qh.getArgument();
        left.setCondType(QueryCondition.QC_SQLEXPR);

        SqlExpression rsql = new SqlExpression();
        rsql.setRightExpr(null);
        rsql.setLeftExpr(null);
        rsql.setConstantValue("1");
        rsql.setExprType(SqlExpression.SQLEX_CONSTANT);

        left.setExpr(rsql);

        QueryCondition right = new QueryCondition();
        right.setCondType(QueryCondition.QC_SQLEXPR);

        right.setExpr(rsql);
        QueryCondition main = new QueryCondition();

        main.setLeftCond(left);
        main.setRightCond(right);

        String operator = "";
        if (n.f0.which == 0) {
            operator = "=";
        } else {
            operator = "<>";
        }
        main.setOperator(operator);
        main.setCondType(QueryCondition.QC_RELOP);
        qh.setArgument(main);
        return null;
    }

    /**
     * f0 -> ( SQLExpressionList(prn) | SQLSimpleExpression(prn) ) f1 -> (
     * SQLRelationalOperatorExpression(prn) | ( SQLInClause(prn) |
     * SQLBetweenClause(prn) | SQLLikeClause(prn) ) | IsNullClause(prn) )?
     */
    @Override
    public Object visit(SQLRelationalExpression n, Object argu) {
        /*
         * we have a node choice embedded in the nodesequence-- There is
         * always only one node The reason we ended up with the nodesequence is
         * beacause we put parenthesis around f0.
         */

        QueryConditionHelper sqlhelper = (QueryConditionHelper) argu;

        switch (n.f0.which) {
        case 0:
            /*
             * In this case we expect a SQLExpression List- TODO: No support for
             * this at this point in time - Need to work on this
             */
            n.f0.accept(this, argu);
            break;
        case 1:
            /* Where as in this case I expect a SQLExpression */
            SQLExpressionHandler sqlh = new SQLExpressionHandler(
                    commandToExecute);
            n.f0.accept(sqlh, argu);
            // Once we have found the expression -- we need to check if we have
            // a condition as expression or expression
            // as expression for eg. where (a > b) -- will be recoginsed as a
            // sqlexpression eventhough it is a condition
            if (sqlh.aroot.getExprType() == SqlExpression.SQLEX_CONDITION) {
                sqlhelper.setArgument(sqlh.aroot.getQueryCondition());
            } else {
                ((QueryCondition) sqlhelper.getArgument()).setExpr(sqlh.aroot);
                ((QueryCondition) sqlhelper.getArgument()).setCondType(QueryCondition.QC_SQLEXPR);
                ((QueryCondition) sqlhelper.getArgument()).setCondString(sqlh.aroot.getExprString());
                if (sqlh.aroot.getExprType() == SqlExpression.SQLEX_SUBQUERY) {
                    sqlh.aroot.getSubqueryTree().setQueryType(QueryTree.SCALAR);
                }
            }
            break;
        default:
            break;
        }

        /*
         * The out put from the above - ispassed on to f1 for processing where
         * it will actually get a operator -- node
         */
        n.f1.accept(this, sqlhelper);
        // The operator node has done its work 0-- We should be all set now --
        // both the left and the
        // right side are filled

        return null;
    }

    /**
     * f0 -> Relop(prn) f1 -> ( ( [ "ALL" | "ANY" | "SOME" ] "(" SubQuery(prn)
     * ")" ) | SQLSimpleExpression(prn) )
     */

    /**
     * Grammar production: f0 -> Relop(prn) f1 -> ( ( [ "ALL" | "ANY" | "SOME" ] (
     * "(" SubQuery(prn) ")" | "(" SQLExpressionList(prn) ")" ) ) |
     * SQLSimpleExpression(prn) )
     */

    @Override
    public Object visit(SQLRelationalOperatorExpression n, Object argu) {
        /*
         * Pre condition : A new Query Condition Helper Object is present in the
         * argu - argument The Argument in this Object is set to QueryCondition.
         *
         * This will be filled Query condition will actually have a Filled Query
         * Condition which should be of type EXPR and will be set to Left of the
         * new created Query Condition
         */
        QueryConditionHelper qh = (QueryConditionHelper) argu;

        if (qh == null || qh.getArgument() == null) {
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_FUNCTION_CALL
                            + "(  visit (SQLRElationalOperator"
                            + "Expression) ) ", 0,
                    ErrorMessageRepository.ILLEGAL_FUNCTION_CALL_CODE);
        }

        // Create a new Query Condition and a helper to forward it to relop
        QueryCondition aQueryCondForOper = createQueryCondition();

        QueryCondition stop = sCurrent.pop();

        aQueryCondForOper.setCondString(stop.getCondString()
                + aQueryCondForOper.getCondString());

        sCurrent.push(aQueryCondForOper);

        QueryConditionHelper aQueryConditionHelper = new QueryConditionHelper(
                aQueryCondForOper);
        // Set the Operator Value -- This call should yeild a call to Relop.
        n.f0.accept(this, aQueryConditionHelper);
        // Get the Query Condition - This should have set the operator
        QueryCondition qc = (QueryCondition) aQueryConditionHelper.getArgument();
        // Now -- we need to set the right arm of this queryCondition with a
        // Query Condition which is of type
        // QueryExpression
        QueryCondition rightQC = createQueryCondition();

        sCurrent.push(rightQC);
        SqlExpression sqlexpr = null;

        String anyAllString = null;
        boolean anyAllFlag = false;
        switch (n.f1.which) {
        case 0:
            NodeSequence aSequence = (NodeSequence) n.f1.choice;
            NodeOptional aOptional = (NodeOptional) aSequence.elementAt(0);
            boolean isScalarSubQuery = true;
            // The clause ANY / ALL tells us that we will have a
            // Non Scalar Query. here Therefore if the node is present
            // we set the isScalar subquery to false.
            if (aOptional.present()) {
                // Then we have a Scalar Query - These queries are of 2 types
                // a.correlated and non-correlated
                isScalarSubQuery = false;
                // Also we need to set the flag when the query will be
                // regenerated to set the any / and all.
                NodeChoice aChoice = (NodeChoice) aOptional.node;
                switch (aChoice.which) {
                case 0:
                    anyAllString = "" + "ALL" + " ";
                    break;
                case 1:
                    anyAllString = "" + "ANY " + " ";
                    break;
                case 2:
                    anyAllString = "" + "SOME " + " ";
                    break;
                default:

                }
                anyAllFlag = true;
            } else {
                // We have a non scalar query - For Scalar SubQueries
                isScalarSubQuery = true;
            }
            NodeChoice exprListOrSubQuery = (NodeChoice) aSequence.elementAt(1);

            if (exprListOrSubQuery.which == 0) {
                QueryTree aSubQueryTree = new QueryTree();
                QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                        commandToExecute);
                NodeSequence aSubQuerySeq = (NodeSequence) exprListOrSubQuery.choice;
                INode sqlSubQuery = aSubQuerySeq.elementAt(1);
                sqlSubQuery.accept(aQueryTreeHandler, aSubQueryTree);
                sqlexpr = new SqlExpression();

                if (System.getProperty("TrackSQLExpression") != null
                        && System.getProperty("TrackSQLExpression").equals("1")) {
                    logger.debug("Optimizer.java  3141  : "
                            + sqlexpr.toString());
                }

                sqlexpr.setExprType(SqlExpression.SQLEX_SUBQUERY);
                sqlexpr.setSubqueryTree(aSubQueryTree);

                // Incase the query is scalar we set the value of querytype to
                // scalar and call process query tree.Similarly if it were non
                // scalar.
                if (isScalarSubQuery == true) {
                    aSubQueryTree.setQueryType(QueryTree.SCALAR);
                } else {
                    aSubQueryTree.setQueryType(QueryTree.NONSCALAR);
                }

                if (commandToExecute.getCommandToExecute() == Command.SELECT) {
                    QuerySubTreeHelper aQuerySubTreeHelper = aSubQueryTree.processSubTree(
                            sqlexpr, commandToExecute.getaQueryTreeTracker());
                    // Now check if we have generated a node
                    if (aQuerySubTreeHelper.createdRelationNode != null) {
                        rightQC.getRelationNodeList().add(aQuerySubTreeHelper.createdRelationNode);
                    }

                    if (aQuerySubTreeHelper.projectedSqlExpression != null) {
                        rightQC.setProjectedColumns(aQuerySubTreeHelper.projectedSqlExpression);
                    }
                    if (aQuerySubTreeHelper.correlatedColumnExprList != null) {
                        rightQC.setCorrelatedColumns(aQuerySubTreeHelper.getCorrelatedColumnAttributed());
                    }
                }
            } else {
                // We have a Expression List

                SQLExpressionListHandler aExprList = new SQLExpressionListHandler(
                        commandToExecute);
                NodeSequence aSubQuerySeq = (NodeSequence) exprListOrSubQuery.choice;
                INode sqlExprList = aSubQuerySeq.elementAt(1);
                sqlExprList.accept(aExprList, argu);
                sqlexpr = new SqlExpression();
                sqlexpr.setExprType(SqlExpression.SQLEX_LIST);
                sqlexpr.setExpressionList(aExprList.vSqlExpressionList);
            }

            if (anyAllFlag == true) {
                rightQC.setAnyAllFlag(anyAllFlag);
                rightQC.setAnyAllString(anyAllString);
            }

            break;
        case 1:
            // SQL Expression -- Implemented

            SQLExpressionHandler sqlh = new SQLExpressionHandler(
                    commandToExecute);
            n.f1.accept(sqlh, argu);
            sqlexpr = sqlh.aroot;
            break;
        default:
            break;
        }

        if (sqlexpr == null) {
            throw new XDBServerException(
                    ErrorMessageRepository.COMMAND_NOT_IMPLEMENTED, 0,
                    ErrorMessageRepository.COMMAND_NOT_IMPLEMENTED_CODE);
        }
        // Set the rest of the parameters --
        rightQC.setCondType(QueryCondition.QC_SQLEXPR);
        rightQC.setExpr(sqlexpr);
        rightQC.setCondString(sqlexpr.getExprString());
        qc.setRightCond(rightQC);
        qc.setLeftCond((QueryCondition) qh.getArgument());
        qh.setArgument(qc);

        sCurrent.pop();
        qc.setCondString(qc.getCondString() + rightQC.getCondString());

        // -- Now we will set the expression -- this one will be a Query
        // Expression
        return null;
    }

    /**
     * f0 -> ( "=" | "!=" | "#" | "<>" | ">" | ">=" | "<" | "<=" )
     */
    @Override
    public Object visit(Relop n, Object argu) {
        /*
         * Pre Condition - argu - Must be intiallized to a Query Condition
         * Helper The Query Condition helper argument should not be null
         */
        // Get the QueryCondition --
        QueryConditionHelper aqueryhandler = (QueryConditionHelper) argu;
        QueryCondition qc = (QueryCondition) aqueryhandler.getArgument();

        // Set the operator
        qc.setOperator(((NodeToken) n.f0.choice).tokenImage);
        qc.setCondType(QueryCondition.QC_RELOP);
        // Dummy Call
        n.f0.accept(this, argu);

        return null;
    }

    /**
     * f0 -> [ <NOT_> ] f1 -> <IN_> f2 -> ( "(" SQLExpressionList(prn) | "("
     * SubQuery(prn) ) f3 -> ")"
     */

    @Override
    public Object visit(SQLInClause n, Object argu) {
        n.f0.accept(this, argu);
        boolean isPositive = true;

        if (n.f0.present()) {
            isPositive = false;
        }
        // Not required
        n.f1.accept(this, argu);
        // Extract the QueryCondition
        QueryConditionHelper qh = (QueryConditionHelper) argu;
        QueryCondition qc = (QueryCondition) qh.getArgument();

        SqlExpression toCheckWith = null;
        // We should can get a SQLExperssion or a SQL Expression List
        // but we are sure to get a SQL Expression :
        // /TODO - check out if a expression list can be used in a between
        // clause
        if (qc.getCondType() == QueryCondition.QC_SQLEXPR) {
            toCheckWith = qc.getExpr();
        }

        Vector exprList = null;
        switch (n.f2.which) {
        case 1:
            SQLExpressionListHandler aSqlExpressionListHandler = new SQLExpressionListHandler(
                    commandToExecute);
            n.f2.accept(aSqlExpressionListHandler, argu);
            exprList = aSqlExpressionListHandler.vSqlExpressionList;
            qc.setACompositeClause(qc.new InClauseList(exprList, toCheckWith,
                    isPositive, commandToExecute));
            break;
        case 0:

            QueryTree aSubQueryTree = new QueryTree();
            QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                    commandToExecute);
            n.f2.accept(aQueryTreeHandler, aSubQueryTree);
            qc.setACompositeClause(qc.new InClauseTree(aSubQueryTree,
                    toCheckWith, isPositive, commandToExecute));
            break;
        default:
        }
        n.f3.accept(this, argu);
        qc.setCondType(QueryCondition.QC_COMPOSITE);
        qh.setArgument(qc);
        qc.setCondString(qc.getACompositeClause().rebuildString());
        return null;
    }

    /**
     * f0 -> [ <NOT_> ] f1 -> "BETWEEN" f2 -> SQLSimpleExpression(prn) f3 ->
     * "AND" f4 -> SQLSimpleExpression(prn)
     */
    @Override
    public Object visit(SQLBetweenClause n, Object argu) {
        // Extract the QueryCondition
        QueryConditionHelper qh = (QueryConditionHelper) argu;
        QueryCondition qc = (QueryCondition) qh.getArgument();
        SqlExpression toCheckWith = null;
        // We should can get a SQLExperssion or a SQL Expression List
        // but we are sure to get a SQL Expression
        // TODO - check out if a expression list can be used in a between
        // clause
        if (qc.getCondType() == QueryCondition.QC_SQLEXPR) {
            toCheckWith = qc.getExpr();
        }
        // Set it to positive by default
        boolean isPositive = true;
        // The between Node Token.
        n.f0.accept(this, argu);
        // Check to see if the boolean is positive
        if (n.f0.present()) {
            isPositive = false;
        }
        // Ignore this is not important
        n.f1.accept(this, argu);
        // Create a new SQLExpression handler
        SQLExpressionHandler aSQLExpressionHandler = new SQLExpressionHandler(
                commandToExecute);

        n.f2.accept(aSQLExpressionHandler, argu);
        // Gather the SQL Expression
        SqlExpression aSqlExpression = aSQLExpressionHandler.aroot;
        // The AND Token
        n.f3.accept(this, argu);
        // Another Expression
        SQLExpressionHandler aSQLExpressionHandler2 = new SQLExpressionHandler(
                commandToExecute);
        // Call the expression
        n.f4.accept(aSQLExpressionHandler2, argu);
        // Gather the SQL Expresion
        SqlExpression aSqlExpression2 = aSQLExpressionHandler2.aroot;
        qc.setACompositeClause(qc.new BetweenClause(aSqlExpression,
                aSqlExpression2, toCheckWith, isPositive, commandToExecute));
        qc.setCondType(QueryCondition.QC_COMPOSITE);
        qh.setArgument(qc);
        qc.setCondString(qc.getACompositeClause().rebuildString());
        return null;
    }

    /**
     * Grammar production: f0 -> ( [ <NOT_> ] ( <LIKE_> | <ILIKE_> |
     * <SIMILAR_TO_> ) SQLSimpleExpression(prn) [ <ESCAPE_>
     * SQLSimpleExpression(prn) ] | ( "~" | "!~" | "~*" | "!~*" )
     * SQLSimpleExpression(prn) )
     */

    @Override
    public Object visit(SQLLikeClause n, Object argu) {
        // We will be surely getting a SqlExpression Condition from the caller

        QueryConditionHelper qh = (QueryConditionHelper) argu;
        QueryCondition qc = (QueryCondition) qh.getArgument();
        SqlExpression toCheckWith = null;
        if (qc.getCondType() == QueryCondition.QC_SQLEXPR) {
            toCheckWith = qc.getExpr();
        }
        boolean isPositive = true;
        String operator = null;
        SqlExpression aSqlExpression = null;
        SqlExpression aSqlExpressionEscape = null;

        NodeSequence nseq = (NodeSequence) n.f0.choice;
        if (n.f0.which == 0) {
            if (((NodeOptional) nseq.elementAt(0)).present()) {
                isPositive = false;
            }
            operator = ((NodeToken) ((NodeChoice) nseq.elementAt(1)).choice).tokenImage;
            SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                    commandToExecute);
            nseq.elementAt(2).accept(aSqlExpressionHandler, argu);
            aSqlExpression = aSqlExpressionHandler.aroot;
            NodeOptional nf3 = (NodeOptional) nseq.elementAt(3);
            if (nf3.present()) {
                SQLExpressionHandler aSqlExpressionHandlerEscape = new SQLExpressionHandler(
                        commandToExecute);
                NodeSequence ns = (NodeSequence) nf3.node;
                ns.elementAt(1).accept(aSqlExpressionHandlerEscape, argu);
                aSqlExpressionEscape = aSqlExpressionHandlerEscape.aroot;
            }
        } else // n.f0.which == 1
        {
            operator = ((NodeToken) ((NodeChoice) nseq.elementAt(0)).choice).tokenImage;
            SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                    commandToExecute);
            nseq.elementAt(1).accept(aSqlExpressionHandler, argu);
            aSqlExpression = aSqlExpressionHandler.aroot;
        }
        qc.setACompositeClause(qc.new CLikeClause(toCheckWith, aSqlExpression,
                isPositive, operator, commandToExecute, aSqlExpressionEscape));
        return null;
    }

    /**
     * f0 -> ( "IS" [ <NOT_> ] | "=" | "!=" ) f1 -> "NULL"
     */
    @Override
    public Object visit(IsNullClause n, Object argu) {
        QueryConditionHelper qh = (QueryConditionHelper) argu;
        QueryCondition qc = (QueryCondition) qh.getArgument();
        SqlExpression toCheckWith = null;
        if (qc.getCondType() == QueryCondition.QC_SQLEXPR) {
            toCheckWith = qc.getExpr();
        }

        boolean isPositive = true;
        switch (n.f0.which) {
        case 0:
            NodeSequence aNodeSeq = (NodeSequence) n.f0.choice;
            NodeOptional aNodeOpt = (NodeOptional) aNodeSeq.elementAt(1);
            if (aNodeOpt.present()) {
                isPositive = false;
            }
            break;
        case 1:
            isPositive = true;
            break;

        case 2:
            isPositive = false;
            break;
        }
        qc.setACompositeClause(qc.new CheckNullClause(toCheckWith, isPositive, commandToExecute));

        return null;
    }
    

    /**
     * f0 -> ( "IS" [ <NOT_> ] | "=" | "!=" )
     * f1 -> ( "TRUE" | "FALSE" )
     */
    @Override
    public Object visit(IsBooleanClause n, Object argu) {
        Object _ret=null;
        /*CheckBooleanClause*/
        QueryConditionHelper qh = (QueryConditionHelper) argu;
        QueryCondition qc = (QueryCondition) qh.getArgument();
        SqlExpression toCheckWith = null;
        if (qc.getCondType() == QueryCondition.QC_SQLEXPR) {
            toCheckWith = qc.getExpr();
        }

        boolean isPositive = true;
        switch (n.f0.which) {
        case 0:
            NodeSequence aNodeSeq = (NodeSequence) n.f0.choice;
            NodeOptional aNodeOpt = (NodeOptional) aNodeSeq.elementAt(1);
            if (aNodeOpt.present()) {
                isPositive = false;
            }
            break;
        case 1:
            isPositive = true;
            break;
        case 2:
            isPositive = false;
            break;
        }

        boolean value = true;
        switch (n.f1.which) {
        case 1:
            value = false;
        }
        qc.setACompositeClause(qc.new CheckBooleanClause(toCheckWith, isPositive,
                value, commandToExecute));
       return _ret;
    }

    /**
     *
     * This function is used to create Query Condition only in this particualar
     * class It helps the class to keep track of the current Query Condition it
     * is working on
     */
    private QueryCondition createQueryCondition() {
        QueryCondition qc = new QueryCondition();
        return qc;
    }

    @Override
    public Object visit(NodeToken n, Object argu) {
        switch (n.kind) {
        case CSQLParserConstants.STAR_:

            // TODO case : I have to take care of case <IDENTIFIERNAME>.<STAR>
            // -- The basic reason for this is that
            // we will be getting a large number of IDENTIFIERS.<STAR> -- will
            // be better if I make a class of
            // expandable objects and handle it there.
        }
        MakeExpressionString(n.tokenImage);
        return null;
    }

    /**
     * The Query Condition maintains a stack which is used for maintaining the
     * conditionStrinig.
     *
     * @param exprToken
     */
    private void MakeExpressionString(String exprToken) {
        try {
            QueryCondition qc = sCurrent.peek();
            qc.setCondString("" + qc.getCondString() + " " + exprToken);
        } catch (Exception ex) {
            // This implies that we have not pushed any expression -- on the
            // stack and we have got some
            // element -- The <STAR_> <IDNAME.star> are caught here
            throw new XDBServerException(
                    ErrorMessageRepository.IMBALANCE_IN_EXPRESSION_STACK, 0,
                    ErrorMessageRepository.IMBALANCE_IN_EXPRESSION_STACK_CODE);
        }
    }

    /* In order to get expression string being added to SQLExpression */
    /**
     *
     * @param condition
     */
    public void updateQuery(String condition) {
        MakeExpressionString(condition);
    }

    /**
     * This function informs all the observers that there has been a change in
     * the class
     *
     * @param obs
     * @param obj
     */
    public void update(Observable obs, Object obj) {
        updateQuery((String) obj);
    }

    /**
     * f0 -> [ <NOT_> ] f1 -> "EXISTS" f2 -> "(" f3 -> SubQuery(prn) f4 -> ")"
     */
    @Override
    public Object visit(ExistsClause n, Object argu) {
        // Extract the QueryCondition
        QueryConditionHelper qh = (QueryConditionHelper) argu;
        QueryCondition qc = (QueryCondition) qh.getArgument();
        n.f0.accept(this, argu);
        boolean isPositive = true;
        if (n.f0.present()) {
            isPositive = false;
        }
        n.f1.accept(this, argu);
        n.f2.accept(this, argu);
        QueryTree aSubQueryTree = new QueryTree();
        aSubQueryTree.setPartOfExistClause(true);
        QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                commandToExecute);

        n.f3.accept(aQueryTreeHandler, aSubQueryTree);
        n.f4.accept(this, argu);
        qc.setACompositeClause(qc.new ExistClause(aSubQueryTree, isPositive,
                commandToExecute));

        qc.setCondType(QueryCondition.QC_COMPOSITE);
        qh.setArgument(qc);
        qc.setCondString(qc.getACompositeClause().rebuildString());

        return null;
    }

}