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

import java.util.List;

import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.core.syntaxtree.CidrLiteral;
import org.postgresql.stado.parser.core.syntaxtree.DateLiteral;
import org.postgresql.stado.parser.core.syntaxtree.FloatingPointNumber;
import org.postgresql.stado.parser.core.syntaxtree.FunctionCall;
import org.postgresql.stado.parser.core.syntaxtree.GeometryLiteral;
import org.postgresql.stado.parser.core.syntaxtree.InetLiteral;
import org.postgresql.stado.parser.core.syntaxtree.IntegerLiteral;
import org.postgresql.stado.parser.core.syntaxtree.IntervalLiterals;
import org.postgresql.stado.parser.core.syntaxtree.IsNullExpression;
import org.postgresql.stado.parser.core.syntaxtree.MacaddrLiteral;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.NullLiterals;
import org.postgresql.stado.parser.core.syntaxtree.PreparedStmtParameter;
import org.postgresql.stado.parser.core.syntaxtree.PseudoColumn;
import org.postgresql.stado.parser.core.syntaxtree.SQLComplexExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLPrecedenceLevel1Expression;
import org.postgresql.stado.parser.core.syntaxtree.SQLPrecedenceLevel1Operand;
import org.postgresql.stado.parser.core.syntaxtree.SQLPrecedenceLevel2Expression;
import org.postgresql.stado.parser.core.syntaxtree.SQLPrecedenceLevel2Operand;
import org.postgresql.stado.parser.core.syntaxtree.SQLPrecedenceLevel3Expression;
import org.postgresql.stado.parser.core.syntaxtree.SQLPrecedenceLevel3Operand;
import org.postgresql.stado.parser.core.syntaxtree.SQLPrimaryExpression;
import org.postgresql.stado.parser.core.syntaxtree.SQLSimpleExpression;
import org.postgresql.stado.parser.core.syntaxtree.TableColumn;
import org.postgresql.stado.parser.core.syntaxtree.TextLiterals;
import org.postgresql.stado.parser.core.syntaxtree.TimeLiteral;
import org.postgresql.stado.parser.core.syntaxtree.TimeStampLiteral;
import org.postgresql.stado.parser.core.syntaxtree.binaryLiteral;
import org.postgresql.stado.parser.core.syntaxtree.booleanLiteral;
import org.postgresql.stado.parser.core.syntaxtree.extendbObject;
import org.postgresql.stado.parser.core.syntaxtree.hex_decimalLiteral;
import org.postgresql.stado.parser.core.syntaxtree.numberValue;
import org.postgresql.stado.parser.core.syntaxtree.stringLiteral;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;

/**
 * This class provides the required functionality to manipulate and fill up
 * the information via calls to visit() method series for SQLExpression object
 * as the expression is processed in the parser phase.
 */

public class SQLExpressionHandler extends DepthFirstRetArguVisitor {
    private Command commandToExecute;

    public SqlExpression aroot = null;

    SysDatabase database = null;

    public SQLExpressionHandler(Command command) {
        this.commandToExecute = command;
        database = commandToExecute.getClientContext().getSysDatabase();
    }

    /**
     * Grammar production: 
     * f0 -> SQLPrecedenceLevel1Expression(prn) 
     * f1 -> ( SQLPrecedenceLevel1Operand(prn) )*
     * 
     * This visitor is an entry point to construct a tree of objects 
     * representing expression. The underlying grammar rules are organized
     * as multiple levels looking like LevelNExpression ( LevelNOperand )*,
     * where LevelNOperand is defined as Operator LevelN-1Expression.
     * This implements precedence of operators, operators listed in higher level
     * take priority.
     * The approach allows to reorganize operators by introducing new precedence 
     * levels and moving operators between levels with very little coding.
     * Among simple and straightforward modification of the grammar rules, the 
     * developer should add here in SQLExpressionHandler couple visitor 
     * functions following simple pattern.
     * The Expression handler should accept first operand, that should return
     * an SqlExplession representing first operand, and if there are Operands,
     * accept them passing in the first operand and using result as a first 
     * operand of the next accept. Last accept result is returned from the 
     * visitor function.
     * The Operand handler is responsible for grouping operations of the same
     * precedence level. Currently they construct a binary tree, but it is
     * possible if operands are accumulated within the same SqlExpression 
     * instance.
     * As far as all Operand handlers follow the same pattern and do not have
     * special handling for operators, developer does not have to change 
     * the code if moving operators between precedence levels. The only thing
     * to do is to move operator tokens between rules in the grammar file.
     * NB: Be careful with skipping visitors that should just accept their inner
     * fields. By default visitor returns null, that breaks passing 
     * SqlExpressions up along the call stack.  
     */
    @Override
    public Object visit(SQLSimpleExpression n, Object obj) {
        SqlExpression leftExpr = (SqlExpression) n.f0.accept(this, null);
        if (n.f1.present()) {
            for (Object node : n.f1.nodes) {
                SQLPrecedenceLevel1Operand operand;
                operand = (SQLPrecedenceLevel1Operand) node;
                leftExpr = (SqlExpression) operand.accept(this, leftExpr);
            }
        }
        aroot = leftExpr;
        return aroot;
    }

    /**
     * Grammar production: 
     * f0 -> SQLPrecedenceLevel2Expression(prn)
     * f1 -> ( SQLPrecedenceLevel2Operand(prn) )*
     */
    @Override
    public Object visit(SQLPrecedenceLevel1Expression n, Object obj) {
        SqlExpression leftExpr = (SqlExpression) n.f0.accept(this, null);
        if (n.f1.present()) {
            for (Object node : n.f1.nodes) {
                SQLPrecedenceLevel2Operand operand;
                operand = (SQLPrecedenceLevel2Operand) node;
                leftExpr = (SqlExpression) operand.accept(this, leftExpr);
            }
        }
        aroot = leftExpr;
        return aroot;
    }

    /**
     * Grammar production: 
     * f0 -> SQLPrecedenceLevel3Expression(prn)
     * f1 -> ( SQLPrecedenceLevel3Operand(prn) )*
     */
    @Override
    public Object visit(SQLPrecedenceLevel2Expression n, Object obj) {
        SqlExpression leftExpr = (SqlExpression) n.f0.accept(this, null);
        if (n.f1.present()) {
            for (Object node : n.f1.nodes) {
                SQLPrecedenceLevel3Operand operand;
                operand = (SQLPrecedenceLevel3Operand) node;
                leftExpr = (SqlExpression) operand.accept(this, leftExpr);
            }
        }
        aroot = leftExpr;
        return aroot;
    }

    /**
     * Grammar production:
     * f0 -> SQLPrimaryExpression(prn)
     */
    @Override
    public Object visit(SQLPrecedenceLevel3Expression n, Object obj) {
        aroot = (SqlExpression) n.f0.accept(this, obj);
        return aroot;
    }

    /**
     * Grammar production: f0 -> ( <PLUS_> | <MINUS_> | <CONCAT_> ) f1 ->
     * SQLPrecedenceLevel1Expression(prn)
     */
    @Override
    public Object visit(SQLPrecedenceLevel1Operand n, Object obj) {
        SqlExpression expr = new SqlExpression();
        SqlExpression leftExpr = (SqlExpression) obj;
        String operator = ((NodeToken) n.f0.choice).tokenImage;
        SqlExpression rightExpr = (SqlExpression) n.f1.accept(this, null);
        expr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
        expr.setLeftExpr(leftExpr);
        expr.setOperator(operator);
        expr.setRightExpr(rightExpr);
        return expr;
    }

    /**
     * Grammar production: f0 -> ( <STAR_> | <DIVIDE_> | <MOD_> | <DIV_> |
     * <MODULO_> | <AND_BITWISE_> | <OR_BITWISE_> | <XOR_BITWISE_> |
     * <SHIFT_LEFT_BITWISE_> | <SHIFT_RIGHT_BITWISE_> ) f1 ->
     * SQLPrecedenceLevel2Expression(prn)
     */
    @Override
    public Object visit(SQLPrecedenceLevel2Operand n, Object obj) {
        SqlExpression expr = new SqlExpression();
        SqlExpression leftExpr = (SqlExpression) obj;
        String operator = ((NodeToken) n.f0.choice).tokenImage;
        SqlExpression rightExpr = (SqlExpression) n.f1.accept(this, null);
        expr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
        expr.setLeftExpr(leftExpr);
        expr.setOperator(operator);
        expr.setRightExpr(rightExpr);
        return expr;
    }

    /**
     * Grammar production: f0 -> ( <CONTAINED_WITHIN_OR_EQUALS_> |
     * <CONTAINS_OR_EQUALS_> | <GREATER_> | <GREATER_EQUALS_> | <LESS_> |
     * <LESS_EQUALS_> | <EQUALS_> | <NOT_EQUALS_> | <NOT_EQUALS_2_> |
     * <GIS_OVERLAPS_> | <GIS_OVERLAPS_RIGHT_> | <GIS_OVERLAPS_LEFT_> |
     * <GIS_OVERLAPS_BELOW_> | <GIS_OVERLAPS_ABOVE_> | <GIS_SAME_> |
     * <GIS_STRICT_BELOW_> | <GIS_STRICT_ABOVE_> | <NOT_BITWISE_> | <ABSOLUTE_>
     * ) f1 -> SQLPrecedenceLevel3Expression(prn)
     */
    @Override
    public Object visit(SQLPrecedenceLevel3Operand n, Object obj) {
        SqlExpression expr = new SqlExpression();
        SqlExpression leftExpr = (SqlExpression) obj;
        String operator = ((NodeToken) n.f0.choice).tokenImage;
        SqlExpression rightExpr = (SqlExpression) n.f1.accept(this, null);
        expr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
        expr.setLeftExpr(leftExpr);
        expr.setOperator(operator);
        expr.setRightExpr(rightExpr);
        return expr;
    }

    /**
     * Grammar production:
     * f0 -> [ <SQUARE_ROOT_> | <CUBE_ROOT_> | <FACTORIAL_PREFIX_> | <ABSOLUTE_> | <NOT_BITWISE_> ]
     * f1 -> [ <PLUS_> | <MINUS_> ]
     * f2 -> ( FunctionCall(prn) | TableColumn(prn) | PseudoColumn(prn) | numberValue(prn) | "(" SQLComplexExpression(prn) ")" | booleanLiteral(prn) | stringLiteral(prn) | NullLiterals(prn) | IntervalLiterals(prn) | TextLiterals(prn) | PreparedStmtParameter(prn) | TimeStampLiteral(prn) | TimeLiteral(prn) | DateLiteral(prn) | binaryLiteral(prn) | hex_decimalLiteral(prn) | IntegerLiteral(prn) )
     * f3 -> [ <FACTORIAL_> ]
     * f4 -> [ "::" types() ]
     */
    @Override
    public Object visit(SQLPrimaryExpression n, Object argu) {
        /*
         * We are getting a SQLExpression in the argument ---
         */

        String sign = null;
        String operandSign = null;
        // check if it's a FACTORIAL_PREFIX, ABSOLUTE, CUBE ROOT or SQUARE ROOT operator
        if (n.f0.present()) {
            NodeToken aNodeToken = (NodeToken) ((NodeChoice) n.f0.node).choice;
            sign = aNodeToken.tokenImage;
        }

        /* We first check if the sign is present */
        if (n.f1.present()) {

            NodeChoice aNodeChoice = (NodeChoice) n.f1.node;
            switch (aNodeChoice.which) {
            case 0:
            case 1:
                NodeToken aToken = (NodeToken) aNodeChoice.choice;
                if (n.f0.present()) {
                    operandSign = aToken.tokenImage;
                } else {
                    sign = aToken.tokenImage;
                }
                break;
            }
        }

        if (n.f3.present()) {
            NodeToken aNodeToken = (NodeToken) n.f3.node;

            if (n.f1.present()) {
                operandSign = sign;
            }

            sign = aNodeToken.tokenImage;
        }

        SqlExpression aSqlExpression;
        if (n.f2.which == 4)
            aSqlExpression = (SqlExpression) ((NodeSequence) n.f2.choice)
                    .elementAt(1).accept(this, null);
        else
            aSqlExpression = (SqlExpression) n.f2.accept(this, null);

        if (sign == null) {
            if (aSqlExpression.getUnaryOperator().equals("")) {
                aSqlExpression.setUnaryOperator("+");
            }
        } else {
            // check if it's sign in context of unary operator
            if (!aSqlExpression.getUnaryOperator().equals("")) {
                aSqlExpression.setOperandSign(aSqlExpression.getUnaryOperator());
            }

            aSqlExpression.setUnaryOperator(sign);
        }

        if (operandSign != null) {
            aSqlExpression.setOperandSign(operandSign);
        }

        /* This statement has no relevance */
        if (aSqlExpression.getExprType() == SqlExpression.SQLEX_PARAMETER) {
            aSqlExpression.setExprDataType(new ExpressionType());
        }

        if (n.f4.present()) {
            SqlExpression newSqlExpression = new SqlExpression();
            newSqlExpression.setExprType(SqlExpression.SQLEX_FUNCTION);
            newSqlExpression.setFunctionId(IFunctionID.CAST_ID);
            newSqlExpression.setFunctionName("::");
            newSqlExpression.getFunctionParams().add(aSqlExpression);
            DataTypeHandler typeHandler = new DataTypeHandler();
            n.f4.accept(typeHandler, argu);
            newSqlExpression.setExpTypeOfCast(typeHandler);
            aSqlExpression = newSqlExpression;
        }

        return aSqlExpression;
    }

    /**
     * Grammar production:
     * f0 -> <PARENTHESIS_START_>
     * f1 -> <SELECT_>
     * f2 -> [ <ALL_> | <DISTINCT_> ]
     * f3 -> SelectList(prn)
     * f4 -> [ FromClause(prn) ]
     * f5 -> [ WhereClause(prn) ]
     * f6 -> [ LimitClause(prn) ]
     * f6 -> [ OffsetClause(prn) ]
     * f7 -> <PARENTHESIS_CLOSE_>
     */

    @Override
    public Object visit(PseudoColumn n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();

        // We will get a SqlExpression - in the argument and we have to make it
        // a Query Tree Expression
        QueryTree subQueryTree = new QueryTree();
        QueryTree parentTree = null;

        parentTree = commandToExecute.getaQueryTreeTracker().GetCurrentTree();
        commandToExecute.getaQueryTreeTracker().registerTree(subQueryTree);

        n.f0.accept(this, argu);

        // Check if the node is present -- all node optionals have this
        // function which indicate whether this particular node is present
        // or not.
        if (n.f2.present()) {
            // Now we know that the node is present - extract the node from the
            // under lying
            // member variable of F2
            NodeChoice aChoice = (NodeChoice) n.f2.node;

            // Check what choice the user has Made
            if (aChoice.which == 0) {
                // 0 - is for ALL as ALL is the first element in th list of F2
                subQueryTree.setDistinct(false);
            } else { // 1 - is for distinct
                subQueryTree.setDistinct(true);
            }
        } else {
            // Just incase - f2 is not present
            subQueryTree.setDistinct(false);
        }
        n.f1.accept(this, argu);
        n.f2.accept(this, argu);
        // This will fill the projection list of the tree
        // In order to deal with this we will generate a projection list
        ProjectionListHandler aProjectionListHandler = new ProjectionListHandler(
                commandToExecute);
        n.f3.accept(aProjectionListHandler, subQueryTree);

        // From Clause handler
        if (n.f4.present()) {
            FromClauseHandler aFromClauseHandler = new FromClauseHandler(
                    commandToExecute);
            n.f4.accept(aFromClauseHandler, subQueryTree);
        } else {
            RelationNode aFakeNode = subQueryTree.newRelationNode();
            aFakeNode.setNodeType(RelationNode.FAKE);
            aFakeNode.setClient(commandToExecute.getClientContext());
        }

        // Where Clause
        WhereClauseHandler aWhereClauseHandler = new WhereClauseHandler(
                commandToExecute);
        n.f5.accept(aWhereClauseHandler, subQueryTree);

        // Limit
        QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                commandToExecute);
        n.f6.accept(aQueryTreeHandler, subQueryTree);

        // Offset
        n.f7.accept(aQueryTreeHandler, subQueryTree);

        // Syntax Sugar
        n.f8.accept(this, argu);
        
        QueryTreeHandler.checkAndExpand(subQueryTree.getProjectionList(),
                subQueryTree.getRelationNodeList(), database, commandToExecute);

        // assign to tree
        for (SqlExpression sqlExpression : subQueryTree.getProjectionList()) {
            QueryTreeHandler.SetBelongsToTree(sqlExpression, subQueryTree);
        }
        
        // Projections
        List<SqlExpression> projectionOrphans = QueryTreeHandler.checkAndFillTableNames(
                subQueryTree.getProjectionList(),
                subQueryTree.getRelationNodeList(), 
                subQueryTree.getProjectionList(),
                QueryTreeHandler.PROJECTION,
                commandToExecute);
        
        subQueryTree.getSelectOrphans().addAll(projectionOrphans);

        // processing is finished later, once the parent's from clause
        aSqlExpression.setExprType(SqlExpression.SQLEX_SUBQUERY);
        aSqlExpression.setSubqueryTree(subQueryTree);
        
        //subQueryTree.setQueryType(QueryTree.SCALAR);
        subQueryTree.setParentQueryTree(parentTree);
        subQueryTree.setTopMostParentQueryTree(parentTree.getTopMostParentQueryTree());
        commandToExecute.getaQueryTreeTracker().deRegisterCurrentTree();

        subQueryTree.setContainsAggregates(QueryTreeHandler.isAggregateQuery(subQueryTree));
        subQueryTree.setQueryType(QueryTree.SCALAR);

        return aSqlExpression;
    }


    /*
     * Broke this out from the preceding visit function so we can call it
     * later when we have a better idea of how to handle correlated column
     * usage when there is a correlated subquery in a SELECT clause.
     */
    public void finishSubQueryAnalysis(SqlExpression aSqlExpression)
    {
        QueryTree subQueryTree = null;

        if (aSqlExpression.getExprType() == SqlExpression.SQLEX_SUBQUERY)
        {
            subQueryTree = aSqlExpression.getSubqueryTree();
        }
        else
        {
            throw new XDBServerException(
                    "Subquery expected", 0,
                    ErrorMessageRepository.PARSER_ERROR_CODE);
        }

        // Where Clause
        if (subQueryTree.getWhereRootCondition() != null) {
            QueryTreeHandler.ProcessWhereCondition(
                    subQueryTree.getWhereRootCondition(), subQueryTree,
                    commandToExecute);
        }
        // Fill Expr Data Types
        QueryTreeHandler.FillAllExprDataTypes(subQueryTree, commandToExecute);

        subQueryTree.processSubTree(aSqlExpression,
                commandToExecute.getaQueryTreeTracker());

        return;
    }

    /**
     * f0 -> Func_CurrentDate() | Func_CurrentTime() | Func_CurrentTimeStamp() |
     * Func_Year() | Func_Month() | Func_Minute() | Func_Second() |
     * Func_AddDate() | Func_AddTime() | Func_Date() | Func_DateDiff() |
     * Func_Day() | Func_DayName() | Func_DayOfMonth() | Func_DayOfWeek() |
     * Func_DayOfYear() | Func_MonthName() | Func_SubDate() | Func_SubTime() |
     * Func_Time() | Func_TimeStamp() | Func_WeekOfYear() | Func_Now() | <ABS_>
     * "(" SQLArgument(prn) ")" | <AVERAGE_> "(" FunctionArgument(prn) ")" |
     * <COUNT_> "(" SQLArgument(prn) ")" | <COUNT_> "(" <STAR_> ")" | <EXTRACT_>
     * "(" types() "," SQLArgument(prn) ")" | <MAX_> "(" SQLArgument(prn) ")" |
     * <UPPER_> "(" SQLArgument(prn) ")" | <VERSION_> "(" ")" | <SUBSTRING_> "("
     * SQLArgument(prn) "," position(prn) "," [ length(prn) ] ")" | <TRIM_>
     * TrimSpec(prn) | <TRUNC_> "(" length(prn) "," length(prn) ")" | <RIGHT_>
     * "(" SQLArgument(prn) ")" | <LEFT_> "(" SQLArgument(prn) "," length(prn)
     * ")" | <LENGHT_> "(" SQLArgument(prn) ")" | <LOWER_> "(" SQLArgument(prn)
     * ")"
     */

    @Override
    public Object visit(FunctionCall n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_FUNCTION);
        FunctionHandler funcHandler = new FunctionHandler(aSqlExpression,
                commandToExecute);
        n.f0.choice.accept(funcHandler, argu);
        return aSqlExpression;
    }

    /**
     * Grammar production: f0 -> <IS_> f1 -> [ <NOT_> ] f2 -> <NULL_>
     */
    @Override
    public Object visit(IsNullExpression n, Object argu) {
        SqlExpression leftsqlexpr = (SqlExpression) argu;

        SqlExpression msqlexpr = new SqlExpression();

        msqlexpr.setLeftExpr(leftsqlexpr);

        msqlexpr.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
        if (n.f1.present()) {
            msqlexpr.setOperator("is not");
        } else {
            msqlexpr.setOperator("is");
        }

        /*
         * We will need to create a new Sql Expression here -- and we will
         * insert this as the right child of the Operand expression just created
         */
        SqlExpression rightSqlExpr = new SqlExpression();

        rightSqlExpr.setExprType(SqlExpression.SQLEX_CONSTANT);
        rightSqlExpr.setConstantValue(null);
        /*
         * TODO: I have to check that the sql expression that I get is not null
         * and is not of type Operand.
         */
        msqlexpr.setRightExpr(rightSqlExpr);
        /*
         * Once we have all the required information we will return the
         * SqlExpression in the argument
         */
        return msqlexpr;
    }

    /**
     * Grammar production:
     * f0 -> extendbObject(prn)
     */
    @Override
    public Object visit(TableColumn n, Object argu) {
        return n.f0.accept(this, argu);
    }
    
    /**
     * Grammar production:
     * f0 -> ( TableName(prn) "." Identifier(prn) | Identifier(prn) )
     */
    @Override
    public Object visit(extendbObject n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_COLUMN);
        aSqlExpression.setColumn(new AttributeColumn());
        IdentifierHandler ih = new IdentifierHandler();
        switch (n.f0.which) {
        case 0:
            TableNameHandler aTableNameHandler = new TableNameHandler(
                    commandToExecute.getClientContext());
            ((NodeSequence) n.f0.choice).elementAt(0).accept(aTableNameHandler, null);
            String tableName = aTableNameHandler.getTableName();

            // Set the alias name same as that of column name - This
            // will later change to the true alias name if an
            // alias is specified.
            aSqlExpression.getColumn().columnName = (String) ((NodeSequence) n.f0.choice).elementAt(2).accept(ih, argu);
            // We cant decide until we have gone through the from clause
            // whether a particular column expression is infact an alias or
            // is it a table Name , so let us just set the tableAlias

            // Incase this is a table Name we will change it to a tempTable
            // format, else we will let it remain as such.

            aSqlExpression.getColumn().setTableAlias(tableName);
            aSqlExpression.setOuterAlias(aSqlExpression.getColumn().columnName);

            break;
        case 1:
            aSqlExpression.getColumn().columnName = (String) n.f0.choice.accept(ih, argu);
            // We dont know the table Name or the table alias at this point
            // however to keep quorum
            // we should populate these fields and keep them in sycnhronization,
            // until they are forced
            // to move out.
            aSqlExpression.setAlias(aSqlExpression.setOuterAlias(aSqlExpression.getColumn().columnName));

            break;
        default:
            throw new XDBServerException(
                    ErrorMessageRepository.ILLEGAL_PARAMETER, 0,
                    ErrorMessageRepository.ILLEGAL_PARAMETER_CODE);
        }
        return aSqlExpression;
    }

    /**
     * Grammar production:
     * f0 -> <STRING_LITERAL>
     */
    @Override
    public Object visit(stringLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        n.f0.accept(this, argu);
        return aSqlExpression;
    }

    /**
     * Grammar production:
     * f0 -> <TEXT_LITERAL>
     */
    @Override
    public Object visit(TextLiterals n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.CLOB_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        return aSqlExpression;
    }

    /**
     * Grammar production:
     * f0 -> <PLACE_HOLDER>
     */
    @Override
    public Object visit(PreparedStmtParameter n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_PARAMETER);
        aSqlExpression.setExprDataType(new ExpressionType());
        int number = Integer.parseInt(n.f0.tokenImage.substring(1));
        aSqlExpression.setParamNumber(number);
        commandToExecute.registerParameter(number, aSqlExpression);
        return aSqlExpression;
    }

    @Override
    public Object visit(hex_decimalLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.BINARY_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }

    @Override
    public Object visit(binaryLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        anExprDataType.type = ExpressionType.BINARY_TYPE;
        anExprDataType.length = n.f0.tokenImage.length() - 3; // do not count b''
        aSqlExpression.setExprDataType(anExprDataType);
        return aSqlExpression;
    }

    /**
     * f0 -> FloatingPointNumber(prn)
     */
    public Object visit(numberValue n, Object argu) {
       return n.f0.accept(this, argu);
    }
    
    /**
     * Grammar production: 
     * f0 -> <DECIMAL_LITERAL> | <INT_LITERAL> | <SCIENTIFIC_LITERAL>
     */
    @Override
    public Object visit(FloatingPointNumber n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        NodeToken aPlainNumberToken = (NodeToken) n.f0.choice;
        aSqlExpression.setConstantValue(aPlainNumberToken.tokenImage);
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        return aSqlExpression;
    }

    /**
     * Instead of delegating individual nodes of SQLComplex Expression -- We are
     * delegating the ComplexExpression itself.
     */

    /**
     * Grammar production: f0 -> SQLAndExpression(prn) f1 -> (
     * SQLORExpression(prn) )*
     */

    @Override
    public Object visit(SQLComplexExpression n, Object argu) {
        QueryConditionHandler qch = new QueryConditionHandler(commandToExecute);
        SqlExpression sqlExpression = new SqlExpression();
        n.accept(qch, argu);
        QueryCondition qc = qch.aRootCondition;
        // The Query Conditions that we get here will be actually a expressions
        // This will return us a Query Condition -- and Query Condition is of
        // many
        // types
        if (qc.getCondType() == QueryCondition.QC_SQLEXPR) {
            SqlExpression.copy(qc.getExpr(), sqlExpression);
        } else {
            sqlExpression.setQueryCondition(qc);
            sqlExpression.setExprType(SqlExpression.SQLEX_CONDITION);
        }
        return sqlExpression;
    }

    /*
     * We will have to set a variable in the SQL expression which indicated that
     * this is a constant of type boolean
     */

    /**
     * f0 -> <TRUE_> | <FALSE_>
     */
    @Override
    public Object visit(booleanLiteral n, Object argu) {
        SqlExpression sqlExpression = new SqlExpression();
        NodeToken aChoiceToken = (NodeToken) n.f0.choice;
        sqlExpression.setConstantValue(aChoiceToken.tokenImage);
        sqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        return sqlExpression;
    }

    /**
     * Grammar production: f0 -> <NULL_>
     */
    @Override
    public Object visit(NullLiterals n, Object argu) {
        SqlExpression sqlExpression = new SqlExpression();
        sqlExpression.setConstantValue(null);
        sqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        return sqlExpression;
    }

    /**
     * Grammar production: f0 -> <INTERVAL_LITERAL>
     */

    @Override
    public Object visit(IntervalLiterals n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.INTERVAL_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }

    /**
    * Grammar production:
    * f0 -> <INTEGER_LITERAL>
    */

    @Override
    public Object visit(IntegerLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.INT_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }

    /**
     * Grammar production: f0 -> <TIMESTAMP_LITERAL>
     */
    @Override
    public Object visit(TimeStampLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.setExpressionType(ExpressionType.TIMESTAMP_TYPE,
                ExpressionType.TIMESTAMPLEN, 0, 0);
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }

    /**
     * Grammar production: f0 -> <TIME_LITERAL>
     */
    @Override
    public Object visit(TimeLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.setExpressionType(ExpressionType.TIME_TYPE,
                ExpressionType.TIMELEN, 0, 0);
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }

    /**
     * Grammar production: f0 -> <DATE_LITERAL>
     */
    @Override
    public Object visit(DateLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.setExpressionType(ExpressionType.DATE_TYPE,
                ExpressionType.DATELEN, 0, 0);
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }

    /**
     * Grammar production: f0 -> <MACADDR_LITERAL>
     */

    @Override
    public Object visit(MacaddrLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.MACADDR_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }

    /**
     * Grammar production: f0 -> <CIDR_LITERAL>
     */

    @Override
    public Object visit(CidrLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.CIDR_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }

    /**
     * Grammar production: f0 -> <INET_LITERAL>
     */

    @Override
    public Object visit(InetLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.INET_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }

    /**
     * Grammar production: f0 -> <GEOMETRY_LITERAL>
     */

    @Override
    public Object visit(GeometryLiteral n, Object argu) {
        SqlExpression aSqlExpression = new SqlExpression();
        aSqlExpression.setExprType(SqlExpression.SQLEX_CONSTANT);
        ExpressionType anExprDataType = new ExpressionType();
        anExprDataType.type = ExpressionType.GEOMETRY_TYPE;
        aSqlExpression.setExprDataType(anExprDataType);
        aSqlExpression.setConstantValue(n.f0.tokenImage);
        return aSqlExpression;
    }
}
