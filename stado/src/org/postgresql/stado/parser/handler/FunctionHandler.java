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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.core.syntaxtree.Func_Avg;
import org.postgresql.stado.parser.core.syntaxtree.Func_BitAnd;
import org.postgresql.stado.parser.core.syntaxtree.Func_BitOr;
import org.postgresql.stado.parser.core.syntaxtree.Func_BoolAnd;
import org.postgresql.stado.parser.core.syntaxtree.Func_BoolOr;
import org.postgresql.stado.parser.core.syntaxtree.Func_Case;
import org.postgresql.stado.parser.core.syntaxtree.Func_Cast;
import org.postgresql.stado.parser.core.syntaxtree.Func_ClockTimeStamp;
import org.postgresql.stado.parser.core.syntaxtree.Func_Coalesce;
import org.postgresql.stado.parser.core.syntaxtree.Func_Convert;
import org.postgresql.stado.parser.core.syntaxtree.Func_CorrCov;
import org.postgresql.stado.parser.core.syntaxtree.Func_Count;
import org.postgresql.stado.parser.core.syntaxtree.Func_CurrentDatabase;
import org.postgresql.stado.parser.core.syntaxtree.Func_CurrentSchema;
import org.postgresql.stado.parser.core.syntaxtree.Func_Custom;
import org.postgresql.stado.parser.core.syntaxtree.Func_Extract;
import org.postgresql.stado.parser.core.syntaxtree.Func_Max;
import org.postgresql.stado.parser.core.syntaxtree.Func_Min;
import org.postgresql.stado.parser.core.syntaxtree.Func_NullIf;
import org.postgresql.stado.parser.core.syntaxtree.Func_Overlay;
import org.postgresql.stado.parser.core.syntaxtree.Func_PgCurrentDate;
import org.postgresql.stado.parser.core.syntaxtree.Func_PgCurrentTime;
import org.postgresql.stado.parser.core.syntaxtree.Func_PgCurrentTimeStamp;
import org.postgresql.stado.parser.core.syntaxtree.Func_Position;
import org.postgresql.stado.parser.core.syntaxtree.Func_Regr;
import org.postgresql.stado.parser.core.syntaxtree.Func_StatementTimeStamp;
import org.postgresql.stado.parser.core.syntaxtree.Func_Stdev;
import org.postgresql.stado.parser.core.syntaxtree.Func_Substring;
import org.postgresql.stado.parser.core.syntaxtree.Func_Sum;
import org.postgresql.stado.parser.core.syntaxtree.Func_TransactionTimeStamp;
import org.postgresql.stado.parser.core.syntaxtree.Func_Trim;
import org.postgresql.stado.parser.core.syntaxtree.Func_User;
import org.postgresql.stado.parser.core.syntaxtree.Func_Variance;
import org.postgresql.stado.parser.core.syntaxtree.Func_Version;
import org.postgresql.stado.parser.core.syntaxtree.INode;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeListOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.SQLArgument;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * This class is responsible for handling function syntax. The information
 * regarding the functions are held in a SqlExpression object.
 *
 */
public class FunctionHandler extends DepthFirstVoidArguVisitor {
    Command commandToExecute;

    /**
     * The SQL Expression which it fills up with information
     */
    SqlExpression aSqlExpression;

    /**
     *
     * @param aSqlExpression The SQLExpression which it is responsible to fill data with
     * @param commandToExecute
     */
    public FunctionHandler(SqlExpression aSqlExpression,
            Command commandToExecute) {
        this.aSqlExpression = aSqlExpression;
        this.commandToExecute = commandToExecute;
    }

    /**
     * This class is responsible to fill in the information regarding Case
     * Statement It fill this information in the SQL Expression
     */
    class CaseHandler extends DepthFirstVoidArguVisitor {
        public String caseString;

        /**
         * Constructor - Which access the SQL Expression of the parent and then
         * sets the expression type to SQLEX_CASE
         */
        CaseHandler() {

            aSqlExpression.setExprType(SqlExpression.SQLEX_CASE);
        }

        /**
         * f0 -> <CASE_> "(" SQLSimpleExpression(prn) ")" ( <WHEN_>
         * SQLSimpleExpression(prn) <THEN_> SQLSimpleExpression(prn) )* [
         * <ELSE_> SQLSimpleExpression(prn) ] <END_> | <CASE_> ( <WHEN_>
         * SQLComplexExpression(prn) <THEN_> SQLSimpleExpression(prn) )* [
         * <ELSE_> SQLSimpleExpression(prn) ] <END_>
         *
         * @param n
         * @param argu
         * @return
         */
        @Override
        public void visit(Func_Case n, Object argu) {
            switch (n.f0.which) {
            case 0:
                processSimpleCase(n.f0.choice, argu);
                break;
            case 1:
                processGeneralCase(n.f0.choice, argu);
                break;

            }
        }

        /**
         * Simple Case Statement <CASE_> "(" SQLSimpleExpression(prn) ")" (
         * <WHEN_> SQLSimpleExpression(prn) <THEN_> SQLSimpleExpression(prn) )*
         * <ELSE_> SQLSimpleExpression(prn) <END_> The Function processes simple
         * Case Statement
         *
         * @param n
         * @param argu
         */
        private void processSimpleCase(INode n, Object argu) {
            NodeSequence caseSequence = (NodeSequence) n;
            INode f0 = caseSequence.elementAt(0);
            // Case Token
            f0.accept(this, argu);
            // Bracket Start
            // Node f1 = caseSequence.elementAt(1);
            // Main expression to check with
            INode f2 = caseSequence.elementAt(1);
            SQLExpressionHandler aSQLHandler = new SQLExpressionHandler(
                    commandToExecute);
            f2.accept(aSQLHandler, argu);
            SqlExpression mainExpression = aSQLHandler.aroot;
            NodeListOptional f3 = (NodeListOptional) caseSequence.elementAt(2);
            if (f3.present()) {
                for (Object node : f3.nodes) {
                    NodeSequence aSimpleClauseSequence = (NodeSequence) node;
                    INode WhenSqlExpressionNode = aSimpleClauseSequence
                    .elementAt(1);
                    SQLExpressionHandler whenhandler = new SQLExpressionHandler(
                            commandToExecute);
                    WhenSqlExpressionNode.accept(whenhandler, argu);
                    SqlExpression whenExpression = whenhandler.aroot;
                    // Create a Query Condition
                    QueryCondition aNewQueryCondition = new QueryCondition(
                            mainExpression, whenExpression, "=");
                    aNewQueryCondition.rebuildString();

                    INode ThenSqlExprNode = aSimpleClauseSequence.elementAt(3);
                    SQLExpressionHandler thenhandler = new SQLExpressionHandler(
                            commandToExecute);
                    ThenSqlExprNode.accept(thenhandler, argu);
                    SqlExpression thenExpression = thenhandler.aroot;
                    aSqlExpression.getCaseConstruct().addCase(aNewQueryCondition,
                            thenExpression);
                }
                NodeOptional f5 = (NodeOptional) caseSequence.elementAt(3);
                if (f5.present()) {
                    NodeSequence fsequence = (NodeSequence) f5.node;
                    INode f20 = fsequence.elementAt(1);
                    SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                            commandToExecute);
                    f20.accept(aSqlExpressionHandler, argu);
                    aSqlExpression.getCaseConstruct().setDefaultexpr(aSqlExpressionHandler.aroot);
                }

            }

        }

        /**
         * <CASE_> ( <WHEN_> SQLComplexExpression(prn) <THEN_>
         * SQLSimpleExpression(prn) )* [ <ELSE_> SQLSimpleExpression(prn) ]
         * <END_>
         *
         * @param n
         * @param argu
         */
        private void processGeneralCase(INode n, Object argu) {
            NodeSequence caseSequence = (NodeSequence) n;
            INode f0 = caseSequence.elementAt(0);
            f0.accept(this, argu);
            NodeListOptional f1 = (NodeListOptional) caseSequence.elementAt(1);
            if (f1.present()) {
                for (Object node : f1.nodes) {
                    NodeSequence ns = (NodeSequence) node;

                    INode n2 = (INode) ns.nodes.get(1);
                    QueryConditionHandler qc = new QueryConditionHandler(
                            commandToExecute);

                    n2.accept(qc, argu);
                    INode n4 = (INode) ns.nodes.get(3);
                    SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                            commandToExecute);
                    n4.accept(aSqlExpressionHandler, argu);
                    aSqlExpression.getCaseConstruct().addCase(qc.aRootCondition,
                            aSqlExpressionHandler.aroot);
                }
            }
            // f4 -> SQLSimpleExpression(prn)
            NodeOptional f2 = (NodeOptional) caseSequence.elementAt(2);
            if (f2.present()) {
                NodeSequence fsequence = (NodeSequence) f2.node;
                INode f20 = fsequence.elementAt(1);
                SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                        commandToExecute);
                f20.accept(aSqlExpressionHandler, argu);
                aSqlExpression.getCaseConstruct().setDefaultexpr(aSqlExpressionHandler.aroot);
            }
        }
    }// CaseHandler Ends

    /**
     * This function makes a case handler object and delegates the responsiblity
     * of this case statement to the case handler object
     *
     * f0 -> <CASE_> f1 -> ( <WHEN_> SQLComplexExpression(prn) <THEN_>
     * SQLSimpleExpression(prn) )* f2 -> <ELSE_> f3 -> SQLSimpleExpression(prn)
     * f4 -> <END_>
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(Func_Case n, Object argu) {
        CaseHandler aCaseHandler = new CaseHandler();
        n.accept(aCaseHandler, argu);
    }

    /**
     * Grammar production: f0 -> <CURRENTDATE_>
     * 
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(Func_PgCurrentDate n, Object argu) {
        aSqlExpression.setConstantValue("date '" 
                + new Date(System.currentTimeMillis()) + "'");
    }

    /**
     * Grammar production: f0 -> <CURRENT_TIME_> f1 -> [ "(" SQLArgument(prn)
     * ")" ]
     * 
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(Func_PgCurrentTime n, Object argu) {
        aSqlExpression.setConstantValue("time '" 
                + new Time(System.currentTimeMillis()) + "'");
    }

    /**
     * Grammar production: f0 -> <CURRENT_TIMESTAMP_> f1 -> [ "("
     * SQLArgument(prn) ")" ]
     * 
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(Func_PgCurrentTimeStamp n, Object argu) {
        aSqlExpression.setConstantValue("timestamp '" 
                + new Timestamp(System.currentTimeMillis()) + "'");
    }

    /**
     * f0 -> SQLSimpleExpression(prn)
     * 
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(SQLArgument n, Object argu) {
        SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                commandToExecute);
        n.f0.accept(aSqlExpressionHandler, argu);
        aSqlExpression.getFunctionParams().add(aSqlExpressionHandler.aroot);
    }

    /**
     * f0 -> <USER_> f1 -> [ "(" ")" ]
     * 
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(Func_User n, Object argu) {
        aSqlExpression.setConstantValue("'"
                + commandToExecute.getClientContext().getCurrentUser()
                        .getName().replaceAll("'", "''") + "'");
    }

    /**
     * f0 -> <COALESCE_> f1 -> "(" f2 -> FunctionArgumentList(prn) f3 -> ")"
     */
    @Override
    public void visit(Func_Coalesce n, Object argu) {
        setFunctionInfo(IFunctionID.COALESCE_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
    }

    /**
     * f0 -> <VERSION_> f1 -> <PARENTHESIS_START_> f2 -> <PARENTHESIS_CLOSE_>
     * 
     * @param n
     * @param argu
     * @return
     */
    @Override
    public void visit(Func_Version n, Object argu) {
        aSqlExpression.setConstantValue("'" 
                + Props.DISPLAY_SERVER_VERSION.replaceAll("'", "''") + "'");
    }

    /**
     * f0 -> <NULLIF_> f1 -> "(" f2 -> FunctionArgument(prn) f3 -> "," f4 -> FunctionArgument(prn) f5 -> ")"
     */
    @Override
    public void visit(Func_NullIf n, Object argu) {
        setFunctionInfo(IFunctionID.NULLIF_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
    }

    /**
     * Grammar production: f0 -> <TRIM_> f1 -> "(" f2 -> ( <BOTH> | <LEADING> |
     * <TRAILING> ) f3 -> [ SQLArgument(prn) ] f4 -> <FROM_> f5 ->
     * SQLArgument(prn) f6 -> ")"
     */
    @Override
    public void visit(Func_Trim n, Object argu) {
        setFunctionInfo(IFunctionID.TRIM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        SqlExpression aExp = new SqlExpression();
        NodeToken theToken = (NodeToken) n.f2.choice;
        aExp.setArgSeparator(" ");
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp.setConstantValue(theToken.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp);

        n.f3.accept(this, argu);

        SqlExpression aExp1 = new SqlExpression();
        aExp1.setArgSeparator(" ");
        aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp1.setConstantValue(n.f4.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp1);

        n.f5.accept(this, argu);
    }

    /**
     * f0 -> <AVERAGE_> f1 -> "(" f2 -> [ "DISTINCT" ] f3 -> SQLArgument(prn) f4 -> ")"
     */
    @Override
    public void visit(Func_Avg n, Object argu) {
        setFunctionInfo(IFunctionID.AVG_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }

    class CountHandle extends DepthFirstVoidArguVisitor {

        /**
         * Grammar production:
         * f0 -> <COUNT_>
         * f1 -> "("
         * f2 -> ( <STAR_> | [ <DISTINCT_> | <ALL_> ] SQLArgument(prn) )
         * f3 -> ")"
         */
        @Override
        public void visit(Func_Count n, Object argu) {
            if (n.f2.which == 0) {
                setFunctionInfo(IFunctionID.COUNT_STAR_ID,
                        IdentifierHandler.normalizeCase("COUNT(*)"));
                return;
            } else {
                NodeSequence ns = (NodeSequence) n.f2.choice;
                NodeOptional no = (NodeOptional) ns.nodes.get(0);
                if (no.present()) {
                    NodeChoice nc = (NodeChoice) no.node;
                    if (nc.which == 0) {
                        aSqlExpression.setDistinctGroupFunction(true);
                    } else {
                        aSqlExpression.setAllCountGroupFunction(true);
                    }
                }
                setFunctionInfo(IFunctionID.COUNT_ID,
                        IdentifierHandler.normalizeCase("COUNT"));
                ((INode) ns.nodes.get(1)).accept(this, argu);
            }
        }

        /**
         * f0 -> SQLSimpleExpression(prn)
         */
        @Override
        public void visit(SQLArgument n, Object argu) {
            SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                    commandToExecute);
            n.f0.accept(aSqlExpressionHandler, argu);
            aSqlExpression.getFunctionParams().add(aSqlExpressionHandler.aroot);
        }
    }

    /**
     * Grammar production:
     * f0 -> <COUNT_> "(" <STAR_> ")"
     *       | <COUNT_> "(" [ "DISTINCT" ] SQLArgument(prn) ")"
     *       | <COUNT_> "(" [ "ALL" ] SQLArgument(prn) ")"
     */
    @Override
    public void visit(Func_Count n, Object argu) {
        n.accept(new CountHandle(), argu);
    }

    /**
     * Grammar production:
     * f0 -> <MAX_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public void visit(Func_Max n, Object argu) {
        setFunctionInfo(IFunctionID.MAX_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }

    /**
     * Grammar production:
     * f0 -> <MIN_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public void visit(Func_Min n, Object argu) {
        setFunctionInfo(IFunctionID.MIN_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }

    /**
     * Grammar production:
     * f0 -> ( <STDDEV_> | <STDDEV_POP_> | <STDDEV_SAMP_> )
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public void visit(Func_Stdev n, Object argu) {
        int funcID = 0;
        switch(n.f0.which) {
        case 0:
            funcID = IFunctionID.STDEV_ID;
            break;
        case 1:
            funcID = IFunctionID.STDEVPOP_ID;
            break;
        case 2:
            funcID = IFunctionID.STDEVSAMP_ID;
            break;
        }
        setFunctionInfo(funcID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));

        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }

    /**
     * Grammar production:
     * f0 -> ( <VARIANCE_> | <VARIANCE_POP_> | <VARIANCE_SAMP_> )
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public void visit(Func_Variance n, Object argu) {
        int funcID = 0;
        switch(n.f0.which) {
        case 0:
            funcID = IFunctionID.VARIANCE_ID;
            break;
        case 1:
        case 3:
            funcID = IFunctionID.VARIANCEPOP_ID;
            break;
        case 2:
        case 4:
            funcID = IFunctionID.VARIANCESAMP_ID;
            break;
        }
        setFunctionInfo(funcID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }

    /*
     * f0 -> <SUM_> f1 -> "(" f2 -> [ "DISTINCT" ] f3 -> SQLArgument(prn) f4 ->
     * ")"
     */
    /**
     * f0 -> <SUM_> f1 -> "(" f2 -> SQLArgument(prn) f3 -> ")"
     */
    @Override
    public void visit(Func_Sum n, Object argu) {
        setFunctionInfo(IFunctionID.SUM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }

    @Override
    public void visit(NodeToken n, Object argu) {
        aSqlExpression.setExprString(aSqlExpression.getExprString()
                + n.tokenImage);
    }

    /**
     * Grammar production: f0 -> <IDENTIFIER_NAME> f1 -> "(" f2 -> [
     * FunctionArgument(prn) ] f3 -> ")"
     */
    @Override
    public void visit(Func_Custom n, Object argu) {
        IdentifierHandler ih = new IdentifierHandler();
        n.f0.accept(ih, argu);
        setFunctionInfo(IFunctionID.CUSTOM_ID, ih.getIdentifier());
        n.f2.accept(this, argu);
    }

    /**
     * Grammar production: f0 -> <CAST_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * <AS_> f4 -> ( types() | <NULL_> ) f5 -> ")"
     */
    @Override
    public void visit(Func_Cast n, Object argu) {
        n.f2.accept(this, argu);
        setFunctionInfo(IFunctionID.CAST_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));

        DataTypeHandler typeHandler = new DataTypeHandler();
        if (n.f4.which == 0) {
            n.f4.accept(typeHandler, argu);
        } else {
            typeHandler = new DataTypeHandler(Types.NULL, 0, 0, 0);
        }
        this.aSqlExpression.setExpTypeOfCast(typeHandler);
    }

    /**
     * Grammar production:
     * f0 -> <CONVERT_>
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ( <USING_> | "," )
     * f4 -> ( <STRING_LITERAL> | Identifier(prn) )
     * f5 -> [ "," ( <STRING_LITERAL> | Identifier(prn) ) ]
     * f6 -> ")"
     */
    @Override
    public void visit(Func_Convert n, Object argu) {
        setFunctionInfo(IFunctionID.CONVERT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        n.f2.accept(this, argu);

        aSqlExpression.setArgSeparator(" ");

        SqlExpression aExp = new SqlExpression();
        aExp.setArgSeparator(" ");
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        NodeToken token = (NodeToken) n.f3.choice;
        if (token.tokenImage.compareTo(",") != 0) {
            aExp.setConstantValue(token.tokenImage);
            aSqlExpression.getFunctionParams().add(aExp);
            aSqlExpression.setArgSeparator(" ");
        } else {
            aSqlExpression.setArgSeparator(",");
        }

        SqlExpression aExp1 = new SqlExpression();
        aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
        IdentifierHandler ih = new IdentifierHandler();
        if (n.f4.which == 0) {
            aExp1.setConstantValue(((NodeToken) n.f4.choice).tokenImage);
        } else {
            aExp1.setConstantValue((String) n.f4.choice.accept(ih, argu));
        }
        aSqlExpression.getFunctionParams().add(aExp1);

        if (n.f5.present()) {
            SqlExpression aExp2 = new SqlExpression();
            aExp2.setExprType(SqlExpression.SQLEX_CONSTANT);
            NodeChoice nc = (NodeChoice) ((NodeSequence) n.f5.node).elementAt(1);
            if (nc.which == 0) {
                aExp2.setConstantValue(((NodeToken) nc.choice).tokenImage);
            } else {
                aExp2.setConstantValue((String) nc.choice.accept(ih, argu));
            }
            aSqlExpression.getFunctionParams().add(aExp2);
        }
    }

    /**
     * Grammar production: f0 -> <EXTRACT_> f1 -> "(" f2 -> DatetimeField() f3 ->
     * <FROM_> f4 -> SQLArgument(prn) f5 -> ")"
     */
    @Override
    public void visit(Func_Extract n, Object argu) {
        setFunctionInfo(IFunctionID.EXTRACT_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        NodeToken theToken = (NodeToken) n.f2.choice;
        SqlExpression aExp = new SqlExpression();
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp.setConstantValue(theToken.tokenImage.substring(0,
                theToken.tokenImage.length() - 4));
        aSqlExpression.getFunctionParams().add(aExp);

        SqlExpression aExp1 = new SqlExpression();
        aExp1.setArgSeparator("  ");
        aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp1.setConstantValue("FROM");
        aSqlExpression.getFunctionParams().add(aExp1);

        n.f3.accept(this, argu);
    }

    /**
     * Grammar production: f0 -> <OVERLAY_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * <PLACING_> f4 -> SQLArgument(prn) f5 -> <FROM_> f6 -> SQLArgument(prn) f7 -> [
     * <FOR_> SQLArgument(prn) ] f8 -> ")"
     */
    @Override
    public void visit(Func_Overlay n, Object argu) {
        setFunctionInfo(IFunctionID.OVERLAY_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        n.f2.accept(this, argu);

        SqlExpression aExp = new SqlExpression();
        aExp.setArgSeparator(" ");
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp.setConstantValue(n.f3.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp);

        n.f4.accept(this, argu);

        SqlExpression aExp1 = new SqlExpression();
        aExp1.setArgSeparator("  ");
        aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp1.setConstantValue(n.f5.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp1);

        n.f6.accept(this, argu);

        if (n.f7.present()) {
            NodeToken tk = (NodeToken) ((NodeSequence) n.f7.node).elementAt(0);
            SQLArgument sqlarg = (SQLArgument) ((NodeSequence) n.f7.node)
            .elementAt(1);
            SqlExpression aExp2 = new SqlExpression();
            aExp2.setArgSeparator("  ");
            aExp2.setExprType(SqlExpression.SQLEX_CONSTANT);
            aExp2.setConstantValue(tk.tokenImage);
            aSqlExpression.getFunctionParams().add(aExp2);
            sqlarg.accept(this, argu);
        }
    }

    /**
     * Grammar production: f0 -> <SUBSTRING_> f1 -> "(" f2 -> SQLArgument(prn)
     * f3 -> [ <FROM_> SQLArgument(prn) ] f4 -> [ <FOR_> SQLArgument(prn) ] f5 ->
     * ")"
     */
    @Override
    public void visit(Func_Substring n, Object argu) {
        setFunctionInfo(IFunctionID.SUBSTRING_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        n.f2.accept(this, argu);

        if (n.f3.present()) {
            NodeToken tk = (NodeToken) ((NodeSequence) n.f3.node).elementAt(0);
            SQLArgument sqlarg = (SQLArgument) ((NodeSequence) n.f3.node)
            .elementAt(1);
            SqlExpression aExp1 = new SqlExpression();
            aExp1.setArgSeparator("  ");
            aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
            aExp1.setConstantValue(tk.tokenImage);
            aSqlExpression.getFunctionParams().add(aExp1);
            sqlarg.accept(this, argu);
        }
        if (n.f4.present()) {
            NodeToken tk = (NodeToken) ((NodeSequence) n.f4.node).elementAt(0);
            SQLArgument sqlarg = (SQLArgument) ((NodeSequence) n.f4.node)
            .elementAt(1);
            SqlExpression aExp1 = new SqlExpression();
            aExp1.setArgSeparator("  ");
            aExp1.setExprType(SqlExpression.SQLEX_CONSTANT);
            aExp1.setConstantValue(tk.tokenImage);
            aSqlExpression.getFunctionParams().add(aExp1);
            sqlarg.accept(this, argu);
        }
    }

    /**
     * Grammar production: f0 -> <POSITION_> f1 -> "(" f2 -> SQLArgument(prn) f3 ->
     * <IN_> f4 -> SQLArgument(prn) f5 -> ")"
     */
    @Override
    public void visit(Func_Position n, Object argu) {
        setFunctionInfo(IFunctionID.POSITION_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setArgSeparator(" ");

        n.f2.accept(this, argu);

        SqlExpression aExp = new SqlExpression();
        aExp.setArgSeparator(" ");
        aExp.setExprType(SqlExpression.SQLEX_CONSTANT);
        aExp.setConstantValue(n.f3.tokenImage);
        aSqlExpression.getFunctionParams().add(aExp);

        n.f4.accept(this, argu);
    }

    /**
     * Grammar production:
     * f0 -> <CLOCK_TIMESTAMP_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public void visit(Func_ClockTimeStamp n, Object argu) {
        setFunctionInfo(IFunctionID.CUSTOM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
    }
    /**
     * Grammar production:
     * f0 -> <STATEMENT_TIMESTAMP_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public void visit(Func_StatementTimeStamp n, Object argu) {
        setFunctionInfo(IFunctionID.CUSTOM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
    }
    /**
     * Grammar production:
     * f0 -> <TRANSACTION_TIMESTAMP_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public void visit(Func_TransactionTimeStamp n, Object argu) {
        setFunctionInfo(IFunctionID.CUSTOM_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
    }

    /**
     * Grammar production:
     * f0 -> <CURRENT_DATABASE_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public void visit(Func_CurrentDatabase n, Object argu) {
        aSqlExpression.setConstantValue("'"
                + commandToExecute.getClientContext().getSysDatabase().getDbname().replaceAll(
                        "'", "''") + "'");
    }
    /**
     * Grammar production:
     * f0 -> <CURRENT_SCHEMA_>
     * f1 -> [ <PARENTHESIS> ]
     */
    @Override
    public void visit(Func_CurrentSchema n, Object argu) {
        aSqlExpression.setConstantValue("'public'");
    }
    /**
     * Grammar production:
     * f0 -> <BIT_AND_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public void visit(Func_BitAnd n, Object argu) {
        setFunctionInfo(IFunctionID.BITAND_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }
    /**
     * Grammar production:
     * f0 -> <BIT_OR_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public void visit(Func_BitOr n, Object argu) {
        setFunctionInfo(IFunctionID.BITOR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }
    /**
     * Grammar production:
     * f0 -> ( <BOOL_AND_> | <EVERY_> )
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public void visit(Func_BoolAnd n, Object argu) {
        setFunctionInfo(n.f0.which == 0 ? IFunctionID.BOOLAND_ID : IFunctionID.EVERY_ID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }
    /**
     * Grammar production:
     * f0 -> <BOOL_OR_>
     * f1 -> "("
     * f2 -> [ "DISTINCT" ]
     * f3 -> SQLArgument(prn)
     * f4 -> ")"
     */
    @Override
    public void visit(Func_BoolOr n, Object argu) {
        setFunctionInfo(IFunctionID.BOOLOR_ID,
                IdentifierHandler.normalizeCase(n.f0.tokenImage));
        aSqlExpression.setDistinctGroupFunction(n.f2.present());
        n.f3.accept(this, argu);
    }
    /**
     * Grammar production:
     * f0 -> ( <CORR_> | <COVAR_POP_> | <COVAR_SAMP_> )
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public void visit(Func_CorrCov n, Object argu) {
        int funcID = 0;
        switch(n.f0.which) {
        case 0:
            funcID = IFunctionID.CORR_ID;
            break;
        case 1:
            funcID = IFunctionID.COVARPOP_ID;
            break;
        case 2:
            funcID = IFunctionID.COVARSAMP_ID;
            break;
        }
        setFunctionInfo(funcID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));

        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
    }
    /**
     * Grammar production:
     * f0 -> ( <REGR_AVGX_> | <REGR_AVGY_> | <REGR_COUNT_> | <REGR_INTERCEPT_> | <REGR_R2_>
     * | <REGR_SLOPE_> | <REGR_SXX_> | <REGR_SXY_> | <REGR_SYY_> )
     * f1 -> "("
     * f2 -> SQLArgument(prn)
     * f3 -> ","
     * f4 -> SQLArgument(prn)
     * f5 -> ")"
     */
    @Override
    public void visit(Func_Regr n, Object argu) {
        int funcID = 0;
        switch(n.f0.which) {
        case 0:
            funcID = IFunctionID.REGRAVX_ID;
            break;
        case 1:
            funcID = IFunctionID.REGRAVY_ID;
            break;
        case 2:
            funcID = IFunctionID.REGRCOUNT_ID;
            break;
        case 3:
            funcID = IFunctionID.REGRINTERCEPT_ID;
            break;
        case 4:
            funcID = IFunctionID.REGRR2_ID;
            break;
        case 5:
            funcID = IFunctionID.REGRSLOPE_ID;
            break;
        case 6:
            funcID = IFunctionID.REGRSXX_ID;
            break;
        case 7:
            funcID = IFunctionID.REGRSXY_ID;
            break;
        case 8:
            funcID = IFunctionID.REGRSYY_ID;
            break;
        }
        setFunctionInfo(funcID,
                IdentifierHandler.normalizeCase(((NodeToken)n.f0.choice).tokenImage));
        n.f2.accept(this, argu);
        n.f4.accept(this, argu);
    }

    // ----------Helper Functions
    /**
     * This is a helper function which sets the function ID and the function
     * name
     *
     * @param funcid
     * @param functionName
     */
    private void setFunctionInfo(int funcid, String functionName) {
        aSqlExpression.setFunctionId(funcid);
        aSqlExpression.setFunctionName(functionName);
    }
}
