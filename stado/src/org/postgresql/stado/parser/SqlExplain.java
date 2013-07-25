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
/**
 *
 */
package org.postgresql.stado.parser;


import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.ResultSetImpl;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.datatypes.VarcharType;
import org.postgresql.stado.engine.datatypes.XData;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.optimizer.OrderByElement;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.core.syntaxtree.Explain;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.Select;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.OrderByClauseHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.queryproc.QueryProcessor;

public class SqlExplain extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable, IPreparable {
    private XDBSessionContext client;

    private boolean verbose;

    private QueryTree aQueryTree = null;

    private Command commandToExecute;

    private QueryProcessor qProc;

    SqlSelect sqlSelectWith = null;
    
    /**
     * @param client
     *          the session context
     */
    public SqlExplain(XDBSessionContext client) {
        this.client = client;
        commandToExecute = new Command(Command.SELECT, this,
                new QueryTreeTracker(), client);
        aQueryTree = new QueryTree();
    }

    /**
	 * Grammar production:
	 * f0 -> <EXPLAIN_>
	 * f1 -> [ <VERBOSE_> ]
	 * f2 -> Select(prn)
     */
    @Override
	public void visit(Explain n, Object argu) {
    	verbose = n.f1.present();
        
        NodeChoice aNodeChoice = (NodeChoice) n.f2;

        SqlSelect aSqlSelect = new SqlSelect(client);
                        
        switch (aNodeChoice.which) {
            case 0:
                aNodeChoice.accept(this, argu);
                break;
            case 1:
                sqlSelectWith = new SqlSelect(client);
                aNodeChoice.accept(sqlSelectWith, argu);
                break;
        }
    }


	/**
     * Grammar production:
     * f0 -> SelectWithoutOrderWithParenthesis(prn)
     * f1 -> [ OrderByClause(prn) ]
     * f2 -> [ LimitClause(prn) ]
     * f3 -> [ OffsetClause(prn) ]
     */
    @Override
    public void visit(Select n, Object argu) {
        QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                commandToExecute);
        n.f0.accept(aQueryTreeHandler, aQueryTree);
        OrderByClauseHandler aOrderByClauseHandler = new OrderByClauseHandler(
                commandToExecute);
        n.f1.accept(aOrderByClauseHandler, aQueryTree);
        aQueryTree.setOrderByList(aOrderByClauseHandler.orderByList);
        aQueryTree.preProcessOrderByList();
        QueryTreeHandler.FillSQLExpressionInformation(aQueryTree, 
                aQueryTree.getOrderByList(), commandToExecute);
        n.f2.accept(aQueryTreeHandler, aQueryTree);
        n.f3.accept(aQueryTreeHandler, aQueryTree);
        preProcessUnionList();
    }

    /**
     *
     */
    private void preProcessUnionList() {
        if (aQueryTree.isHasUnion()) {
            for (int i = 0; i < aQueryTree.getUnionQueryTreeList().size(); i++) {
                aQueryTree.getUnionQueryTreeList().get(i).checkExpressionTypes(aQueryTree.getProjectionList());
            }
        }
    }

    /**
     * This function pre process the order by list , The pre processing of the
     * order by list allows us to get the right expression from the select list
     * if we have a numeric number in the order list
     */
    private void preProcessOrderByList() {
        // The pre processing of the order by list allows us to get the right
        // expression from the select list if we have a numeric number in the
        // order list
        for (OrderByElement aOrderExpr : aQueryTree.getOrderByList()) {
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
                if (indexToSelect >= aQueryTree.getProjectionList().size()
                        || indexToSelect < 0) {

                    throw new XDBServerException(
                            ErrorMessageRepository.ORDERBY_CLAUSE_POINTS_TO_ILLEGAL_PROJ_COLUMN,
                            0,
                            ErrorMessageRepository.ORDERBY_CLAUSE_POINTS_TO_ILLEGAL_PROJ_COLUMN_CODE);
                    // throw new XDBSemanticException("Value in the order list
                    // is greater than the number of " +
                    // "projected expressions in the query OR is less than 1 ");
                } else {
                    SqlExpression aProjectedExpression = aQueryTree.getProjectionList().get(indexToSelect);
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
            }
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return Collections.emptyList();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    public long getCost() {
        return ILockCost.LOW_COST;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getLockSpecs()
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Collection<SysTable> emptyList = Collections.emptyList();
        return new LockSpecification<SysTable>(emptyList, emptyList);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        if (!isPrepared()) {
            prepare();
        }
        ColumnMetaData[] columnMeta = new ColumnMetaData[] {
                new ColumnMetaData("Query Plan", "Query Plan", 0, Types.VARCHAR, 0, 0, "", (short) 0, false)};
        List<XData[]> rows = new LinkedList<XData[]>();
        for (String planStr : qProc.getQueryPlan().toString().split("\n")) {
        	rows.add(new XData[] {new VarcharType(planStr)});
        }
        if (verbose) {
        	rows.add(new XData[] {new VarcharType("")});
        	rows.add(new XData[] {new VarcharType("----------------")});
        	rows.add(new XData[] {new VarcharType(" Execution Plan")});
        	rows.add(new XData[] {new VarcharType("----------------")});
        	for (String planStr : qProc.getExecPlan().toString().split("\n")) {
            	rows.add(new XData[] {new VarcharType(planStr)});
        	}
        }
        ResultSetImpl rs = new ResultSetImpl(columnMeta, rows);
        return ExecutionResult.createResultSetResult(ExecutionResult.COMMAND_EXPLAIN, rs);
    }

    public boolean isPrepared() {
        return qProc != null && qProc.isPrepared();
    }

    public void prepare() throws Exception {
        if (sqlSelectWith != null) {
            qProc = new QueryProcessor(client, sqlSelectWith.aQueryTree);
        } else {
            qProc = new QueryProcessor(client, aQueryTree);
        }
        qProc.prepare();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return false;
    }

	@Override
	public boolean isReadOnly() {
		return true;
	}

}