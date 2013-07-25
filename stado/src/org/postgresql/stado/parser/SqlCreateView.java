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
package org.postgresql.stado.parser;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncCreateView;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.core.syntaxtree.createView;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.ColumnNameListHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.TableNameHandler;


public class SqlCreateView extends DepthFirstVoidArguVisitor implements IXDBSql,
IPreparable {
    private static final XLogger logger = XLogger
    .getLogger(SqlCreateView.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private Command commandToExecute;

    private QueryTree aQueryTree = null;

    private String selectString;

    private boolean isReplace = false;

    private List<String> colList = new ArrayList<String>();

    private List<ExpressionType> colDef = new ArrayList<ExpressionType>();

    private Collection<SysColumn> dependedSysCol = null;

    private String viewName;

    private static int genCount = 0;

    private boolean prepared = false;

    public SqlCreateView(XDBSessionContext client) {
        this.client = client;
        this.database = client.getSysDatabase();
        commandToExecute = new Command(Command.INSERT, this,
                new QueryTreeTracker(), client);
    }

    public LockSpecification<SysTable> getLockSpecs() {
        List<SysTable> empty = Collections.emptyList();
        return new LockSpecification<SysTable>(empty, empty);
    }

    public Collection<DBNode> getNodeList() {
        return Collections.emptySet();
    }

    /**
     * Gives the static cost of creating a table
     */
    public long getCost() {
        return HIGH_COST;
    }

    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method, new Object[] {});
        try {

            if (client.getCurrentUser().getUserClass() == SysLogin.USER_CLASS_STANDARD) {
                XDBSecurityException ex = new XDBSecurityException(
                        "You are not allowed to create views");
                logger.throwing(ex);
                throw ex;
            }
            // check name
            if (database.isViewExists(viewName) && isReplace == false) {
                throw new XDBServerException("Duplicate View Name :" + "("
                        + viewName + ")");
            }
            // check column names
            for (int i = 0; i < colList.size(); i++) {
                String colName = colList.get(i);
                for (int j = i + 1; j < colList.size(); j++) {
                    if (colName.equals(colList.get(j))) {
                        throw new XDBServerException("column \"" + colName
                                + "\" specified more than once");
                    }
                }
            }
            prepared = true;

        } finally {
            logger.exiting(method);
        }
    }

    public boolean isPrepared() {
        return prepared;
    }

    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] { engine });
        try {

            if (!isPrepared()) {
                prepare();
            }
            SyncCreateView sync = new SyncCreateView(this);
            MetaData meta = MetaData.getMetaData();
            meta.beginTransaction();
            try {
                sync.execute(database);
                meta.commitTransaction(sync);
            } catch (Exception e) {
                logger.catching(e);
                meta.rollbackTransaction();
                throw e;
            }
            return ExecutionResult
            .createSuccessResult(ExecutionResult.COMMAND_CREATE_VIEW);

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Grammar production: f0 -> ( <CREATE_> | <REPLACE_> ) f1 -> <VIEW_> f2 ->
     * TableName(prn) f3 -> [ ColumnNameListWithParenthesis(prn) ] f4 -> <AS_>
     * f5 -> SelectWithoutOrder(prn)
     */
    @Override
    public void visit(createView n, Object argu) {
        switch (n.f0.which) {
        case 0: // create
            isReplace = false;
            break;
        case 1: // replace
            isReplace = true;
            break;
        }

        n.f1.accept(this, argu);
        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f2.accept(aTableNameHandler, argu);
        viewName = aTableNameHandler.getTableName();
        QueryTreeHandler aSelectQuery = new QueryTreeHandler(commandToExecute);
        aQueryTree = new QueryTree();
        n.f5.accept(aSelectQuery, aQueryTree);

        for (int i = 0; i < aQueryTree.getProjectionList().size(); i++) {
            if (aQueryTree.getProjectionList().get(i).getAlias() == null
                    || aQueryTree.getProjectionList().get(i).getAlias()
                    .equals("")) {
                if (aQueryTree.getProjectionList().get(i).getExprType() != SqlExpression.SQLEX_COLUMN) {
                    aQueryTree.getProjectionList().get(i).setAlias("VIEW_EXPRESSION"
                            + ++genCount);
                } else {
                    aQueryTree.getProjectionList().get(i).setAlias(aQueryTree.getProjectionList()
                            .get(i).getColumn().columnName);
                }
            }
            colList.add(aQueryTree.getProjectionList().get(i).getAlias());
            colDef.add(aQueryTree.getProjectionList().get(i).getExprDataType());
        }

        if (n.f3.present()) {
            ColumnNameListHandler aColumnListHandler = new ColumnNameListHandler();
            n.f3.accept(aColumnListHandler, null);
            if (aQueryTree.getProjectionList().size() < colList.size()) {
                throw new XDBServerException(
                "CREATE VIEW specifies more column names than columns");
            }
            for (int i = 0; i < aColumnListHandler.getColumnNameList().size(); i++) {
                aQueryTree.getProjectionList().get(i).setAlias(colList
                        .get(i));
                // colList.remove(i);
                colList.set(i, aColumnListHandler.getColumnNameList().get(i));
            }

        }

        String cmd = ((String)argu).trim();
        // Remove trailing semicolon if presents
        if (cmd.endsWith(";")) {
            cmd = cmd.substring(0, cmd.length() - 1);
            cmd = cmd.trim();
        }

        // Replace all tabs('\t') into white space (' ')
        cmd = cmd.replaceAll("\t", " ");
        cmd = cmd.replaceAll("\n", " ");
        cmd = cmd.replaceAll("\r", " ");

        // Replace multiple white space into single white space
        while (cmd.contains("  ")) {
            cmd = cmd.replaceAll("  ", " ");
        }

        // look for " AS SELECT " in "CREATE|REPLACE VIEW ... AS SELECT ..." statement
        int strIndex = cmd.toUpperCase().indexOf(" AS SELECT ");
        if (strIndex != -1) {
            selectString = cmd.substring(strIndex + 4);
        } else {
            // " AS SELECT " not found, means " AS ( SELECT " pattern should be found
            // Make sure, we always find " AS ( SELECT ", not " AS( SELECT " | " AS(SELECT " | " AS (SELECT "
            cmd = cmd.replaceAll("\\(SELECT ", " ( SELECT ");
            // Replace double-white space, introduced during replacing " AS (SELECT " into " AS  ( SELECT "
            cmd = cmd.replaceAll("  ", " ");
            // look for " AS ( SELECT " in the statement
            strIndex = cmd.toUpperCase().indexOf(" AS ( SELECT ");
            if (strIndex != -1) {
                selectString = cmd.substring(strIndex + 6, cmd.length() - 1);
            }
            else {
                // It will never reach here
                selectString = aQueryTree.rebuildString();
            }
        }
    }

    private Collection<SysColumn> getDependedColumns(QueryTree queryTree) {
        Collection<SysColumn> result = new HashSet<SysColumn>();
        // check subuery
        if (queryTree.getRelationSubqueryList() != null
                && queryTree.getRelationSubqueryList().size() > 0) {
            for (RelationNode aRelNode : queryTree.getRelationSubqueryList()) {
                getDependedColumns(aRelNode.getSubqueryTree());
            }
        }
        // check proj list
        result.addAll(getAllSysColumns(queryTree.getProjectionList()));

        // check where
        ArrayList<SqlExpression> vExprs = new ArrayList<SqlExpression>();
        if (queryTree.getWhereRootCondition() != null) {
            for (QueryCondition qc : QueryCondition.getNodes(
                    queryTree.getWhereRootCondition(),
                    QueryCondition.QC_SQLEXPR)) {
                vExprs.add(qc.getExpr());
            }
            result.addAll(getAllSysColumns(vExprs));
            vExprs.clear();
        }
        //
        for (QueryCondition qc : queryTree.getConditionList()) {
            for (QueryCondition qc1 : QueryCondition.getNodes(qc,
                    QueryCondition.QC_SQLEXPR)) {
                vExprs.add(qc1.getExpr());
            }
            result.addAll(getAllSysColumns(vExprs));
            vExprs.clear();
        }
        // check from list ( select n_nationkey from nation, region)
        for (RelationNode rel : queryTree.getRelationNodeList()) {
            if (rel.getNodeType() == RelationNode.TABLE) {
                result.addAll(client.getSysDatabase().getSysTable(
                        rel.getTableName()).getRowID());
            }
        }
        return result;
    }

    private List<SysColumn> getAllSysColumns(List<SqlExpression> anExprs) {
        List<SysColumn> result = new ArrayList<SysColumn>();
        for (SqlExpression expr : anExprs) {
            for (SqlExpression aColExp : SqlExpression.getNodes(expr, SqlExpression.SQLEX_COLUMN)) {
                while (aColExp.getMappedExpression() != null) {
                    aColExp = aColExp.getMappedExpression();
                }
                if (aColExp.getColumn() != null
                        && aColExp.getColumn().relationNode.getNodeType() == RelationNode.TABLE) {
                    result.add(aColExp.getColumn().getSysColumn(
                            client.getSysDatabase()));
                }
            }
        }
        return result;
    }

    public String getSelectString() {
        return selectString;
    }

    public void setSelectString(String selectString) {
        this.selectString = selectString;
    }

    public String getViewName() {
        return viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    /**
     * @return Returns the colList.
     */
    public List<String> getColList() {
        return colList;
    }

    public List<ExpressionType> getColDef() {
        return colDef;
    }

    public Collection<SysColumn> getDependedSysCol() {
        if (dependedSysCol == null) {

            dependedSysCol = getDependedColumns(aQueryTree);
            for (QueryTree aUnionQueryTree : aQueryTree.getUnionQueryTreeList()) {
                dependedSysCol.addAll(getDependedColumns(aUnionQueryTree));
            }

        }
        return dependedSysCol;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return true;
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
