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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.constraintchecker.DeleteForeignReferenceChecker;
import org.postgresql.stado.constraintchecker.IConstraintChecker;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.misc.RSHelper;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.core.syntaxtree.Delete;
import org.postgresql.stado.parser.core.syntaxtree.WhereClause;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryConditionHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.TableNameHandler;
import org.postgresql.stado.queryproc.QueryProcessor;


/**
 *
 */
public class SqlDeleteTable extends SqlModifyTable {
    private static final XLogger logger = XLogger.getLogger(SqlDeleteTable.class);

    private QueryCondition whereCondition;

    private Collection<SysColumn> columnsInvolved;

    private QueryTree aQueryTree = null;

    private Collection<DBNode> executionNodes = null;

    public SqlDeleteTable(XDBSessionContext client) {
        super(client);
        commandToExecute = new Command(Command.DELETE, this,
                new QueryTreeTracker(), client);
        aQueryTree = new QueryTree();
    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * f0 -> <DELETE_> f1 -> "FROM" f2 -> TableName(prn) f3 -> [
     * WhereClause(prn) ]
     */
    @Override
    public Object visit(Delete n, Object argu) {
        Object _ret = null;

        QueryTreeTracker aQueryTreeTracker = commandToExecute.getaQueryTreeTracker();
        aQueryTreeTracker.registerTree(aQueryTree);

        RelationNode aRelationNode = aQueryTree.newRelationNode();
        aRelationNode.setNodeType(RelationNode.TABLE);

        TableNameHandler aTableNameHandler = new TableNameHandler(client);

        n.f2.accept(aTableNameHandler, argu);
        tableName = aTableNameHandler.getTableName();
        aRelationNode.setTableName(aTableNameHandler.getTableName());
        aRelationNode.setTemporaryTable(aTableNameHandler.isTemporary());
        aRelationNode.setClient(commandToExecute.getClientContext());
        aRelationNode.setAlias(aTableNameHandler.getReferenceName());

        n.f3.accept(this, aQueryTree);
        return _ret;
    }

    /**
     * Grammar production: f0 -> <WHERE_> f1 -> SQLComplexExpression(prn)
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(WhereClause n, Object argu) {
        QueryConditionHandler aQueryConditionHandler = new QueryConditionHandler(
                commandToExecute);
        n.f1.accept(aQueryConditionHandler, aQueryTree);
        whereCondition = aQueryConditionHandler.aRootCondition;
        if (whereCondition != null) {
            // verifyQueryCondition(whereCondition);
            QueryTreeHandler.ProcessWhereCondition(whereCondition, aQueryTree,
                    commandToExecute);
        }
        return argu;
    }

    // ******************************
    // END GRAMMAR
    // ******************************

    @Override
    public Collection<SysTable> getReadTables() {
        Collection<SysTable> readTables = super.getReadTables();
        readTables.add(getTargetTable());
        return readTables;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#getColumnsInvolved()
     */
    @Override
    protected Collection<SysColumn> getColumnsInvolved() {
        if (columnsInvolved == null) {
            columnsInvolved = new HashSet<SysColumn>(
                    getTargetTable().getRowID());
        }
        return columnsInvolved;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#getPKChecker(org.postgresql.stado.MetaData.SysTable,
     *      java.util.List)
     */
    @Override
    protected IConstraintChecker getPKChecker(SysTable targetTable,
            Collection columnsInvolved) {
        // DELETE can not violate primary or unique key
        return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#getFKChecker(org.postgresql.stado.MetaData.SysTable,
     *      java.util.List)
     */
    @Override
    protected IConstraintChecker getFKChecker(SysTable targetTable,
            Collection columnsInvolved) {
        final String method = "getFKChecker";
        logger.entering(method, new Object[] {});
        try {

            return null;

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#getFKChecker(org.postgresql.stado.MetaData.SysTable,
     *      java.util.List)
     */
    @Override
    protected IConstraintChecker getFRChecker(SysTable targetTable,
            Collection columnsInvolved) {
        final String method = "getFKChecker";
        logger.entering(method, new Object[] {});
        try {

            IConstraintChecker frChecker = new DeleteForeignReferenceChecker(
                    getTargetTable(), whereCondition, client);
            columnsInvolved.addAll(frChecker.scanConstraints(getColumnsInvolved()));
            return frChecker;

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#createTempTableMetadata(org.postgresql.stado.MetaData.SysTable,
     *      java.util.List)
     */
    @Override
    protected SysTable createTempTableMetadata(SysTable targetTable,
            Collection<SysColumn> columnsInvolved) throws Exception {
        if (whereCondition == null || whereCondition.isSimple()) {
            return null;
        }
        return super.createTempTableMetadata(targetTable, columnsInvolved);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#prepareDataSource(org.postgresql.stado.MetaData.SysTable,
     *      org.postgresql.stado.MetaData.SysTable)
     */
    @Override
    protected Object prepareDataSource(SysTable targetTable, SysTable tempTable)
            throws Exception {
        final String method = "prepareDataSource";
        logger.entering(method, new Object[] {});
        try {

            QueryProcessor select = null;
            if (tempTable != null) {
                for (SysColumn column : getColumnsInvolved()) {
                    SqlExpression rowidExpr = new SqlExpression();
                    rowidExpr.setExprString(column.getColName());
                    rowidExpr.setExprType(SqlExpression.SQLEX_COLUMN);
                    rowidExpr.setColumn(new AttributeColumn());
                    rowidExpr.getColumn().columnName = column.getColName();
                    rowidExpr.setAlias(column.getColName());
                    rowidExpr.setOuterAlias(column.getColName());
                    aQueryTree.getProjectionList().add(rowidExpr);
                }
                QueryTreeHandler.checkAndExpand(aQueryTree.getProjectionList(),
                        aQueryTree.getRelationNodeList(), database, commandToExecute);
                QueryTreeHandler.checkAndFillTableNames(aQueryTree.getProjectionList(),
                                aQueryTree.getRelationNodeList(),
                                new ArrayList<SqlExpression>(),
                                QueryTreeHandler.PROJECTION,
                                commandToExecute);
                QueryTreeHandler.FillAllExprDataTypes(aQueryTree, commandToExecute);
                QueryTreeHandler.setOwnerShipColumns(aQueryTree);
                select = new QueryProcessor(client, aQueryTree);
                select.setSkipPermissionCheck(Collections.singleton(tableName));
                select.prepare();
            }
            return select;

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#prepareFinalStatements()
     */
    @Override
    protected Object prepareFinalStatements(SysTable targetTable,
            SysTable tempTable) throws Exception {
        final String method = "prepareFinalStatements";
        logger.entering(method, new Object[] {});
        try {

            StringBuffer sbFinalDelete = new StringBuffer("DELETE FROM ");
            sbFinalDelete.append(IdentifierHandler.quote(targetTable.getTableName()));
            if (tempTable == null) {
                if (whereCondition != null) {
                    sbFinalDelete.append(" WHERE ").append(
                            whereCondition.rebuildString());
                }
            } else {
                sbFinalDelete.append(" WHERE EXISTS (SELECT 1 FROM ");
                sbFinalDelete.append(IdentifierHandler.quote(tempTable.getTableName())).append(" WHERE ");
                for (SysColumn sysColumn : targetTable.getRowID()) {
                    if (sysColumn.isNullable()) {
                        sbFinalDelete.append("(");
                        sbFinalDelete.append(IdentifierHandler.quote(targetTable.getTableName())).append(".");
                        sbFinalDelete.append(IdentifierHandler.quote(sysColumn.getColName())).append("=");
                        sbFinalDelete.append(IdentifierHandler.quote(tempTable.getTableName())).append(".");
                        sbFinalDelete.append(IdentifierHandler.quote(sysColumn.getColName())).append(" or ");
                        sbFinalDelete.append(IdentifierHandler.quote(targetTable.getTableName())).append(".");
                        sbFinalDelete.append(IdentifierHandler.quote(sysColumn.getColName())).append(" IS NULL AND ");
                        sbFinalDelete.append(IdentifierHandler.quote(tempTable.getTableName())).append(".");
                        sbFinalDelete.append(IdentifierHandler.quote(sysColumn.getColName())).append(" IS NULL) AND ");
                    } else {
                        sbFinalDelete.append("(");
                        sbFinalDelete.append(IdentifierHandler.quote(targetTable.getTableName())).append(".");
                        sbFinalDelete.append(IdentifierHandler.quote(sysColumn.getColName())).append("=");
                        sbFinalDelete.append(IdentifierHandler.quote(tempTable.getTableName())).append(".");
                        sbFinalDelete.append(IdentifierHandler.quote(sysColumn.getColName())).append(") AND ");
                    }
                }
                sbFinalDelete.setLength(sbFinalDelete.length() - 5);
                sbFinalDelete.append(")");
            }
            return sbFinalDelete.toString();

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#fillTable(java.lang.Object,
     *      org.postgresql.stado.MetaData.SysTable, org.postgresql.stado.Engine.Engine)
     */
    @Override
    protected long fillTable(Object source, SysTable tempTable,
            Engine engine) throws Exception {
        final String method = "fillTable";
        logger.entering(method, new Object[] {});
        try {
            long rowCount = 0;
            if (source instanceof IExecutable) {
                ResultSet rs = ((IExecutable) source).execute(engine).getResultSet();
                try {

                    StringBuffer sbInsert = new StringBuffer("INSERT INTO ");
                    sbInsert.append(IdentifierHandler.quote(tempTable.getTableName())).append(" (");
                    for (SysColumn sysCol : tempTable.getColumns()) {
                        String colName = sysCol.getColName();
                        sbInsert.append(IdentifierHandler.quote(colName)).append(", ");
                    }
                    sbInsert.setLength(sbInsert.length() - 2);
                    sbInsert.append(") VALUES (");
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int colCount = rsmd.getColumnCount();
                    while (rs.next()) {
                        StringBuffer sbValues = new StringBuffer();
                        int i = 1;
                        for (SysColumn sysCol : tempTable.getColumns()) {
                            if (i <= colCount) {
                                String value = rs.getString(i);
                                if (value != null
                                        && RSHelper.getQuoteInfo(sysCol)) {
                                    sbValues.append("'").append(
                                            value.replaceAll("'", "''")).append(
                                            "'");
                                } else {
                                    sbValues.append(value);
                                }
                            }
                            sbValues.append(", ");
                            i++;
                        }
                        sbValues.setLength(sbValues.length() - 2);
                        sbValues.append(")");
                        String insertStr = sbInsert.toString() + sbValues.toString();
                        if (!engine.addToBatchOnNodes(insertStr,
                                tempTable.getNodeList(), client)) {
                            executeCurrentBatch(engine);
                        }
                        rowCount++;
                    }
                } finally {
                    rs.close();
                }
                executeCurrentBatch(engine);
            }
            return rowCount;
        } finally {
            logger.exiting(method);
        }
    }

    @Override
    protected Collection<DBNode> getExecutionNodes() {
        if (executionNodes == null) {
            // default
            executionNodes = getTargetTable().getNodeList();
            if (getTempTable() == null
                    && getTargetTable().getPartitionedColumn() != null
                    && whereCondition != null) {
                Collection<DBNode> aNodeList = whereCondition.getPartitionedNode(client);
                if (aNodeList != null) {
                    executionNodes = aNodeList;
                }
            }
        }
        return executionNodes;
    }

    @Override
    protected short getPrivilege() {
        return SysPermission.PRIVILEGE_DELETE;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    @Override
    public boolean needCoordinatorConnection() {
        return false;
    }

    /**
     * We try and do a fast manual parse of a simple update statement
     *
     * @param cmd -
     *                The delete commandToExecute
     *
     * @return whether or not it was parseable.
     *
     */
    public boolean manualParse(String cmd) {
        String token;

        Lexer aLexer = new Lexer(cmd);

        if (!aLexer.hasMoreTokens()
                || !aLexer.nextToken().equalsIgnoreCase("DELETE")) {
            return false;
        }
        if (!aLexer.hasMoreTokens()
                || !aLexer.nextToken().equalsIgnoreCase("FROM")) {
            return false;
        }

        if (!aLexer.hasMoreTokens()) {
            return false;
        }
        tableName = aLexer.nextToken();

        if (!aLexer.hasMoreTokens()) {
            return true; // dangerous, no WHERE clause
        }

        // Now process WHERE conditions

        // Get tree ready, used in costing
        QueryTreeTracker aQueryTreeTracker = commandToExecute.getaQueryTreeTracker();
        aQueryTreeTracker.registerTree(aQueryTree);
        RelationNode aRelationNode = aQueryTree.newRelationNode();
        aRelationNode.setNodeType(RelationNode.TABLE);

        aRelationNode.setTableName(tableName);
        aRelationNode.setTemporaryTable(client.getTempTableName(tableName) != null);
        aRelationNode.setClient(commandToExecute.getClientContext());
        aRelationNode.setAlias("");

        token = aLexer.nextToken();

        // ok, check for where clause
        if (!token.equalsIgnoreCase("WHERE")) {
            return false;
        }

        if (!aLexer.hasMoreTokens()) {
            return false;
        }

        // We assume simple conditions only: column = value
        while (true) {
            QueryCondition aQC = ParserHelper.getSimpleCondition(aLexer,
                    getTargetTable(), aQueryTree, client);
            if (aQC == null) {
                // parser error
                return false;
            }
            if (whereCondition == null) {
                whereCondition = aQC;
            } else {
                whereCondition = ParserHelper.chainQueryConditions(
                        whereCondition, "AND", aQC);
            }

            if (!aLexer.hasMoreTokens()
                    || !aLexer.nextToken().equalsIgnoreCase("AND")) {
                break;
            }
        }

        if (whereCondition != null) {
            QueryTreeHandler.ProcessWhereCondition(whereCondition, aQueryTree,
                    commandToExecute);
        }

        if (aLexer.hasMoreTokens()) {
            token = aLexer.nextToken();
            if (!token.equals(";")) {
                return false;
            }
            if (aLexer.hasMoreTokens()) {
                return false;
            }
        }
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.parser.SqlModifyTable#getResultType()
     */
    @Override
    public int getResultType() {
        return ExecutionResult.COMMAND_DELETE_TABLE;
    }

}
