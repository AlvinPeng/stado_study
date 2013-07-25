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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.constraintchecker.IConstraintChecker;
import org.postgresql.stado.constraintchecker.UpdateForeignKeyChecker;
import org.postgresql.stado.constraintchecker.UpdateForeignReferenceChecker;
import org.postgresql.stado.constraintchecker.UpdatePrimaryKeyChecker;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
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
import org.postgresql.stado.parser.core.syntaxtree.SetUpdateClause;
import org.postgresql.stado.parser.core.syntaxtree.UpdateTable;
import org.postgresql.stado.parser.core.syntaxtree.WhereClause;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryConditionHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.SQLExpressionHandler;
import org.postgresql.stado.parser.handler.TableNameHandler;
import org.postgresql.stado.queryproc.QueryProcessor;


/**
 * This class is responsible for holding the information for Update Table
 * Syntax: UPDATE <target_table> SET <column1> = <value1>, ..., <columnN> =
 * <valueN> WHERE <condition> Temp Table: INSERT INTO <temp_table> (<column1>_new,
 * ..., <columnN>_new, <keycol1>_old, <keycolM>_old) SELECT <value1>, ...,
 * <valueN>, <keycol1>, ..., <keycolM> FROM <target_table> WHERE <condition>
 * Final statements: Update: UPDATE <target_table> SET <column1> = <temp_table>.<column1>_new,
 * ..., <columnN> = <temp_table>.<columnN>_new FROM <temp_table> WHERE
 * <target_table>.<keycol1> = <temp_table>.<keycol1>_old AND ... AND
 * <target_table>.<keycolM> = <temp_table>.<keycolM>_old [AND <node_id> = i]
 * Insert: INSERT INTO <target_table> SELECT <temp_table>.<column1>_new,...,<temp_table>.<columnN>_new
 * FROM <temp_table> LEFT JOIN <target_table> ON <target_table>.<keycol1> =
 * <temp_table>.<keycol1>_old AND ... AND <target_table>.<keycolM> =
 * <temp_table>.<keycolM>_old WHERE <target_table>.<keycol1> IS NULL AND ...
 * AND <target_table>.<keycolM> IS NULL AND <temp_table>.nodeid = <nodeID>
 * Delete: DELETE FROM <target_table> WHERE EXISTS (SELECT 1 FROM <temp_table>
 * WHERE <target_table>.<keycol1> = <temp_table>.<keycol1>_old AND ... AND
 * <target_table>.<keycolM> = <temp_table>.<keycolM>_old AND
 * <temp_table>.nodeid <> nodeid
 */
public class SqlUpdateTable extends SqlModifyTable {
    private static final XLogger logger = XLogger.getLogger(SqlUpdateTable.class);

    private Map<String, SqlExpression> setClause = new LinkedHashMap<String, SqlExpression>();

    // The column name which will be set
    // private Vector columnNameList = new Vector();
    // SQL expression to be evaluated to be set to
    // private Vector setToExpressionList = new Vector();

    // Query Condition according to which the
    // selection will take place
    private QueryCondition filterCondition;

    private QueryTree aQueryTree;

    /**
     * List of temp table columns
     */
    private Collection<SysColumn> sysColumnList;

    /**
     * List of altered columns of target table
     */
    private Collection<SysColumn> modifiedColumnList;

    private int maxSuppliedSerial = -1;

    private boolean doRowMove;

    private Collection<DBNode> executionNodes = null;

    public SqlUpdateTable(XDBSessionContext client) {
        super(client);
        commandToExecute = new Command(Command.UPDATE, this,
                new QueryTreeTracker(), client);
        aQueryTree = new QueryTree();
    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * Grammar production:
     * f0 -> <UPDATE_>
     * f1 -> TableName(prn)
     * f2 -> <SET_>
     * f3 -> SetUpdateClause(prn)
     * f4 -> ( "," SetUpdateClause(prn) )*
     * f5 -> [ WhereClause(prn) ]
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(UpdateTable n, Object argu) {
        Object _ret = null;
        QueryTreeTracker aQueryTreeTracker = commandToExecute.getaQueryTreeTracker();
        aQueryTreeTracker.registerTree(aQueryTree);

        RelationNode aRelationNode = aQueryTree.newRelationNode();
        aRelationNode.setNodeType(RelationNode.TABLE);

        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f1.accept(aTableNameHandler, argu);
        tableName = aTableNameHandler.getTableName();
        aRelationNode.setTableName(aTableNameHandler.getTableName());
        aRelationNode.setTemporaryTable(aTableNameHandler.isTemporary());
        aRelationNode.setClient(commandToExecute.getClientContext());
        aRelationNode.setAlias(aTableNameHandler.getReferenceName());

        n.f3.accept(this, argu);
        n.f4.accept(this, argu);
        n.f5.accept(this, argu);

        // Add target table's columns needed to apply updates to the query tree
        // and prepare columns of the temp table
        expandTargetTable();
        QueryTreeHandler.checkAndExpand(aQueryTree.getProjectionList(),
                aQueryTree.getRelationNodeList(), database, commandToExecute);
        for (SqlExpression expr : setClause.values()) {
            QueryTreeHandler.SetBelongsToTree(expr, aQueryTree);
        }
        List<SqlExpression> projectionOrphans = QueryTreeHandler.checkAndFillTableNames(
                aQueryTree.getProjectionList(), aQueryTree.getRelationNodeList(),
                new ArrayList<SqlExpression>(), QueryTreeHandler.PROJECTION, commandToExecute);
        aQueryTree.getSelectOrphans().addAll(projectionOrphans);

        QueryTreeHandler.FillAllExprDataTypes(aQueryTree, commandToExecute);
        QueryTreeHandler.setOwnerShipColumns(aQueryTree);

        return _ret;
    }

    /**
     * Grammar production:
     * f0 -> [ TableName(prn) "." ]
     * f1 -> Identifier(prn)
     * f2 -> "="
     * f3 -> SQLSimpleExpression(prn)
     */
    @Override
    public Object visit(SetUpdateClause n, Object argu) {
        Object _ret = null;
        if (n.f0.present()) {
            TableNameHandler aTableNameHandler = new TableNameHandler(client);
            n.f0.accept(aTableNameHandler, argu);
            if (!tableName.equalsIgnoreCase(aTableNameHandler.getTableName())) {
                throw new XDBServerException(
                "Column prefix does not match to target table name");
            }
        }
        String columnName = (String) n.f1.accept(new IdentifierHandler(), argu);
        SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                commandToExecute);
        n.f3.accept(aSqlExpressionHandler, aQueryTree);
        setClause.put(columnName, aSqlExpressionHandler.aroot);
        aQueryTree.getProjectionList().add(aSqlExpressionHandler.aroot);
        return _ret;
    }

    /**
     * f0 -> <WHERE_> f1 -> SQLComplexExpression(prn)
     *
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(WhereClause n, Object argu) {
        Object _ret = null;

        QueryConditionHandler qch = new QueryConditionHandler(commandToExecute);
        n.f1.accept(qch, aQueryTree);
        filterCondition = qch.aRootCondition;
        if (filterCondition != null) {
            aQueryTree.setWhereRootCondition(filterCondition);
            QueryTreeHandler.ProcessWhereCondition(filterCondition, aQueryTree,
                    commandToExecute);
        }
        return _ret;
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
        if (modifiedColumnList == null) {
            modifiedColumnList = new LinkedHashSet<SysColumn>();
            for (String string : setClause.keySet()) {
                modifiedColumnList.add(getTargetTable().getSysColumn(string));
            }
        }
        return modifiedColumnList;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#getPKChecker(org.postgresql.stado.MetaData.SysTable,
     *      java.util.List)
     */
    @Override
    protected IConstraintChecker getPKChecker(SysTable targetTable,
            Collection<SysColumn> columnsInvolved) {
        IConstraintChecker pkChecker = new UpdatePrimaryKeyChecker(
                getTargetTable(), client);
        columnsInvolved.addAll(pkChecker.scanConstraints(getColumnsInvolved()));
        return pkChecker;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#getFKChecker(org.postgresql.stado.MetaData.SysTable,
     *      java.util.List)
     */
    @Override
    protected IConstraintChecker getFKChecker(SysTable targetTable,
            Collection<SysColumn> columnsInvolved) {
        final String method = "getFKChecker";
        logger.entering(method, new Object[] {});
        try {

            IConstraintChecker fkChecker = new UpdateForeignKeyChecker(
                    getTargetTable(), client);
            columnsInvolved.addAll(fkChecker.scanConstraints(getColumnsInvolved()));
            return fkChecker;

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
            Collection<SysColumn> columnsInvolved) {
        final String method = "getFKChecker";
        logger.entering(method, new Object[] {});
        try {

            IConstraintChecker frChecker = new UpdateForeignReferenceChecker(
                    getTargetTable(), client);
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
        boolean tempTableRequired = true;
        if (validators.isEmpty()) {
            tempTableRequired = false;
        }
        if (!tempTableRequired) {
            if (doRowMove
                    || (filterCondition != null && !filterCondition.isSimple())) {
                tempTableRequired = true;
            }
        }
        if (tempTableRequired) {
            return super.createTempTableMetadata(targetTable, sysColumnList);
        } else {
            return null;
        }
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
                select = new QueryProcessor(client, aQueryTree);
                select.setSkipPermissionCheck(Collections.singleton(tableName));
                select.prepare();
            }
            return select;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Partitioning key is not changed: 1: UPDATE <target> SET (<cols>) =
     * (SELECT <cols>_new FROM <temp> WHERE <target>.<key> = <temp>.<key>_old)
     * WHERE <key> IN (SELECT <key>_old FROM <temp>) Postgres: UPDATE <target>
     * SET <col1> = <temp>.<col1>_new, ..., <colN> = <temp_table>.<colN>_new
     * FROM <temp> WHERE <target>.<keycol1> = <temp>.<keycol1>_old AND ... AND
     * <target>.<keycolM> = <temp>.<keycolM>_old : UPDATE <target>,
     * <temp> SET <target>.<col1> = <temp>.<col1>_new, ..., <target>.<colN> =
     * <temp_table>.<colN>_new WHERE <target>.<keycol1> = <temp>.<keycol1>_old
     * AND ... AND <target>.<keycolM> = <temp>.<keycolM>_old Partitioning key
     * is changed: Update: Additonal conditions: 1: WHERE "NODE_ID"_new =
     * nodeid AND "NODE_ID"_old = nodeid before last ) Postgres : AND
     * "NODE_ID"_new = nodeid AND "NODE_ID"_old = nodeid
     *
     * Delete rows that are gone to another node: DELETE FROM <target> WHERE
     * EXISTS (SELECT 1 FROM <temp> WHERE <target>.<keycol1> = <temp>.<keycol1>
     * AND ... AND <target>.<keycolM> = <temp>.<keycolM> AND "NODE_ID"_new <>
     * nodeid AND "NODE_ID"_old = nodeid)
     *
     * INSERT rows that are came from another node: INSERT INTO <target> (<cols>)
     * SELECT <temp>.<cols> WHERE "NODE_ID"_new = nodeid AND "NODE_ID"_old <>
     * nodeid
     *
     * @see org.postgresql.stado.parser.SqlModifyTable#prepareFinalStatements()
     */
    @Override
    protected Object prepareFinalStatements(SysTable targetTable,
            SysTable tempTable) throws Exception {
        final String method = "prepareFinalStatements";
        logger.entering(method, new Object[] {});
        try {

            String targetTableName = targetTable.getTableName();
            if (tempTable == null) {
                // Reconstruct original statement
                StringBuffer sbUpdate = new StringBuffer("UPDATE ");
                sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(" SET ");
                for (Iterator<Map.Entry<String, SqlExpression>> it = setClause.entrySet().iterator(); it.hasNext();) {
                    Map.Entry<String, SqlExpression> entry = it.next();
                    sbUpdate.append(IdentifierHandler.quote(entry.getKey())).append("=");
                    SqlExpression value = entry.getValue();
                    SqlExpression.setExpressionResultType(value, commandToExecute);
                    sbUpdate.append(value.rebuildString()).append(", ");
                }
                sbUpdate.setLength(sbUpdate.length() - 2);
                if (filterCondition != null) {
                    sbUpdate.append(" WHERE ").append(
                            filterCondition.rebuildString());
                }
                return sbUpdate.toString();
            } else {
                String tempTableName = tempTable.getTableName();
                // Prepare UPDATE statement
                StringBuffer sbUpdate = new StringBuffer("UPDATE ");
                if (Props.XDB_SQLCOMMAND_UPDATE_CORELLATEDSTYLE == 1) {
                    // 1
                    sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(" SET (");
                    StringBuffer sbTempFieldList = new StringBuffer();
                    for (String colName : setClause.keySet()) {
                        sbUpdate.append(IdentifierHandler.quote(colName)).append(", ");
                        sbTempFieldList.append(IdentifierHandler.quote(colName + "_new")).append(", ");
                    }
                    sbUpdate.setLength(sbUpdate.length() - 2);
                    sbTempFieldList.setLength(sbTempFieldList.length() - 2);
                    sbUpdate.append(")=(SELECT ");
                    sbUpdate.append(sbTempFieldList).append(" FROM ");
                    sbUpdate.append(IdentifierHandler.quote(tempTableName));
                    sbUpdate.append(" WHERE ");
                    StringBuffer sbTargetKeyList = new StringBuffer();
                    StringBuffer sbTempKeyList = new StringBuffer();
                    for (SysColumn column : targetTable.getRowID()) {
                        String colName = column.getColName();
                        if (column.isNullable()) {
                            sbUpdate.append("(");
                        }
                        sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(".");
                        sbUpdate.append(IdentifierHandler.quote(colName)).append("=");
                        sbUpdate.append(IdentifierHandler.quote(tempTableName)).append(".");
                        sbUpdate.append(IdentifierHandler.quote(colName));
                        if (column.isNullable()) {
                            sbUpdate.append(" OR ");
                            sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(".");
                            sbUpdate.append(IdentifierHandler.quote(colName)).append(" IS NULL AND ");
                            sbUpdate.append(IdentifierHandler.quote(tempTableName)).append(".");
                            sbUpdate.append(IdentifierHandler.quote(colName + "_old")).append(" IS NULL");
                            sbUpdate.append(")");
                        }
                        sbUpdate.append(" AND ");
                        sbTargetKeyList.append(IdentifierHandler.quote(colName)).append(", ");
                        sbTempKeyList.append(IdentifierHandler.quote(colName + "_old")).append(", ");
                    }
                    sbUpdate.setLength(sbUpdate.length() - 5);
                    sbTargetKeyList.setLength(sbTargetKeyList.length() - 2);
                    sbTempKeyList.setLength(sbTempKeyList.length() - 2);
                    sbUpdate.append(") WHERE ");
                    sbUpdate.append(sbTargetKeyList).append(" IN (SELECT ");
                    sbUpdate.append(sbTempKeyList).append(" FROM ");
                    sbUpdate.append(IdentifierHandler.quote(tempTableName));
                    if (!doRowMove) {
                        sbUpdate.append(")");
                        return sbUpdate.toString();
                    }
                } else if (Props.XDB_SQLCOMMAND_UPDATE_CORELLATEDSTYLE == 2) {
                    // Postres
                    sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(" SET ");
                    for (String colName : setClause.keySet()) {
                        sbUpdate.append(IdentifierHandler.quote(colName)).append("=");
                        sbUpdate.append(IdentifierHandler.quote(tempTableName)).append(".");
                        sbUpdate.append(IdentifierHandler.quote(colName + "_new")).append(", ");
                    }
                    sbUpdate.setLength(sbUpdate.length() - 2);
                    sbUpdate.append(" FROM ").append(IdentifierHandler.quote(tempTableName)).append(
                    " WHERE ");
                    for (SysColumn column : targetTable.getRowID()) {
                        String colName = column.getColName();
                        if (column.isNullable()) {
                            sbUpdate.append("(");
                        }
                        sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(".");
                        sbUpdate.append(IdentifierHandler.quote(colName)).append("=");
                        sbUpdate.append(IdentifierHandler.quote(tempTableName)).append(".");
                        sbUpdate.append(IdentifierHandler.quote(colName + "_old"));
                        if (column.isNullable()) {
                            sbUpdate.append(" OR ");
                            sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(".");
                            sbUpdate.append(IdentifierHandler.quote(colName)).append(" IS NULL AND ");
                            sbUpdate.append(IdentifierHandler.quote(tempTableName)).append(".");
                            sbUpdate.append(IdentifierHandler.quote(colName + "_old")).append(" IS NULL");
                            sbUpdate.append(")");
                        }
                        sbUpdate.append(" AND ");
                    }
                    if (!doRowMove) {
                        sbUpdate.setLength(sbUpdate.length() - 5);
                        return sbUpdate.toString();
                    }
                } else if (Props.XDB_SQLCOMMAND_UPDATE_CORELLATEDSTYLE == 3) {
                    //
                    sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(", ");
                    sbUpdate.append(IdentifierHandler.quote(tempTableName)).append(" SET ");
                    for (String colName : setClause.keySet()) {
                        sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(".");
                        sbUpdate.append(IdentifierHandler.quote(colName)).append("=");
                        sbUpdate.append(IdentifierHandler.quote(tempTableName)).append(".");
                        sbUpdate.append(IdentifierHandler.quote(colName + "_new")).append(", ");
                    }
                    sbUpdate.setLength(sbUpdate.length() - 2);
                    sbUpdate.append(" WHERE ");
                    for (SysColumn column : targetTable.getRowID()) {
                        String colName = column.getColName();
                        if (column.isNullable()) {
                            sbUpdate.append("(");
                        }
                        sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(".");
                        sbUpdate.append(IdentifierHandler.quote(colName)).append("=");
                        sbUpdate.append(IdentifierHandler.quote(tempTableName)).append(".");
                        sbUpdate.append(IdentifierHandler.quote(colName + "_old"));
                        if (column.isNullable()) {
                            sbUpdate.append(" OR ");
                            sbUpdate.append(IdentifierHandler.quote(targetTableName)).append(".");
                            sbUpdate.append(IdentifierHandler.quote(colName)).append(" IS NULL AND ");
                            sbUpdate.append(IdentifierHandler.quote(tempTableName)).append(".");
                            sbUpdate.append(IdentifierHandler.quote(colName + "_old")).append(" IS NULL");
                            sbUpdate.append(")");
                        }
                        sbUpdate.append(" AND ");
                    }
                    if (!doRowMove) {
                        sbUpdate.setLength(sbUpdate.length() - 5);
                        return sbUpdate.toString();
                    }
                } else {
                    throw new XDBServerException(
                    "Invalid xdb.sqlcommand.update.correlatedstyle value");
                }

                ArrayList<Map<DBNode, String>> finalStatements = new ArrayList<Map<DBNode, String>>(
                        3);
                StringBuffer sbInsert = new StringBuffer("INSERT INTO ");
                sbInsert.append(IdentifierHandler.quote(targetTableName)).append(" (");
                for (SysColumn col : targetTable.getColumns()) {
                    sbInsert.append(IdentifierHandler.quote(col.getColName())).append(", ");
                }
                sbInsert.setLength(sbInsert.length() - 2);
                sbInsert.append(") SELECT ");
                for (SysColumn col : targetTable.getColumns()) {
                    sbInsert.append(IdentifierHandler.quote(col.getColName() + "_new")).append(", ");
                }
                sbInsert.setLength(sbInsert.length() - 2);
                sbInsert.append(" FROM ").append(IdentifierHandler.quote(tempTableName));

                StringBuffer sbDelete = new StringBuffer("DELETE FROM ");
                sbDelete.append(IdentifierHandler.quote(targetTableName)).append(
                " WHERE EXISTS (SELECT 1 FROM ");
                sbDelete.append(IdentifierHandler.quote(tempTableName)).append(" WHERE ");
                for (SysColumn column : targetTable.getRowID()) {
                    String colName = column.getColName();
                    if (column.isNullable()) {
                        sbDelete.append("(");
                    }
                    sbDelete.append(IdentifierHandler.quote(tempTableName)).append(".");
                    sbDelete.append(IdentifierHandler.quote(colName + "_old")).append("=");
                    sbDelete.append(IdentifierHandler.quote(targetTableName)).append(".");
                    sbDelete.append(IdentifierHandler.quote(colName));
                    if (column.isNullable()) {
                        sbDelete.append(" OR ");
                        sbDelete.append(IdentifierHandler.quote(targetTableName)).append(".");
                        sbDelete.append(IdentifierHandler.quote(colName)).append(" IS NULL AND ");
                        sbDelete.append(IdentifierHandler.quote(tempTableName)).append(".");
                        sbDelete.append(IdentifierHandler.quote(colName + "_old")).append(" IS NULL");
                        sbDelete.append(")");
                    }
                    sbDelete.append(" AND ");
                }

                Map<DBNode, String> insertStatements = new HashMap<DBNode, String>();
                Map<DBNode, String> updateStatements = new HashMap<DBNode, String>();
                Map<DBNode, String> deleteStatements = new HashMap<DBNode, String>();
                for (Object element : targetTable.getNodeList()) {
                    DBNode node = (DBNode) element;
                    int nodeID = node.getNodeId();
                    String statement = sbInsert.toString() + " WHERE "
                    + IdentifierHandler.quote(NODE_ID + "_new") + " = " + nodeID + " AND "
                    + IdentifierHandler.quote(NODE_ID + "_old") + " <> " + nodeID;
                    insertStatements.put(node, statement);
                    if (Props.XDB_SQLCOMMAND_UPDATE_CORELLATEDSTYLE == 1) {
                        // Mx
                        statement = sbUpdate.toString() + " WHERE "
                        + IdentifierHandler.quote(NODE_ID + "_new") + " = " + nodeID + " AND "
                        + IdentifierHandler.quote(NODE_ID + "_old") + " = " + nodeID + ")";
                    } else if (Props.XDB_SQLCOMMAND_UPDATE_CORELLATEDSTYLE == 2) {
                        // Postres
                        statement = sbUpdate.toString()
                        + IdentifierHandler.quote(NODE_ID + "_new") + " = " + nodeID + " AND "
                        + IdentifierHandler.quote(NODE_ID + "_old") + " = " + nodeID;
                    } else if (Props.XDB_SQLCOMMAND_UPDATE_CORELLATEDSTYLE == 3) {
                        //  (finished with " AND ")
                        statement = sbUpdate.toString()
                        + IdentifierHandler.quote(NODE_ID + "_new") + " = " + nodeID + " AND "
                        + IdentifierHandler.quote(NODE_ID + "_old") + " = " + nodeID;
                    } else {
                        throw new XDBServerException(
                        "Invalid xdb.sqlcommand.update.correlatedstyle value");
                    }
                    updateStatements.put(node, statement);
                    statement = sbDelete.toString()
                    + IdentifierHandler.quote(NODE_ID + "_new") + " <> " + nodeID + " AND "
                    + IdentifierHandler.quote(NODE_ID + "_old") + " = " + nodeID + ")";
                    deleteStatements.put(node, statement);
                }
                finalStatements.add(deleteStatements);
                finalStatements.add(updateStatements);
                finalStatements.add(insertStatements);
                return finalStatements;
            }

        } finally {
            logger.exiting(method);
        }
    }

    @Override
    protected int internalExecute(Object finalStatements, Engine engine)
    throws Exception {
        if (finalStatements instanceof Collection) {
            int[] res = new int[3];
            int i = 0;
            for (Iterator it = ((Collection) finalStatements).iterator(); it.hasNext(); i++) {
                res[i] += super.internalExecute(it.next(), engine);
            }
            if (res[0] != res[2]) {
                logger.log(
                        Level.WARN,
                        "UPDATE TABLE %0%: %1% rows inserted, %2% rows deleted, numbers must be equal\n"
                        + "Final Statements dump:\n%3%", new Object[] {
                                getTableName(), new Integer(res[2]),
                                new Integer(res[0]), finalStatements });
            }
            return res[0] + res[1];
        } else {
            return super.internalExecute(finalStatements, engine);
        }

    }

    @Override
    protected Collection<DBNode> getExecutionNodes() {
        if (executionNodes == null) {
            // default
            executionNodes = getTargetTable().getNodeList();
            if (!doRowMove && getTargetTable().getPartitionedColumn() != null
                    && filterCondition != null) {
                Collection<DBNode> aNodeList = filterCondition.getPartitionedNode(client);
                if (aNodeList != null) {
                    executionNodes = aNodeList;
                }
            }
        }
        return executionNodes;
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

                    int partitionColumnNewIdx = 0;
                    int partitionColumnOldIdx = 0;
                    StringBuffer sbAddUpdate = new StringBuffer("INSERT INTO ");
                    ExpressionType partColType = null;
                    SysColumn partCol = getTargetTable().getPartitionedColumn();
                    if (partCol != null) {
                        String newAlias = partCol.getColName() + "_new";
                        String oldAlias = partCol.getColName() + "_old";
                        int i = 1;
                        for (SqlExpression columnEx : aQueryTree.getProjectionList()) {
                            if (newAlias.equals(columnEx.getAlias())) {
                                partitionColumnNewIdx = i;
                            } else if (oldAlias.equals(columnEx.getAlias())) {
                                partitionColumnOldIdx = i;
                            }
                            i++;
                        }
                        partColType = new ExpressionType(partCol);
                    }

                    SysColumn oldNodeID = null;
                    SysColumn newNodeID = null;
                    sbAddUpdate.append(IdentifierHandler.quote(tempTable.getTableName())).append(" (");
                    for (SysColumn sysCol : tempTable.getColumns()) {
                        String colName = sysCol.getColName();
                        sbAddUpdate.append(IdentifierHandler.quote(colName)).append(", ");
                        if (colName.equals(NODE_ID + "_new")) {
                            newNodeID = sysCol;
                        } else if (colName.equals(NODE_ID + "_old")) {
                            oldNodeID = sysCol;
                        }
                    }
                    sbAddUpdate.setLength(sbAddUpdate.length() - 2);
                    sbAddUpdate.append(") VALUES (");

                    ResultSetMetaData rsmd = rs.getMetaData();
                    int colCount = rsmd.getColumnCount();
                    while (rs.next()) {
                        DBNode oldNode = null;
                        DBNode newNode = null;
                        if (partCol != null) {
                            String value = rs.getString(partitionColumnOldIdx);
                            oldNode = getTargetTable().getNode(
                                    SqlExpression.createConstantExpression(
                                            value, partColType).getNormalizedValue()).iterator().next();
                            if (partitionColumnNewIdx > 0
                                    && partitionColumnNewIdx <= colCount) {
                                value = rs.getString(partitionColumnNewIdx);
                                if (value == null) {
                                    SqlExpression partExpr = partCol.getDefaultExpr(client);
                                    if (partExpr != null) {
                                        value = partExpr.rebuildString();
                                    }
                                }
                                newNode = getTargetTable().getNode(
                                        SqlExpression.createConstantExpression(
                                                value, partColType).getNormalizedValue()).iterator().next();
                            } else {
                                newNode = oldNode;
                            }
                        }
                        int i = 1;
                        StringBuffer sbUpdateValues = new StringBuffer();
                        for (Iterator<SysColumn> it = tempTable.getColumns().iterator(); it.hasNext(); i++) {
                            SysColumn sysCol = it.next();
                            if (sysCol == newNodeID) {
                                sbUpdateValues.append(newNode.getNodeId()).append(
                                ", ");
                                continue;
                            }
                            if (sysCol == oldNodeID) {
                                sbUpdateValues.append(oldNode.getNodeId()).append(
                                ", ");
                                continue;
                            }
                            String value = rs.getString(i);
                            if (value != null && RSHelper.getQuoteInfo(sysCol)) {
                                value = "'" + value.replaceAll("'", "''") + "'";
                                switch (sysCol.getColType()) {
                                case ExpressionType.TIME_TYPE:
                                    value = SqlExpression.normalizeTime(value);
                                    break;
                                case ExpressionType.TIMESTAMP_TYPE:
                                    value = SqlExpression.normalizeTimeStamp(value);
                                    break;
                                case ExpressionType.DATE_TYPE:
                                    value = SqlExpression.normalizeDate(value);
                                    break;
                                default:
                                    break;
                                }
                            }
                            sbUpdateValues.append(value).append(", ");
                            if (sysCol == getTargetTable().getSerialColumn()) {
                                maxSuppliedSerial = Math.max(maxSuppliedSerial,
                                        Integer.parseInt(value));
                            }
                        }
                        if (sbUpdateValues.length() > 0) {
                            sbUpdateValues.setLength(sbUpdateValues.length() - 2);
                            String insertStr = sbAddUpdate.toString()
                            + sbUpdateValues.toString() + ")";
                            if (oldNode == null || validators != null
                                    && !validators.isEmpty()) {
                                // Replicated table or need to run constraint
                                // checkers
                                if (engine.addToBatchOnNodes(insertStr,
                                        tempTable.getNodeList(), client)) {
                                    executeCurrentBatch(engine);
                                }
                            } else {
                                if (engine.addToBatchOnNode(insertStr, oldNode,
                                        client)) {
                                    executeCurrentBatch(engine);
                                }
                                if (oldNode != newNode) {
                                    if (engine.addToBatchOnNode(insertStr,
                                            newNode, client)) {
                                        executeCurrentBatch(engine);
                                    }
                                }
                            }
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
    protected short getPrivilege() {
        return SysPermission.PRIVILEGE_UPDATE;
    }

    /**
     * We try and do a fast manual parse of a simple update statement
     *
     * @param cmd -
     *                The update commandToExecute
     *
     * @return whether or not it was parseable.
     */
    public boolean manualParse(String cmd) {
        String token;

        Lexer aLexer = new Lexer(cmd);

        if (!aLexer.hasMoreTokens()
                || !aLexer.nextToken().equalsIgnoreCase("UPDATE")) {
            return false;
        }

        if (!aLexer.hasMoreTokens()) {
            return false;
        }
        tableName = aLexer.nextToken();

        // Get tree ready, used in costing
        QueryTreeTracker aQueryTreeTracker = commandToExecute.getaQueryTreeTracker();
        aQueryTreeTracker.registerTree(aQueryTree);

        RelationNode aRelationNode = aQueryTree.newRelationNode();
        aRelationNode.setNodeType(RelationNode.TABLE);

        aRelationNode.setTableName(tableName);
        aRelationNode.setTemporaryTable(client.getTempTableName(tableName) != null);
        aRelationNode.setClient(commandToExecute.getClientContext());
        aRelationNode.setAlias("");

        if (!aLexer.hasMoreTokens()
                || !aLexer.nextToken().equalsIgnoreCase("SET")) {
            return false;
        }

        if (!aLexer.hasMoreTokens()) {
            return false;
        }
        token = aLexer.nextToken();

        XDBSessionContext client = commandToExecute.getClientContext();
        while (true) {
            String columnName = token;

            if (!aLexer.hasMoreTokens()
                    || !aLexer.nextToken().equalsIgnoreCase("=")) {
                return false;
            }

            if (!aLexer.hasMoreTokens()) {
                return false;
            }
            SqlExpression aSE = ParserHelper.createSimpleSqlExpression(aLexer,
                    client.getSysDatabase().getSysTable(tableName),
                    aRelationNode, client);
            if (aSE == null) {
                // parse error
                return false;
            }
            setClause.put(columnName, aSE);
            aQueryTree.getProjectionList().add(aSE);
            if (!aLexer.hasMoreTokens()) {
                return false;
            }
            token = aLexer.nextToken();

            if (token.equalsIgnoreCase("WHERE")) {
                break;
            }
            if (!token.equals(",")) {
                return false;
            }

            if (!aLexer.hasMoreTokens()) {
                return false;
            }
            token = aLexer.nextToken();
        }

        // ok, check for where clause
        if (!token.equalsIgnoreCase("WHERE")) {
            return false;
        }

        // Now process WHERE conditions

        // see if there is no where clause
        if (!aLexer.hasMoreTokens()) {
            return false;
        }

        // We assume simple conditions only
        while (true) {
            QueryCondition aQC = ParserHelper.getSimpleCondition(aLexer,
                    client.getSysDatabase().getSysTable(tableName), aQueryTree,
                    client);

            if (aQC == null) {
                // parser error
                return false;
            }

            if (filterCondition == null) {
                filterCondition = aQC;
            } else {
                filterCondition = ParserHelper.chainQueryConditions(
                        filterCondition, "AND", aQC);
            }

            if (!aLexer.hasMoreTokens()
                    || !aLexer.nextToken().equalsIgnoreCase("AND")) {
                break;
            }
        }

        if (filterCondition != null) {
            QueryTreeHandler.ProcessWhereCondition(filterCondition, aQueryTree,
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

        expandTargetTable();

        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.parser.SqlModifyTable#getResultType()
     */
    @Override
    public int getResultType() {
        return ExecutionResult.COMMAND_UPDATE_TABLE;
    }

    private void expandTargetTable() {
        sysColumnList = new LinkedHashSet<SysColumn>();
        SysTable targetTable = getTargetTable();
        String partColumn = targetTable.getPartitionColumn();
        if (partColumn != null && setClause.containsKey(partColumn)) {
            /*
             * Partitioning key modified *_new - all table columns
             */
            doRowMove = true;
            for (SysColumn origin : targetTable.getColumns()) {
                int pos = origin.getNativeColDef().indexOf(origin.getColName())
                        + origin.getColName().length();
                String nativeDef = origin.getNativeColDef().substring(0, pos)
                        + "_new" + origin.getNativeColDef().substring(pos);
                SysColumn col = new SysColumn(targetTable, 0, 0,
                        origin.getColName() + "_new", origin.getColType(),
                        origin.getColLength(), origin.getColScale(),
                        origin.getColPrecision(), origin.isNullable(),
                        origin.isSerial(), nativeDef, origin.getSelectivity(),
                        origin.getDefaultExpr());
                sysColumnList.add(col);
                SqlExpression setToExpression = setClause.get(origin
                        .getColName());
                if (setToExpression == null) {
                    setToExpression = new SqlExpression();
                    setToExpression.setExprString(origin.getColName());
                    setToExpression.setExprType(SqlExpression.SQLEX_COLUMN);
                    setToExpression.setColumn(new AttributeColumn());
                    setToExpression.getColumn().columnName = origin
                            .getColName();
                    aQueryTree.getProjectionList().add(setToExpression);
                } else {
                    // Ensure the expression has proper position in the
                    // projection list
                    aQueryTree.getProjectionList().remove(setToExpression);
                    aQueryTree.getProjectionList().add(setToExpression);
                }
                setToExpression.setAlias(col.getColName());
                setToExpression.setOuterAlias(col.getColName());
                // added for update with typecast issue
                setToExpression.setExprDataType(new ExpressionType());
                setToExpression.getExprDataType().setExpressionType(
                        col.getColType(), col.getColLength(),
                        col.getColPrecision(), col.getColScale());
            }
        } else {
            /*
             * Partitioning key unchanged *_new - columns being updated
             */
            for (Entry<String, SqlExpression> setEntry : setClause.entrySet()) {
                String colName = setEntry.getKey();
                SysColumn origin = targetTable.getSysColumn(colName);
                if (origin == null) {
                    throw new XDBServerException("Column " + colName
                            + " is not found in table " + tableName);
                }
                int pos = origin.getNativeColDef().indexOf(colName)
                        + colName.length();
                String nativeDef = origin.getNativeColDef().substring(0, pos)
                        + "_new" + origin.getNativeColDef().substring(pos);
                SysColumn column = new SysColumn(targetTable, 0, 0,
                        origin.getColName() + "_new", origin.getColType(),
                        origin.getColLength(), origin.getColScale(),
                        origin.getColPrecision(), origin.isNullable(),
                        origin.isSerial(), nativeDef, origin.getSelectivity(),
                        origin.getDefaultExpr());
                sysColumnList.add(column);
            }
        }
        /*
         * In any case *_old - key columns + partitioning key
         */
        for (SysColumn origin : targetTable.getRowID()) {
            int pos = origin.getNativeColDef().indexOf(origin.getColName())
                    + origin.getColName().length();
            String nativeDef = origin.getNativeColDef().substring(0, pos)
                    + "_old" + origin.getNativeColDef().substring(pos);
            SysColumn col = new SysColumn(targetTable, 0, 0,
                    origin.getColName() + "_old", origin.getColType(),
                    origin.getColLength(), origin.getColScale(),
                    origin.getColPrecision(), origin.isNullable(),
                    origin.isSerial(), nativeDef, origin.getSelectivity(),
                    origin.getDefaultExpr());
            sysColumnList.add(col);
            SqlExpression colExpr = new SqlExpression();
            colExpr.setExprString(origin.getColName());
            colExpr.setExprType(SqlExpression.SQLEX_COLUMN);
            colExpr.setColumn(new AttributeColumn());
            colExpr.getColumn().columnName = origin.getColName();
            colExpr.setAlias(col.getColName());
            colExpr.setOuterAlias(col.getColName());
            // added for update with typecast issue
            colExpr.setExprDataType(new ExpressionType());
            colExpr.getExprDataType().setExpressionType(col.getColType(),
                    col.getColLength(), col.getColPrecision(),
                    col.getColScale());
            aQueryTree.getProjectionList().add(colExpr);
        }

        SysColumn origin = targetTable.getPartitionedColumn();
        if (origin != null) {
            int pos = origin.getNativeColDef().indexOf(origin.getColName())
                    + origin.getColName().length();
            String nativeDef = origin.getNativeColDef().substring(0, pos)
                    + "_old" + origin.getNativeColDef().substring(pos);
            SysColumn col = new SysColumn(targetTable, 0, 0,
                    origin.getColName() + "_old", origin.getColType(),
                    origin.getColLength(), origin.getColScale(),
                    origin.getColPrecision(), origin.isNullable(),
                    origin.isSerial(), nativeDef, origin.getSelectivity(),
                    origin.getDefaultExpr());
            if (!sysColumnList.contains(col)) {
                // sysColumnList.add(col);
                SqlExpression colExpr = new SqlExpression();
                colExpr.setExprString(origin.getColName());
                colExpr.setExprType(SqlExpression.SQLEX_COLUMN);
                colExpr.setColumn(new AttributeColumn());
                colExpr.getColumn().columnName = origin.getColName();
                colExpr.setAlias(col.getColName());
                colExpr.setOuterAlias(col.getColName());
                // added for update with typecast issue
                colExpr.setExprDataType(new ExpressionType());
                colExpr.getExprDataType().setExpressionType(col.getColType(),
                        col.getColLength(), col.getColPrecision(),
                        col.getColScale());
                aQueryTree.getProjectionList().add(colExpr);
            }
        }
        if (doRowMove) {
            // Temp table will contain info about node where the row was and
            // where it will be
            SysColumn column = new SysColumn(
                    targetTable,
                    0,
                    0,
                    NODE_ID + "_new",
                    Types.INTEGER,
                    -1,
                    -1,
                    -1,
                    false,
                    false,
                    IdentifierHandler.quote(NODE_ID + "_new") + " INT NOT NULL",
                    1, null);
            sysColumnList.add(column);
            column = new SysColumn(
                    targetTable,
                    0,
                    0,
                    NODE_ID + "_old",
                    Types.INTEGER,
                    -1,
                    -1,
                    -1,
                    false,
                    false,
                    IdentifierHandler.quote(NODE_ID + "_old") + " INT NOT NULL",
                    1, null);
            sysColumnList.add(column);
        }
    }
}
