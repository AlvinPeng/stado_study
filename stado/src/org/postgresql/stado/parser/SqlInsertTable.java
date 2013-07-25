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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.constraintchecker.IConstraintChecker;
import org.postgresql.stado.constraintchecker.InsertForeignKeyChecker;
import org.postgresql.stado.constraintchecker.InsertPrimaryKeyChecker;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.NodeProducerThread;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.loader.Loader;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.metadata.scheduler.LockType;
import org.postgresql.stado.misc.RSHelper;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.core.syntaxtree.ColumnNameList;
import org.postgresql.stado.parser.core.syntaxtree.InsertTable;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.SQLExpressionListItem;
import org.postgresql.stado.parser.core.syntaxtree.Select;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.SQLExpressionHandler;
import org.postgresql.stado.parser.handler.TableNameHandler;
import org.postgresql.stado.queryproc.QueryProcessor;


/**
 *
 *
 */
public class SqlInsertTable extends SqlModifyTable {
    public static final String GROUP_ROW_ID = "group__id";

    private static final XLogger logger = XLogger.getLogger(SqlInsertTable.class);

    /**
     * The tuple to insert
     */
    private Tuple tuple;

    /**
     * A list of columns
     */
    private Vector<String> columnList = new Vector<String>();

    /**
     * A list of columns
     */
    private Collection<SysColumn> sysColumnList;

    /**
     * A list of values
     */
    private Vector<SqlExpression> valueList = new Vector<SqlExpression>();

    /**
     * A Query Tree - In case we have a select query tree structure
     */

    private QueryTree aQueryTree = null;

    private Collection<DBNode> executionNodes = null;

    /* By default, the resulting rows are iterated through on
     * the coordinator. This is unnecessary for some cases.
     * We recognize the special case where we can send over
     * to the target destination node on the last step of the
     * SELECT statement.
     * Only use special INTO table optimization if we have no
     * constraint check validators, and dealing with a query
     * without DISTINCT and no UNION.
     * Optimizations for these other cases can
     * be done later, and are more complicated.
     */
    private boolean hasAutoincrement = false;

    /**
     * Constructor
     */
    public SqlInsertTable(XDBSessionContext client) {
        super(client);
        commandToExecute = new Command(Command.INSERT, this,
                new QueryTreeTracker(), client);
    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * Grammar production:
     * f0 -> <INSERT_>
     * f1 -> [ <INTO_> ]
     * f2 -> TableName(prn)
     * f3 -> [ "(" ColumnNameList(prn) ")" ]
     * f4 -> ( <VALUES_> "(" SQLExpressionList(prn) ")" | SelectWithoutOrderWithParenthesis(prn) )
     *
     * A handler for obtaining information, on a call to accept using "this"
     * object we can retrieve information as we are following the Visitor
     * pattern
     *
     * @param n
     * @param argu
     * @return
     *
     */
    @Override
    public Object visit(InsertTable n, Object argu) {
        Object _ret = null;


        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f2.accept(aTableNameHandler, argu);
        tableName = aTableNameHandler.getTableName();
        n.f3.accept(this, argu);
        if (n.f4.which == 0) {
            // Values ( SqlExpression List )
            n.f4.accept(this, argu);
            ArrayList<SysColumn> colList = new ArrayList<SysColumn>(
                    getColumnsInvolved());
            if (colList.size() < valueList.size())
                throw new XDBServerException(
                        "INSERT has more expressions than target columns");
            tuple = new Tuple(getTableName(), colList, valueList, client);
        } else {
            // Select Statement
            QueryTreeHandler aSelectQuery = new QueryTreeHandler(
                    commandToExecute);
            aQueryTree = new QueryTree();
            n.f4.choice.accept(aSelectQuery, aQueryTree);
            ArrayList<SysColumn> colList = new ArrayList<SysColumn>(
                    getColumnsInvolved());
            addProjColsIfFoundNullColumn(aQueryTree, colList);
        }
        return _ret;
    }

    /**
     *
     * If Select query is similar to "SELECT null FROM <TABLE_LIST>",
     * Add null expression for the all columns of the target table
     * in that queryTree
     *
     * @param aQueryTree
     * @return
     *
     */
    private void addProjColsIfFoundNullColumn(QueryTree aQueryTree,
            List<SysColumn> colList) {
        List<SqlExpression> projList = aQueryTree.getProjectionList();
        if (projList.size() < colList.size()) {
            for (int index = projList.size(); index < colList.size(); index++) {
                SqlExpression nullSE = new SqlExpression();
                nullSE.setExprType(SqlExpression.SQLEX_CONSTANT);
                nullSE.setConstantValue(null);
                nullSE.setExprString(null);
                nullSE.setExprDataType(new ExpressionType(colList.get(index)));

                projList.add(nullSE);
            }
        } else if (projList.size() > colList.size()) {
            /* Target table does not contain any columns */
            throw new XDBServerException(
                    "INSERT has more expressions than target columns");
        }
    }

    /**
     * Grammar production:
     * f0 -> Identifier(prn)
     * f1 -> ( "," Identifier(prn) )*
     */
    @Override
    public Object visit(ColumnNameList n, Object argu) {
        Object _ret = null;
        IdentifierHandler ih = new IdentifierHandler();
        columnList.add((String) n.f0.accept(ih, argu));
        for (Object node : n.f1.nodes) {
            NodeSequence NodSeq = (NodeSequence) node;
            columnList.add((String) NodSeq.elementAt(1).accept(ih, argu));
        }
        return _ret;
    }

    /**
     * f0 -> SQLSimpleExpression(prn)
     */
    @Override
    public Object visit(SQLExpressionListItem n, Object argu) {
        Object _ret = null;
        SQLExpressionHandler aSqlExpressionHandler = new SQLExpressionHandler(
                commandToExecute);
        SqlExpression aSqlExpression = (SqlExpression) n.f0.accept(
                aSqlExpressionHandler, argu);
        valueList.add(aSqlExpression);
        return _ret;
    }

    /**
     * f0 -> SelectWithoutOrder(prn) f1 -> [ OrderByClause(prn) ]
     */
    @Override
    public Object visit(Select n, Object argu) {
        Object _ret = null;
        n.f0.accept(this, argu);
        n.f1.accept(this, argu);
        return _ret;
    }

    // ******************************
    // END GRAMMAR
    // ******************************

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getLockSpecs(java.lang.Object)
     */
    @Override
    public LockSpecification<SysTable> getLockSpecs() {
        if (lockSpecs == null) {
            Collection<SysTable> empty = Collections.emptyList();
            lockSpecs = new LockSpecification<SysTable>(getReadTables(), empty);
            lockSpecs.add(getTargetTable(), LockType.get(
                    LockType.LOCK_SHARE_READ_INT, false));
        }
        return lockSpecs;
    }

    /**
     * This will provide the functionalty to get the columns invloved in the
     * insert statement.
     *
     * @return
     * @see org.postgresql.stado.parser.SqlModifyTable#getColumnsInvolved()
     */
    @Override
    protected Collection<SysColumn> getColumnsInvolved() {
        if (sysColumnList == null) {
            sysColumnList = new LinkedHashSet<SysColumn>();
            // If we find the table -- check for the column list size. incase it
            // is zero
            // this implies that the user did not specify any columns to insert
            // into therefore
            // get all the columns from the systable and add them to the list of
            // columns
            if (columnList.size() == 0) {
                // Create a new List, which will be filled and returned
                for (SysColumn aSysColumn : getTargetTable().getColumns()) {
                    if (!SqlCreateTableColumn.XROWID_NAME.equals(aSysColumn.getColName())) {
                        sysColumnList.add(aSysColumn);
                        columnList.add(aSysColumn.getColName());
                    }
                }
            } else {
                // Incase the user did specifiy some columns.
                // Go though them , get there column name and add them to the
                // sysColumnList
                for (String colName : columnList) {
                    SysColumn column = getTargetTable().getSysColumn(colName);
                    if (column == null) {
                        throw new XDBServerException("Column " + colName
                                + " is not found in table " + tableName);
                    }
                    sysColumnList.add(column);
                }
            }
        }
        return sysColumnList;
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
        final String method = "getPKChecker";
        logger.entering(method, new Object[] {});
        try {

            IConstraintChecker pkChecker = new InsertPrimaryKeyChecker(
                    getTargetTable(), tuple, client);
            columnsInvolved.addAll(pkChecker.scanConstraints(getColumnsInvolved()));
            return pkChecker;

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
    protected IConstraintChecker getFKChecker(SysTable targetTable,
            Collection<SysColumn> columnsInvolved) {
        final String method = "getFKChecker";
        logger.entering(method, new Object[] {});
        try {

            IConstraintChecker fkChecker = new InsertForeignKeyChecker(
                    getTargetTable(), tuple, client);
            columnsInvolved.addAll(fkChecker.scanConstraints(getColumnsInvolved()));
            return fkChecker;

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.SqlModifyTable#getPKChecker(org.postgresql.stado.MetaData.SysTable,
     *      java.util.List)
     */
    @Override
    protected IConstraintChecker getFRChecker(SysTable targetTable,
            Collection<SysColumn> columnsInvolved) {
        return null;
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
        if (tuple == null) {
            for (Object element : targetTable.getColumns()) {
                SysColumn col = (SysColumn) element;
                if (!columnsInvolved.contains(col)
                        && (col.getDefaultExpr() != null
                        || SqlCreateTableColumn.XROWID_NAME.equals(col.getColName())
                        || col.isSerial())) {
                    columnsInvolved.add(col);
                    hasAutoincrement = true;
                }
            }
            SysTable tempTable = super.createTempTableMetadata(targetTable,
                    columnsInvolved);
            tempTable.setTableTemporary(!Props.XDB_USE_LOAD_FOR_STEP);
            return tempTable;
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
            if (tuple == null) {
                if (validators != null && validators.size() == 0
                        && aQueryTree.getUnionQueryTreeList().size() == 0
                        && !aQueryTree.isDistinct()
                        && (tempTable.getPartitionScheme() == SysTable.PTYPE_HASH
                                || tempTable.getPartitionScheme() == SysTable.PTYPE_ROBIN)
                                && !hasAutoincrement) {

                    // We could insert directly into the final table for this case,
                    // but there is an issue with the loader. Until that is fixed,
                    // we copy to the final temp table, and load from it.
                    //setUsesFinalTempTable(false);
                    //aQueryTree.setIntoTable(tableName, tableName, false);

                    aQueryTree.setIntoTable(tempTable.getTableName(), tempTable.getTableName(), true);

                    aQueryTree.setIntoTablePartitioning(
                            tempTable.getPartitionScheme(),
                            tempTable.getPartitionedColumn() != null ?
                                    tempTable.getPartitionedColumn().getColName() : null,
                                    tempTable.getPartitionMap());

                    aQueryTree.setIsInsertSelect(true);
                }

                QueryProcessor qProcessor = new QueryProcessor(client, aQueryTree);
                qProcessor.prepare(false);
                return qProcessor;
            } else {
                return tuple;
            }

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

            if (tuple != null) {
                StringBuffer sbInsert = new StringBuffer("INSERT INTO ");
                sbInsert.append(IdentifierHandler.quote(getTableName())).append(" (");
                StringBuffer sbValues = new StringBuffer(") VALUES (");
                for (SysColumn column : getTargetTable().getColumns()) {
                    String colName = column.getColName();
                    sbInsert.append(IdentifierHandler.quote(colName)).append(", ");
                    String value = tuple.getValue(column);
                    sbValues.append(value).append(", ");
                }
                sbInsert.setLength(sbInsert.length() - 2);
                sbValues.setLength(sbValues.length() - 2);
                sbInsert.append(sbValues).append(")");
                return sbInsert.toString();
            }
            StringBuffer sbCommand = new StringBuffer("INSERT INTO ");
            sbCommand.append(IdentifierHandler.quote(targetTable.getTableName())).append(" (");
            StringBuffer sbValues = new StringBuffer(") SELECT ");
            for (SysColumn column : targetTable.getColumns()) {
                if (sysColumnList.contains(column)) {
                    String colName = column.getColName();
                    sbCommand.append(IdentifierHandler.quote(colName)).append(", ");
                    sbValues.append(IdentifierHandler.quote(colName)).append(", ");
                }
            }
            sbCommand.setLength(sbCommand.length() - 2);
            sbValues.setLength(sbValues.length() - 2);
            sbCommand.append(sbValues);
            sbCommand.append(" FROM ").append(IdentifierHandler.quote(tempTable.getTableName()));
            return sbCommand.toString();

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
            if (tuple == null) {
                if (source instanceof QueryProcessor) {
                    QueryProcessor queryProc = (QueryProcessor) source;
                    if (getParamCount() > 0) {
                        queryProc.prepareParameters(commandToExecute.getParameters());
                        queryProc.resetExecPlan(commandToExecute.getParameters());
                    }

                    // Check if we can run via the shorter path, where the
                    // final step will send to the appropriate nodes.
                    if (aQueryTree.getIntoTableName() != null) {
                        queryProc.execute(engine);
                    } else {
                        ResultSet rs = queryProc.execute(engine).getResultSet();
                        try {
                            if (Props.XDB_USE_LOAD_FOR_STEP) {
                                Loader loader = new Loader(rs, null);
                                loader.setLocalTableInfo(tempTable, client,
                                        columnList, false);
                                try {
                                    // loader.setVerbose(logger.isDebugEnabled());
                                    loader.prepareLoad();
                                    boolean success = false;
                                    try {
                                        NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Run Writers");
                                        loader.runWriters();
                                        success = true;
                                    } finally {
                                        NodeProducerThread.CATEGORY_NODEQUERYTIME.debug("Finish load");
                                        loader.finishLoad(success);
                                    }
                                    return loader.getRowCount();
                                } catch (Exception e) {
                                    logger.catching(e);
                                    XDBServerException ex = new XDBServerException(
                                            "Can not send data to Nodes", e);
                                    logger.throwing(ex);
                                    throw ex;
                                }
                            } else {
                                int partitionColumnIdx = 0;
                                StringBuffer sbInsert = new StringBuffer(
                                "INSERT INTO ");
                                sbInsert.append(IdentifierHandler.quote(tempTable.getTableName())).append(
                                " (");
                                for (SysColumn sysCol : tempTable.getColumns()) {
                                    String colName = sysCol.getColName();
                                    sbInsert.append(IdentifierHandler.quote(colName)).append(", ");
                                }
                                SysColumn partCol = getTargetTable().getPartitionedColumn();
                                ExpressionType partColType = null;
                                if (partCol != null) {
                                    partitionColumnIdx = tempTable.getSysColumn(
                                            partCol.getColName()).getColSeq() + 1;
                                    partColType = new ExpressionType(partCol);
                                }
                                sbInsert.setLength(sbInsert.length() - 2);
                                sbInsert.append(") VALUES (");
                                ResultSetMetaData rsmd = rs.getMetaData();
                                int colCount = rsmd.getColumnCount();
                                while (rs.next()) {
                                    StringBuffer sbValues = new StringBuffer();
                                    String partitioningValue = null;
                                    if (partitionColumnIdx > 0
                                            && partitionColumnIdx <= colCount) {
                                        partitioningValue = rs.getString(partitionColumnIdx);
                                    } else if (partCol != null) {
                                        SqlExpression partExpr = partCol.getDefaultExpr(client);
                                        if (partExpr != null) {
                                            partitioningValue = partExpr.rebuildString();
                                        }
                                    }
                                    Collection<DBNode> targetNodes = getTargetTable().getNode(
                                            SqlExpression.createConstantExpression(
                                                    partitioningValue, partColType).getNormalizedValue());
                                    int i = 1;
                                    for (SysColumn sysCol : tempTable.getColumns()) {
                                        String value = null;
                                        if (i == partitionColumnIdx) {
                                            value = partitioningValue;
                                        } else {
                                            if (i <= colCount) {
                                                value = rs.getString(i);
                                            } else {
                                                SqlExpression defExpr = sysCol.getDefaultExpr(client);
                                                if (defExpr != null) {
                                                    partitioningValue = defExpr.rebuildString();
                                                }
                                            }
                                        }

                                        if (value != null
                                                && RSHelper.getQuoteInfo(sysCol)) {
                                            value = "'"
                                                + value.replaceAll("'", "''")
                                                + "'";
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
                                        sbValues.append(value);
                                        sbValues.append(", ");
                                        i++;
                                    }
                                    sbValues.setLength(sbValues.length() - 2);
                                    String insertStr = sbInsert.toString()
                                    + sbValues.toString() + ")";
                                    if (engine.addToBatchOnNodes(insertStr,
                                            targetNodes, client)) {
                                        executeCurrentBatch(engine);
                                    }
                                    rowCount++;
                                }
                            }
                        } finally {
                            rs.close();
                        }
                        executeCurrentBatch(engine);
                    }
                } else {
                    XDBServerException ex = new XDBServerException(
                    "Data Source is not supported");
                    logger.throwing(ex);
                    throw ex;
                }
            }
            return rowCount;
        } finally {
            logger.exiting(method);
        }
    }

    @Override
    protected Collection<DBNode> getExecutionNodes() {
        if (executionNodes == null) {
            SqlExpression partValue = null;
            if (tuple == null) {
                executionNodes = getTargetTable().getNodeList();
            } else {
                SysColumn partColumn = getTargetTable().getPartitionedColumn();
                if (partColumn != null) {
                    try {
                        partValue = Engine.getInstance().evaluate(
                                client, tuple.getExpression(partColumn));
                    } catch (SQLException se) {
                        logger.catching(se);
                        throw new XDBServerException(
                                "Can not determine partitioning value", se);
                    }
                }
                executionNodes = getTargetTable().getNode(
                        partValue == null ? null
                                : partValue.getNormalizedValue());
            }
        }
        return executionNodes;
    }

    @Override
    protected short getPrivilege() {
        return SysPermission.PRIVILEGE_INSERT;
    }

    /**
     * We try and do a fast manual parse of a simple insert statement
     *
     * @param cmd -
     *                The insert command
     *
     * @return whether or not it was parseable.
     *
     */
    public boolean manualParse(String cmd) {

        String token;
        Lexer aLexer = new Lexer(cmd);

        if (!aLexer.hasMoreTokens()
                || !aLexer.nextToken().equalsIgnoreCase("INSERT")) {
            return false;
        }
        if (!aLexer.hasMoreTokens()
                || !aLexer.nextToken().equalsIgnoreCase("INTO")) {
            return false;
        }

        if (!aLexer.hasMoreTokens()) {
            return false;
        }
        tableName = aLexer.nextToken();
        // no column list, we need to add
        client.getSysDatabase().getSysTable(tableName);

        if (!aLexer.hasMoreTokens()) {
            return false;
        }
        token = aLexer.nextToken();

        // Columns listed
        if (token.equals("(")) {
            while (aLexer.hasMoreTokens()) {
                if (!aLexer.hasMoreTokens()) {
                    return false;
                }
                token = aLexer.nextToken();

                columnList.add(token);

                if (!aLexer.hasMoreTokens()) {
                    return false;
                }
                token = aLexer.nextToken();

                if (token.equals(")")) {
                    break;
                }
                if (!token.equals(",")) {
                    return false;
                }
            }
            if (!aLexer.hasMoreTokens()) {
                return false;
            }
            token = aLexer.nextToken();
        }

        if (!token.equalsIgnoreCase("VALUES")) {
            return false;
        }

        if (!aLexer.hasMoreTokens()) {
            return false;
        }
        token = aLexer.nextToken();

        if (!token.equals("(")) {
            return false;
        }

        // int i = 0;
        XDBSessionContext client = commandToExecute.getClientContext();
        while (true) {
            if (!aLexer.hasMoreTokens()) {
                return false;
            }
            SqlExpression aSE = ParserHelper.createSimpleSqlExpression(aLexer,
                    client.getSysDatabase().getSysTable(tableName), null,
                    client);
            if (aSE == null) {
                // parser error
                return false;
            }

            this.valueList.add(aSE);

            if (!aLexer.hasMoreTokens()) {
                return false;
            }
            token = aLexer.nextToken();

            if (token.equals(")")) {
                break;
            }
            if (!token.equals(",")) {
                return false;
            }
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

        // Everything looks ok
        tuple = new Tuple(getTableName(), new ArrayList<SysColumn>(
                getColumnsInvolved()), valueList, client);

        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.parser.SqlModifyTable#getResultType()
     */
    @Override
    public int getResultType() {
        return ExecutionResult.COMMAND_INSERT_TABLE;
    }

    @Override
    public void setParamValues(String[] values)
            throws ArrayIndexOutOfBoundsException, XDBServerException {
        super.setParamValues(values);
        // clear the cache
        executionNodes = null;
    }

    @Override
    public void setParamValue(int index, String value)
            throws ArrayIndexOutOfBoundsException, XDBServerException {
        super.setParamValue(index, value);
        // clear the cache
        executionNodes = null;
    }

    public void reset() {
        super.reset();
        if (tuple != null)
            tuple.reset();
    }
}
