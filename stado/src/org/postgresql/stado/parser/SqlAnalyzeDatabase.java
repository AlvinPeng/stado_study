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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.AnalyzeDatabase;
import org.postgresql.stado.parser.core.syntaxtree.ColumnNameList;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.TableName;
import org.postgresql.stado.parser.core.syntaxtree.UpdateStats;
import org.postgresql.stado.parser.core.syntaxtree.VacuumDatabase;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.ColumnNameListHandler;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.TableNameHandler;


public class SqlAnalyzeDatabase extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger.getLogger(SqlAnalyzeDatabase.class);

    private static final String VACUUM_TYPE_NONE = "";

    private static final String VACUUM_TYPE_FULL = "FULL";

    private static final String VACUUM_TYPE_FREEZE = "FREEZE";

    private XDBSessionContext client;

    private SysDatabase database;

    private boolean doVacuum = false;

    private boolean doAnalyze = false;

    private String vacuumType = VACUUM_TYPE_NONE;

    private String tableName = null;

    private List<String> columnNameList = null;

    private String templateUpdate;

    private String templateQuery;

    private HashMap<String, String> params;

    private Map<SysTable, Object[]> statements;

    public SqlAnalyzeDatabase(XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
    }

    /**
     * Grammar production: f0 -> ( <UPDATE_STAT_> | <UPDATE_> <STATISTICS_> ) f1 -> (
     * <COLUMN_> ( ( "(" <STAR_> ")" | "(" ColumnNameList(prn) ")" ) <FOR_>
     * TableName(prn) ) | TableName(prn) | <STAR_> )
     */
    @Override
    public void visit(UpdateStats n, Object argu) {
        doVacuum = true;
        doAnalyze = true;
        n.f1.accept(this, argu);
    }

    /**
     * Grammar production: f0 -> <VACUUM_> f1 -> [ <FULL_> | <FREEZE_> ] f2 -> [
     * TableName(prn) | AnalyzeDatabase(prn) ]
     */
    @Override
    public void visit(VacuumDatabase n, Object obj) {
        doVacuum = true;
        if (n.f1.present()) {
            NodeChoice nc = (NodeChoice) n.f1.node;
            if (nc.which == 0) {
                vacuumType = VACUUM_TYPE_FULL;
            } else {
                vacuumType = VACUUM_TYPE_FREEZE;
            }
        }
        n.f2.accept(this, obj);
    }

    /**
     * Grammar production: f0 -> <ANALYZE_> f1 -> [ TableName(prn) [ "("
     * ColumnNameList(prn) ")" ] ]
     */
    @Override
    public void visit(AnalyzeDatabase n, Object obj) {
        doAnalyze = true;
        n.f1.accept(this, obj);
    }

    /**
     * Grammar production: f0 -> ( <IDENTIFIER_NAME> | <TEMPDOT_>
     * <IDENTIFIER_NAME> )
     */
    @Override
    public void visit(TableName n, Object obj) {
        TableNameHandler aHandler = new TableNameHandler(client);
        n.accept(aHandler, obj);
        tableName = aHandler.getTableName();
    }

    @Override
    public void visit(ColumnNameList n, Object obj) {
        ColumnNameListHandler aHandler = new ColumnNameListHandler();
        n.accept(aHandler, obj);
        columnNameList = aHandler.getColumnNameList();
    }

    /**
     * This will return the cost of executing this statement in time , milli
     * seconds
     */
    public long getCost() {
        return ILockCost.HIGH_COST;
    }

    /**
     * This return the Lock Specification for the system
     *
     * @param theMData
     * @return
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Collection<SysTable> tables;
        if (tableName == null) {
            tables = new LinkedList<SysTable>();
            Enumeration tablesEnum = database.getAllTables();
            while (tablesEnum.hasMoreElements()) {
                SysTable table = (SysTable) tablesEnum.nextElement();
                if (!table.isTemporary()) {
                    tables.add(table);
                }
            }
        } else {
            tables = Collections.singleton(database.getSysTable(tableName));
        }
        Collection<SysTable> empty = Collections.emptyList();
        if (vacuumType == VACUUM_TYPE_FULL) {
            return new LockSpecification<SysTable>(empty, tables);
        } else {
            return new LockSpecification<SysTable>(tables, empty);
        }
    }

    public Collection<DBNode> getNodeList() {
        if (tableName == null) {
            return database.getDBNodeList();
        } else {
            return database.getSysTable(tableName).getNodeList();
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return statements != null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        if (!isPrepared()) {
            if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA) {
                XDBSecurityException ex = new XDBSecurityException("Only "
                        + SysLogin.USER_CLASS_DBA_STR
                        + " can update statistics");
                logger.throwing(ex);
                throw ex;
            }
            if (columnNameList == null) {
                if (doVacuum && doAnalyze) {
                    templateUpdate = Props.XDB_SQLCOMMAND_VACUUM_ANALYZE_TEMPLATE_TABLE;
                } else if (doVacuum) {
                    templateUpdate = Props.XDB_SQLCOMMAND_VACUUM_TEMPLATE_TABLE;
                } else if (doAnalyze) {
                    templateUpdate = Props.XDB_SQLCOMMAND_ANALYZE_TEMPLATE_TABLE;
                }
            } else {
                if (doVacuum && doAnalyze) {
                    templateUpdate = Props.XDB_SQLCOMMAND_VACUUM_ANALYZE_TEMPLATE_COLUMN;
                } else if (doVacuum) {
                    templateUpdate = Props.XDB_SQLCOMMAND_VACUUM_TEMPLATE_COLUMN;
                } else if (doAnalyze) {
                    templateUpdate = Props.XDB_SQLCOMMAND_ANALYZE_TEMPLATE_COLUMN;
                }
            }

            if (templateUpdate == null || templateUpdate.length() == 0) {
                XDBServerException ex = new XDBServerException(
                        "Template is not specified for such kind of VACUUM ... ANALYZE");
                logger.throwing(ex);
                throw ex;
            }

            templateQuery = Props.XDB_SQLCOMMAND_UPDATESTATISTICS_QUERY;
            params = new HashMap<String, String>();

            // User can be different for every Node and astually should be taken
            // as Node.getJdbcUser(). But we can use in template user()
            // function,
            // if needed(?) So, comment it out.
            // params.put("dbuser", database.getDbusername());
            params.put("vacuum_type", vacuumType);
            if (tableName == null) {
                statements = new HashMap<SysTable, Object[]>();
                for (Enumeration tables = database.getAllTables(); tables.hasMoreElements();) {
                    SysTable table = (SysTable) tables.nextElement();
                    if (!table.isTemporary()) {
                        statements.put(table, createStatements(table));
                    }
                }
            } else {
                SysTable table = database.getSysTable(tableName);
                statements = Collections.singletonMap(table,
                        createStatements(table));
            }
        }
    }

    private Object[] createStatements(SysTable table) throws Exception {
        Object[] result = doAnalyze ? new Object[4] : new Object[1];
        params.put("table", IdentifierHandler.quote(table.getTableName()));
        // result[0] = vacuum analyze (update statistics) command
        Collection<SysColumn> columns = createColumnList(table);
        if (columnNameList != null) {
            StringBuffer sb = new StringBuffer();
            for (SysColumn column : columns) {
                sb.append(IdentifierHandler.quote(column.getColName())).append(", ");
            }
            params.put("column_list", sb.substring(0, sb.length() - 2));
        }
        result[0] = ParseCmdLine.substitute(templateUpdate, params);
        if (doAnalyze) {
            // result[1] = row count query
            String template = Props.XDB_SQLCOMMAND_UPDATESTATISTICS_ROWCOUNT;
            boolean quoted = Props.XDB_SQLCOMMAND_UPDATESTATISTICS_ROWCOUNT_QUOTED;
            if (templateQuery == null || template.length() == 0) {
                template = "SELECT count(*) FROM {table}";
                quoted = true;
            }
            if (!quoted) {
                params.put("table", table.getTableName());
            }
            result[1] = ParseCmdLine.substitute(template, params);
            if (!quoted) {
                params.put("table", IdentifierHandler.quote(table.getTableName()));
            }
            // result[2] = query to get distinct counts from nodes
            // String array (one for every column) if template is defined
            // single String otherwise
            if (templateQuery == null || templateQuery.length() == 0) {
                StringBuffer sb = new StringBuffer("SELECT ");
                for (SysColumn col : columns) {
                    if (columnNameList == null && isUnique(col)) {
                        sb.append("null, ");
                    } else {
                        sb.append("count (distinct ").append(col.getColName()).append(
                                "), ");
                    }
                }
                sb.append("count (*) from ").append(table.getTableName());
                result[2] = sb.toString();
            } else {
                String[] queries = new String[columns.size()];
                int pos = 0;
                
                if (quoted) {
                    params.put("table", IdentifierHandler.quote(table.getTableName()));
                } else {        
                    params.put("table", table.getTableName());
                }
                for (SysColumn col : columns) {
                    if (quoted) {
                        params.put("column", IdentifierHandler.quote(col.getColName()));
                    } else {
                        params.put("column", col.getColName());
                    }
                    queries[pos++] = ParseCmdLine.substitute(templateQuery, params);
                }
                result[2] = queries;
            }
            // result[3] = column list
            result[3] = columns;
        }
        return result;
    }

    private boolean isUnique(SysColumn col) {
        return col.bestIndexColPos == 1
                && col.bestIndex.getIndexKeys().size() == 1
                && (col.bestIndex.idxtype == 'U' || col.bestIndex.idxtype == 'P');
    }

    private Collection<SysColumn> createColumnList(SysTable table) {
        Collection<SysColumn> columns;
        if (columnNameList == null) {
            // Do only indexed columns
            columns = new ArrayList<SysColumn>(table.getColumns().size());
            for (Object element : table.getColumns()) {
                SysColumn col = (SysColumn) element;
                if (!col.getColName().equalsIgnoreCase(
                        SqlCreateTableColumn.XROWID_NAME)) {
                    columns.add(col);
                }
            }
        } else {
            columns = new ArrayList<SysColumn>(columnNameList.size());
            for (Object element : columnNameList) {
                String colName = (String) element;
                SysColumn column = table.getSysColumn(colName);
                if (column == null) {
                    throw new XDBServerException("Column " + colName
                            + " is not found in table " + table.getTableName());
                }
                columns.add(column);
            }
        }
        return columns;
    }

    public ExecutionResult execute(Engine engine) throws Exception {
        if (!isPrepared()) {
            prepare();
        }
        LinkedList<String> failedTables = new LinkedList<String>();
        LinkedList<String> failedMessages = new LinkedList<String>();
        for (Map.Entry<SysTable,Object[]> entry : statements.entrySet()) {
            SysTable table = entry.getKey();
            Object[] toRun = entry.getValue();
            try {
                // Step one: execute vacuum analyze (update stats) on nodes

                String command = (String) toRun[0];
                // amart: UPDATE STATISTICS does implicit commit on
                // nodes,
                // so subsequent "subtrans end" generates error on Nodes
                // using ME directly is workaround
                // amart: ... and do autocommit for Postgres' VACUUM
                // compatibility
                MultinodeExecutor anExecutor = client.getMultinodeExecutor(table.getNodeList());
                anExecutor.execute(command, table.getNodeList(), true);
                if (!doAnalyze) {
                    continue;
                }

                // Step two: count all rows and put a result to MetaData
                long count = 0;
                Collection<DBNode> nodeList = table.getJoinNodeList();
                Map<Integer, ResultSet> results = engine.executeQueryOnMultipleNodes(
                        (String) toRun[1], nodeList, client);
                for (ResultSet rs : results.values()) {
                    try {
                        if (rs.next()) {
                            count += rs.getLong(1);
                        } else {
                            throw new XDBServerException(
                                    "Could not count rows for table: " + table.getTableName());
                        }
                    } finally {
                        rs.close();
                    }
                }
                String metaUpdateStr = "UPDATE xsystables SET numrows=" + count
                        + " where tableid=" + table.getTableId();
                MetaData.getMetaData().executeUpdate(metaUpdateStr);
                table.setNumrows(count);
                // Step three: query statistics info and calculate number of
                // distinct values
                Collection columns = (Collection) toRun[3];
                long[] distinctValuesNums = new long[columns.size()];
                Arrays.fill(distinctValuesNums, 0);
                if (count > 0) {
                    if (toRun[2] instanceof String) {
                        Map resultSets = engine.executeQueryOnMultipleNodes(
                                (String) toRun[2], nodeList, client);
                        for (Iterator iter = resultSets.values().iterator(); iter.hasNext();) {
                            ResultSet rsDistinct = (ResultSet) iter.next();
                            try {
                                if (rsDistinct.next()) {
                                    int countOnNode = rsDistinct.getInt(distinctValuesNums.length + 1);
                                    if (countOnNode == 0) {
                                        continue;
                                    }
                                    for (int i = 0; i < distinctValuesNums.length; i++) {
                                        long distCount = rsDistinct.getLong(i + 1);
                                        if (!rsDistinct.wasNull()) {
                                            distinctValuesNums[i] += distCount
                                                    * countOnNode / count;
                                        }
                                    }
                                }
                            } finally {
                                rsDistinct.close();
                            }
                        }
                    } else // String []
                    {
                        String[] queries = (String[]) toRun[2];
                        for (int i = 0; i < queries.length; i++) {
                            Map resultSets = engine.executeQueryOnMultipleNodes(
                                    queries[i], nodeList, client);
                            int nodecount = nodeList.size();
                            for (Iterator iter = resultSets.values().iterator(); iter.hasNext();) {
                                ResultSet rsDistinct = (ResultSet) iter.next();
                                try {
                                    if (rsDistinct.next()) {
                                        long distCount = rsDistinct.getLong(1);
                                        if (distCount > 0) {
                                            distinctValuesNums[i] += distCount
                                                    / nodecount;
                                        } else {
                                            distinctValuesNums[i] += (0.0 - rsDistinct.getDouble(1))
                                                    * count / nodecount;
                                        }
                                    }
                                } finally {
                                    rsDistinct.close();
                                }
                            }
                        }
                    }
                }

                // Step four: update selectivity
                int pos = 0;
                for (Iterator it1 = columns.iterator(); it1.hasNext();) {
                    double selectivity = 0;
                    SysColumn col = (SysColumn) it1.next();
                    long sum = distinctValuesNums[pos] == 0 ? count
                            : distinctValuesNums[pos];
                    pos++;
                    if (sum > 0) {
                        selectivity = 1.0 / sum;
                    }
                    col.setSelectivity(selectivity);
                    String updateSelectivity = "update xsyscolumns set selectivity = "
                            + selectivity + " where colid = " + col.getColID();
                    MetaData.getMetaData().executeUpdate(updateSelectivity);
                }
            } catch (Exception ex) {
                logger.catching(ex);
                failedTables.add(table.getTableName());
                failedMessages.add(ex.getLocalizedMessage());
            }
        }
        if (failedTables.size() > 0) {
            StringBuffer msg = new StringBuffer(
                    "Failed to update statistics for table(s): ");
            // Do not use \n here, it does not get displayed to the user
            for (Object element : failedTables) {
                msg.append(element).append(" ");
            }
            msg.append("; ");
            for (Object element : failedMessages) {
                msg.append(element).append("; ");
            }
            
            XDBServerException ex = new XDBServerException(msg.toString());
            logger.throwing(ex);
            throw ex;
        }
        return ExecutionResult.createSuccessResult(ExecutionResult.COMMAND_VACUUM_ANALYZE);
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
