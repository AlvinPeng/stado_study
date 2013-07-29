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
/*
 * MetaDataUtil.java
 */

package org.postgresql.stado.common.util;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Pattern;

import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.ResultSetImpl;
import org.postgresql.stado.engine.ExecutableRequest;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.datatypes.BooleanType;
import org.postgresql.stado.engine.datatypes.IntegerType;
import org.postgresql.stado.engine.datatypes.LongType;
import org.postgresql.stado.engine.datatypes.ShortType;
import org.postgresql.stado.engine.datatypes.TimestampType;
import org.postgresql.stado.engine.datatypes.VarcharType;
import org.postgresql.stado.engine.datatypes.XData;
import org.postgresql.stado.engine.io.DataTypes;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.Node;
import org.postgresql.stado.metadata.SysAgent;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysConstraint;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysForeignKey;
import org.postgresql.stado.metadata.SysIndex;
import org.postgresql.stado.metadata.SysIndexKey;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysReference;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysUser;
import org.postgresql.stado.metadata.SysView;
import org.postgresql.stado.metadata.SysViewColumns;
import org.postgresql.stado.parser.SqlCreateTableColumn;


/**
 * getCatalogs(): cmdline ->'SHOW DATABASES' return format:DATABASE;STATUS;NODES
 * getTables(): cmdline ->'SHOW TABLES' return
 * format:TABLE_NAME;TABLE_PARTITIONING_COLUMN;TABLE_NODES getColumns(): cmdline
 * ->'DESCRIBE <tablename>' return
 * format:COLUMN_NAME;SQL_DATA_TYPE;TYPE_NAME;IS_NULLABLE;KEY;DEFAULT
 *
 *  Clients are not allowed to query the sys tables. This class
 *         allows us to utilize the XDB MetaData package to support some
 *         function calls on the jdbc side. Most of the APIs have arguments
 *         passed to them, so we need to do the same here. Each constant below
 *         will map to a method in the java.sql.DatabaseMetaData class
 *
 */

public class MetaDataUtil implements XDBServerPropNames {
    // private static final XLogger logger =
    // XLogger.getLogger(MetaDataUtil.class);
    private static final XLogger logger = XLogger.getLogger("Server");

    public static final String GET_CATALOGS = "getCatalogs";

    public static final String GET_COLUMNS = "getColumns";

    public static final String GET_DESCRIBE_TABLE = "getDescribeTable";

    public static final String GET_TABLES = "getTables";

    public static final String GET_STATEMENTS = "getStatements";

    public static final String GET_PRIMARY_KEY = "getPrimaryKey";

    public static final String GET_INDEX_INFO = "getIndexInfo";

    public static final String GET_IMPORTED_KEYS = "getImportedKeys";

    public static final String GET_EXPORTED_KEYS = "getExportedKeys";

    public static final String GET_CROSS_REFERENCE = "getCrossReference";

    public static final String GET_TABLE_TYPES = "getTableTypes";

    public static final String GET_TYPE_INFO = "getTypeInfo";

    /** Creates a new instance of MetaDataUtil */

    private MetaDataUtil() {

    }

    /**
     * @param cmd
     *                the method name + its params
     */

    public static ResultSet getInfo(String cmd, XDBSessionContext client)
            throws SQLException {

        String[] p = parseParams(cmd);
        String api = p[0];

        try {
            if (api.equals(GET_CATALOGS)) {
                return getCatalogs();
            } else if (api.equals(GET_COLUMNS)) {
                checkParamCount(p, 4);
                return getColumns(p[1], p[2], p[3], p[4], client);
            } else if (api.equals(GET_DESCRIBE_TABLE)) {
                checkParamCount(p, 1);
                return getDescribeTable(p[1], client);
            } else if (api.equals(GET_TABLES)) {
                checkParamCount(p, 3);
                HashSet<String> types = null;
                if (p.length > 4) {
                    types = new HashSet<String>();
                    for (int i = 4; i < p.length; i++) {
                        String type = p[i].trim().toUpperCase();
                        if ("TABLE".equals(type) || "VIEW".equals(type)
                                || "LOCAL TEMPORARY".equals(type)) {
                            types.add(type);
                        }
                    }
                    // If nothing requested return all
                    if (types.isEmpty()) {
                        types = null;
                    }
                }
                return getTables(p[1], p[2], p[3], types, client);
            } else if (api.equals(GET_STATEMENTS)) {
                return getStatements(client);
            } else if (api.equals(GET_PRIMARY_KEY)) {
                checkParamCount(p, 3);
                return getPrimaryKey(p[1], p[2], p[3]);
            } else if (api.equals(GET_INDEX_INFO)) {
                checkParamCount(p, 5);
                return getIndexInfo(p[1], p[2], p[3], "true"
                        .equalsIgnoreCase(p[4]), "true".equalsIgnoreCase(p[5]));
            } else if (api.equals(GET_IMPORTED_KEYS)) {
                checkParamCount(p, 3);
                return getImportedKeys(p[1], p[2], p[3]);
            } else if (api.equals(GET_EXPORTED_KEYS)) {
                checkParamCount(p, 3);
                return getExportedKeys(p[1], p[2], p[3]);
            } else if (api.equals(GET_CROSS_REFERENCE)) {

                checkParamCount(p, 3);
                return getCrossReference(p[1], p[2], p[3], p[4], p[5], p[6]);
            } else if (api.equals(GET_TABLE_TYPES)) {
                return getTableTypes();
            } else if (api.equals(GET_TYPE_INFO)) {
                return getTypeInfo();
            }

        } catch (SQLException s) {
            logger.catching(s);
            throw s;
        } catch (Exception e) {
            logger.catching(e);
            throw SQLErrorHandler.getError(ErrorCodes.AN_ERROR_HAS_OCCURRED,
                    "metadata info error");
        }
        return null;
    }

    private static String[] parseParams(String params) throws SQLException {
        ArrayList<String> out = new ArrayList<String>();
        int previous = 0;
        int next = params.indexOf("|");
        while (next >= 0) {
            String token = params.substring(previous, next);
            out.add("null".equals(token) ? null : token);
            previous = next + 1;
            next = params.indexOf("|", previous);
        }
        out.add(params.substring(previous));
        return out.toArray(new String[out.size()]);
    }

    private static void checkParamCount(String[] params, int expected)
            throws SQLException {
        if (params.length <= expected) {
            throw new SQLException("Invalid command parameters");
        }
    }

    public static ResultSet getStatements(XDBSessionContext client)
            throws SQLException {
        if (logger.isDebugEnabled()) {
            synchronized (client.getSysDatabase().getScheduler()) {
                logger.debug(client.getSysDatabase().getLm().dumpLockManager());
            }
        }

        final String tableNamePattern = "STATEMENTS";
        final short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("REQUEST_ID", "REQUEST_ID", 0,
                        Types.INTEGER, 0, 0, tableNamePattern, flags, false),
                new ColumnMetaData("SESSION_ID", "SESSION_ID", 0,
                        Types.INTEGER, 0, 0, tableNamePattern, flags, false),
                new ColumnMetaData("SUBMIT_TIME", "SUBMIT_TIME", 0,
                        Types.TIMESTAMP, 0, 0, tableNamePattern, flags, false),
                new ColumnMetaData("STATUS", "STATUS", 1, Types.CHAR, 0, 0,
                        tableNamePattern, flags, false),
                new ColumnMetaData("STATEMENT", "STATEMENT", 8192,
                        Types.VARCHAR, 0, 0, tableNamePattern, flags, false),
                new ColumnMetaData("NODES", "NODES", 1024, Types.VARCHAR, 0, 0,
                        tableNamePattern, flags, false),
                new ColumnMetaData("CURRENT_STEP", "CURRENT_STEP", 8192,
                        Types.VARCHAR, 0, 0, tableNamePattern, flags, false), };
        Vector<XData[]> rows = new Vector<XData[]>();
        for (Map.Entry<ExecutableRequest, XDBSessionContext> entry : client
                .getRequests().entrySet()) {
            rows.add(new XData[] {
                    new IntegerType(entry.getKey().getRequestID()),
                    new IntegerType(entry.getValue().getSessionID()),
                    new TimestampType(new Timestamp(entry.getKey()
                            .getSubmitTime()), null),
                    new VarcharType("" + entry.getKey().getStatus()),// table
                    new VarcharType(entry.getKey().getStatement()),
                    new VarcharType(null), new VarcharType(null) });
        }
        return new ResultSetImpl(headers, rows);
    }

    private static ResultSet getTables(String catalog, String schemaPattern,
            String tableNamePattern, Collection types, XDBSessionContext client)
            throws SQLException {
        short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("TABLE_CAT", "TABLE_CAT", 20, Types.VARCHAR,
                        0, 0, null, flags, false),
                new ColumnMetaData("TABLE_SCHEM", "TABLE_SCHEM", 20,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("TABLE_NAME", "TABLE_NAME", 25,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("TABLE_TYPE", "TABLE_TYPE", 10,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("REMARKS", "REMARKS", 10, Types.VARCHAR, 0,
                        0, null, flags, false),
                new ColumnMetaData("TYPE_CAT", "TYPE_CAT", 10, Types.VARCHAR,
                        0, 0, null, flags, false),
                new ColumnMetaData("TYPE_SCHEM", "TYPE_SCHEM", 10,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("TYPE_NAME", "TYPE_NAME", 10, Types.VARCHAR,
                        0, 0, null, flags, false),
                new ColumnMetaData("SELF_REFERENCING_COL_NAME",
                        "SELF_REFERENCING_COL_NAME", 10, Types.VARCHAR, 0, 0,
                        null, flags, false),
                new ColumnMetaData("REF_GENERATION", "REF_GENERATION", 10,
                        Types.VARCHAR, 0, 0, null, flags, false) };

        Vector<XData[]> rows = new Vector<XData[]>();

        if (types == null || types.contains("TABLE")) {
            Collection tables = getMatchingTables(
                    getMatchingDatabases(catalog), tableNamePattern);
            TreeMap<String, XData[]> sorter = new TreeMap<String, XData[]>();
            for (Iterator it = tables.iterator(); it.hasNext();) {
                SysTable table = (SysTable) it.next();
                sorter.put(table.getTableName(), new XData[] {
                        new VarcharType(table.getSysDatabase().getDbname()),
                        new VarcharType(null),
                        new VarcharType(table.getTableName()),
                        new VarcharType("TABLE"),// table type
                        new VarcharType(null), new VarcharType(null),
                        new VarcharType(null), new VarcharType(null),
                        new VarcharType(null), new VarcharType(null) });
            }
            for (XData[] name : sorter.values()) {
                rows.add(name);
            }
        }

        if (types == null || types.contains("VIEW")) {
            Collection views = getMatchingViews(getMatchingDatabases(catalog),
                    tableNamePattern);
            TreeMap<String, XData[]> sorter = new TreeMap<String, XData[]>();
            for (Iterator it = views.iterator(); it.hasNext();) {
                SysView view = (SysView) it.next();
                sorter.put(view.getViewName(), new XData[] {
                        new VarcharType(view.getSysDatabase().getDbname()),
                        new VarcharType(null),
                        new VarcharType(view.getViewName()),
                        new VarcharType("VIEW"),// table type
                        new VarcharType(null), new VarcharType(null),
                        new VarcharType(null), new VarcharType(null),
                        new VarcharType(null), new VarcharType(null) });
            }
            for (XData[] name : sorter.values()) {
                rows.add(name);
            }
        }

        if ((types == null || types.contains("LOCAL TEMPORARY"))
                && getMatchingDatabases(catalog).contains(
                        client.getSysDatabase())) {
            Map<String, SysTable> tables = getMatchingTempTables(client,
                    tableNamePattern);
            TreeMap<String, SysTable> sorter = new TreeMap<String, SysTable>(
                    tables);
            for (Entry<String, SysTable> entry : sorter.entrySet()) {
                // SysTable table = (SysTable) entry.getValue();
                rows.add(new XData[] {
                        new VarcharType(client.getDBName()),
                        new VarcharType(null),
                        new VarcharType(entry.getKey()),
                        new VarcharType("LOCAL TEMPORARY"),// table type
                        new VarcharType(null), new VarcharType(null),
                        new VarcharType(null), new VarcharType(null),
                        new VarcharType(null), new VarcharType(null) });
            }
        }

        return new ResultSetImpl(headers, rows);
    }

    /**
     * Distinguish between SHOW TABLES and DatabaseMetaData.getTables()
     */
    public static ResultSet getShowTables(String params,
            XDBSessionContext client) throws SQLException {

        String schemaPattern = "*", tableNamePattern = "*";
        if (params.length() > 0) {
            String[] p = parseParams(params);

            if (p.length > 1) {
                schemaPattern = p[0];
            }

            if (p.length > 2) {
                tableNamePattern = p[1];
            }

        }

        boolean allTables = schemaPattern.equals("*")
                || tableNamePattern.equals("*") || tableNamePattern.equals("%");

        short flags = 0;
        // we assume we show tables info for current database only, so name of
        // database is not nessary
        ColumnMetaData[] headers = new ColumnMetaData[] {

                // new
                // ColumnMetaData("DATABASE_NAME","DATABASE_NAME",20,Types.VARCHAR,
                // 0,0, tableNamePattern, flags, false),

                new ColumnMetaData("TABLE_NAME", "TABLE", 25, Types.VARCHAR, 0,
                        0, tableNamePattern, flags, false),

                new ColumnMetaData("TABLE_PARTITIONING_COLUMN",
                        "TABLE_PARTITIONING_COLUMN", 25, Types.VARCHAR, 0, 0,
                        tableNamePattern, flags, false),

                new ColumnMetaData("TABLE_NODES", "TABLE_NODES", 1024,
                        Types.VARCHAR, 0, 0, tableNamePattern, flags, false),

        };

        SysDatabase db = null;
        Collection<SysTable> currList = null;
        SysTable table = null;
        Vector<XData[]> rows = new Vector<XData[]>();
        db = MetaData.getMetaData().getSysDatabase(client.getDBName());
        Iterator itn = null;
        DBNode tablenode = null;
        String tablenodes = "";

        /*
         * if(!db.isStarted()) { db.getNoActiveDatabaseInfo(); }
         */

        currList = new ArrayList<SysTable>(db.getSysTables());
        TreeMap<String, XData[]> sorter = new TreeMap<String, XData[]>();

        for (String string : client.getTempTableNames()) {
            currList.add(db.getSysTable(string));
        }

        for (Iterator<SysTable> itCurrList = currList.iterator(); itCurrList
                .hasNext();) {
            table = itCurrList.next();
            if (!allTables
                    && table.getTableName().toUpperCase().indexOf(
                            tableNamePattern.toUpperCase()) < 0) {
                continue;
            }

            if (!table.checkPermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_SELECT)) {
                continue;
            }

            itn = table.getNodeList().iterator();
            tablenodes = "";
            while (itn.hasNext()) {
                tablenode = (DBNode) itn.next();
                tablenodes = tablenodes + tablenode.getNodeId();
                if (itn.hasNext()) {
                    tablenodes = tablenodes + ',';
                }
            }
            sorter.put(table.getTableName(), new XData[] {
                    new VarcharType(table.getTableName()),
                    new VarcharType(table.getPartitionColumn()),
                    new VarcharType(tablenodes) });

        }

        for (XData[] name : sorter.values()) {
            rows.add(name);
        }

        return new ResultSetImpl(headers, rows);

    }

    private static ResultSet getColumns(String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern,
            XDBSessionContext client) throws SQLException {
        ColumnMetaData[] headers = {};
        Vector<XData[]> rows = new Vector<XData[]>();

        try {
            // build the RS
            short flags = 0;
            headers = new ColumnMetaData[] {
                    new ColumnMetaData("TABLE_CAT", "TABLE_CAT", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("TABLE_SCHEM", "TABLE_SCHEM", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("TABLE_NAME", "TABLE_NAME", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("COLUMN_NAME", "COLUMN_NAME", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("DATA_TYPE", "DATA_TYPE", 0,
                            Types.SMALLINT, 0, 0, null, flags, false),
                    new ColumnMetaData("TYPE_NAME", "TYPE_NAME", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("COLUMN_SIZE", "COLUMN_SIZE", 0,
                            Types.INTEGER, 0, 0, null, flags, false),
                    new ColumnMetaData("BUFFER_LENGTH", "BUFFER_LENGTH", 0,
                            Types.INTEGER, 0, 0, null, flags, false),
                    new ColumnMetaData("DECIMAL_DIGITS", "DECIMAL_DIGITS", 0,
                            Types.INTEGER, 0, 0, null, flags, false),
                    new ColumnMetaData("NUM_PREC_RADIX", "NUM_PREC_RADIX", 0,
                            Types.INTEGER, 0, 0, null, flags, false),
                    new ColumnMetaData("NULLABLE", "NULLABLE", 0,
                            Types.INTEGER, 0, 0, null, flags, false),
                    new ColumnMetaData("REMARKS", "REMARKS", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("COLUMN_DEF", "COLUMN_DEF", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("SQL_DATA_TYPE", "SQL_DATA_TYPE", 0,
                            Types.INTEGER, 0, 0, null, flags, false),
                    new ColumnMetaData("SQL_DATETIME_SUB", "SQL_DATETIME_SUB",
                            0, Types.INTEGER, 0, 0, null, flags, false),
                    new ColumnMetaData("CHAR_OCTET_LENGTH",
                            "CHAR_OCTET_LENGTH", 0, Types.INTEGER, 0, 0, null,
                            flags, false),
                    new ColumnMetaData("ORDINAL_POSITION", "ORDINAL_POSITION",
                            0, Types.INTEGER, 0, 0, null, flags, false),
                    new ColumnMetaData("IS_NULLABLE", "IS_NULLABLE", 3,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("SCOPE_CATLOG", "SCOPE_CATLOG", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("SCOPE_SCHEMA", "SCOPE_SCHEMA", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("SCOPE_TABLE", "SCOPE_TABLE", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),
                    new ColumnMetaData("SOURCE_DATA_TYPE", "SOURCE_DATA_TYPE",
                            255, Types.VARCHAR, 0, 0, null, flags, false) };

            Pattern pattern = Pattern.compile(
                    patternToRegexp(columnNamePattern),
                    Pattern.CASE_INSENSITIVE);
            Collection<SysTable> tables = getMatchingTables(
                    getMatchingDatabases(catalog), tableNamePattern);
            TreeMap<String, SysTable> tableSorter = new TreeMap<String, SysTable>();
            for (SysTable sysTable : tables) {
                SysTable table = (SysTable) sysTable;
                tableSorter.put(table.getTableName(), table);
            }
            for (Object element : tableSorter.values()) {
                SysTable table = (SysTable) element;
                for (Object element2 : table.getColumns()) {
                    SysColumn column = (SysColumn) element2;
                    if (!SqlCreateTableColumn.XROWID_NAME.equals(column
                            .getColName())
                            && pattern.matcher(column.getColName()).matches()) {
                        rows
                                .add(new XData[] {
                                        new VarcharType(table.getSysDatabase()
                                                .getDbname()),// catalog
                                        new VarcharType(null),// schema
                                        new VarcharType(table.getTableName()),
                                        new VarcharType(column.getColName()),
                                        new ShortType((short) column
                                                .getColType()),// java.sql.Types
                                        new VarcharType(DataTypes
                                                .getJavaTypeDesc(column
                                                        .getColType())),
                                        new IntegerType(column.getColLength()), // int
                                        new IntegerType(column.getColLength()), // new
                                        // IntegerType(null),//buffer_length,
                                        // not
                                        // used
                                        new IntegerType(column.getColScale()),
                                        new IntegerType(10),// radix, BIN or DEC
                                        new IntegerType(
                                                column.isNullable() ? DatabaseMetaData.columnNullable
                                                        : DatabaseMetaData.columnNoNulls),
                                        new VarcharType(column.getColName()),
                                        new VarcharType(null),// default
                                        // value, ??
                                        new IntegerType(null),// unused
                                        new IntegerType(null),// unused
                                        new IntegerType(column.getColLength()),// max
                                        // length
                                        // for
                                        // char
                                        // columns
                                        new IntegerType(column.getColSeq()),// starting
                                        // from
                                        // one
                                        new VarcharType(
                                                column.isNullable() ? "YES"
                                                        : "NO"),// is nullablle?
                                        new VarcharType(null),// dont care
                                        new VarcharType(null),// dont care
                                        new VarcharType(null),// dont care
                                        new VarcharType(null) });
                    }
                }// for
            }

            Collection<SysView> views = getMatchingViews(
                    getMatchingDatabases(catalog), tableNamePattern);
            TreeMap<String, SysView> viewSorter = new TreeMap<String, SysView>();
            for (SysView view : views) {
                viewSorter.put(view.getViewName(), view);
            }
            for (SysView view : viewSorter.values()) {
                for (SysViewColumns column : view.getViewColumns()) {
                    if (pattern.matcher(column.getViewColumn()).matches()) {
                        rows.add(new XData[] {
                                new VarcharType(view.getSysDatabase()
                                        .getDbname()),// catalog
                                new VarcharType(null),// schema
                                new VarcharType(view.getViewName()),
                                new VarcharType(column.getViewColumn()),
                                new ShortType((short) column.getColtype()),// java.sql.Types
                                new VarcharType(DataTypes
                                        .getJavaTypeDesc(column.getColtype())),
                                new IntegerType(column.getCollength()), // int
                                new IntegerType(column.getCollength()), // new
                                // IntegerType(null),//buffer_length,
                                // not
                                // used
                                new IntegerType(column.getColscale()),
                                new IntegerType(10),// radix, BIN or DEC
                                new IntegerType(null),
                                new VarcharType(column.getViewColumn()),
                                new VarcharType(null),// default value, ??
                                new IntegerType(null),// unused
                                new IntegerType(null),// unused
                                new IntegerType(column.getCollength()),// max
                                // length
                                // for
                                // char
                                // columns
                                new IntegerType(null),// starting from one
                                new VarcharType(null),// is nullablle?
                                new VarcharType(null),// dont care
                                new VarcharType(null),// dont care
                                new VarcharType(null),// dont care
                                new VarcharType(null) });
                    }
                }// for
            }
        } catch (Exception e) {
            String msg = e != null ? e.getMessage() : null;
            logger.catching(e);
            throw SQLErrorHandler.getError(ErrorCodes.AN_ERROR_HAS_OCCURRED,
                    msg);
        }
        return new ResultSetImpl(headers, rows);
    }

    public static ResultSet getDescribeView(String tableName,
            XDBSessionContext client) throws SQLException {
        ColumnMetaData[] headers = {};
        Vector<XData[]> rows = new Vector<XData[]>();

        String viewText;
        // build the RS
        short flags = 0;
        headers = new ColumnMetaData[] {

                new ColumnMetaData("VIEW_TEXT", "VIEW_TEXT", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("VIEW_COLUMN", "VIEW_COLUMN", 255,
                        Types.VARCHAR, 0, 0, null, flags, false), };

        SysView view = client.getSysDatabase().getSysView(tableName);
        // add to sysColumns
        viewText = view.getViewText();
        String viewColumns = "";
        if (view.getViewColumns() != null && view.getViewColumns().size() > 0) {
            for (Object element : view.getViewColumns()) {
                SysViewColumns el = (SysViewColumns) element;
                viewColumns = viewColumns + el.getViewColumn() + ", ";

            }
            viewColumns = viewColumns.substring(0, viewColumns.length() - 2);
        }

        rows.add(new XData[] { new VarcharType(viewText),
                new VarcharType(viewColumns) });

        return new ResultSetImpl(headers, rows);

    }

    public static ResultSet getDescribeTable(String tableName,
            XDBSessionContext client) throws SQLException {
        ColumnMetaData[] headers = {};
        Vector<XData[]> rows = new Vector<XData[]>();

        try {
            Vector<SysColumn> sysColumns = new Vector<SysColumn>();
            // build the RS
            short flags = 0;
            headers = new ColumnMetaData[] {

                    new ColumnMetaData("COLUMN_NAME", "COLUMN_NAME", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),

                    new ColumnMetaData("SQL_DATA_TYPE", "SQL_DATA_TYPE", 0,
                            Types.INTEGER, 0, 0, null, flags, false),

                    new ColumnMetaData("TYPE_NAME", "TYPE_NAME", 255,
                            Types.VARCHAR, 0, 0, null, flags, false),

                    new ColumnMetaData("IS_NULLABLE", "IS_NULLABLE", 3,
                            Types.VARCHAR, 0, 0, null, flags, false),

                    new ColumnMetaData("KEY", "KEY", 3, Types.VARCHAR, 0, 0,
                            null, flags, false),

                    new ColumnMetaData("DEFAULT", "DEFAULT", 255,
                            Types.VARCHAR, 0, 0, null, flags, false), };

            SysTable table = client.getSysDatabase().getSysTable(tableName);
            // add to sysColumns
            table.ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_SELECT);
            sysColumns.addAll(table.getColumns());
            List<SysColumn> vpk = table.getPrimaryKey();
            Hashtable hFK = table.getReferringSys_ReferrencedColumns_Map();
            for (int j = 0; j < sysColumns.size(); j++) {

                SysColumn column = (SysColumn) sysColumns.get(j);

                String spk = "NO";
                String s = "";
                String s1 = column.getColName().toUpperCase().trim();

                if (vpk != null && vpk.size() > 0) {
                    for (SysColumn sysColumn : vpk) {
                        s = sysColumn.getColName().toUpperCase().trim();
                        if (s1.equals(s)) {
                            spk = "P";// 'P'rimary
                            // Key
                        }
                    }

                }// end check of PK

                if (hFK != null && hFK.size() > 0) {

                    Enumeration foreignEnumeration = hFK.elements();
                    while (foreignEnumeration.hasMoreElements()) {
                        SysColumn sycCol = (SysColumn) foreignEnumeration
                                .nextElement();
                        s = sycCol.getColName().toUpperCase().trim();

                        // s = ((String) foreignEnumeration.nextElement())
                        // .toUpperCase().trim();
                        if (s1.equals(s)) {
                            if (spk.equals("NO")) {
                                spk = "R";
                            } else {
                                spk = spk + " + R";
                                // 'R'eference
                            }
                        }
                        // =
                        // Foreign
                        // Key
                    }

                }// end check of FK

                String str_type = DataTypes
                        .getJavaTypeDesc(column.getColType(), 
                        		column.getColLength(), 
                        		column.getColPrecision(),
                        		column.getColScale(),
                        		column.isWithTimeZone);
                if (column.getColType() == java.sql.Types.NUMERIC
                        || column.getColType() == java.sql.Types.DECIMAL) {
                    str_type += "(" + column.getColPrecision() + ","
                            + column.getColScale() + ")";
                } else if (column.getColType() == java.sql.Types.REAL
                        || column.getColType() == java.sql.Types.FLOAT
                        || column.getColType() == java.sql.Types.CHAR
                        || column.getColType() == java.sql.Types.VARCHAR) {
                    str_type += "(" + column.getColLength() + ")";

                }
                if (column.isSerial()) {
                    str_type += " SERIAL";
                }
                rows.add(new XData[] { new VarcharType(column.getColName()),
                        new ShortType((short) column.getColType()),
                        new VarcharType(str_type),
                        new VarcharType(column.isNullable() ? "YES" : "NO"),// is
                        // nullablle?
                        new VarcharType(spk),// key
                        new VarcharType(column.getDefaultExpr()),// default
                // value
                        });
            }// for
        } catch (Exception e) {
            String msg = e != null ? e.getMessage() : null;
            logger.catching(e);
            throw SQLErrorHandler.getError(ErrorCodes.AN_ERROR_HAS_OCCURRED,
                    msg);
        }
        return new ResultSetImpl(headers, rows);
    }

    /**
     * Retrieves the catalog names available in this database. The results are
     * ordered by catalog name.
     *
     * <P>
     * The catalog column is:
     * <OL>
     * <LI><B>TABLE_CAT</B> String => catalog name
     * </OL>
     *
     * @return a <code>ResultSet</code> object in which each row has a single
     *         <code>String</code> column that is a catalog name
     */
    public static ResultSet getCatalogs() {
        ColumnMetaData[] headers = new ColumnMetaData[] { new ColumnMetaData(
                "TABLE_CAT", "TABLE_CAT", 250, Types.VARCHAR, 0, 0, null,
                (short) 0, false) };
        TreeSet<String> orderedList = new TreeSet<String>();
        for (SysDatabase db : MetaData.getMetaData().getSysDatabases()) {
            orderedList.add(db.getDbname());
        }
        Vector<XData[]> rows = new Vector<XData[]>(orderedList.size());
        for (String dbName : orderedList) {
            rows.add(new XData[] { new VarcharType(dbName) });
        }
        return new ResultSetImpl(headers, rows);
    }

    public static ResultSet getShowCluster() {
        short flags = 0;
        int maxNodeLen = 1;
        
        int nodeCount = MetaData.getMetaData().getNodes().size();
        int agentCount = SysAgent.getNodeAgents().size();
        int dbCount = MetaData.getMetaData().getSysDatabases().size();
        int loginCount = MetaData.getMetaData().getSysLogins().size();
        boolean isReadOnly = SysDatabase.isReadOnly();

        Vector<XData[]> rows = new Vector<XData[]>();
        rows.add(new XData[] { new IntegerType(nodeCount),
                new IntegerType(agentCount),
                new IntegerType(dbCount),
                new IntegerType(loginCount),
                new BooleanType(isReadOnly) });	

        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("NODES", "NODES", 0,
                        Types.INTEGER, 0, 0, null, flags, false),
                new ColumnMetaData("AGENTS", "AGENTS", 0,
                        Types.INTEGER, 0, 0, null, flags, false),
                new ColumnMetaData("DATABASES", "DATABASES", 0,
                        Types.INTEGER, 0, 0, null, flags, false),
                new ColumnMetaData("LOGINS", "LOGINS", 0,
                        Types.INTEGER, 0, 0, null, flags, false),
                new ColumnMetaData("READ_ONLY", "READ_ONLY", 0,
                        Types.BOOLEAN, 0, 0, null, flags, false),
                };

        return new ResultSetImpl(headers, rows);
    }

    public static ResultSet getAgents() {
        short flags = 0;
        int maxNodeLen = 1;

        Vector<XData[]> rows = new Vector<XData[]>();
        for (SysAgent agent : SysAgent.getNodeAgents()) {
            StringBuilder sNodes = new StringBuilder();
            for (Node node : agent.getNodes()) {
                sNodes.append(node.getNodeid()).append(",");
            }
            if (sNodes.length() > 0) {
                sNodes.setLength(sNodes.length() - 1);
            }
            if (maxNodeLen < sNodes.length()) {
                maxNodeLen = sNodes.length();
            }

            rows.add(new XData[] { new VarcharType(agent.getHost()),
                    new VarcharType(agent.isConnected() ? "Connected"
                            : "Disconnected"),
                    new VarcharType(sNodes.toString()) });
        }

        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("HOST", "HOST", 250, Types.VARCHAR, 0, 0,
                        null, flags, false),
                new ColumnMetaData("STATUS", "STATUS", 10, Types.VARCHAR, 0, 0,
                        null, flags, false),
                new ColumnMetaData("NODES", "NODES", maxNodeLen, Types.VARCHAR,
                        0, 0,
                        null, flags, false), };

        return new ResultSetImpl(headers, rows);
    }

    public static ResultSet getShowNodes() {
        short flags = 0;

        Vector<XData[]> rows = new Vector<XData[]>();
        for (Node n : MetaData.getMetaData().getNodes()) {
        	String status;
        	if (n.isUp())
        		status = "UP";
        	else
        		status = "DOWN";
        	
            rows.add(new XData[] { new IntegerType(n.getNodeid()),
            		new VarcharType(n.getSHost()),
                    new IntegerType(n.getPort()),
            		new VarcharType(n.getJdbcUser()),
                    new VarcharType(status) });	
        }
        
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("NODE", "NODE", 0,
                        Types.INTEGER, 0, 0, null, flags, false),
                new ColumnMetaData("HOST", "HOST", 250, Types.VARCHAR, 0, 0,
                        null, flags, false),
                new ColumnMetaData("PORT", "PORT", 0,
                                Types.INTEGER, 0, 0, null, flags, false),
                new ColumnMetaData("USER", "USER", 250, Types.VARCHAR, 0, 0,
                                null, flags, false),
                new ColumnMetaData("STATUS", "STATUS", 10, Types.VARCHAR, 0, 0,
                        null, flags, false),
                };

        return new ResultSetImpl(headers, rows);
    }
    
    public static ResultSet getCatalogsExt() {
        short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("DATABASE", "DATABASE", 250, Types.VARCHAR,
                        0, 0, null, flags, false),
                new ColumnMetaData("STATUS", "STATUS", 10, Types.VARCHAR, 0, 0,
                        null, flags, false),
                new ColumnMetaData("NODES", "NODES", 1024, Types.VARCHAR, 0, 0,
                        null, flags, false), };

        Vector<XData[]> rows = new Vector<XData[]>();
        for (SysDatabase db : MetaData.getMetaData().getSysDatabases()) {
            // Zahid: Don't show virtual admin database (represented by Props.XDB_ADMIN_DATABASE, default = xdbadmin)
            if (db.getDbname().equalsIgnoreCase(Props.XDB_ADMIN_DATABASE)) {
                continue;
            }

            Iterator it1 = db.getDBNodeList().iterator();
            String sNodes = "";
            DBNode dbn;
            if (!it1.hasNext()) {
                sNodes = " ";
            }
            while (it1.hasNext()) {
                dbn = (DBNode) it1.next();
                sNodes = sNodes + dbn.getNode().getNodeid();
                if (it1.hasNext()) {
                    sNodes = sNodes + ',';
                }
            }
            rows.add(new XData[] { new VarcharType(db.getDbname()),
                    new VarcharType(db.isStarted() ? "Started" : "Down"),
                    new VarcharType(sNodes) });
        }

        return new ResultSetImpl(headers, rows);

    }

    public static ResultSet getPrimaryKey(String catalog, String schema,
            String table) {
        short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("TABLE_CAT", "TABLE_CAT", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("TABLE_SCHEM", "TABLE_SCHEM", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("TABLE_NAME", "TABLE_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("COLUMN_NAME", "COLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("KEY_SEQ", "KEY_SEQ", 0, Types.SMALLINT, 0,
                        0, null, flags, false),
                new ColumnMetaData("PK_NAME", "PK_NAME", 255, Types.VARCHAR, 0,
                        0, null, flags, false), };
        Vector<XData[]> rows = new Vector<XData[]>();
        Collection<SysTable> tables = getMatchingTables(
                getMatchingDatabases(catalog), table);
        for (SysTable theSysTable : tables) {
            short theKeySeq = 0;
            SysIndex thePrimaryIndex = theSysTable.getPrimaryIndex();
            if (thePrimaryIndex == null) {
                continue;
            }
            List<SysColumn> theCols = thePrimaryIndex.getKeyColumns();
            for (SysColumn col : theCols) {
                theKeySeq++;
                rows.add(new XData[] {
                        new VarcharType(theSysTable.getSysDatabase()
                                .getDbname()), new VarcharType(null),
                        new VarcharType(theSysTable.getTableName()),
                        new VarcharType(col.getColName()),
                        new ShortType(theKeySeq),
                        new VarcharType(thePrimaryIndex.idxname), });
            }
        }
        return new ResultSetImpl(headers, rows);
    }

    public static ResultSet getIndexInfo(String catalog, String schema,
            String table, boolean unique, boolean approximate) {
        short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("TABLE_CAT", "TABLE_CAT", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("TABLE_SCHEM", "TABLE_SCHEM", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("TABLE_NAME", "TABLE_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("NON_UNIQUE", "NON_UNIQUE", 0,
                        Types.BOOLEAN, 0, 0, null, flags, false),
                new ColumnMetaData("INDEX_QUALIFIER", "INDEX_QUALIFIER", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("INDEX_NAME", "INDEX_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("TYPE", "TYPE", 0, Types.SMALLINT, 0, 0,
                        null, flags, false),
                new ColumnMetaData("ORDINAL_POSITION", "ORDINAL_POSITION", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),
                new ColumnMetaData("COLUMN_NAME", "COLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("ASC_OR_DESC", "ASC_OR_DESC", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("CARDINALITY", "CARDINALITY", 0,
                        Types.INTEGER, 0, 0, null, flags, false),
                new ColumnMetaData("PAGES", "PAGES", 0, Types.INTEGER, 0, 0,
                        null, flags, false),
                new ColumnMetaData("FILTER_CONDITION", "FILTER_CONDITION", 255,
                        Types.VARCHAR, 0, 0, null, flags, false), };
        Vector<XData[]> rows = new Vector<XData[]>();
        Collection<SysTable> tables = getMatchingTables(
                getMatchingDatabases(catalog), table);
        for (SysTable theSysTable : tables) {
            List<SysConstraint> theConstraints = theSysTable
                    .getConstraintList();
            for (SysConstraint theConstr : theConstraints) {
                boolean qnic = false; // boolean => Can index values be
                // non-unique.
                // PK, U => false Fk => true
                if (theConstr.getConstType() == 'R') {
                    qnic = true;
                }
                SysIndex theIndex = theSysTable.getSysIndex(theConstr
                        .getIdxID());
                List<SysIndexKey> theKeys = theIndex.getIndexKeys();
                for (SysIndexKey theKey : theKeys) {
                    String theAscOrDesc = new String("A");
                    if (theKey.idxascdesc == 1) {
                        theAscOrDesc = "D";
                    }
                    rows.add(new XData[] {
                            new VarcharType(theSysTable.getSysDatabase()
                                    .getDbname()), new VarcharType(schema),
                            new VarcharType(theSysTable.getTableName()),
                            new BooleanType(qnic), // NON_UNIQUE
                            new VarcharType(""), // INDEX_QUALIFIER null
                            new VarcharType(theIndex.idxname), // INDEX_NAME
                            new ShortType((short) 0), // TYPE
                            new ShortType((short) theKey.idxkeyseq), // ORDINAL_POSITION
                            new VarcharType(theKey.sysColumn.getColName()), // COLUMN_NAME
                            new VarcharType(theAscOrDesc), // ASC_OR_DESC
                            new IntegerType(0), // CARDINALITY
                            new IntegerType(0), // PAGES
                            new VarcharType(""), // FILTER_CONDITION
                    });
                } // keys
            } // constr
        } // tables
        return new ResultSetImpl(headers, rows);
    }

    public static ResultSet getImportedKeys(String catalog, String schema,
            String table) {
        short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("PKTABLE_CAT", "PKTABLE_CAT", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("PKTABLE_SCHEM", "PKTABLE_SCHEM", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("PKTABLE_NAME", "PKTABLE_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("PKCOLUMN_NAME", "PKCOLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("FKTABLE_CAT", "FKTABLE_CAT", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("FKTABLE_SCHEM", "FKTABLE_SCHEM", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("FKTABLE_NAME", "FKTABLE_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("FKCOLUMN_NAME", "FKCOLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("KEY_SEQ", "KEY_SEQ", 0, Types.SMALLINT, 0,
                        0, null, flags, false),
                new ColumnMetaData("UPDATE_RULE", "UPDATE_RULE", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),
                new ColumnMetaData("DELETE_RULE", "DELETE_RULE", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),
                new ColumnMetaData("FK_NAME", "FK_NAME", 255, Types.VARCHAR, 0,
                        0, null, flags, false),
                new ColumnMetaData("PK_NAME", "PK_NAME", 255, Types.VARCHAR, 0,
                        0, null, flags, false),
                new ColumnMetaData("DEFERRABILITY", "DEFERRABILITY", 0,
                        Types.SMALLINT, 0, 0, null, flags, false), };
        Vector<XData[]> rows = new Vector<XData[]>();
        Collection<SysTable> tables = getMatchingTables(
                getMatchingDatabases(catalog), table);
        for (SysTable theSysTable : tables) {
            for (SysConstraint theConst : theSysTable.getConstraintList()) {
                if (theConst.getConstType() != 'R') {
                    continue;
                }
                SysIndex idx = theSysTable.getSysIndex(theConst.getIdxID());

                Enumeration allTables = theSysTable.getSysDatabase()
                        .getAllTables();

                while (allTables.hasMoreElements()) {
                    SysTable table2 = (SysTable) allTables.nextElement();
                    Vector theRefs = table2.getSysReferences();
                    for (Iterator itRef = theRefs.iterator(); itRef.hasNext();) {
                        SysReference theRef = (SysReference) itRef.next();
                        if (theConst != theRef.getConstraint()) {
                            continue;
                        }
                        SysIndex idx2 = table2
                                .getSysIndex(theRef.getRefIdxID());
                        Vector theFks = theRef.getForeignKeys();
                        short theKeySeq = 0;
                        for (Iterator itFk = theFks.iterator(); itFk.hasNext();) {
                            SysForeignKey theFk = (SysForeignKey) itFk.next();
                            theKeySeq++;

                            rows
                                    .add(new XData[] {
                                            new VarcharType(theSysTable
                                                    .getSysDatabase()
                                                    .getDbname()), // PKTABLE_CAT
                                            new VarcharType(schema), // PKTABLE_SCHEM
                                            new VarcharType(table2
                                                    .getTableName()), // PKTABLE_NAME
                                            new VarcharType(
                                                    table2
                                                            .getSysColumn(
                                                                    theFk
                                                                            .getRefcolid())
                                                            .getColName()), // PKCOLUMN_NAME
                                            new VarcharType(theSysTable
                                                    .getSysDatabase()
                                                    .getDbname()), // FKTABLE_CAT
                                            new VarcharType(schema), // FKTABLE_SCHEM
                                            new VarcharType(theSysTable
                                                    .getTableName()), // FKTABLE_NAME
                                            new VarcharType(theSysTable
                                                    .getSysColumn(
                                                            theFk.getColid())
                                                    .getColName()), // FKCOLUMN_NAME
                                            new ShortType(theKeySeq), // KEY_SEQ
                                            new ShortType(
                                                    (short) DatabaseMetaData.importedKeyRestrict), // UPDATE_RULE
                                            new ShortType(
                                                    (short) DatabaseMetaData.importedKeyRestrict), // DELETE_RULE
                                            new VarcharType(idx.idxname), // FK_NAME
                                            new VarcharType(idx2.idxname), // PK_NAME
                                            new ShortType(
                                                    (short) DatabaseMetaData.importedKeyInitiallyImmediate), // DEFERRABILITY
                                    });
                        }
                    }
                }
            }
        }
        return new ResultSetImpl(headers, rows);
    }

    public static ResultSet getExportedKeys(String catalog, String schema,
            String table) {
        short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] {

                new ColumnMetaData("PKTABLE_CAT", "PKTABLE_CAT", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("PKTABLE_SCHEM", "PKTABLE_SCHEM", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("PKTABLE_NAME", "PKTABLE_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("PKCOLUMN_NAME", "PKCOLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("FKTABLE_CAT", "FKTABLE_CAT", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("FKTABLE_SCHEM", "FKTABLE_SCHEM", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("FKTABLE_NAME", "FKTABLE_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("FKCOLUMN_NAME", "FKCOLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("KEY_SEQ", "KEY_SEQ", 0, Types.SMALLINT, 0,
                        0, null, flags, false),

                new ColumnMetaData("UPDATE_RULE", "UPDATE_RULE", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),

                new ColumnMetaData("DELETE_RULE", "DELETE_RULE", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),

                new ColumnMetaData("FK_NAME", "FKCOLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("PK_NAME", "PKCOLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),

                new ColumnMetaData("DEFERRABILITY", "DEFERRABILITY", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),

        };
        Vector<XData[]> rows = new Vector<XData[]>();
        Collection<SysTable> tables = getMatchingTables(
                getMatchingDatabases(catalog), table);
        for (SysTable theSysTable : tables) {
            for (SysReference aReferringTab : theSysTable.getSysReferences()) {
                Vector Fks = aReferringTab.getForeignKeys();
                for (Iterator itFks = Fks.iterator(); itFks.hasNext();) {
                    SysForeignKey el = (SysForeignKey) itFks.next();
                    SysColumn col1 = el.getReferencedSysColumn(theSysTable
                            .getSysDatabase());
                    SysColumn col2 = el.getReferringSysColumn(theSysTable
                            .getSysDatabase());

                    rows
                            .add(new XData[] {
                                    new VarcharType(theSysTable
                                            .getSysDatabase().getDbname()),
                                    new VarcharType(""),
                                    new VarcharType(theSysTable.getTableName()),
                                    new VarcharType(col1.getColName()),
                                    new VarcharType(theSysTable
                                            .getSysDatabase().getDbname()),
                                    new VarcharType(""),
                                    new VarcharType(col2.getSysTable()
                                            .getTableName()),
                                    new VarcharType(col2.getColName()),
                                    new ShortType((short) col1.getColSeq()),
                                    new ShortType(
                                            (short) DatabaseMetaData.importedKeyRestrict),
                                    new ShortType(
                                            (short) DatabaseMetaData.importedKeyRestrict),
                                    new VarcharType(aReferringTab
                                            .getConstraint().getConstName()),
                                    new VarcharType(
                                            col1
                                                    .getSysTable()
                                                    .getSysIndex(
                                                            aReferringTab
                                                                    .getRefIdxID()).idxname),
                                    new ShortType(
                                            (short) DatabaseMetaData.importedKeyInitiallyImmediate), });

                }

            }
        }
        return new ResultSetImpl(headers, rows);
    }

    public static ResultSet getCrossReference(String primaryCatalog,
            String primarySchema, String primaryTable, String foreignCatalog,
            String foreignSchema, String foreignTable) {
        short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("PKTABLE_CAT", "PKTABLE_CAT", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("PKTABLE_SCHEM", "PKTABLE_SCHEM", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("PKTABLE_NAME", "PKTABLE_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("PKCOLUMN_NAME", "PKCOLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("FKTABLE_CAT", "FKTABLE_CAT", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("FKTABLE_SCHEM", "FKTABLE_SCHEM", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("FKTABLE_NAME", "FKTABLE_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("FKCOLUMN_NAME", "FKCOLUMN_NAME", 255,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("KEY_SEQ", "KEY_SEQ", 0, Types.SMALLINT, 0,
                        0, null, flags, false),
                new ColumnMetaData("UPDATE_RULE", "UPDATE_RULE", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),
                new ColumnMetaData("DELETE_RULE", "DELETE_RULE", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),
                new ColumnMetaData("FK_NAME", "FK_NAME", 255, Types.VARCHAR, 0,
                        0, null, flags, false),
                new ColumnMetaData("PK_NAME", "PK_NAME", 255, Types.VARCHAR, 0,
                        0, null, flags, false),
                new ColumnMetaData("DEFERRABILITY", "DEFERRABILITY", 0,
                        Types.SMALLINT, 0, 0, null, flags, false), };
        Vector<XData[]> rows = new Vector<XData[]>();
        Collection<SysTable> pkTables = getMatchingTables(
                getMatchingDatabases(primaryCatalog), primaryTable);
        Collection<SysTable> fkTables = getMatchingTables(
                getMatchingDatabases(foreignCatalog), foreignTable);
        List<SysConstraint> thePkConstraints = new ArrayList<SysConstraint>();
        List<SysConstraint> theFkConstraints = new ArrayList<SysConstraint>();
        for (SysTable table : pkTables) {
            thePkConstraints.addAll(table.getConstraintList());
        }
        for (SysTable table : fkTables) {
            theFkConstraints.addAll(table.getConstraintList());
        }
        for (SysTable pkTable : pkTables) {
            for (SysReference theRef : pkTable.getSysReferences()) {
                for (SysTable fkTable : fkTables) {
                    for (SysReference theFk : fkTable.getSysFkReferenceList()) {
                        if (theFk != theRef) {
                            continue;
                        }
                        Vector theCols = theFk.getForeignKeys();
                        for (Iterator itCol = theCols.iterator(); itCol
                                .hasNext();) {
                            SysForeignKey el = (SysForeignKey) itCol.next();
                            rows
                                    .add(new XData[] {
                                            new VarcharType(pkTable
                                                    .getSysDatabase()
                                                    .getDbname()),
                                            new VarcharType(null),
                                            new VarcharType(pkTable
                                                    .getTableName()),
                                            new VarcharType(pkTable
                                                    .getSysColumn(
                                                            el.getRefcolid())
                                                    .getColName()), // PKCOLUMN_NAME
                                            new VarcharType(fkTable
                                                    .getSysDatabase()
                                                    .getDbname()),
                                            new VarcharType(null),
                                            new VarcharType(fkTable
                                                    .getTableName()),
                                            new VarcharType(
                                                    fkTable.getSysColumn(
                                                            el.getColid())
                                                            .getColName()), // FKCOLUMN_NAME
                                            new ShortType((short) el.fkeyseq), // KEY_SEQ
                                            new ShortType(
                                                    (short) DatabaseMetaData.importedKeyRestrict),
                                            new ShortType(
                                                    (short) DatabaseMetaData.importedKeyRestrict),
                                            new VarcharType(theFk
                                                    .getConstraint()
                                                    .getConstName()), // FK_NAME
                                            new VarcharType(
                                                    pkTable.getSysIndex(theRef
                                                            .getRefIdxID()).idxname), // PK_NAME
                                            new ShortType(
                                                    (short) DatabaseMetaData.importedKeyInitiallyImmediate), });
                        }
                    }
                }
            }
        }
        return new ResultSetImpl(headers, rows);
    }

    private static ResultSet getTableTypes() {
        short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] { new ColumnMetaData(
                "TABLE_TYPE", "TABLE_TYPE", 0, Types.VARCHAR, 0, 0, null,
                flags, false) };
        Vector<XData[]> rows = new Vector<XData[]>(3);
        rows.add(new XData[] { new VarcharType("TABLE") });
        rows.add(new XData[] { new VarcharType("VIEW") });
        rows.add(new XData[] { new VarcharType("LOCAL TEMPORARY") });
        return new ResultSetImpl(headers, rows);
    }

    public static ResultSet getTypeInfo() {
        short flags = 0;
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("TYPE_NAME", "TYPE_NAME", 0, Types.VARCHAR,
                        0, 0, null, flags, false),
                new ColumnMetaData("DATA_TYPE", "DATA_TYPE", 0, Types.SMALLINT,
                        0, 0, null, flags, false),
                new ColumnMetaData("PRECISION", "PRECISION", 0, Types.INTEGER,
                        0, 0, null, flags, false),
                new ColumnMetaData("LITERAL_PREFIX", "LITERAL_PREFIX", 0,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("LITERAL_SUFFIX", "LITERAL_SUFFIX", 0,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("CREATE_PARAMS", "CREATE_PARAMS", 0,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("NULLABLE", "NULLABLE", 0, Types.SMALLINT,
                        0, 0, null, flags, false),
                new ColumnMetaData("CASE_SENSITIVE", "CASE_SENSITIVE", 0,
                        Types.BOOLEAN, 0, 0, null, flags, false),
                new ColumnMetaData("SEARCHABLE", "SEARCHABLE", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),
                new ColumnMetaData("UNSIGNED_ATTRIBUTE", "UNSIGNED_ATTRIBUTE",
                        0, Types.BOOLEAN, 0, 0, null, flags, false),
                new ColumnMetaData("FIXED_PREC_SCALE", "FIXED_PREC_SCALE", 0,
                        Types.BOOLEAN, 0, 0, null, flags, false),
                new ColumnMetaData("AUTO_INCREMENT", "AUTO_INCREMENT", 0,
                        Types.BOOLEAN, 0, 0, null, flags, false),
                new ColumnMetaData("LOCAL_TYPE_NAME", "LOCAL_TYPE_NAME", 0,
                        Types.VARCHAR, 0, 0, null, flags, false),
                new ColumnMetaData("MINIMUM_SCALE", "MINIMUM_SCALE", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),
                new ColumnMetaData("MAXIMUM_SCALE", "MAXIMUM_SCALE", 0,
                        Types.SMALLINT, 0, 0, null, flags, false),
                new ColumnMetaData("SQL_DATA_TYPE", "SQL_DATA_TYPE", 0,
                        Types.INTEGER, 0, 0, null, flags, false),
                new ColumnMetaData("SQL_DATETIME_SUB", "SQL_DATETIME_SUB", 0,
                        Types.INTEGER, 0, 0, null, flags, false),
                new ColumnMetaData("NUM_PREC_RADIX", "NUM_PREC_RADIX", 0,
                        Types.INTEGER, 0, 0, null, flags, false) };
        Vector<XData[]> rows = new Vector<XData[]>(20);

        /*
         * rows.add(new XData[]{ new VarcharType("BLOB"), new
         * ShortType((short)java.sql.Types.BLOB), null,//precision new
         * VarcharType(null), new VarcharType(null), new
         * VarcharType(null),//don't care new
         * ShortType((short)DatabaseMetaData.typeNullable), new
         * BooleanType(false),//case sensitive? new
         * ShortType((short)DatabaseMetaData.typePredNone),//not searchable new
         * BooleanType(false), //unsigned? new BooleanType(false), //can this be
         * money value? new BooleanType(false), //auto-increment new
         * VarcharType(null), //local type name new ShortType(null), new
         * ShortType(null), //max/min scale new LongType(null), new
         * LongType(null), new LongType(null) });
         */
        rows.add(new XData[] {
                new VarcharType("BOOLEAN"),
                new ShortType((short) java.sql.Types.BOOLEAN),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),// searchable
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("CHAR"),
                new ShortType((short) java.sql.Types.CHAR),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(true),// case sensitive?
                new ShortType((short) DatabaseMetaData.typeSearchable),// searchable
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("NCHAR"),
                new ShortType((short) java.sql.Types.CHAR),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(true),// case sensitive?
                new ShortType((short) DatabaseMetaData.typeSearchable),// searchable
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        /*
         * rows.add(new XData[]{ new VarcharType("CLOB"), new
         * ShortType((short)java.sql.Types.CLOB), null,//precision new
         * VarcharType(null), new VarcharType(null), new
         * VarcharType(null),//don't care new
         * ShortType((short)DatabaseMetaData.typeNullable), new
         * BooleanType(false),//case sensitive? new
         * ShortType((short)DatabaseMetaData.typePredNone),//not searchable new
         * BooleanType(false), //unsigned? new BooleanType(false), //can this be
         * money value? new BooleanType(false), //auto-increment new
         * VarcharType(null), //local type name new ShortType(null), new
         * ShortType(null), //max/min scale new LongType(null), new
         * LongType(null), new LongType(null) });
         */
        rows.add(new XData[] {
                new VarcharType("DATE"),
                new ShortType((short) java.sql.Types.DATE),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),// not
                // searchable
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("DECIMAL"),
                new ShortType((short) java.sql.Types.DECIMAL),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(false), // unsigned?
                new BooleanType(true), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType("DEC"), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("DOUBLE PRECISION"),
                new ShortType((short) java.sql.Types.DOUBLE),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),// not
                // searchable
                new BooleanType(false), // unsigned?
                new BooleanType(true), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType("DOUBLE PRECISION"), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("FLOAT"),
                new ShortType((short) java.sql.Types.FLOAT),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),// not
                // searchable
                new BooleanType(false), // unsigned?
                new BooleanType(true), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType("FLOAT"), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("INTEGER"),
                new ShortType((short) java.sql.Types.INTEGER),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),// not
                // searchable
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType("INT"), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        /*
         * rows.add(new XData[]{ new VarcharType("NCLOB"), new
         * ShortType((short)java.sql.Types.LONGVARCHAR), null,//precision new
         * VarcharType(null), new VarcharType(null), new
         * VarcharType(null),//don't care new
         * ShortType((short)DatabaseMetaData.typeNullable), new
         * BooleanType(true),//case sensitive? new
         * ShortType((short)DatabaseMetaData.typePredBasic), new
         * BooleanType(false), //unsigned? new BooleanType(false), //can this be
         * money value? new BooleanType(false), //auto-increment new
         * VarcharType(null), //local type name new ShortType(null), new
         * ShortType(null), //max/min scale new LongType(null), new
         * LongType(null), new LongType(null) });
         */
        rows.add(new XData[] {
                new VarcharType("NUMERIC"),
                new ShortType((short) java.sql.Types.NUMERIC),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(false), // unsigned?
                new BooleanType(true), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("FIXED"),
                new ShortType((short) java.sql.Types.DECIMAL),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(false), // unsigned?
                new BooleanType(true), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("REAL"),
                new ShortType((short) java.sql.Types.REAL),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(false), // unsigned?
                new BooleanType(true), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("SERIAL"),
                new ShortType((short) java.sql.Types.INTEGER),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(true), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(true), // auto-increment
                new VarcharType("SERIAL"), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("BIGSERIAL"),
                new ShortType((short) java.sql.Types.BIGINT),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(true), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(true), // auto-increment
                new VarcharType("BIGSERIAL"), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("SMALLINT"),
                new ShortType((short) java.sql.Types.SMALLINT),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("BIGINT"),
                new ShortType((short) java.sql.Types.BIGINT),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType("BIGINT"), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("TIME"),
                new ShortType((short) java.sql.Types.TIME),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("TIMESTAMP"),
                new ShortType((short) java.sql.Types.TIMESTAMP),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(false),// case sensitive?
                new ShortType((short) DatabaseMetaData.typePredBasic),
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });
        rows.add(new XData[] {
                new VarcharType("VARCHAR"),
                new ShortType((short) java.sql.Types.VARCHAR),
                null,// precision
                new VarcharType(null),
                new VarcharType(null),
                new VarcharType(null),// don't care
                new ShortType((short) DatabaseMetaData.typeNullable),
                new BooleanType(true),// case sensitive?
                new ShortType((short) DatabaseMetaData.typeSearchable),// not
                // searchable
                new BooleanType(false), // unsigned?
                new BooleanType(false), // can this be money value?
                new BooleanType(false), // auto-increment
                new VarcharType(null), // local type name
                new ShortType(null), new ShortType(null), // max/min scale
                new LongType(null), new LongType(null), new LongType(null) });

        return new ResultSetImpl(headers, rows);
    }

    /**
     * @param client
     * @return result set with all users in the database
     */
    public static ResultSet getShowUsers(XDBSessionContext client) {
        ColumnMetaData[] headers = new ColumnMetaData[] {
                new ColumnMetaData("USER_NAME", "USER_NAME", 250,
                        Types.VARCHAR, 0, 0, null, (short) 0, false),
                new ColumnMetaData("USER_CLASS", "USER_CLASS", 8,
                        Types.VARCHAR, 0, 0, null, (short) 0, false), };
        Collection sysUsers = client.getSysDatabase().getSysUsers();
        Vector<XData[]> rows = new Vector<XData[]>(sysUsers.size());
        for (Iterator it = sysUsers.iterator(); it.hasNext();) {
            SysUser user = (SysUser) it.next();
            String className = null;
            switch (user.getUserClass()) {
            case SysLogin.USER_CLASS_DBA:
                className = SysLogin.USER_CLASS_DBA_STR;
                break;
            case SysLogin.USER_CLASS_STANDARD:
                className = SysLogin.USER_CLASS_STANDARD_STR;
                break;
            case SysLogin.USER_CLASS_RESOURCE:
                className = SysLogin.USER_CLASS_RESOURCE_STR;
                break;
            }
            rows.add(new XData[] { new VarcharType(user.getName()),
                    new VarcharType(className) });
        }
        return new ResultSetImpl(headers, rows);
    }

    /**
     * Enumerate views in the current database
     *
     * @param client
     * @return result set containing names of views
     */
    public static ResultSet getShowViews(XDBSessionContext client) {
        ColumnMetaData[] headers = new ColumnMetaData[] { new ColumnMetaData(
                "VIEW_NAME", "VIEW_NAME", 250, Types.VARCHAR, 0, 0, null,
                (short) 0, false), };
        Collection<SysView> sysViews = client.getSysDatabase().getSysViews();
        Vector<XData[]> rows = new Vector<XData[]>(sysViews.size());
        for (SysView view : sysViews) {
            rows.add(new XData[] { new VarcharType(view.getViewName()), });
        }
        return new ResultSetImpl(headers, rows);
    }

    /**
     * Converts SQL pattern to regular expression<br>
     *
     * <pre>
     *
     * Rules:
     * Wildcards:
     *     % is converted to \w*
     *     _ is converted to \w
     * Escapes:
     * \&lt;anychar&gt; is converted to &lt;anychar&gt;
     * but \\ is converted to \\ (backslash should be escaped in regular expressions)
     * &lt;\pre&gt;
     *
     * @param ptr the pattern
     * @return the regular expression
     *
     */
    private static String patternToRegexp(String ptr) {
        StringBuffer regexp = new StringBuffer(ptr.length());
        for (int i = 0; i < ptr.length(); i++) {
            char ch = ptr.charAt(i);
            switch (ch) {
            case '%':
                regexp.append("\\w*");
                break;
            case '_':
                regexp.append("\\w");
                break;
            case '\\':
                if (i < ptr.length() - 1) {
                    i++;
                    if (ptr.charAt(i) == '\\') {
                        regexp.append("\\");
                    }
                    regexp.append(ptr.charAt(i));
                }
                break;
            default:
                regexp.append(ch);
            }
        }
        return regexp.toString();
    }

    private static Collection<SysDatabase> getMatchingDatabases(String dbNamePtr) {
        Collection<SysDatabase> databases = MetaData.getMetaData()
                .getSysDatabases();
        if (dbNamePtr == null) {
            return databases;
        } else {
            ArrayList<SysDatabase> out = new ArrayList<SysDatabase>(databases
                    .size());
            Pattern pattern = Pattern.compile(patternToRegexp(dbNamePtr),
                    Pattern.CASE_INSENSITIVE);
            for (SysDatabase database : databases) {
                if (pattern.matcher(database.getDbname()).matches()) {
                    out.add(database);
                }
            }
            return out;
        }
    }

    private static Collection<SysTable> getMatchingTables(
            Collection<SysDatabase> databases, String tableNamePtr) {
        ArrayList<SysTable> out = new ArrayList<SysTable>();
        Pattern pattern = Pattern.compile(patternToRegexp(tableNamePtr),
                Pattern.CASE_INSENSITIVE);
        for (SysDatabase database : databases) {
            for (SysTable table : database.getSysTables()) {
                if (pattern.matcher(table.getTableName()).matches()) {
                    out.add(table);
                }
            }
        }
        return out;
    }

    private static Collection<SysView> getMatchingViews(
            Collection<SysDatabase> databases, String tableNamePtr) {
        ArrayList<SysView> out = new ArrayList<SysView>();
        Pattern pattern = Pattern.compile(patternToRegexp(tableNamePtr),
                Pattern.CASE_INSENSITIVE);
        for (SysDatabase database : databases) {
            for (SysView view : database.getSysViews()) {
                if (pattern.matcher(view.getViewName()).matches()) {
                    out.add(view);
                }
            }
        }
        return out;
    }

    private static Map<String, SysTable> getMatchingTempTables(
            XDBSessionContext client, String tableNamePtr) {
        HashMap<String, SysTable> out = new HashMap<String, SysTable>();
        Pattern pattern = Pattern.compile(patternToRegexp(tableNamePtr),
                Pattern.CASE_INSENSITIVE);
        SysDatabase database = client.getSysDatabase();
        for (Object element : client.getTempTableNames()) {
            String tableName = (String) element;
            if (pattern.matcher(tableName).matches()) {
                out.put(tableName, database.getSysTable(client
                        .getTempTableName(tableName)));
            }
        }
        return out;
    }
/*
    public static java.util.Properties getServerProperties() {
        java.util.Properties props = new java.util.Properties();

        props.put(ALL_PROCEDURES_ARE_CALLABLE, "false"); // Not supported
        props.put(ALL_TABLES_ARE_SELECTABLE, "false"); // To be changed
        props.put(DATA_DEFINITION_CAUSES_COMMIT, "true");
        props.put(DATA_DEFINITION_IGNORED_IN_TRANS, "false");
        props.put(DELETES_ARE_DETECTED, "false"); // Not supported
        props.put(DOES_MAX_ROW_SIZE_INCLUDE_BLOBS, "true"); // BLOBs are not
        // supported
        props.put(CATALOG_SEPARATOR, ":"); // Not supported
        props.put(CATALOG_TERM, "database");
        props.put(DATABASE_MAJOR_VERSION, "0");
        props.put(DATABASE_MINOR_VERSION, "9");
        props.put(DATABASE_PRODUCT_NAME, "Stado");
        props.put(DATABASE_VERSION, "Stado 0.9");
        props.put(DEFAULT_TRANS_ISOLATION, ""
                + Connection.TRANSACTION_READ_COMMITTED); // Switching not
        // supported
        props.put(EXTRA_NAME_CHARACTERS, ""); // None
        props.put(IDENTIFIER_QUOTE_STRING, " "); // Not supported
        props.put(MAX_BINARY_LITERAL_LENGTH, "0"); // Not supported
        props.put(MAX_CATALOG_NAME_LENGTH, "0"); // TODO Backend
        // MAX_CATALOG_NAME_LENGTH -
        // MAX_NODE_ID_LEN (N*** - 2
        // for 8 nodes, 3 for 16, 4
        // for 256)
        props.put(MAX_CHAR_LITERAL_LENGTH, "0"); // No limit ?
        props.put(MAX_COLUMN_NAME_LENGTH, "0"); // TODO from backend
        props.put(MAX_COLUMNS_IN_GROUPBY, "0"); // No limit ?
        props.put(MAX_COLUMNS_IN_INDEX, "0"); // No limit ?
        props.put(MAX_COLUMNS_IN_ORDERBY, "0"); // No limit ?
        props.put(MAX_COLUMNS_IN_SELECT, "0"); // No limit ?
        props.put(MAX_COLUMNS_IN_TABLE, "0"); // TODO from backend - 1
        props.put(MAX_CONNECTIONS, Property.get("xdb.maxconnections", "0"));
        props.put(MAX_CURSOR_NAME_LENGTH, "0"); // Not supported
        props.put(MAX_INDEX_LENGTH, "0"); // TODO from backend
        props.put(MAX_PROCEDURE_NAME_LENGTH, "0"); // Not supported
        props.put(MAX_ROW_SIZE, "0"); // TODO from backend
        props.put(MAX_SCHEMA_NAME_LENGTH, "0"); // Not supported
        props.put(MAX_STATEMENT_LENGTH, "0"); // No limit ?
        props.put(MAX_STATEMENTS, "0"); // No limit ?
        props.put(MAX_TABLE_NAME_LENGTH, "0"); // TODO from backend
        props.put(MAX_TABLES_IN_SELECT, "0"); // No limit
        props.put(MAX_USER_NAME_LENGTH, "30"); // XSYSUSERS.username
        props
                .put(
                        NUMERIC_FUNCTIONS,
                        "ABS,ACOS,ASIN,ATAN,ATAN2,COS,COT,"
                                + "DEGREES,EXP,FLOOR,LOG,MOD,PI,RADIANS,ROUND,SIGN,SIN,SQRT,TAN");
        props.put(PROCEDURE_TERM, "procedure"); // Not supported
        props.put(RESULT_SET_HOLDABILITY, ""
                + ResultSet.HOLD_CURSORS_OVER_COMMIT);
        props.put(SQL_KEYWORDS, "AFTER,BINARY,BOOLEAN,DATABASES,DBA,ESTIMATE,"
                + "MODIFY,NODE,NODES,OWNER,PARENT,PARTITION,PARTITIONING,"
                + "PASSWORD,PERCENT,PUBLIC,RENAME,REPLICATED,RESOURCE,SAMPLE,"
                + "SERIAL,SHOW,STANDARD,STAT,STATISTICS,TABLES,TEMP,TRAN,"
                + "UNSIGNED,ZEROFILL");
        props.put(SQL_STATE_TYPE, "0"); // TODO ???
        props.put(SCHEMA_TERM, "schema"); // Not supported
        props.put(SEARCH_STRING_ESCAPE, "\\");
        props.put(STRING_FUNCTIONS, "ASCII,INDEX,LENGTH,LOWER,LTRIM,"
                + "REPLACE,RPAD,RTRIM,SUBSTR,TRIM,UPPER");
        props.put(SYSTEM_FUNCTIONS, "");
        props.put(TIME_DATE_FUNCTIONS,
                "CURRENT_TIME,CURRENT_DATE,CURRENT_TIMESTAMP");
        props.put(INSERTS_ARE_DETECTED, "false"); // Not supported
        props.put(CATALOG_AT_START, "true"); // Not supported
        props.put(READ_ONLY, "false"); // Not supported
        props.put(LOCATORS_UPDATE_COPY, "true"); // Not supported
        props.put(NULL_PLUS_NON_NULL_IS_NULL, "true"); // From backend ?
        props
                .put(
                        NULLS_SORTED_AT_END,
                        ""
                                + (org.postgresql.stado.common.util.Props.XDB_COMBINED_RESULTSET_SORT_NULLS_STYLE == org.postgresql.stado.common.util.Props.SORT_NULLS_AT_END));
        props
                .put(
                        NULLS_SORTED_AT_START,
                        ""
                                + (org.postgresql.stado.common.util.Props.XDB_COMBINED_RESULTSET_SORT_NULLS_STYLE == org.postgresql.stado.common.util.Props.SORT_NULLS_AT_START));
        props
                .put(
                        NULLS_SORTED_HIGH,
                        ""
                                + (org.postgresql.stado.common.util.Props.XDB_COMBINED_RESULTSET_SORT_NULLS_STYLE == org.postgresql.stado.common.util.Props.SORT_NULLS_HIGH));
        props
                .put(
                        NULLS_SORTED_LOW,
                        ""
                                + (org.postgresql.stado.common.util.Props.XDB_COMBINED_RESULTSET_SORT_NULLS_STYLE == org.postgresql.stado.common.util.Props.SORT_NULLS_LOW));
        props.put(OTHERS_DELETES_VISIBLE, "false");
        props.put(OTHERS_INSERTS_VISIBLE, "false");
        props.put(OTHERS_UPDATES_VISIBLE, "false");
        props.put(OWN_DELETES_VISIBLE, "true");
        props.put(OWN_INSERTS_VISIBLE, "true");
        props.put(OWN_UPDATES_VISIBLE, "true");
        props.put(STORES_LOWERCASE_IDENTIFIERS, "false");
        props.put(STORES_LOWERCASE_QUOTED_IDENTIFIERS, "false");
        props.put(STORES_MIXEDCASE_IDENTIFIERS, "true");
        props.put(STORES_MIXEDCASE_QUOTED_IDENTIFIERS, "true");
        props.put(STORES_UPPERCASE_IDENTIFIERS, "false");
        props.put(STORES_UPPERCASE_QUOTED_IDENTIFIERS, "false");
        props.put(SUPPORTS_ANSI92_ENTRY_LEVEL, "true");
        props.put(SUPPORTS_ANSI92_INTERMEDIATE, "falsee");
        props.put(SUPPORTS_ANSI92_FULL, "false");
        props.put(SUPPORTS_ALTER_TABLE_ADD_COLUMN, "true");
        props.put(SUPPORTS_ALTER_TABLE_DROP_COLUMN, "true");
        props.put(SUPPORTS_BATCH_UPDATES, "true");
        props.put(SUPPORTS_CATALOGS_IN_DML, "false");
        props.put(SUPPORTS_CATALOGS_IN_INDEX_DEFS, "false");
        props.put(SUPPORTS_CATALOGS_IN_PRIVILEGE_DEFS, "false");
        props.put(SUPPORTS_CATALOGS_IN_PROCEDURE_CALLS, "false");
        props.put(SUPPORTS_CATALOGS_IN_TABLE_DEFS, "false");
        props.put(SUPPORTS_COLUMN_ALIASING, "true");
        props.put(SUPPORTS_CONVERT, "true");
        props.put(SUPPORTS_CORE_SQL_GRAMMAR, "true");
        props.put(SUPPORTS_CORELLATED_CUBQUERIES, "true");
        props.put(SUPPORTS_DDL_AND_DML_IN_TRANSACTIONS, "true");
        props.put(SUPPORTS_DML_IN_TRANSACTIONS_ONLY, "false");
        props.put(SUPPORTS_DIFFERENT_TABLE_CORELLATION_NAMES, "false");
        props.put(SUPPORTS_EXPRESSIONS_IN_ORDER_BY, "true");
        props.put(SUPPORTS_EXTENDED_SQL_GRAMMAR, "false");
        props.put(SUPPORTS_FULL_OUTER_JOINS, "false");
        props.put(SUPPORTS_GET_GENERATED_KEYS, "false");
        props.put(SUPPORTS_GROUP_BY, "true");
        props.put(SUPPORTS_GROUP_BY_BEYOND_SELECT, "false");
        props.put(SUPPORTS_GROUP_BY_UNRELATED, "false");
        props.put(SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY, "false");
        props.put(SUPPORTS_LIKE_ESCAPE_CLAUSE, "false");
        props.put(SUPPORTS_LIMITED_OUTER_JOINS, "true");
        props.put(SUPPORTS_MINIMUM_SQL_GRAMMAR, "true");
        props.put(SUPPORTS_MIXED_CASE_IDENTIFIERS, "true");
        props.put(SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS, "false");
        props.put(SUPPORTS_MULTIPLE_OPEN_RESULTS, "true");
        props.put(SUPPORTS_MULTIPLE_RESULT_SETS, "false");
        props.put(SUPPORTS_MULTIPLE_TRANSACTIONS, "true");
        props.put(SUPPORTS_NAMED_PARAMETERS, "false");
        props.put(SUPPORTS_NON_NULLABLE_COLUMNS, "true");
        props.put(SUPPORTS_OPEN_CURSORS_ACROSS_COMMIT, "true");
        props.put(SUPPORTS_OPEN_CURSORS_ACROSS_ROLLBACK, "true");
        props.put(SUPPORTS_OPEN_STATEMENTS_ACROSS_COMMIT, "true");
        props.put(SUPPORTS_OPEN_STATEMENTS_ACROSS_ROLLBACK, "true");
        props.put(SUPPORTS_ORDER_BY_UNRELATED, "true");
        props.put(SUPPORTS_OUTER_JOINS, "true");
        props.put(SUPPORTS_POSITIONED_DELETE, "false");
        props.put(SUPPORTS_POSITIONED_UPDATE, "false");
        props.put(SUPPORTS_RESULT_SET_CONCURRENCY + "."
                + ResultSet.TYPE_FORWARD_ONLY + "."
                + ResultSet.CONCUR_READ_ONLY, "true");
        props.put(SUPPORTS_RESULT_SET_HOLDABILITY + "."
                + ResultSet.HOLD_CURSORS_OVER_COMMIT, "true");
        props.put(SUPPORTS_RESULT_SET_TYPE + "." + ResultSet.TYPE_FORWARD_ONLY,
                "true");
        props.put(SUPPORTS_SAVEPOINTS, "true");
        props.put(SUPPORTS_SCHEMAS_IN_DML, "false");
        props.put(SUPPORTS_SCHEMAS_IN_INDEX_DEFS, "false");
        props.put(SUPPORTS_SCHEMAS_IN_PRIVILEGE_DEFS, "false");
        props.put(SUPPORTS_SCHEMAS_IN_PROCEDURE_CALLS, "false");
        props.put(SUPPORTS_SCHEMAS_IN_TABLE_DEFS, "false");
        props.put(SUPPORTS_SELECT_FOR_UPDATE, "false");
        props.put(SUPPORTS_STATEMENT_POOLING, "false");
        props.put(SUPPORTS_STORED_PROCEDURES, "false");
        props.put(SUPPORTS_SUBQUERIES_IN_COMPARISONS, "true");
        props.put(SUPPORTS_SUBQUERIES_IN_EXISTS, "true");
        props.put(SUPPORTS_SUBQUERIES_IN_INS, "true");
        props.put(SUPPORTS_SUBQUERIES_IN_QUANIFIEDS, "true");
        props.put(SUPPORTS_TABLE_CORELLATION_NAMES, "true");
        props.put(SUPPORTS_TRANSACTION_ISOLATION_LEVEL + "."
                + Connection.TRANSACTION_SERIALIZABLE, "true");
        props.put(SUPPORTS_TRANSACTIONS, "true");
        props.put(SUPPORTS_UNION, "true");
        props.put(SUPPORTS_UNION_ALL, "true");
        props.put(UPDATES_ARE_DETECTED, "false");
        props.put(USES_LOCAL_FILE_PER_TABLE, "false");
        props.put(USES_LOCAL_FILES, "false");

        return props;
    }
  */
}
