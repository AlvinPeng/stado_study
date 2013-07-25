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
 * Properties.java
 *
 *
 */

package org.postgresql.stado.common.util;

/**
 *
 *
 */
public class Props {

    public static final String SERVER_MAJOR_VERSION = "2";

    public static final String SERVER_MINOR_VERSION = "5.0.0";

    public static final String SERVER_VERSION = SERVER_MAJOR_VERSION + "."
            + SERVER_MINOR_VERSION;
    
    public static final String DISPLAY_SERVER_VERSION = "PostgreSQL 9.1.0 (Stado Edition)";

    public static final String XDB_PROTOCOL_HANDLER_CLASS = Property.get(
            "xdb.protocol.handler.class",
            "org.postgresql.stado.protocol.PgProtocolHandler");

    public static final int XDB_COORDINATOR_NODE = Property.getInt(
            "xdb.coordinator.node", -1);

    public static final int XDB_MAXCONNECTIONS = Property.getInt(
            "xdb.maxconnections", 50);

    public static final String XDB_SAVEPOINTTYPE = Property.get(
            "xdb.savepointType", "S");

    public static final boolean XDB_SQL_USECROSSJOIN = Property.getBoolean(
            "xdb.sql.usecrossjoin", true);

    public static final boolean XDB_INDEX_USEASCDESC = Property.getBoolean(
            "xdb.index.useAscDesc", false);

    public static final String XDB_TEMPTABLEPREFIX = Property.get(
            "xdb.tempTablePrefix", "TMPT");

    public static final int XDB_ALLOWTEMPTABLEINDEX = Property.getInt(
            "xdb.allowtemptableindex", 1);

    public static final boolean XDB_COMMIT_AFTER_CREATE_TEMP_TABLE = Property.getBoolean(
            "xdb.commit_after_create_temp_table", false);

    public static final String XDB_SQLCOMMAND_CREATETEMPTABLE_START = Property.get(
            "xdb.sqlcommand.createTempTable.start", "CREATE TEMP TABLE");

    public static final String XDB_SQLCOMMAND_CREATETEMPTABLE_SUFFIX = Property.get(
            "xdb.sqlcommand.createTempTable.suffix", " WITHOUT OIDS");

    public static final String XDB_SQLCOMMAND_CREATEGLOBALTEMPTABLE_START = Property.get(
            "xdb.sqlcommand.createGlobalTempTable.start", "CREATE UNLOGGED TABLE");

    public static final String XDB_SQLCOMMAND_CREATEGLOBALTEMPTABLE_SUFFIX = Property.get(
            "xdb.sqlcommand.createGlobalTempTable.suffix", " WITHOUT OIDS");

    // 2 = PostgreSQL
    public static final int XDB_SQLCOMMAND_UPDATE_CORELLATEDSTYLE = Property.getInt(
            "xdb.sqlcommand.update.correlatedstyle", 2);

    public static final String XDB_SQLCOMMAND_RENAMETABLE_TEMPLATE = Property.get(
            "xdb.sqlcommand.renametable.template",
            "ALTER TABLE {oldname} RENAME TO {newname}");

    public static final String XDB_SQLCOMMAND_SELECTINTO_TEMPLATE = Property.get(
            "xdb.sqlcommand.selectinto.template",
            "CREATE UNLOGGED TABLE {newname} AS SELECT * FROM {oldname}");

    public static final String XDB_SQLCOMMAND_SELECTINTOTEMP_TEMPLATE = Property.get(
            "xdb.sqlcommand.selectintotemp.template",
            "CREATE TEMP TABLE {newname} AS SELECT * FROM {oldname}");

    public static final String XDB_SQLCOMMAND_VACUUM_TEMPLATE_TABLE = Property.get(
            "xdb.sqlcommand.vacuum.template.table",
            "VACUUM {vacuum_type} {table}");

    public static final String XDB_SQLCOMMAND_VACUUM_TEMPLATE_COLUMN = Property.get("xdb.sqlcommand.vacuum.template.column");

    public static final String XDB_SQLCOMMAND_ANALYZE_TEMPLATE_TABLE = Property.get(
            "xdb.sqlcommand.analyze.template.table", "ANALYZE {table}");

    public static final String XDB_SQLCOMMAND_ANALYZE_TEMPLATE_COLUMN = Property.get(
            "xdb.sqlcommand.analyze.template.column",
            "ANALYZE {table} ({column_list})");

    public static final String XDB_SQLCOMMAND_VACUUM_ANALYZE_TEMPLATE_TABLE = Property.get(
            "xdb.sqlcommand.vacuum.analyze.template.table",
            "VACUUM {vacuum_type} ANALYZE {table}");

    public static final String XDB_SQLCOMMAND_VACUUM_ANALYZE_TEMPLATE_COLUMN = Property.get(
            "xdb.sqlcommand.vacuum.analyze.template.column",
            "VACUUM {vacuum_type} ANALYZE {table} ({column_list})");

    public static final String XDB_SQLCOMMAND_UPDATESTATISTICS_QUERY = Property.get(
            "xdb.sqlcommand.updatestatistics.query", "SELECT s.stadistinct "
                    + "FROM pg_statistic s, pg_class c, pg_attribute a "
                    + "WHERE s.starelid = c.oid AND s.staattnum = a.attnum "
                    + "AND c.relname = '{table}' AND a.attname = '{column}'");

    public static final String XDB_SQLCOMMAND_UPDATESTATISTICS_ROWCOUNT = Property.get(
            "xdb.sqlcommand.updatestatistics.rowcount",
            "SELECT c.reltuples FROM pg_class c WHERE c.relname = '{table}'");

    public static final boolean XDB_SQLCOMMAND_UPDATESTATISTICS_ROWCOUNT_QUOTED = Property.getBoolean(
            "xdb.sqlcommand.updatestatistics.rowcount.quoted", false);

    public static final String XDB_SQLCOMMAND_DROP_INDEX = Property.get(
            "xdb.sqlcommand.dropindex", "drop index {index_list}");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_DROP_INDEX_AFTER_DROP_CONSTRAINT = Property.getBoolean(
            "xdb.sqlcommand.altertable.isneeddropindex", true);

    // templates for "alter table" command
    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.modifycolumn.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN = Property.get(
            "xdb.sqlcommand.altertable.modifycolumn",
            "alter {column} type {column_type}");

    public static final String XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_USING = Property.get(
            "xdb.sqlcommand.altertable.modifycolumn.using",
            "using {using_expr}");

    public static final String XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_SETDEFAULT = Property.get(
            "xdb.sqlcommand.altertable.modifycolumn.setdefault",
            "alter {column} set default {default_expr}");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_SETDEFAULT_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.modifycolumn.setdefault.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_SETNOTNULL = Property.get(
            "xdb.sqlcommand.altertable.modifycolumn.setnotnull",
            "alter {column} set not null");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_SETNOTNULL_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.modifycolumn.setnotnull.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_DROPNOTNULL = Property.get(
            "xdb.sqlcommand.altertable.modifycolumn.dropnotnull",
            "alter {column} drop not null");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_DROPNOTNULL_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.modifycolumn.dropnotnull.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_DROPDEFAULT = Property.get(
            "xdb.sqlcommand.altertable.modifycolumn.dropdefault",
            "alter {column} drop default");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_DROPDEFAULT_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.modifycolumn.dropdefault.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_RENAMECOLUMN = Property.get(
            "xdb.sqlcommand.altertable.renamecolumn",
            "rename column {old_colname} to {new_colname}");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_RENAMECOLUMN_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.renamecolumn.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_ADDCOLUMN = Property.get(
            "xdb.sqlcommand.altertable.addcolumn", "add {colname}");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_ADDCOLUMN_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.addcolumn.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_ADDPRIMARY = Property.get(
            "xdb.sqlcommand.altertable.addprimary",
            "add constraint {constr_name} primary key({col_list})");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_ADDPRIMARY_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.addprimary.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_ADDCHECK = Property.get(
            "xdb.sqlcommand.altertable.addcheck",
            "add constraint {constr_name} check({check_def})");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_ADDCHECK_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.addcheck.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT = Property.get(
            "xdb.sqlcommand.altertable.dropconstraint",
            "drop constraint {constr_name} ");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.dropconstraint.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_PRIMARY = Property.get(
            "xdb.sqlcommand.altertable.dropconstraint.primary",
            XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT);

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_PRIMARY_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.dropconstraint.primary.toparent",
            XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_TO_PARENT);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_UNIQUE = Property.get(
            "xdb.sqlcommand.altertable.dropconstraint.unique",
            XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT);

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_UNIQUE_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.dropconstraint.unique.toparent",
            XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_TO_PARENT);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_REFERENCE = Property.get(
            "xdb.sqlcommand.altertable.dropconstraint.reference",
            XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT);

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_REFERENCE_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.dropconstraint.reference.toparent",
            XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_TO_PARENT);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_CHECK = Property.get(
            "xdb.sqlcommand.altertable.dropconstraint.check",
            XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT);

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_CHECK_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.dropconstraint.check.toparent",
            XDB_SQLCOMMAND_ALTERTABLE_DROPCONSTRAINT_TO_PARENT);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_DROPPRIMAY = Property.get(
            "xdb.sqlcommand.altertable.dropprimary",
            "drop constraint {constr_name}");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_DROPPRIMAY_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.dropprimary.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_DROPCOLUMN = Property.get(
            "xdb.sqlcommand.altertable.dropcolumn", "drop {column}");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_DROPCOLUMN_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.dropcolumn.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_ADDFOREIGNKEY = Property.get(
            "xdb.sqlcommand.altertable.addforeignkey",
            "add constraint {constr_name} foreign key ({col_list}) references {reftable}({col_map_list})");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_ADDFOREIGNKEY_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.addforeignkey.toparent", true);

    public static final String XDB_SQLCOMMAND_ALTERTABLE_SETTABLESPACE = Property.get(
            "xdb.sqlcommand.altertable.settablespace",
            "set tablespace {tablespace}");

    public static final boolean XDB_SQLCOMMAND_ALTERTABLE_SETTABLESPACE_TO_PARENT = Property.getBoolean(
            "xdb.sqlcommand.altertable.settablespace.toparent", true);

    public static final boolean XDB_USE_LOAD_FOR_STEP = Property.getBoolean(
            "xdb.use_load_for_step", true);

    public static final boolean XDB_USE_COPY_OUT_FOR_STEP = Property.getBoolean(
            "xdb.use_copy_out_for_step", XDB_USE_LOAD_FOR_STEP);

    public static final boolean XDB_JUST_DATA_VALUES = Property.getBoolean(
            "xdb.message.data.justvalues", false);

    public static final String XDB_LOADER_PGWRITER_TEMPLATE = Property.get(
            "xdb.loader.edbwriter.template",
            "COPY {table} {columninfo} FROM STDIN WITH DELIMITER AS E'{delimiter}' NULL AS E'{null}'");

    public static final String XDB_LOADER_NODEWRITER_TEMPLATE = Property.get(
            "xdb.loader.nodewriter.template",
            "{psql-util-name} -h {dbhost} -p {dbport} -d {database} -U {dbusername} -a -e -E -c \"COPY {table} {columninfo} FROM STDIN WITH DELIMITER AS E'{delimiter}' NULL AS E'{null}'\"");

    public static final String XDB_LOADER_NODEWRITER_DEFAULT_DELIMITER = Property.get(
            "xdb.loader.nodewriter.default.delimiter", "\t");

    public static final String XDB_LOADER_NODEWRITER_DEFAULT_NULL = Property.get(
            "xdb.loader.nodewriter.default.null", "\\N");

    public static final String XDB_LOADER_NODEWRITER_COLUMNINFO = Property.get(
            "xdb.loader.nodewriter.columninfo", "({columns})");

    public static final String XDB_LOADER_NODEWRITER_COLUMNINFO_NONE = Property.get(
            "xdb.loader.nodewriter.columninfo.none", "");

    public static final String XDB_LOADER_NODEWRITER_ROW_DELIMITER = Property.get(
            "xdb.loader.nodewriter.rowdelimiter", "\n");

    public static final String XDB_LOADER_EDBWRITER_CSV_TEMPLATE = Property.get(
            "xdb.loader.pgwriter.csv.template",
            "COPY {table} {columninfo} FROM STDIN WITH DELIMITER AS E'{delimiter}' NULL AS E'{null}' CSV QUOTE AS E'{quote}' ESCAPE AS E'{escape}' {forcenotnullinfo}");

    public static final String XDB_LOADER_NODEWRITER_CSV_TEMPLATE = Property.get(
            "xdb.loader.nodewriter.csv.template",
            "{psql-util-name} -h {dbhost} -p {dbport} -d {database} -U {dbusername} -a -e -E -c \"COPY {table} {columninfo} FROM STDIN WITH DELIMITER AS E'{delimiter}' NULL AS E'{null}' CSV QUOTE AS E'{quote}' ESCAPE AS E'{escape}' {forcenotnullinfo}\"");

    public static final String XDB_LOADER_NODEWRITER_CSV_QUOTE = Property.get(
            "xdb.loader.nodewriter.csv.quote", "");

    public static final String XDB_LOADER_NODEWRITER_CSV_ESCAPE = Property.get(
            "xdb.loader.nodewriter.csv.quote", "");

    public static final String XDB_LOADER_NODEWRITER_CSV_DEFAULT_DELIMITER = Property.get(
            "xdb.loader.nodewriter.csv.default.delimiter", ",");

    public static final String XDB_LOADER_NODEWRITER_CSV_DEFAULT_NULL = Property.get(
            "xdb.loader.nodewriter.csv.default.null", "");

    public static final String XDB_LOADER_NODEWRITER_CSV_FORCENOTNULLINFO = Property.get(
            "xdb.loader.nodewriter.csv.forcenotnullinfo", "FORCE NOT NULL {columns}");

    public static final String XDB_LOADER_NODEWRITER_CSV_FORCENOTNULLINFO_NONE = Property.get(
            "xdb.loader.nodewriter.csv.forcenotnullinfo.none", "");

    public static final boolean XDB_LOADER_ROW_VALUE_ESCAPE_BACKSLASHES = Property.getBoolean(
            "xdb.loader.row.escape.backslashes", true);

    public static final int XDB_LOADER_BUFFER_SIZE = Property.getInt(
            "xdb.loader.buffer.size", 0x10000); // 64K

    public static final String XDB_TEMP_TABLE_SELECT = Property.get(
            "xdb.tempTableSelect",
            "select tablename as TABLE_NAME from pg_tables where tablename LIKE '"
                    + XDB_TEMPTABLEPREFIX + "%'");

    public static final int XDB_NODECOUNT = Property.getInt("xdb.nodecount", 0);

    public static final String XDB_DEFAULT_JDBCDRIVER = Property.get(
            "xdb.default.jdbcdriver", "org.postgresql.driver.Driver");

    public static final String XDB_PSQL_UTIL_NAME = Property.get(
            "xdb.psql-util-name", "psql");

    public static final String XDB_PGCONFIG_UTIL_NAME = Property.get(
            "xdb.pgconfig-util-name", "pg_config");

    public static final String XDB_DEFAULT_JDBCSTRING = Property.get(
            "xdb.default.jdbcstring", "jdbc:postgresql://{dbhost}:{dbport}/{database}");

    public static final int XDB_DEFAULT_DBPORT = Property.getInt(
            "xdb.default.dbport", 5432);

    public static final String XDB_DEFAULT_DBUSER = Property.get("xdb.default.dbusername");

    public static final String XDB_DEFAULT_DBPASSWORD = Property.get("xdb.default.dbpassword");

    public static final int XDB_LOADER_READER_COUNT = Property.getInt(
            "xdb.loader.readercount", 1);

    public static final int XDB_LOADER_DATAPROCESSORS_COUNT = Property.getInt(
            "xdb.loader.dataprocessors.count", 1);

    public static final int XDB_LOADER_DATAPROCESSOR_BUFFER = Property.getInt(
            "xdb.loader.dataprocessor.buffer", 2048);

    /**
     * Use extended copy API provided by JDBC driver
     */
    public static final boolean USE_JDBC_COPY_API = Property.getBoolean(
            "xdb.loader.nodewriter.use_jdbc_copy_api", true);

    public static final int XDB_DEFAULT_THREADS_POOL_INITSIZE = Property.getInt(
            "xdb.default.threads.pool.initsize", 5);

    public static final int XDB_DEFAULT_THREADS_POOL_MAXSIZE = Property.getInt(
            "xdb.default.threads.pool.maxsize", 10);

    public static final int XDB_DEFAULT_THREADS_POOL_TIMEOUT = Property.getInt(
            "xdb.default.threads.pool.timeout", 3600000);

    public static final int XDB_DEFAULT_THREADS_POOL_MAX_LIFETIME = Property.getInt(
            "xdb.default.threads.pool.max_lifetime", 600000);
        
    public static final int XDB_DEFAULT_THREADS_POOL_IDLE = Property.getInt(
            "xdb.default.threads.pool.idle", 300000);

    public static final int XDB_DEFAULT_THREADS_POOL_RELEASE_DELAY = Property.getInt(
            "xdb.default.threads.pool.release.delay", 1000);

    public static final boolean XDB_STEP_RUNANALYZE = Property.getBoolean(
            "xdb.step.runanalyze", true);

    public static final boolean XDB_STEP_INDEX_CORRELATED = Property.getBoolean(
            "xdb.step.indexcorrelated", true);

    public static final int XDB_NODEFETCHSIZE = Property.getInt(
            "xdb.nodeFetchSize", 1000);

    public static final int XDB_COMBINED_RESULTSET_BUFFER = Property.getInt(
            "xdb.combined.resultset.buffer", 1000);

    // How to sort nulls (should match to backend)
    public static final int SORT_NULLS_AT_START = 0;

    public static final int SORT_NULLS_AT_END = 1;

    public static final int SORT_NULLS_HIGH = 2;

    public static final int SORT_NULLS_LOW = 3;

    public static final int XDB_COMBINED_RESULTSET_SORT_NULLS_STYLE = Property.getInt(
            "xdb.sort.nulls.style", SORT_NULLS_HIGH);

    // Trim string before sorting (ignore leading witespaces)
    public static final boolean XDB_COMBINED_RESULTSET_SORT_TRIM = Property.getBoolean(
            "xdb.sort.trim", true);

    public static final boolean XDB_COMBINED_RESULTSET_SORT_CASE_SENSITIVE = Property.getBoolean(
            "xdb.sort.case.sensitive", false);

    public static final String XDB_GATEWAY_STARTDB = Property.get("xdb.gateway.startdb");

    public static final String XDB_GATEWAY_STOPDB = Property.get("xdb.gateway.stopdb");

    public static final String XDB_GATEWAY_CREATEDB = Property.get(
            "xdb.gateway.createdb",
            "createdb -h {dbhost} -p {dbport} -U {dbusername} -O {dbusername} {database}");

    public static final String XDB_GATEWAY_DROPDB = Property.get(
            "xdb.gateway.dropdb",
            "dropdb -h {dbhost} -p {dbport} -U {dbusername} {database}");

    public static final String XDB_GATEWAY_EXECSCRIPT = Property.get(
            "xdb.gateway.execscript",
            "{psql-util-name} -h {dbhost} -p {dbport} -d  {database} -U {dbusername} -f {inputfile}");

    public static final String XDB_GATEWAY_EXECCOMMAND = Property.get(
            "xdb.gateway.execcommand",
            "{psql-util-name} -h {dbhost} -p {dbport} -d  {database} -U {dbusername} -c {dbcommand}");

    /* Path to use when trying to execute any gateway commands.
       The default is null- use whatever is in the environment's PATH */
    public static final String XDB_GATEWAY_PATH = Property.get(
            "xdb.gateway.path", null);

    public static final String XDB_GATEWAY_PATH_SEPARATOR = Property.get(
            "xdb.gateway.path.separator", "/");

    public static final int XDB_SUBSECOND_PRECISION = Property.getInt(
            "xdb.subsecondPrecision", 6);

    public static final int XDB_STEP_ENDWAITTIME = Property.getInt(
            "xdb.step.endwaittime", 30000);

    public static final boolean XDB_AVOID_CORRELATED_SELF_JOINS = Property.getBoolean(
            "xdb.avoid.correlated.selfjoins", true);

    public static final String XDB_BACKEND_PING_STATEMENT = Property.get(
            "xdb.connectiontest.statement", "select 1");

    public static final String XDB_BACKEND_PING_SETUP = Property.get("xdb.connectiontest.createtable");

    public static final int XDB_CONSTANT_EXPRESSION_THRESHOLD = Property.getInt(
            "xdb.constantexpression.threshold", 1000);

    public static final String XDB_DUMPSTEPPATH = Property.get(
            "xdb.dumpStepPath", null);

    // if xdb.dumpStepPath is not null, this command will be used
    public static final String XDB_DUMPCOMMAND = Property.get(
            "xdb.dumpCommand",
            "COPY \"{table}\" TO '{path}/{table}_NODE_{node}' WITH DELIMITER '|'");

    public static final boolean XDB_FASTPARSE_INSERT = Property.getBoolean(
            "xdb.fastparse.insert", true);

    public static final boolean XDB_FASTPARSE_UPDATE = Property.getBoolean(
            "xdb.fastparse.update", true);

    public static final boolean XDB_FASTPARSE_DELETE = Property.getBoolean(
            "xdb.fastparse.delete", true);

    public static final boolean XDB_FASTPARSE_SELECT = Property.getBoolean(
            "xdb.fastparse.select", true);

    // Note that the mapping below is currently not used in standard data type
    // mapping,
    // and is just used internally when we need to generate a unique key.
    // It is separate from those definitions in
    // org.postgresql.stado.Parser.Handler.TypeConstants.java
    public static final String XDB_SERIAL_INTERNAL_TEMPLATE = Property.get(
            "xdb.sqltype.internal.serial.map", "SERIAL");

    public static final boolean XDB_USE_OID_IN_OUTER = Property.getBoolean(
            "xdb.use_oid_in_outer", false);

    //
    public static final boolean XDB_STRIP_INTERVAL_QUOTES = Property.getBoolean(
            "xdb.strip_interval_quotes", false);

    // Allow down joins, which may be faster.
    // Discovered a bug with this, so adding this to turn it off by default.
    public static final boolean XDB_ALLOW_DOWN_JOIN = Property.getBoolean(
            "xdb.allow_down_join", false);

    public static final long XDB_LARGE_QUERY_COST = Property.getLong(
            "xdb.jdbc.pool.largequery.threshold", 25000L);

    public static final boolean XDB_ENABLE_ACTIVITY_LOG = Property.getBoolean(
            "xdb.enable_activity_log", false);

    public static final String XDB_ADMIN_DATABASE = Property.get(
            "xdb.admin_database", "xdbadmin");

    public static final int XDB_SELECTOR_SLEEP_NANO = Property.getInt(
            "xdb.selector_sleep_nano", 0);

    public static final boolean XDB_ALLOW_PARTITION_INTEGER = Property.getBoolean(
            "xdb.allow.partition.integer", true);

    public static final boolean XDB_ALLOW_PARTITION_CHAR = Property.getBoolean(
            "xdb.allow.partition.char", true);

    public static final boolean XDB_ALLOW_PARTITION_DECIMAL = Property.getBoolean(
            "xdb.allow.partition.decimal", true);

    public static final boolean XDB_ALLOW_PARTITION_FLOAT = Property.getBoolean(
            "xdb.allow.partition.float", false);

    public static final boolean XDB_ALLOW_PARTITION_DATETIME = Property.getBoolean(
            "xdb.allow.partition.datetime", false);

    public static final boolean XDB_ALLOW_PARTITION_MACADDR = Property.getBoolean(
            "xdb.allow.partition.macaddr", true);

    public static final boolean XDB_ALLOW_PARTITION_INET = Property.getBoolean(
            "xdb.allow.partition.inet", true);

    /** If SET command is executed on connection, whether to persist
     * the underlying connections. If set, SET/SHOW will behave
     * more properly, but connection pooling may be limited. */
    public static final boolean XDB_PERSIST_ON_SET = Property.getBoolean(
            "xdb.persist_on_set", true);

    public static final boolean XDB_TEMPORARY_INTERMEDIATE_TABLES = Property.getBoolean(
            "xdb.temporary_intermediate_tables", !XDB_USE_LOAD_FOR_STEP);

    public static final String XDB_IDENTIFIER_CASE_LOWER = "lower";
    public static final String XDB_IDENTIFIER_CASE_UPPER = "upper";
    public static final String XDB_IDENTIFIER_CASE_PRESERVE = "preserve";

    public static final String XDB_IDENTIFIER_CASE;
    static {
        String identCase = Property.get("xdb.identifier.case");
        if (XDB_IDENTIFIER_CASE_PRESERVE.equalsIgnoreCase(identCase)) {
            XDB_IDENTIFIER_CASE = XDB_IDENTIFIER_CASE_PRESERVE;
        } else if (XDB_IDENTIFIER_CASE_UPPER.equalsIgnoreCase(identCase)) {
            XDB_IDENTIFIER_CASE = XDB_IDENTIFIER_CASE_UPPER;
        } else {
            XDB_IDENTIFIER_CASE = XDB_IDENTIFIER_CASE_LOWER;
        }
    }

    public static final String XDB_IDENTIFIER_QUOTE = Property.get("xdb.identifier.quote", "\"");

    public static final String XDB_IDENTIFIER_QUOTE_OPEN = Property.get("xdb.identifier.quote.open", XDB_IDENTIFIER_QUOTE);

    public static final String XDB_IDENTIFIER_QUOTE_CLOSE = Property.get("xdb.identifier.quote.close", XDB_IDENTIFIER_QUOTE);

    public static final String XDB_IDENTIFIER_QUOTE_ESCAPE = Property.get("xdb.identifier.quote.escape", XDB_IDENTIFIER_QUOTE);

    /** Set this to limit how many expressions to look at in determining
     * group by hashing. 0 == unlimited  */
    public static final int XDB_MAX_GROUP_HASH_COUNT = Property.getInt(
            "xdb.max_group_hash_count", 5);

    public static final boolean XDB_CLIENT_ENCODING_IGNORE = Property.getBoolean("xdb.client_encoding.ignore", false);

    public static final boolean XDB_ALLOW_MULTISTATEMENT_QUERY = Property.getBoolean(
            "xdb.allow.multistatement.query", true);

    // Password, or ldap
    public static final String XDB_AUTHENTICATION = Property.get("xdb.authentication", "password");
    
    // If we authenticate externally like, LDAP, what class of user to create for the new user
    public static final String XDB_AUTHENTICATION_NEW_USER_CLASS = Property.get(
            "xdb.authentication.new_user.class", "STANDARD");   

    public static final boolean XDB_AUTHENTICATION_NEW_USER_GRANT = Property.getBoolean(
            "xdb.authentication.new_user.grant", true);
        
    public static final boolean XDB_AUTHENTICATION_NEW_USER_GRANT_SELECT = Property.getBoolean(
            "xdb.authentication.new_user.grant.select", true);

    public static final boolean XDB_AUTHENTICATION_NEW_USER_GRANT_INSERT = Property.getBoolean(
            "xdb.authentication.new_user.grant.insert", false);
        
    public static final boolean XDB_AUTHENTICATION_NEW_USER_GRANT_UPDATE = Property.getBoolean(
            "xdb.authentication.new_user.grant.update", false);

    public static final boolean XDB_AUTHENTICATION_NEW_USER_GRANT_DELETE = Property.getBoolean(
            "xdb.authentication.new_user.grant.delete", false);
    
    public static final String XDB_AUTHENTICATION_NEW_USER_GRANT_TABLES = Property.get(
            "xdb.authentication.new_user.grant.tables", "*");    
        
    public static final String XDB_LDAP_PROVIDER_URL = Property.get("xdb.ldap.provider_url",""); 
    public static final String XDB_LDAP_SECURITY_AUTHENTICATION = Property.get("xdb.ldap.security_athentication","simple"); 
    public static final String XDB_LDAP_SECURITY_PRINCIPAL = Property.get("xdb.ldap.security_principal","{username}"); 
    public static final String XDB_LDAP_SECURITY_CREDENTIALS = Property.get("xdb.ldap.security_credentials","{password}"); 
      
    // backslash underscore substitute
    // Business Objects seems to be sometimes escaping "_" as "\\_" in string 
    // literals causing problems, we instead replace as "_"
    public static final boolean XDB_BSU_SUBSTITUTE = Property.getBoolean("xdb.bsu_substitute", false);    
    
    /** Creates a new instance of Properties */
    public Props() {

    }

}
