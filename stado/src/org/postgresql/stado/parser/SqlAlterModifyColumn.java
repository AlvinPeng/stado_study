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
import java.util.HashMap;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SyncAlterTableModifyColumn;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.parser.core.syntaxtree.AlterDefOperationSet;
import org.postgresql.stado.parser.core.syntaxtree.AlterDefOperationType;
import org.postgresql.stado.parser.core.syntaxtree.DropDefaultNotNull;
import org.postgresql.stado.parser.core.syntaxtree.IntervalLiterals;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.DataTypeHandler;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 * For adding a new Column
 */
public class SqlAlterModifyColumn extends DepthFirstVoidArguVisitor implements
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterModifyColumn.class);

    private static final int KIND_UNKNOWN = -1;

    private static final int KIND_CHANGE_TYPE = 0;

    private static final int KIND_SET_DEFAULT = 1;

    private static final int KIND_SET_NOT_NULL = 2;

    private static final int KIND_DROP_DEFAULT = 3;

    private static final int KIND_DROP_NOT_NULL = 4;

    private XDBSessionContext client;

    private SqlAlterTable parent;

    private SysColumn sysColumn;

    private SqlCreateTableColumn aSqlCreateTableColumn;

    private String usingExp;

    private int kind = KIND_UNKNOWN;

    // Incase the user specifies the position after which we have to
    // create the column - isPositionAfter should be changed to true
    // and the after column should contain the name of the column
    // We then need to verify if the column Name is a valid column name.
    // private String afterColumnName = null;
    private String[] commands;

    /**
     * @param table
     * @param client
     */
    public SqlAlterModifyColumn(SqlAlterTable parent, String columnName,
            XDBSessionContext client) {
        this.client = client;
        this.parent = parent;
        sysColumn = parent.getTable().getSysColumn(columnName);
        if (sysColumn == null) {
            throw new XDBServerException("The Table Has Not A Column Named "
                    + columnName);
        }
        if (parent.getTable() != sysColumn.getSysTable()) {
            throw new XDBServerException("Can not modify inherited column");
        }

        aSqlCreateTableColumn = new SqlCreateTableColumn(columnName,
                new DataTypeHandler(sysColumn.getColType(), sysColumn
                        .getColLength(), sysColumn.getColPrecision(), sysColumn
                        .getColScale()), sysColumn.isNullable(), sysColumn
                        .getDefaultExpr());
    }

    /**
     * Grammar production: f0 -> <TYPE_> f1 -> types() f2 -> [ <USING_> (
     * <STRING_LITERAL> | <NULL_> | <DATE_> | <TIME_> | <TIMESTAMP_> |
     * <DECIMAL_LITERAL> "." <DECIMAL_LITERAL> | <DECIMAL_LITERAL> |
     * IntervalLiterals(prn) ) ]
     */
    @Override
    public void visit(AlterDefOperationType n, Object argu) {
        aSqlCreateTableColumn.setTypeHandler(new DataTypeHandler());
        kind = KIND_CHANGE_TYPE;
        n.f1.accept(aSqlCreateTableColumn.getTypeHandler(), argu);
        if (n.f2.present()) {
            NodeSequence seq = (NodeSequence) n.f2.node;
            NodeChoice ch = (NodeChoice) seq.nodes.get(1);
            if (ch.which == 7) // interval
            {
                usingExp = ((IntervalLiterals) ch.choice).f0.tokenImage;
                // usingExp += ((IntervalLiterals)ch.choice).f1.f0.tokenImage;
                return;
            }
            if (ch.which != 5) {
                usingExp = ((NodeToken) ch.choice).tokenImage;
            } else {
                NodeSequence aNodeSequence = (NodeSequence) ch.choice;
                usingExp = "";
                for (Object node : aNodeSequence.nodes) {
                    usingExp += ((NodeToken) node).tokenImage;
                }

            }

        }
    }

    /**
     * Grammar production: f0 -> <SET_> f1 -> ( DefaultSpec(prn) | <NOT_>
     * <NULL_> | <STATISTICS_> <DECIMAL_LITERAL> | <STORAGE_> )
     */
    @Override
    public void visit(AlterDefOperationSet n, Object argu) {
        switch (n.f1.which) {
        case 0: // set default
            kind = KIND_SET_DEFAULT;
            n.f1.accept(aSqlCreateTableColumn, argu);
            break;
        case 1: // set not null
            kind = KIND_SET_NOT_NULL;
            aSqlCreateTableColumn.isnullable = 0;
            break;
        case 2: // set <STATISTICS_> <DECIMAL_LITERAL>

            break;
        case 3: // set <STORAGE_>

            break;

        default:
            break;
        }
    }

    /**
     * Grammar production: f0 -> <DROP_> f1 -> ( <DEFAULT_> | <NOT_> <NULL_> )
     */
    @Override
    public void visit(DropDefaultNotNull n, Object argu) {
        if (n.f1.which == 0) {
            kind = KIND_DROP_DEFAULT;
            aSqlCreateTableColumn.defaultValue = null;
        } else {
            kind = KIND_DROP_NOT_NULL;
            aSqlCreateTableColumn.isnullable = 1;
        }
    }

    /**
     * @return Returns the aSqlCreateTableColumn.
     */
    public SqlCreateTableColumn getColDef() {
        return aSqlCreateTableColumn;
    }

    /**
     * @return Returns the parent.
     */
    public SqlAlterTable getParent() {
        return parent;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return commands != null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";

        logger.entering(method, new Object[] {});
        ArrayList<String> comm = new ArrayList<String>();
        try {

            SysTable table = parent.getTable();
            table.ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);

            if (kind == KIND_CHANGE_TYPE && aSqlCreateTableColumn.isSerial()
                    && table.getSerialColumn() != null) {
                throw new XDBServerException(
                        "The Table Already Has A Serial Column");
            }

            HashMap<String, String> arguments = new HashMap<String, String>();
            arguments.put("table", IdentifierHandler.quote(table.getTableName()));
            arguments.put("column", IdentifierHandler.quote(aSqlCreateTableColumn.columnName));
            arguments.put("column_type", aSqlCreateTableColumn
                    .getTypeHandlerString());
            arguments.put("default_expr", aSqlCreateTableColumn
                    .getDefaultValue());
            arguments
                    .put("null_expr",
                            aSqlCreateTableColumn.isnullable == 1 ? "NULL"
                                    : "NOT NULL");
            if (kind == KIND_CHANGE_TYPE) {
                aSqlCreateTableColumn.rebuildString();
                String sql = Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN;

                sql = ParseCmdLine.substitute(sql, arguments);
                if (usingExp != null) {
                    String sql_using = Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_USING;
                    arguments = new HashMap<String, String>();
                    arguments.put("using_expr", usingExp);
                    sql_using = ParseCmdLine.substitute(sql_using, arguments);
                    sql = sql + " " + sql_using;
                }
                if (Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_TO_PARENT) {
                    parent.addCommonCommand(sql);
                } else {
                    comm.add(sql);
                }
            }
            if (kind == KIND_SET_DEFAULT) {
                String sql = Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_SETDEFAULT;
                sql = ParseCmdLine.substitute(sql, arguments);
                if (Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_SETDEFAULT_TO_PARENT) {
                    parent.addCommonCommand(sql);
                } else {
                    comm.add(sql);
                }

            }
            if (kind == KIND_SET_NOT_NULL) {
                String sql = Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_SETNOTNULL;
                sql = ParseCmdLine.substitute(sql, arguments);
                if (Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_SETNOTNULL_TO_PARENT) {
                    parent.addCommonCommand(sql);
                } else {
                    comm.add(sql);
                }

            }

            if (kind == KIND_DROP_DEFAULT) {
                String sql = Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_DROPDEFAULT;
                sql = ParseCmdLine.substitute(sql, arguments);
                if (Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_DROPDEFAULT_TO_PARENT) {
                    parent.addCommonCommand(sql);
                } else {
                    comm.add(sql);
                }
            }
            if (kind == KIND_DROP_NOT_NULL) {
                String sql = Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_DROPNOTNULL;
                sql = ParseCmdLine.substitute(sql, arguments);
                if (Props.XDB_SQLCOMMAND_ALTERTABLE_MODIFYCOLUMN_DROPNOTNULL_TO_PARENT) {
                    parent.addCommonCommand(sql);
                } else {
                    comm.add(sql);
                }
            }

            if (aSqlCreateTableColumn.isSerial()) {
                // TODO
                String index = "create index "
                        + IdentifierHandler.quote(SqlCreateTableColumn.IDX_SERIAL_NAME) + " on "
                        + IdentifierHandler.quote(table.getTableName()) + " ( "
                        + IdentifierHandler.quote(aSqlCreateTableColumn.columnName) + " ) ";
                comm.add(index);
            }
            commands = comm.toArray(new String[comm.size()]);
        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] {});
        try {

            engine.executeDDLOnMultipleNodes(commands, parent.getNodeList(),
                    new SyncAlterTableModifyColumn(this), client);
            return null;

        } finally {
            logger.exiting(method);
        }
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

}
