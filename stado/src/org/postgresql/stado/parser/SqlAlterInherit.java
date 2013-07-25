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
 * SqlAlterAddPrimary.java
 *
 *
 */

package org.postgresql.stado.parser;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SyncAlterTableInherit;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.parser.core.syntaxtree.Inherit;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.TableNameHandler;

/**
 * Class for adding a PRIMARY KEY to a table
 *
 *
 */

public class SqlAlterInherit extends DepthFirstVoidArguVisitor implements IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterInherit.class);

    private XDBSessionContext client;

    private SqlAlterTable parent;

    private String tableName = null;

    private SysTable table = null;

    private boolean noInherit;

    private String[] commands;

    /**
     * @param table
     * @param client
     */
    public SqlAlterInherit(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        this.parent = parent;
    }

    /**
     * Grammar production:
     * f0 -> [ <NO_> ]
     * f1 -> <INHERIT_>
     * f2 -> TableName(prn)
     */
    @Override
    public void visit(Inherit n, Object argu) {
        noInherit = n.f0.present();
        TableNameHandler tnh = new TableNameHandler(client);
        n.f2.accept(tnh, argu);
        tableName = tnh.getTableName();
    }

    public boolean isNoInherit() {
        return noInherit;
    }

    public String getTableName() {
        return tableName;
    }

    public SysTable getTable() {
        if (table == null) {
            table = client.getSysDatabase().getSysTable(tableName);
        }
        return table;
    }

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
        try {

            commands = new String[0];
            parent.getTable().ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);
            SysTable parentTable = getTable();
            SysTable targetTable = parent.getTable();
            if (noInherit) {
                if (parentTable != targetTable.getParentTable()) {
                    throw new XDBServerException("Table "
                            + parent.getTableName()
                            + " does not inherits from " + tableName);
                }
            } else {
                if (targetTable.getParentTable() != null) {
                    throw new XDBServerException("Table "
                            + parent.getTableName()
                            + " already inherits from "
                            + targetTable.getParentTable().getTableName());
                }
                // Check partitioning
                if (!parentTable.getPartitionMap().equals(
                        targetTable.getPartitionMap())) {
                    throw new XDBServerException("Partitioning of table "
                            + parent.getTableName() + " differs from "
                            + tableName);
                }
                if (parentTable.getPartitionColumn() == null) {
                    if (targetTable.getPartitionColumn() != null) {
                        throw new XDBServerException("Partitioning of table "
                                + parent.getTableName() + " differs from "
                                + tableName);
                    }
                } else {
                    if (targetTable.getPartitionColumn() == null
                            || !targetTable.getPartitionColumn().equals(
                                    parentTable.getPartitionColumn())) {
                        throw new XDBServerException("Partitioning of table "
                                + parent.getTableName() + " differs from "
                                + tableName);
                    }
                }
                // Check columns
                if (parentTable.getColumns().size() > targetTable.getColumns().size()) {
                    throw new XDBServerException("Columns of table "
                            + parent.getTableName()
                            + " are not compatible with "
                            + tableName);
                }
                for (int i = 0; i < parentTable.getColumns().size(); i++) {
                    SysColumn parentColumn = parentTable.getColumns().get(i);
                    SysColumn targetColumn = targetTable.getColumns().get(i);
                    if (!parentColumn.getColName().equals(
                            targetColumn.getColName())) {
                        throw new XDBServerException(
                                "Columns of table "
                                        + parent.getTableName()
                                        + " are not compatible with "
                                        + tableName);
                    }
                    if (parentColumn.getColType() != targetColumn.getColType()) {
                        throw new XDBServerException(
                                "Columns of table "
                                        + parent.getTableName()
                                        + " are not compatible with "
                                        + tableName);
                    }
                    if (parentColumn.getColLength() != targetColumn.getColLength()) {
                        throw new XDBServerException(
                                "Columns of table "
                                        + parent.getTableName()
                                        + " are not compatible with "
                                        + tableName);
                    }
                    if (parentColumn.getColScale() != targetColumn.getColScale()) {
                        throw new XDBServerException(
                                "Columns of table "
                                        + parent.getTableName()
                                        + " are not compatible with "
                                        + tableName);
                    }
                    if (parentColumn.getColPrecision() != targetColumn.getColPrecision()) {
                        throw new XDBServerException(
                                "Columns of table "
                                        + parent.getTableName()
                                        + " are not compatible with "
                                        + tableName);
                    }
                }
            }
            String sql = "ALTER TABLE "
                    + IdentifierHandler.quote(parent.getTableName())
                    + (noInherit ? " NO" : "") + " INHERIT "
                    + IdentifierHandler.quote(tableName);
            commands = new String[] { sql };

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
            if (commands != null && commands.length != 0) {
                engine.executeDDLOnMultipleNodes(commands,
                        parent.getNodeList(), new SyncAlterTableInherit(this),
                        client);
            }
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
