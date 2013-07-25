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

import java.util.HashMap;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SyncAlterTableRenameColumn;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.parser.core.syntaxtree.RenameDef;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;

/**
 * For adding a new Column
 */
public class SqlAlterRenameColumn extends DepthFirstVoidArguVisitor implements
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterRenameColumn.class);

    private XDBSessionContext client;

    private SqlAlterTable parent;

    private SysColumn column;

    private String oldName;

    private String newName;

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
    public SqlAlterRenameColumn(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        this.parent = parent;
    }

    /**
     * Grammar production:
     * f0 -> <RENAME_>
     * f1 -> [ <COLUMN_> ]
     * f2 -> Identifier(prn)
     * f3 -> <TO_>
     * f4 -> Identifier(prn)
     */
    @Override
    public void visit(RenameDef n, Object argu) {
        IdentifierHandler ih = new IdentifierHandler();
        oldName = (String) n.f2.accept(ih, argu);
        newName = (String) n.f4.accept(ih, argu);
    }

    public String getNewName() {
        return newName;
    }

    public SysColumn getSysColumn() {
        return column;
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
        try {

            SysTable table = parent.getTable();
            table.ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);
            column = table.getSysColumn(oldName);
            if (column == null) {
                throw new XDBServerException(
                        "The Table Has Not A Column Named " + oldName);
            }
            if (table != column.getSysTable()) {
                throw new XDBServerException("Can not modify inherited column");
            }

            if (table.getSysColumn(newName) != null) {
                throw new XDBServerException(
                        "The Table Already Has A Column Named " + newName);
            }

            String sql = Props.XDB_SQLCOMMAND_ALTERTABLE_RENAMECOLUMN;

            HashMap<String, String> arguments = new HashMap<String, String>();
            arguments.put("table", IdentifierHandler.quote(table.getTableName()));
            arguments.put("old_colname", IdentifierHandler.quote(oldName));
            arguments.put("new_colname", IdentifierHandler.quote(newName));
            sql = ParseCmdLine.substitute(sql, arguments);
            if (Props.XDB_SQLCOMMAND_ALTERTABLE_RENAMECOLUMN_TO_PARENT) {
                parent.addCommonCommand(sql);
                commands = new String[0];
            } else {
                commands = new String[] { sql };
            }
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
                    new SyncAlterTableRenameColumn(this), client);
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
