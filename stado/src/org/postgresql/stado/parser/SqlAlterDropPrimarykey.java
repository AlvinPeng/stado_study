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
 * SqlAlterDropPrimarykey.java
 *
 *
 */
package org.postgresql.stado.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SyncAlterTableDropPrimaryKey;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysIndex;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 * Class SqlAlterDropPrimarykey Removes a Primary key Defined on a table.
 *
 *
 */

public class SqlAlterDropPrimarykey extends DepthFirstVoidArguVisitor implements
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterDropPrimarykey.class);

    private XDBSessionContext client;

    private SqlAlterTable parent;

    private String[] commands;

    /**
     * Constructor
     */
    public SqlAlterDropPrimarykey(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        this.parent = parent;
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
        Vector<String> comm = new Vector<String>();
        String sql;

        final String method = "prepare";
        logger.entering(method, new Object[] {});
        try {

            // table has a primary key defined ?
            SysTable theTable = parent.getTable();
            theTable.ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);
            List<SysColumn> primaryKey = theTable.getPrimaryKey();
            if (primaryKey == null || primaryKey.size() == 0) {
                throw new XDBServerException(
                        ErrorMessageRepository.NO_PRIMARY_UNQIUE_INDEX + " ( "
                                + theTable.getTableName() + " )", 0,
                        ErrorMessageRepository.NO_PRIMARY_UNQIUE_INDEX_CODE);
            }

            // check primary key index for any references defined on it
            SysIndex primaryIndex = theTable.getPrimaryIndex();
            if (primaryIndex == null) {
                // FATAL error !!
                throw new XDBServerException(
                        ErrorMessageRepository.NO_PRIMARY_UNQIUE_INDEX + " ( "
                                + theTable.getTableName() + " )", 0,
                        ErrorMessageRepository.NO_PRIMARY_UNQIUE_INDEX_CODE);
            }
            if (theTable.getPrimaryConstraint().getSysTable() != theTable) {
                throw new XDBServerException(
                        "Can not drop inherited primary key");
            }
            if (theTable.isIndexReferenced(primaryIndex)) {
                throw new XDBServerException(
                        ErrorMessageRepository.PRIMARYKEY_REFRENCED, 0,
                        ErrorMessageRepository.PRIMARYKEY_REFRENCED_CODE);
            }
            HashMap<String,String> arguments = new HashMap<String,String>();
            arguments.put("table", IdentifierHandler.quote(theTable.getTableName()));
            arguments.put("constr_name", IdentifierHandler.quote(primaryIndex.idxname));

            sql = ParseCmdLine.substitute(
                    Props.XDB_SQLCOMMAND_ALTERTABLE_DROPPRIMAY, arguments);
            if (Props.XDB_SQLCOMMAND_ALTERTABLE_DROPPRIMAY_TO_PARENT) {
                parent.addCommonCommand(sql);
            } else {
                comm.add(sql);
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
                    new SyncAlterTableDropPrimaryKey(this), client);
            return null;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @return
     */
    public SqlAlterTable getParent() {
        return parent;
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
