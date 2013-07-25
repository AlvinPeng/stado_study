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

import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncAlterDropColumn;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysView;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 * Class for dropping a column
 */
public class SqlAlterDropColumn extends DepthFirstVoidArguVisitor implements IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterAddColumn.class);

    private XDBSessionContext client;

    private SqlAlterTable parent;

    private String columnName;

    private String[] commands = null;

    /**
     * @param table
     * @param client
     */
    public SqlAlterDropColumn(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        this.parent = parent;
    }

    /**
     * f0 -> <COLUMN_> f1 -> IdentifierAndUnreservedWords
     */
    @Override
    public void visit(NodeSequence n, Object argu) {
        columnName = (String) n.elementAt(1).accept(new IdentifierHandler(), argu);
    }

    public SqlAlterTable getParent() {
        return parent;
    }

    /**
     * @return Returns the columnName.
     */
    public String getColumnName() {
        return columnName;
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
        LinkedList<String> comm = new LinkedList<String>();
        String sql;

        logger.entering(method, new Object[] {});
        try {

            SysTable table = parent.getTable();
            table.ensurePermission(client.getCurrentUser(),
                    SysPermission.PRIVILEGE_ALTER);
            // also make sure that this table contains
            // the col we are trying to drop
            SysColumn sysCol = table.getSysColumn(columnName);
            if (sysCol == null) {
                throw new XDBServerException(
                        ErrorMessageRepository.COLUMN_NOT_IN_TABLE + " ( "
                                + columnName + " , " + table.getTableName()
                                + " ) ", 0,
                        ErrorMessageRepository.COLUMN_NOT_IN_TABLE_CODE);
            }
            //
            Enumeration eViews = parent.getDatabase().getAllViews();
            while (eViews.hasMoreElements()) {
                SysView view = (SysView) eViews.nextElement();
                if (view.hasDependedColumn(sysCol.getColID())) {
                    XDBSecurityException ex = new XDBSecurityException(
                            "cannot drop table " + table.getTableName()
                                    + " column " + sysCol.getColName()
                                    + " because other objects depend on it");
                    throw ex;
                }
            }

            // Make sure this is not a partitoning column
            if (table.getPartitionedColumn() == sysCol) {
                throw new XDBServerException(
                        ErrorMessageRepository.PARTITIONING_COLUMN_OF_TABLE
                                + " ( " + columnName + " , "
                                + table.getTableName() + " ) ",
                        0,
                        ErrorMessageRepository.PARTITIONING_COLUMN_OF_TABLE_CODE);
            }
            // Make sure this is not a primary key for this table
            if (sysCol.getIndexType() == MetaData.INDEX_TYPE_PRIMARY_KEY) {
                throw new XDBServerException(
                        ErrorMessageRepository.PRIMARYKEY_COLUMN_OF_TABLE
                                + " ( " + columnName + " , "
                                + table.getTableName() + " ) ", 0,
                        ErrorMessageRepository.PRIMARYKEY_COLUMN_OF_TABLE_CODE);
            }
            // Make sure there are NO foreign references to this column
            Enumeration e = sysCol.getChildColumns();
            if (e.hasMoreElements()) {
                throw new XDBServerException(
                        ErrorMessageRepository.COLUMN_REFFERENCES_EXIST + " ( "
                                + columnName + " , " + table.getTableName()
                                + " ) ", 0,
                        ErrorMessageRepository.COLUMN_REFFERENCES_EXIST_CODE);
            }

            if (table != sysCol.getSysTable()) {
                throw new XDBServerException("Can not drop inherited column");
            }

            HashMap<String,String> arguments = new HashMap<String,String>();
            arguments.put("table", IdentifierHandler.quote(table.getTableName()));
            arguments.put("column", IdentifierHandler.quote(columnName));
            sql = ParseCmdLine.substitute(
                    Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCOLUMN, arguments);

            if (Props.XDB_SQLCOMMAND_ALTERTABLE_DROPCOLUMN_TO_PARENT) {
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
                    new SyncAlterDropColumn(this), client);
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
