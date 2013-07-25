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

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.ResultSetImpl;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.datatypes.VarcharType;
import org.postgresql.stado.engine.datatypes.XData;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysIndex;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.parser.core.syntaxtree.ShowIndexes;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.QueryTreeTracker;
import org.postgresql.stado.parser.handler.TableNameHandler;

/**
 * This class implements the functionalty for update statistics depending on the
 * query executed
 */
public class SqlShowIndexes extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable {

    private XDBSessionContext client;

    public QueryTree aQueryTree = null;

    Command commandToExecute;

    public String TableName;

    public SqlShowIndexes(XDBSessionContext client) {
        this.client = client;
        // I will add a new command object to the QueryTree handler
        // and then set that particular object with the command object
        commandToExecute = new Command(Command.SELECT, this,
                new QueryTreeTracker(), client);

        // Secondly I will create a Query Tree Tracker object and will also
        // set it here in all other location below this when ever a QueryTree
        // Handler is created the query tree tracker will always set it.
        aQueryTree = new QueryTree();
    }

    @Override
    public void visit(ShowIndexes n, Object obj) {
        n.f0.accept(this, obj);
        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f2.accept(aTableNameHandler, obj);
        TableName = aTableNameHandler.getTableName();
    }

    /**
     * This will return the cost of executing this statement in time , milli
     * seconds
     */

    public long getCost() {
        return ILockCost.LOW_COST;
    }

    /**
     * This return the Lock Specification for the system
     *
     * @param theMData
     * @return
     */
    public LockSpecification getLockSpecs() {
        LockSpecification aLspec = new LockSpecification(
                Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        return aLspec;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection getNodeList() {
        return Collections.EMPTY_LIST;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        short flags = 0;

        ColumnMetaData[] headers = new ColumnMetaData[] {

                new ColumnMetaData("indexname", "indexname", 250,
                        Types.VARCHAR, 0, 0, TableName, flags, false),
                new ColumnMetaData("isunique", "isunique", 10, Types.VARCHAR,
                        0, 0, TableName, flags, false),
                new ColumnMetaData("columns", "columns", 1024, Types.VARCHAR,
                        0, 0, TableName, flags, false),

        };
        Vector rows = new Vector();

        SysDatabase db = MetaData.getMetaData().getSysDatabase(
                client.getDBName());

        SysTable theSysTable = db.getSysTable(TableName);
        theSysTable.ensurePermission(client.getCurrentUser(),
                SysPermission.PRIVILEGE_SELECT);
        Collection<SysIndex> theConstraintList = theSysTable.getSysIndexList();
        String aConstrName = "null";
        String aType = "";
        StringBuffer aSourceColumns = new StringBuffer();

        for (Object element : theConstraintList) {
            aConstrName = "null";
            aType = "";
            aSourceColumns = new StringBuffer();

            SysIndex theIndx = (SysIndex) element;
            if (theIndx.idxname.toUpperCase().startsWith(
                    SysIndex.ROWID_INDEXNAME)) {
                continue;
            }
            aConstrName = theIndx.idxname;
            aType = "" + theIndx.idxtype;
            if (aType.equals("U") || aType.equals("P")) {
                aType = "YES";
            } else {
                aType = "NO";
            }
            List<SysColumn> theSysColumns = theIndx.getKeyColumns();
            for (SysColumn el : theSysColumns) {
                String theCol = el.getColName();
                aSourceColumns.append(theCol).append(", ");

            }
            if (aSourceColumns != null && aSourceColumns.length() > 2
                    && !aSourceColumns.toString().equals("null")) {
                aSourceColumns.delete(aSourceColumns.length() - 2,
                        aSourceColumns.length());
            }
            rows.add(new XData[] { new VarcharType(aConstrName),
                    new VarcharType(aType),
                    new VarcharType(aSourceColumns.toString()) });
        }
        return ExecutionResult.createResultSetResult(
                ExecutionResult.COMMAND_SHOW, new ResultSetImpl(headers, rows));
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return false;
    }
    
	@Override
	public boolean isReadOnly() {
		return true;
	}

}
