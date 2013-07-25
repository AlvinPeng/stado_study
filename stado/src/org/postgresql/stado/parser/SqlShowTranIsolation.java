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

import java.sql.Connection;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.Vector;

import org.postgresql.stado.common.ColumnMetaData;
import org.postgresql.stado.common.ResultSetImpl;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.datatypes.VarcharType;
import org.postgresql.stado.engine.datatypes.XData;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * 
 * 
 */
public class SqlShowTranIsolation extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable {

    private XDBSessionContext client;

    /**
     * 
     */
    public SqlShowTranIsolation(XDBSessionContext client) {
        this.client = client;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return Collections.emptyList();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    public long getCost() {
        return ILockCost.LOW_COST;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getLockSpecs()
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Collection<SysTable> empty = Collections.emptyList();
        return new LockSpecification<SysTable>(empty, empty);
    }

    public ExecutionResult execute(Engine engine) throws Exception {
        String tranIsolation;
        switch (client.getTransactionIsolation()) {
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            tranIsolation = "read uncommitted";
            break;
        case Connection.TRANSACTION_READ_COMMITTED:
            tranIsolation = "read committed";
            break;
        case Connection.TRANSACTION_REPEATABLE_READ:
            tranIsolation = "repeatable read";
            break;
        case Connection.TRANSACTION_SERIALIZABLE:
            tranIsolation = "serializable";
            break;
        default:
            tranIsolation = "unknown";
        }
        ColumnMetaData[] headers = new ColumnMetaData[] {

        new ColumnMetaData("TRANSACTION_ISOLATION", "TRANSACTION_ISOLATION",
                25, Types.VARCHAR, 0, 0, "", (short) 0, false)

        };
        Vector<XData[]> rows = new Vector<XData[]>();
        rows.add(new XData[] { new VarcharType(tranIsolation) });
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
