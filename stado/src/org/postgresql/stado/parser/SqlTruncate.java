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
/**
 *
 */
package org.postgresql.stado.parser;

import java.util.Collection;
import java.util.Collections;

import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysPermission;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.Truncate;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.TableNameHandler;

public class SqlTruncate extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable, IPreparable {
    private XDBSessionContext client;

    private String aTableName;

    private String truncateStatement;

    private SysTable table;

    /**
     *
     */
    public SqlTruncate(XDBSessionContext client) {
        this.client = client;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return table.getNodeList();
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
        return new LockSpecification<SysTable>(empty, Collections.singletonList(table));
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        if (!isPrepared()) {
            prepare();
        }
        MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(table.getNodeList());
        aMultinodeExecutor.executeCommand(truncateStatement, table.getNodeList(), true);
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_TRUNCATE);
    }

    public boolean isPrepared() {
        return truncateStatement != null;
    }

    public void prepare() throws Exception {
        table = client.getSysDatabase().getSysTable(aTableName);
        table.ensurePermission(client.getCurrentUser(), SysPermission.PRIVILEGE_DELETE);
        // TRUNCATE cannot be used if there are foreign-key references
        // to the table from other tables. Checking validity in such
        // cases would require table scans, and the whole point is not
        // to do one.
        if (!table.getSysReferences().isEmpty()) {
            throw new XDBServerException(
                    "cannot truncate a table referenced in a foreign key constraint");
        }
        truncateStatement = "TRUNCATE TABLE " + IdentifierHandler.quote(table.getTableName());
    }

    /**
     * Grammar production:
     * f0 -> <TRUNCATE_>
     * f1 -> [ <TABLE_> ]
     * f2 -> TableName(prn)
     */
    @Override
    public void visit(Truncate n, Object argu) {
        TableNameHandler tnh = new TableNameHandler(client);
        n.f2.accept(tnh, argu);
        aTableName = tnh.getTableName();
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
