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
import java.util.Collection;
import java.util.Collections;

import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.Deallocate;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;


public class SqlDeallocate extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable {
    //    private static final XLogger logger = XLogger.getLogger(SqlDeallocate.class);

    private XDBSessionContext client;

    private String cursorName = null;

    public SqlDeallocate(XDBSessionContext client) {
        this.client = client;
    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * Grammar production:
     * f0 -> <DEALLOCATE_>
     * f1 -> [ <PREPARE_> ]
     * f2 -> Identifier(prn)
     */
    @Override
    public void visit(Deallocate n, Object argu) {
        cursorName = (String) n.f2.accept(new IdentifierHandler(), argu);
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
    public LockSpecification<SysTable> getLockSpecs() {
        Collection<SysTable> empty = Collections.emptyList();
        LockSpecification<SysTable> aLspec = new LockSpecification<SysTable>(
                empty, empty);
        return aLspec;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        Collection<DBNode> empty = Collections.emptyList();
        return new ArrayList<DBNode>(empty);
    }

    public ExecutionResult execute(Engine engine) throws Exception {
        client.closeStatement(cursorName);
        return ExecutionResult.createSuccessResult(ExecutionResult.COMMAND_DEALLOCATE);
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
		// TODO Auto-generated method stub
		return false;
	}
}
