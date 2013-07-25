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
import org.postgresql.stado.engine.ExecutableRequest;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.parser.core.syntaxtree.Deallocate;
import org.postgresql.stado.parser.core.syntaxtree.DeclareCursor;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
import org.postgresql.stado.parser.handler.QueryTreeHandler;
import org.postgresql.stado.parser.handler.QueryTreeTracker;

public class SqlDeclareCursor extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable {

    private XDBSessionContext client;

    private String cursorName = null;

    public QueryTree aQueryTree = null;

    private Command commandToExecute;

    public SqlDeclareCursor(XDBSessionContext client) {
        this.client = client;

        commandToExecute = new Command(Command.SELECT, this,
                new QueryTreeTracker(), client);

        aQueryTree = new QueryTree();

    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * Grammar production:
     * f0 -> <DECLARE_>
     * f1 -> Identifier(prn)
     * f2 -> <CURSOR_>
     * f3 -> <FOR_>
     * f4 -> Select(prn)
     */
    @Override
    public void visit(DeclareCursor n, Object argu) {
        cursorName = (String) n.f1.accept(new IdentifierHandler(), argu);
        
        QueryTreeHandler aQueryTreeHandler = new QueryTreeHandler(
                commandToExecute);
        n.f4.accept(aQueryTreeHandler, aQueryTree);
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
        if (!client.isInTransaction()) {
            throw new XDBServerException(
                    ErrorMessageRepository.DECLARE_CURSOR_TRANS,
                    0,
                    ErrorMessageRepository.DECLARE_CURSOR_TRANS_CODE);        	
        }
        
        client.createCursor(cursorName, aQueryTree, false, false);
    	
        return ExecutionResult.createSuccessResult(ExecutionResult.COMMAND_DECLARE_CURSOR);
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
