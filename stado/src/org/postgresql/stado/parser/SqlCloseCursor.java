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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.DeclaredCursor;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.CloseCursor;
import org.postgresql.stado.parser.core.syntaxtree.Deallocate;
import org.postgresql.stado.parser.core.syntaxtree.INode;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;

public class SqlCloseCursor extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable {

    private static final XLogger logger = XLogger.getLogger(SqlCloseCursor.class);

    private XDBSessionContext client;
    
    private DeclaredCursor cursor = null;
    
    private boolean allCursors = false;

    public SqlCloseCursor(XDBSessionContext client) {
        this.client = client;
    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * Grammar production:
     * f0 -> <CLOSE_>
     * f1 -> ( Identifier(prn) | <ALL_> )
     */
    @Override
    public void visit(CloseCursor n, Object argu) {
        if (n.f1.which == 1)
        	allCursors = true;
        else { 
            String cursorName = (String) n.f1.accept(new IdentifierHandler(), argu);
    		cursor = client.getCursor(cursorName);
        }
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
    	if (allCursors) {
    		client.clearAllCursors();
    	} else {
        	try {
        		if (cursor.isMaterialized()) {
        			cursor.getOpenStatement().execute("CLOSE " + cursor.getName());
        			cursor.getOpenStatement().close();
        			cursor.getOpenStatement().getConnection().close();
        			
        			dropMaterializedTable(cursor.getMaterializedTable(), engine);
        		}
            } catch (SQLException se) {
                logger.catching(se);
                cursor.getOpenStatement().getConnection().rollback();
                return ExecutionResult.createErrorResult(se);
            }
    	}
        return ExecutionResult.createSuccessResult(ExecutionResult.COMMAND_CLOSE_CURSOR);
    }

    private void dropMaterializedTable(SysTable materializedTable, Engine engine) {
        engine.executeOnMultipleNodes("DROP TABLE "
                + IdentifierHandler.quote(materializedTable.getTableName()),
                materializedTable.getNodeList(), client);
        // If drop temp table is failed metadata will have lost SysTable in
        // memory
        // Maybe we should do additional cleanup after all ?
        client.getSysDatabase().dropSysTable(materializedTable.getTableName());
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
