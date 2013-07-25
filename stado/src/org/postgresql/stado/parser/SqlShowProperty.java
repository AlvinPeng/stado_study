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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.ShowProperty;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

/**
 * 
 * 
 */
public class SqlShowProperty extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable {
    
    private String propertyToShow = null;
    private XDBSessionContext client;
    /**
     * 
     */
    public SqlShowProperty(XDBSessionContext client) {
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
        Collection<SysTable> empty = Collections.emptySet();
        return new LockSpecification<SysTable>(empty, empty);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    
    @Override
    public void visit(ShowProperty n, Object argu) {
        propertyToShow = n.f1.choice.toString();
    }
    
    public ExecutionResult execute(Engine engine) throws Exception {
        
        String query = "SHOW " + propertyToShow;
        
        try
        {
            Connection oConn = client.getAndSetCoordinatorConnection();
            try {
                Statement stmt = oConn.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                return ExecutionResult.createResultSetResult(ExecutionResult.COMMAND_SHOW, rs);
            } catch (SQLException se) {
                try { 
                    oConn.rollback();
                } catch (SQLException ignore) {
                }
                throw se;
            }
        }
        catch (SQLException se)
        {            
            return ExecutionResult.createErrorResult(se);
        }        
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

