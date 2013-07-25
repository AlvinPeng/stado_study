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
import java.util.Map;

import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutableRequest;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysUser;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.Kill;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

public class SqlKill extends DepthFirstVoidArguVisitor implements IXDBSql, IExecutable,
        IPreparable {
    private XDBSessionContext client;

    private int requestID;

    private XDBSessionContext targetSession;

    private ExecutableRequest targetRequest;

    /**
     * 
     */
    public SqlKill(XDBSessionContext client) {
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

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado..Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        if (targetSession != null) {
            if (targetRequest == null) {
                targetSession.kill();
            } else {
                targetSession.kill(targetRequest);
            }
        }
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_KILL);
    }

    private boolean prepared;

    public boolean isPrepared() {
        return prepared;
    }

    public void prepare() throws Exception {
        prepared = true;
        SysUser user = client.getCurrentUser();
        XDBSessionContext targetSession = null;
        if (requestID > 0) {
            Map<ExecutableRequest, XDBSessionContext> execRequests = client
                    .getRequests();
            for (Map.Entry<ExecutableRequest, XDBSessionContext> entry : execRequests
                    .entrySet()) {
                if (entry.getKey().getRequestID() == requestID) {
                    targetSession = entry.getValue();
                    targetRequest = entry.getKey();
                    break;
                }
            }
        }
        if (targetSession != null) {
            if (user.getUserClass() == SysLogin.USER_CLASS_DBA
                    || targetSession.getCurrentUser() == user) {
                this.targetSession = targetSession;
            } else {
                throw new XDBSecurityException("Permission denied");
            }
        }
    }

    /**
     * Grammar production: f0 -> <KILL_> f1 -> <INT_LITERAL>
     */
    @Override
    public void visit(Kill n, Object argu) {
        try {
            requestID = Integer.parseInt(n.f1.tokenImage);
        } catch (NumberFormatException nfe) {
            requestID = -1;
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
		// TODO Auto-generated method stub
		return false;
	}
}
