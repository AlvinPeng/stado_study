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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.IMetaDataUpdate;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncDropTablespace;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysTablespace;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.DropTablespace;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;


public class SqlDropTableSpace extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlDropTableSpace.class);

    private XDBSessionContext client;

    private String tablespaceName;

    private SysTablespace tablespace;

    private HashMap<DBNode,String> statements = null;

    public SqlDropTableSpace(XDBSessionContext client) {
        this.client = client;
    }

    /**
     * Grammar production:
     * f0 -> <DROP_>
     * f1 -> <TABLESPACE_>
     * f2 -> Identifier(prn)
     */
    @Override
    public void visit(DropTablespace n, Object argu) {
        tablespaceName = (String) n.f2.accept(new IdentifierHandler(), argu);
    }

    public SysTablespace getTablespace() {
        return tablespace;
    }

    public long getCost() {
        return LOW_COST;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        if (!isPrepared()) {
            try {
                prepare();
            } catch (Exception ignore) {
                logger.catching(ignore);
            }
        }
        return statements.keySet();
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
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return statements != null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        tablespace = MetaData.getMetaData().getTablespace(tablespaceName);
        if (tablespace == null) {
            throw new XDBServerException("Tablespace \"" + tablespaceName
                    + "\" does not exist");
        }
        if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA) {
            throw new XDBSecurityException("Only DBA user can drop tablespaces");
        }
        // Check references on the tablespace or rely on Metadata DB constraints
        statements = new HashMap<DBNode,String>();
        for (Integer nodeID : tablespace.getLocations().keySet()) {
            DBNode dbNode = client.getSysDatabase().getDBNode(nodeID);
            String statement = "DROP TABLESPACE "
                + IdentifierHandler.quote(tablespaceName + "_" + nodeID);
            statements.put(dbNode, statement);
        }
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
        IMetaDataUpdate metaUpdate = new SyncDropTablespace(this);
        MetaData meta = MetaData.getMetaData();
        meta.beginTransaction();
        try {
            metaUpdate.execute(client);
            MultinodeExecutor executor = client
                    .getMultinodeExecutor(getNodeList());
            executor.executeCommand(statements, true);
            meta.commitTransaction(metaUpdate);
        } catch (Exception ex) {
            logger.catching(ex);
            meta.rollbackTransaction();
            logger.throwing(ex);
            throw ex;
        }
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_DROP_TABLESPACE);
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
