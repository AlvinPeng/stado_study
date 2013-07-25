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
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.IMetaDataUpdate;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncAlterTablespace;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.SysTablespace;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.AlterTableSpace;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 *
 */

public class SqlAlterTableSpace extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterTableSpace.class);

    private XDBSessionContext client;

    private String tablespaceName;

    private String newName;

    private SysTablespace tablespace;

    private HashMap<DBNode,String> statements = null;

    public SqlAlterTableSpace(XDBSessionContext client) {
        this.client = client;
    }

    /**
     * Grammar production:
     * f0 -> <TABLESPACE_>
     * f1 -> Identifier(prn)
     * f2 -> <RENAME_>
     * f3 -> <TO_>
     * f4 -> Identifier(prn)
     */
    @Override
    public void visit(AlterTableSpace n, Object argu) {
        IdentifierHandler ih = new IdentifierHandler();
        tablespaceName = (String) n.f1.accept(ih, argu);
        newName = (String) n.f4.accept(ih, argu);
    }

    public SysTablespace getTablespace() {
        return tablespace;
    }

    public String getNewName() {
        return newName;
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
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    public long getCost() {
        return LOW_COST;
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
        MetaData meta = MetaData.getMetaData();
        SysTablespace tablespace = meta.getTablespace(tablespaceName);
        if (tablespace == null) {
            throw new XDBServerException("Tablespace \"" + tablespaceName
                    + "\" does not exist");
        }
        if (tablespace.getOwnerID() != client.getCurrentUser().getUserID()) {
            throw new XDBServerException("Only Owner can alter tablespace");
        }
        if (meta.hasTablespace(newName)) {
            throw new XDBServerException("Tablespace \"" + tablespaceName
                    + "\" already exists");
        }
        statements = new HashMap<DBNode,String>();
        for (Integer nodeID : tablespace.getLocations().keySet()) {
            DBNode dbNode = client.getSysDatabase().getDBNode(nodeID);
            String statement = "ALTER TABLESPACE "
                + IdentifierHandler.quote(tablespaceName + "_" + nodeID)
                + " RENAME TO "
                + IdentifierHandler.quote(newName + "_" + nodeID);
            statements.put(dbNode, statement);
        }
        this.tablespace = tablespace;
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
        IMetaDataUpdate metaUpdate = new SyncAlterTablespace(this);
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
                .createSuccessResult(ExecutionResult.COMMAND_ALTER_TABLESPACE);
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
