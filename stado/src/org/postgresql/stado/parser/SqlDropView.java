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

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncDropView;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.DropView;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.TableNameHandler;

public class SqlDropView extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger.getLogger(SqlDropView.class);

    private XDBSessionContext client;

    private String iViewName = null;

    private boolean prepare = false;

    public SqlDropView(XDBSessionContext client) {
        this.client = client;
    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * Grammar production: f0 -> <DROP_> f1 -> <VIEW_> f2 -> TableName(prn)
     */
    @Override
    public void visit(DropView n, Object argu) {
        TableNameHandler aTableNameHandler = new TableNameHandler(client);
        n.f2.accept(aTableNameHandler, argu);
        iViewName = aTableNameHandler.getTableName();
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
        return new ArrayList(Collections.EMPTY_LIST);
    }

    /**
     * @return Returns the iUserName.
     */
    public String getViewrName() {
        return iViewName;
    }

    public boolean isPrepared() {
        return prepare;
    }

    public void prepare() throws Exception {
        if (!isPrepared()) {
            if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA) {
                XDBSecurityException ex = new XDBSecurityException("Only "
                        + SysLogin.USER_CLASS_DBA_STR + " can drop views");
                logger.throwing(ex);
                throw ex;
            }
            prepare = true;
        }
    }

    public ExecutionResult execute(Engine engine) throws Exception {
        if (!isPrepared()) {
            prepare();
        }
        SyncDropView sync = new SyncDropView(this);
        MetaData meta = MetaData.getMetaData();
        meta.beginTransaction();
        try {
            sync.execute(client);
            meta.commitTransaction(sync);
        } catch (Exception e) {
            logger.catching(e);
            meta.rollbackTransaction();
            throw e;
        }
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_DROP_VIEW);
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
