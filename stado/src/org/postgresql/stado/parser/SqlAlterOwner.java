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

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncAlterOwner;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysUser;
import org.postgresql.stado.parser.core.syntaxtree.OwnerDef;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;

/**
 */
public class SqlAlterOwner extends DepthFirstVoidArguVisitor implements IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterOwner.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private SqlAlterTable parent;

    private SysUser user = null;

    /**
     * @param table
     * @param client
     */
    public SqlAlterOwner(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
        this.parent = parent;
    }

    /**
     * Grammar production:
     * f0 -> <OWNER_TO_>
     * f1 -> ( <PUBLIC_> | Identifier(prn) )
     */
    @Override
    public void visit(OwnerDef n, Object argu) {
        if (n.f1.which == 1) {
            user = database.getSysUser((String) n.f1.accept(new IdentifierHandler(), argu));
        }
    }

    public SqlAlterTable getParent() {
        return parent;
    }

    /**
     * @return Returns the columnName.
     */
    public SysUser getUser() {
        return user;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    private boolean prepared = false;

    public boolean isPrepared() {
        return prepared;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method, new Object[] {});
        try {

            if (!isPrepared()) {
                if (getParent().getTable().getParentTable() != null) {
                    throw new XDBServerException(
                            "Owner of child table can not be changed");
                }
                if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA
                        && client.getCurrentUser() != parent.getTable()
                                .getOwner()) {
                    XDBSecurityException ex = new XDBSecurityException("Only "
                            + SysLogin.USER_CLASS_DBA_STR
                            + " or owner can change table ownership");
                    logger.throwing(ex);
                    throw ex;
                }
                if (user != null
                        && user.getUserClass() == SysLogin.USER_CLASS_STANDARD) {
                    XDBSecurityException ex = new XDBSecurityException(
                            SysLogin.USER_CLASS_STANDARD_STR
                                    + " user can not own table");
                    logger.throwing(ex);
                    throw ex;
                }
                prepared = true;
            }

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        final String method = "execute";
        logger.entering(method, new Object[] {});
        try {

            if (!isPrepared()) {
                prepare();
            }
            SyncAlterOwner sync = new SyncAlterOwner(this);
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
            return null;

        } finally {
            logger.exiting(method);
        }
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
