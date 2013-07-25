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

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncCreateUser;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.CreateUser;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;

public class SqlCreateUser extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlCreateUser.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private String iUserName = null;

    private String iPassword = null;

    private int iUserClass = SysLogin.USER_CLASS_RESOURCE;

    public SqlCreateUser(XDBSessionContext client) {
        this.client = client;
        this.database = client.getSysDatabase();
    }

    // Used to create defaul DB user
    public SqlCreateUser(SysDatabase database, String userName,
            String password, int userClass) {
        this.database = database;
        iUserName = userName;
        iPassword = password;
        iUserClass = userClass;
        // Skip validation
        prepared = true;
    }

    // ******************************
    // BEGIN GRAMMAR
    // ******************************

    /**
     * Grammar production:
     * f0 -> <CREATE_>
     * f1 -> <USER_>
     * f2 -> Identifier(prn)
     * f3 -> <PASSWORD_>
     * f4 -> Identifier(prn)
     * f5 -> [ <DBA_> | <RESOURCE_> | <STANDARD_> ]
     */
    @Override
    public void visit(CreateUser n, Object argu) {
        IdentifierHandler ih = new IdentifierHandler();
        iUserName = (String) n.f2.accept(ih, argu);
        iPassword = (String) n.f4.accept(ih, argu);
        if (n.f5.present()) {
            switch (((NodeChoice) n.f5.node).which) {
            case 0:
                iUserClass = SysLogin.USER_CLASS_DBA;
                break;
            case 1:
                iUserClass = SysLogin.USER_CLASS_RESOURCE;
                break;
            case 2:
                iUserClass = SysLogin.USER_CLASS_STANDARD;
                break;
            }
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
        return new LockSpecification<SysTable>(empty, empty);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return Collections.emptyList();
    }

    /**
     * @return Returns the iPassword.
     */
    public String getPassword() {
        return iPassword;
    }

    /**
     * @return Returns the iUserClass.
     */
    public int getUserClass() {
        return iUserClass;
    }

    
    /**
     * @return Returns the iUserClass.
     */
    public String getUserClassStr() {
        return SysLogin.getUserClassStr(iUserClass);
    }

    /**
     * @return Returns the iUserName.
     */
    public String getUserName() {
        return iUserName;
    }

    private boolean prepared = false;

    public boolean isPrepared() {
        return prepared;
    }

    public void prepare() throws Exception {
        if (!prepared) {
            if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA) {
                XDBSecurityException ex = new XDBSecurityException("Only "
                        + SysLogin.USER_CLASS_DBA_STR + " can create users");
                logger.throwing(ex);
                throw ex;
            }
            if (client.getSysDatabase().hasSysUser(iUserName)) {
                XDBServerException ex = new XDBServerException("User "
                        + iUserName + " already exists");
                logger.throwing(ex);
                throw ex;
            }
        }
        prepared = true;
    }

    public ExecutionResult execute(Engine engine) throws Exception {

        final String method = "execute";
        logger.entering(method, new Object[] { engine });
        try {

            if (!isPrepared()) {
                prepare();
            }
            SyncCreateUser sync = new SyncCreateUser(this);
            MetaData meta = MetaData.getMetaData();
            meta.beginTransaction();
            try {
                sync.execute(database);
                meta.commitTransaction(sync);
            } catch (Exception e) {
                logger.catching(e);
                meta.rollbackTransaction();
                throw e;
            }
            return ExecutionResult
                    .createSuccessResult(ExecutionResult.COMMAND_CREATE_USER);

        } finally {
            logger.exiting(method);
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
