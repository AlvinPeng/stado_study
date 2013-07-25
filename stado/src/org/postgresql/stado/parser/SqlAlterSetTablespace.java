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

import java.util.HashMap;
import java.util.Map;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SyncAlterTableSetTablespace;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTablespace;
import org.postgresql.stado.parser.core.syntaxtree.SetTablespace;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;


/**
 *
 *
 */
public class SqlAlterSetTablespace extends DepthFirstVoidArguVisitor implements
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlAlterSetTablespace.class);

    private XDBSessionContext client;

    private SqlAlterTable parent;

    private String tablespaceName = null;

    private SysTablespace tablespace = null;

    private Map<DBNode,String> commands = null;

    /**
     *
     */
    public SqlAlterSetTablespace(SqlAlterTable parent, XDBSessionContext client) {
        this.client = client;
        this.parent = parent;
    }

    /**
     * Grammar production:
     * f0 -> <SET_>
     * f1 -> <TABLESPACE_>
     * f2 -> Identifier(prn)
     */
    @Override
    public void visit(SetTablespace n, Object argu) {
        tablespaceName = (String) n.f2.accept(new IdentifierHandler(), argu);
    }

    public SqlAlterTable getParent() {
        return parent;
    }

    public SysTablespace getTablespace() {
        return tablespace;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return tablespace != null;
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
                if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA) {
                    XDBSecurityException ex = new XDBSecurityException("Only "
                            + SysLogin.USER_CLASS_DBA_STR
                            + " can change table workspace");
                    logger.throwing(ex);
                    throw ex;
                }
                SysTablespace newTablespace = MetaData.getMetaData()
                        .getTablespace(tablespaceName);
                commands = new HashMap<DBNode,String>();
                for (DBNode dbNode : parent.getTable().getNodeList()) {
                    if (!newTablespace.getLocations().containsKey(dbNode.getNodeId())) {
                        throw new XDBServerException("Tablespace "
                                + IdentifierHandler.quote(tablespaceName)
                                + " does not exist on Node "
                                + dbNode.getNodeId());
                    }
                    String command = "ALTER TABLE "
                        + IdentifierHandler.quote(parent.getTableName())
                        + " SET TABLESPACE "
                        + IdentifierHandler.quote(tablespaceName + "_" + dbNode.getNodeId());
                    commands.put(dbNode, command);
                }
                tablespace = newTablespace;
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

            engine.executeDDLOnMultipleNodes(commands,
                    new SyncAlterTableSetTablespace(this), client);
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
