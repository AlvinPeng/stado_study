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
/*
 * SqlStartDatabase.java
 *
 *
 */

package org.postgresql.stado.parser;

import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.io.MessageTypes;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.INode;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.StartDatabase;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;

/**
 *
 *
 */
public class SqlStartDatabase extends DepthFirstVoidArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlStartDatabase.class);

    private XDBSessionContext client;

    private boolean prepared = false;

    //private String dbName;

    // Default one minute
    private int timeout = 60;

    private String[] dbListStr;

    /** Creates a new instance of SqlStartDatabase */
    public SqlStartDatabase(XDBSessionContext client) {
        this.client = client;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return prepared;
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
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return false;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IPreparable#prepare()
     */
    public void prepare() throws Exception {
        final String method = "prepare";
        logger.entering(method);
        try {

            if (!prepared) {
                if (client.getCurrentUser().getLogin().getUserClass() != SysLogin.USER_CLASS_DBA) {
                    throw new XDBSecurityException(
                            "You are not allowed to start database");
                }
            }
            prepared = true;

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
        logger.entering(method, new Object[] { engine });
        try {

            if (!isPrepared()) {
                prepare();
            }
            XDBSessionContext newClient = XDBSessionContext.createSession();
            try {
                for (String element : dbListStr) {
                    newClient.useDB(element, MessageTypes.CONNECTION_MODE_ADMIN);
                    newClient.login(client.getCurrentUser().getLogin());
                    newClient.startDatabase(timeout);
                }

            } finally {
                newClient.logout();
            }

        }
        finally {
            logger.exiting(method);
        }
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_START_DATABASE);
    }

    /**
     * Grammar production:
     * f0 -> <START_DB_>
     * f1 -> Identifier(prn)
     * f2 -> ( "," Identifier(prn) )*
     * f3 -> [ <WAIT_TIMEOUT_> <INT_LITERAL> ]
     */
    @Override
    public void visit(StartDatabase n, Object argu) {
        dbListStr = new String[n.f2.size() + 1];
        @SuppressWarnings("unchecked")
		String firstNode = (String) n.f1.accept(new IdentifierHandler(), argu);
        int i = 0;
        dbListStr[i++] = firstNode;
        Iterator<INode> dbEnum = n.f2.elements();
        while (dbEnum.hasNext()) {
            Object nextDb = dbEnum.next();
            INode actualDb = (INode) ((NodeSequence) nextDb).nodes.get(1);
            dbListStr[i++] = (String) actualDb.accept(new IdentifierHandler(), argu);;
        }
        //timeout = n.f3.present();
        if (n.f3.present()) {
            NodeSequence seq = (NodeSequence) n.f3.node;
            try {
                timeout = Integer.parseInt(seq.nodes.get(1).toString());
            } catch (NumberFormatException nfe) {
                // ignore
            }
        }
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
