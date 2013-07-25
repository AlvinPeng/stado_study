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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.IsolationLevel;
import org.postgresql.stado.parser.core.syntaxtree.INode;
import org.postgresql.stado.parser.core.syntaxtree.NodeChoice;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.SetProperty;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;
/**
 *
 *
 */
public class SqlSetProperty extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable {

    private static final String TRANSACTION_ISOLATION = "TRANSACTION ISOLATION LEVEL";

    private String propertyToSet = null;
    private String propertyValue = null;
    private XDBSessionContext client;
    private List<DBNode> nodeList;
    private int desiredLevel = Connection.TRANSACTION_NONE;

    /**
     *
     */
    public SqlSetProperty(XDBSessionContext client) {
        this.client = client;

        nodeList = new ArrayList<DBNode>(client.getSysDatabase()
                    .getDBNodeList());
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return nodeList;
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

    /**
     * Grammar production:
     * f0 -> <SET_>
     * f1 -> ( Identifier(prn) [ <TO_> | "=" ] ( <STRING_LITERAL> | Identifier(prn) | <ON_> | <TRUE_> | <FALSE_> | <INT_LITERAL> | <DECIMAL_LITERAL>) | <ESCAPE_> | <TRANSACTION_> <ISOLATION_LEVEL_> IsolationLevel(prn) )
     */
    @Override
    public void visit(SetProperty n, Object argu) {
        if (n.f1.which == 0) {
            NodeSequence ns = (NodeSequence) n.f1.choice;
            IdentifierHandler ih = new IdentifierHandler();
            propertyToSet = (String) ((INode) ns.nodes.get(0)).accept(ih, argu);
            NodeChoice nc = (NodeChoice) ns.nodes.get(2);
            switch (nc.which) {
                //STRING_LITERAL
                case 0:
                //ON
                case 2:
                //TRUE
                case 3:
                //FALSE
                case 4:
                //INT_LITERAL
                case 5:
                //DECIMAL_LITERAL
                case 6:
                //ESCAPE
                case 7:
                    propertyValue = nc.choice.toString();
                    break;
                //IDENTIFIER
                case 1:
                    propertyValue = (String) nc.accept(ih, argu);
                    break;
            }
        } else {
            propertyToSet = TRANSACTION_ISOLATION;
            n.f1.choice.accept(this, argu);
            nodeList = Collections.emptyList();
        }
    }

    /**
     * Grammar production:
     * f0 -> ( <SERIALIZABLE_> | <REPEATABLE_READ_> | <READ_COMMITTED_> | <READ_UNCOMMITTED_> )
     */
    @Override
    public void visit(IsolationLevel n, Object argu) {
        switch (n.f0.which) {
        case 0:
            desiredLevel = Connection.TRANSACTION_SERIALIZABLE;
            break;
        case 1:
            desiredLevel = Connection.TRANSACTION_REPEATABLE_READ;
            break;
        case 2:
            desiredLevel = Connection.TRANSACTION_READ_COMMITTED;
            break;
        case 3:
            desiredLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
            break;
        }
    }

    /**
     * Pass on the SET command to the underlying connections,
     * and persist them for this session.
     *
     * @param engine Execution engine
     * @return success code
     */
    public ExecutionResult execute(Engine engine) throws Exception {

        /** TODO: parse out some "special" settings that need
         * to be handled at the GridSQL level, instead of
         * just passing on. */
        if (propertyToSet == TRANSACTION_ISOLATION) {
            if (desiredLevel != Connection.TRANSACTION_NONE) {
                if (client.isInTransaction()) {
                    engine.commitTransaction(client, getNodeList());
                }

                client.setTransactionIsolation(desiredLevel);
            }
            return ExecutionResult.createSuccessResult(ExecutionResult.COMMAND_SET);
        }

        // JDBC requires unicode
        if (propertyToSet.equalsIgnoreCase("client_encoding")) {
            if (!Props.XDB_CLIENT_ENCODING_IGNORE && !("UNICODE".equalsIgnoreCase(propertyValue) || "'UNICODE'".equalsIgnoreCase(propertyValue) 
                        || "UTF8".equalsIgnoreCase(propertyValue) || "'UTF8'".equalsIgnoreCase(propertyValue))) {
                throw new SQLException ("Setting client_encoding is not allowed; must use UNICODE");
            }
            return ExecutionResult.createSuccessResult(ExecutionResult.COMMAND_SET);
        }

        /* We need to flag to persist these connections for our
         session so that the property can be set on all of the
         underlying connections. */
        client.setUsedSet();

        try {
            String sqlStatement = "SET " + propertyToSet + " TO ";

            // Do not double-add quotes
            if (propertyValue.startsWith("'") || propertyValue.startsWith("\"")) {
                sqlStatement += propertyValue;
            } else {
                sqlStatement += "'" + propertyValue + "'";
            }

            MultinodeExecutor aMultinodeExecutor = client
                    .getMultinodeExecutor(getNodeList());

            aMultinodeExecutor.executeCommand(sqlStatement, getNodeList(), true);

            // Execute it on the designated coordinator connection, too
            Connection oConn = client.getAndSetCoordinatorConnection();
            Statement stmt = oConn.createStatement();
            stmt.execute(sqlStatement);
        } catch (Exception e) {
            /* We just ignore errors here- there may be some issues
             * with some of these settings and there are limitations
             * being in a clustered environment. We still try though,
             * since enabling here and ignoring errors will
             * help with driver/utility compatibility.
             */
        }

        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_SET);
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
