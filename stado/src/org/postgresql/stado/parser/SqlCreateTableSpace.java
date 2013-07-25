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
import java.util.HashMap;
import java.util.Map;

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
import org.postgresql.stado.metadata.SyncCreateTablespace;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.CreateTablespace;
import org.postgresql.stado.parser.core.syntaxtree.NodeListOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.syntaxtree.TablespaceLocation;
import org.postgresql.stado.parser.core.syntaxtree.stringLiteral;
import org.postgresql.stado.parser.core.visitor.DepthFirstRetArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;

public class SqlCreateTableSpace extends DepthFirstRetArguVisitor implements IXDBSql,
        IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlCreateTableSpace.class);

    private XDBSessionContext client;

    private SysDatabase database;

    private String tablespaceName;

    private HashMap<DBNode,String> locations;

    private HashMap<DBNode,String> statements = null;

    private HashMap<DBNode,String> rollbackStatements = null;

    /**
     * Constructor
     */
    public SqlCreateTableSpace(XDBSessionContext client) {
        this.client = client;
        database = client.getSysDatabase();
    }

    /**
     * Gives the static cost of creating a table
     */
    public long getCost() {
        return LOW_COST;
    }

    /**
     * This is for getting the lock specs
     */
    public LockSpecification<SysTable> getLockSpecs() {
        Collection<SysTable> empty = Collections.emptyList();
        return new LockSpecification<SysTable>(empty, empty);
    }

    /**
     * Grammar production:
     * f0 -> <CREATE_>
     * f1 -> <TABLESPACE_>
     * f2 -> Identifier(prn)
     * f3 -> TablespaceLocation(prn)
     * f4 -> ( "," TablespaceLocation(prn) )*
     */
    @Override
    public Object visit(CreateTablespace n, Object argu) {
        tablespaceName = (String) n.f2.accept(new IdentifierHandler(), argu);
        n.f3.accept(this, argu);
        n.f4.accept(this, argu);
        return null;
    }

    /**
     * Grammar production: f0 -> <LOCATION_> f1 -> stringLiteral(prn) f2 ->
     * <ON_> f3 -> ( <ALL_> | ( <NODE_> | <NODES_> ) <DECIMAL_LITERAL> ( ","
     * <DECIMAL_LITERAL> )* )
     */
    @Override
    public Object visit(TablespaceLocation n, Object argu) {
        String filePath = (String) n.f1.accept(this, argu);
        Collection<DBNode> nodeList;
        Collection<DBNode> theNodes = database.getDBNodeList();
        switch (n.f3.which) {
        default:
        case 0: // ALL
            nodeList = theNodes;
        case 1:
            nodeList = new ArrayList<DBNode>(theNodes.size());
            NodeSequence ns = (NodeSequence) n.f3.choice;
            /**
             * Grammar production: f0 -> ( <NODE_> | <NODES_> ) f1 ->
             * <DECIMAL_LITERAL> f2 -> ( "," <DECIMAL_LITERAL> )*
             */
            NodeToken nt = (NodeToken) ns.elementAt(1);
            int nodeID = Integer.parseInt(nt.tokenImage);
            nodeList.add(database.getDBNode(nodeID));
            for (Object node : ((NodeListOptional) ns.elementAt(2)).nodes) {
                NodeSequence ns1 = (NodeSequence) node;
                nodeID = Integer
                        .parseInt(((NodeToken) ns1.nodes.get(1)).tokenImage);
                nodeList.add(database.getDBNode(nodeID));
            }
            break;
        }

        if (locations == null) {
            locations = new HashMap<DBNode,String>();
        }
        for (DBNode dbNode : nodeList) {
            if (locations.put(dbNode, filePath) != null) {
                throw new XDBServerException("Duplicate NodeID: "
                        + dbNode.getNodeId());
            }
        }
        return null;
    }

    /**
     * Grammar production: f0 -> <STRING_LITERAL>
     */
    @Override
    public Object visit(stringLiteral n, Object argu) {
        return n.f0.tokenImage;
    }

    public String getName() {
        return tablespaceName;
    }

    public Map<DBNode,String> getLocations() {
        return locations;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return locations.keySet();
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
        if (client.getCurrentUser().getUserClass() != SysLogin.USER_CLASS_DBA) {
            throw new XDBSecurityException(
                    "Only DBA user can create tablespaces");
        }
        if (MetaData.getMetaData().hasTablespace(tablespaceName)) {
            throw new XDBServerException("Tablespace \"" + tablespaceName
                    + "\" already exists");
        }
        statements = new HashMap<DBNode,String>();
        rollbackStatements = new HashMap<DBNode,String>();
        for (Map.Entry<DBNode,String> entry : locations.entrySet()) {
            DBNode dbNode = entry.getKey();
            String command = "CREATE TABLESPACE "
                + IdentifierHandler.quote(tablespaceName + "_" + dbNode.getNodeId())
                + " LOCATION " + entry.getValue();
            statements.put(dbNode, command);
            rollbackStatements.put(dbNode, "DROP TABLESPACE "
                    + IdentifierHandler.quote(tablespaceName + "_" + dbNode.getNodeId()));
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
        MultinodeExecutor executor = client.getMultinodeExecutor(getNodeList());
        IMetaDataUpdate metaUpdate = new SyncCreateTablespace(this);
        MetaData meta = MetaData.getMetaData();
        meta.beginTransaction();
        try {
            metaUpdate.execute(client);
            executor.executeCommand(statements, true);
            meta.commitTransaction(metaUpdate);
        } catch (Exception ex) {
            logger.catching(ex);
            meta.rollbackTransaction();
            // If some tablespaces were created try and drop them
            try {
                executor.executeCommand(rollbackStatements, true);
            } catch (Exception ignore) {
            }
            logger.throwing(ex);
            throw ex;
        }
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_CREATE_TABLESPACE);
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