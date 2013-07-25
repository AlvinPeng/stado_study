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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.postgresql.stado.communication.message.NodeMessage;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.ExecDirect;
import org.postgresql.stado.parser.core.syntaxtree.NodeListOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.syntaxtree.NodeToken;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;

public class SqlExecDirect extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable, IPreparable {
    private XDBSessionContext client;

    private List<DBNode> nodeList;

    private String command;

    /**
     *
     */
    public SqlExecDirect(XDBSessionContext client) {
        this.client = client;
    }

    /**
     * Grammar production: f0 -> <EXEC_> f1 -> <DIRECT_> f2 -> <ON_> f3 -> (
     * <ALL_> | ( <NODE_> | <NODES_> ) <DECIMAL_LITERAL> ( "," <DECIMAL_LITERAL> )* )
     * f4 -> <STRING_LITERAL>
     */
    @Override
    public void visit(ExecDirect n, Object argu) {
        switch (n.f3.which) {
        case 0: // ALL
            nodeList = new ArrayList<DBNode>(client.getSysDatabase()
                    .getDBNodeList());
            break;
        case 1:
            nodeList = new ArrayList<DBNode>();
            NodeSequence ns = (NodeSequence) n.f3.choice;
            /**
             * Grammar production: f0 -> ( <NODE_> | <NODES_> ) f1 ->
             * <DECIMAL_LITERAL> f2 -> ( "," <DECIMAL_LITERAL> )*
             */
            NodeToken nt = (NodeToken) ns.elementAt(1);
            int nodeID = Integer.parseInt(nt.tokenImage);
            nodeList.add(client.getSysDatabase().getDBNode(nodeID));
            for (Iterator it = ((NodeListOptional) ns.elementAt(2)).nodes
                    .iterator(); it.hasNext();) {
                NodeSequence ns1 = (NodeSequence) it.next();
                nodeID = Integer
                        .parseInt(((NodeToken) ns1.nodes.get(1)).tokenImage);
                nodeList.add(client.getSysDatabase().getDBNode(nodeID));
            }
            break;
        }
        command = n.f4.tokenImage;
        // Strip quotes
        command = command.substring(1, command.length() - 1).replaceAll("''",
                "'");
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
        Collection<SysTable> emptyList = Collections.emptyList();
        return new LockSpecification<SysTable>(emptyList, emptyList);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        MultinodeExecutor aMultinodeExecutor = client
                .getMultinodeExecutor(nodeList);
        List<NodeMessage> resultMsgs = new ArrayList<NodeMessage>(
                aMultinodeExecutor.execute(command, nodeList,
                        nodeList.size() == 1));
        Map<Integer,ExecutionResult> execResults = new TreeMap<Integer,ExecutionResult>();
        for (NodeMessage aMessage : resultMsgs) {
            if (aMessage.getMessageType() == NodeMessage.MSG_EXEC_COMMAND_RESULT) {
                execResults.put(aMessage.getSourceNodeID(),
                        ExecutionResult.createRowCountResult(
                                ExecutionResult.COMMAND_UNKNOWN,
                                aMessage.getNumRowsResult()));
            } else if (aMessage.getMessageType() == NodeMessage.MSG_EXEC_QUERY_RESULT) {
                execResults.put(aMessage.getSourceNodeID(),
                        ExecutionResult.createResultSetResult(
                                ExecutionResult.COMMAND_UNKNOWN,
                                aMessage.getResultSet()));
            }
        }
        return ExecutionResult.createMultipleResult(
                ExecutionResult.COMMAND_DIRECT_EXEC, execResults);
    }

    public boolean isPrepared() {
        return true;
    }

    public void prepare() throws Exception {
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
