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

import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;

import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.MultinodeExecutor;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.MetaData;
import org.postgresql.stado.metadata.SysLogin;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.ILockCost;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.parser.core.syntaxtree.Cluster;
import org.postgresql.stado.parser.core.syntaxtree.NodeOptional;
import org.postgresql.stado.parser.core.syntaxtree.NodeSequence;
import org.postgresql.stado.parser.core.visitor.DepthFirstVoidArguVisitor;
import org.postgresql.stado.parser.handler.IdentifierHandler;


public class SqlCluster extends DepthFirstVoidArguVisitor implements IXDBSql,
        IExecutable, IPreparable {
    private XDBSessionContext client;

    private String aTableName;

    private String aIndexName;

    private String clusterStatement;

    /**
     *
     */
    public SqlCluster(XDBSessionContext client) {
        this.client = client;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        return client.getSysDatabase().getDBNodeList();
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

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.Engine.IExecutable#execute(org.postgresql.stado.Engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        SysTable table = null;
        Collection<DBNode> nodeList = getNodeList();
        MultinodeExecutor aMultinodeExecutor = client.getMultinodeExecutor(nodeList);
        if (aTableName != null) {
            table = MetaData.getMetaData().getSysDatabase(client.getDBName())
                    .getSysTable(aTableName.trim());
            clusterTable(table, table.getNodeList(), aMultinodeExecutor,
                    clusterStatement, aIndexName);
        } else {
            Enumeration allTables = MetaData.getMetaData().getSysDatabase(
                    client.getDBName()).getAllTables();
            while (allTables.hasMoreElements()) {
                SysTable tab = (SysTable) allTables.nextElement();

                if (client.getCurrentUser().getUserClass() == SysLogin.USER_CLASS_DBA
                        || client.getCurrentUser() == tab.getOwner()) {
                    if (tab.getClusteridx() != null) {
                        String sql = "CLUSTER " + IdentifierHandler.quote(tab.getClusteridx())
                            + " ON " + IdentifierHandler.quote(tab.getTableName());
                        clusterTable(tab, tab.getNodeList(),
                                aMultinodeExecutor, sql, tab.getClusteridx());
                    }
                }
            }
        }
        return ExecutionResult
                .createSuccessResult(ExecutionResult.COMMAND_CLUSTER);
    }

    public void clusterTable(SysTable table, Collection<DBNode> nodeList,
            MultinodeExecutor aMultinodeExecutor, String sqlStatement,
            String clusterIdx) {
        aMultinodeExecutor.executeCommand(sqlStatement, nodeList, true);

        String sql = "update xsystables set clusteridx = '" + clusterIdx
                + "' where tableid = " + table.getTableId();
        table.setClusteridx(clusterIdx);
        MetaData.getMetaData().executeUpdate(sql);

    }

    public boolean isPrepared() {
        return clusterStatement != null;
    }

    public void prepare() throws Exception {
        SysTable table = null;
        if (aIndexName == null && aTableName == null) {
            return;
        }
        if (aTableName != null) {
            table = MetaData.getMetaData().getSysDatabase(client.getDBName())
                    .getSysTable(aTableName.trim());

            if (aIndexName == null) {
                if (table.getClusteridx() == null) {
                    throw new XDBServerException(
                            "there is no previously clustered index for table \""
                                    + aTableName + "\"");
                }
                aIndexName = table.getClusteridx().trim();
            }
            clusterStatement = "CLUSTER " + IdentifierHandler.quote(aIndexName)
                + " ON " + IdentifierHandler.quote(aTableName);
        }
    }

    /**
     * Grammar production:
     * f0 -> <CLUSTER_>
     * f1 -> [ Identifier(prn) [ <ON_> Identifier(prn) ] ]
     */
    @Override
    public void visit(Cluster n, Object argu) {
        if (n.f1.present()) {
            IdentifierHandler ih = new IdentifierHandler();
            NodeSequence ns0 = (NodeSequence) n.f1.node;
            NodeOptional no = (NodeOptional) ns0.elementAt(1);
            NodeSequence ns1 = (NodeSequence) no.node;
            if (ns1 == null) {
                // cluster tablename;
                aTableName = (String) ns0.elementAt(0).accept(ih, argu);
            } else {
                // cluster indexname on tablename;
                aIndexName = (String) ns0.elementAt(0).accept(ih, argu);
                aTableName = (String) ns1.elementAt(1).accept(ih, argu);
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#needCoordinatorConnection()
     */
    public boolean needCoordinatorConnection() {
        return true;
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

}
