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
 * SqlDropTempTables.java
 * 
 *  
 */
package org.postgresql.stado.parser;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IPreparable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.misc.combinedresultset.ServerResultSetImpl;
import org.postgresql.stado.queryproc.QueryCombiner;


/**
 *  
 */
public class SqlDropTempTables implements IXDBSql, IPreparable {
    private static final XLogger logger = XLogger
            .getLogger(SqlDropTempTables.class);

    private XDBSessionContext client;

    private String rsKey;

    private ResultSet rs;

    private Collection<String> dropOnNodes;

    private Collection<String> dropOnCoordinator;

    /**
     * 
     */
    public SqlDropTempTables(String rsKey, XDBSessionContext client) {
        this.client = client;
        this.rsKey = rsKey;
    }

    public SqlDropTempTables(ResultSet rs, XDBSessionContext client) {
        this.client = client;
        this.rs = rs;
        if (rs instanceof ServerResultSetImpl) {
            ServerResultSetImpl srs = (ServerResultSetImpl) rs;
            dropOnNodes = srs.getFinalNodeTempTableList();
            dropOnCoordinator = srs.getFinalCoordTempTableList();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        // Use only current nodes, it should be possible to get node list from
        // ServerResultSetImpl
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
     * @see org.postgresql.stado.Engine.IPreparable#isPrepared()
     */
    public boolean isPrepared() {
        return rs != null;
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

            if (isPrepared()) {
                return;
            }
            rs = client.getResultSet(rsKey);
            if (rs instanceof ServerResultSetImpl) {
                ServerResultSetImpl srs = (ServerResultSetImpl) rs;
                dropOnNodes = srs.getFinalNodeTempTableList();
                dropOnCoordinator = srs.getFinalCoordTempTableList();
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
            try {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (SQLException ignore) {
                    }
                }
                if (dropOnNodes != null && !dropOnNodes.isEmpty()) {
                    engine.dropNodeTempTables(dropOnNodes, getNodeList(),
                            client);
                }
                if (dropOnCoordinator != null && !dropOnCoordinator.isEmpty()) {
                    QueryCombiner qc = new QueryCombiner(client, "");
                    qc.dropTempTables(dropOnCoordinator);
                }
            } finally {
                client.closeCursor(rsKey);
            }

            return ExecutionResult
                    .createSuccessResult(ExecutionResult.COMMAND_DROP_TABLE);

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
        return true;
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

}
