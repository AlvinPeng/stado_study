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
 * SqlBulkInsert.java
 * 
 *  
 */
package org.postgresql.stado.parser;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.io.XMessage;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.metadata.scheduler.LockSpecification;
import org.postgresql.stado.metadata.scheduler.LockType;


/**
 *  
 */
public class SqlBulkInsert implements IXDBSql, IExecutable {
    private static final XLogger logger = XLogger
            .getLogger(SqlBulkInsert.class);

    private SysTable table;

    private XDBSessionContext client;

    private String address;

    /**
     * 
     */
    public SqlBulkInsert(String cmd, XDBSessionContext client) {
        this.client = client;
        String tableName;
        int pos = cmd.indexOf(XMessage.ARGS_DELIMITER);
        if (pos < 0) {
            tableName = cmd;
        } else {
            tableName = cmd.substring(0, pos);
            address = cmd.substring(pos + XMessage.ARGS_DELIMITER.length());
        }
        table = client.getSysDatabase().getSysTable(tableName);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.Parser.IXDBSql#getNodeList()
     */
    public Collection<DBNode> getNodeList() {
        final String method = "getNodeList";
        logger.entering(method, new Object[] {});
        try {

            return table.getNodeList();

        } finally {
            logger.exiting(method);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getCost()
     */
    public long getCost() {
        final String method = "getCost";
        logger.entering(method, new Object[] {});
        try {

            return HIGH_COST;

        } finally {
            logger.exiting(method);
        }
    }

    private LockSpecification<SysTable> lockSpecs = null;

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.MetaData.Scheduler.ILockCost#getLockSpecs()
     */
    public LockSpecification<SysTable> getLockSpecs() {
        final String method = "getLockSpecs";
        logger.entering(method, new Object[] {});
        try {

            if (lockSpecs == null) {
                lockSpecs = new LockSpecification<SysTable>();
                lockSpecs.add(table, LockType.get(
                        LockType.LOCK_SHARE_WRITE_INT, false));
            }
            return lockSpecs;

        } finally {
            logger.exiting(method);
        }
    }

    public SysTable getSysTable() {
        return table;
    }

    public void startLoaders(Engine engine) throws Exception {
        if (address != null) {
            engine.startLoaders(table.getTableName(), address, getNodeList(),
                    client);
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

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.engine.IExecutable#execute(org.postgresql.stado.engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        if (!client.isInTransaction()) {
            engine.beginTransaction(client, getNodeList());
        }
        startLoaders(engine);
        PartitionMap map = table.getPartitionMap();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            try {
                oos.writeObject(map);
                oos.flush();
                return ExecutionResult
                        .createSerializedObjectResult(
                                ExecutionResult.COMMAND_BULK_INSERT, baos
                                        .toByteArray());
            } finally {
                oos.close();
            }
        } finally {
            baos.close();
        }
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
