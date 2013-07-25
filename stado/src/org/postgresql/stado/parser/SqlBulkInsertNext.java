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

import java.util.Collection;
import java.util.Collections;

import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.Engine;
import org.postgresql.stado.engine.ExecutionResult;
import org.postgresql.stado.engine.IExecutable;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.engine.io.XMessage;
import org.postgresql.stado.metadata.DBNode;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.metadata.scheduler.LockSpecification;


/**
 *  
 */
public class SqlBulkInsertNext implements IXDBSql, IExecutable {
    private static final XLogger logger = XLogger
            .getLogger(SqlBulkInsertNext.class);

    private XDBSessionContext client;

    private SysTable table;

    private boolean serial = false;

    private int range = 10000;

    /**
     * 
     */
    public SqlBulkInsertNext(String cmd, XDBSessionContext client) {
        this.client = client;
        int pos = cmd.indexOf(XMessage.ARGS_DELIMITER);
        String tableName = cmd.substring(0, pos);
        pos += XMessage.ARGS_DELIMITER.length();
        if (cmd.startsWith("SERIAL" + XMessage.ARGS_DELIMITER, pos)) {
            serial = true;
            pos += 6 + XMessage.ARGS_DELIMITER.length();
        }
        try {
            range = Integer.parseInt(cmd.substring(pos));
        } catch (NumberFormatException nfe) {
            // ignore
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

            return Collections.emptyList();

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

            return LOW_COST;

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
            }
            return lockSpecs;

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

    /*
     * (non-Javadoc)
     * 
     * @see org.postgresql.stado.engine.IExecutable#execute(org.postgresql.stado.engine.Engine)
     */
    public ExecutionResult execute(Engine engine) throws Exception {
        long first;
        if (serial) {
            first = table.getSerialHandler().allocateRange(range, client);
        } else {
            first = table.getRowIDHandler().allocateRange(range, client);
        }
        return ExecutionResult.createGeneratorRangeResult(
                ExecutionResult.COMMAND_BULK_INSERT, first);
    }

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}
}
