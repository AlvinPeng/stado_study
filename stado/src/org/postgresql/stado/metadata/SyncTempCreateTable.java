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
package org.postgresql.stado.metadata;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.SqlCreateTable;
import org.postgresql.stado.parser.SqlCreateTableColumn;

/**
 *
 */
public class SyncTempCreateTable implements IMetaDataUpdate {

    private XDBSessionContext client;

    private SysDatabase database;

    SqlCreateTable aSqlCreateTable;

    SysTable createdTable;

    // @ any given point in time, there can be 4095, temp tables - we can later
    // decide how many
    // ID's we want to allocate for temptables
    static final int MIN_TEMP_TABLE_ID = 0x7FFFF000;

    static final int MAX_TEMP_TABLE_ID = 0x7FFFFFFF;

    // this is 255

    static final int MIN_COLUMN_ID = 0x7FFFF000;

    static final int MAX_COLUMNID = 0x7FFFFFFF;

    static int tempTableID = MIN_TEMP_TABLE_ID;

    static int tempColumnID = MIN_TEMP_TABLE_ID;

    /**
     *
     * @param aSqlCreateTable
     */
    public SyncTempCreateTable(SqlCreateTable aSqlCreateTable) {
        this.aSqlCreateTable = aSqlCreateTable;
    }

    /**
     *
     * @param client
     * @throws java.lang.Exception
     */
    public void execute(XDBSessionContext client) throws Exception {
        this.client = client;
        database = MetaData.getMetaData().getSysDatabase(client.getDBName());
        // Create a new Table
        int tableid = getNextTempTableID(database);
        SysTable aSysTable = new SysTable(database, tableid, aSqlCreateTable
                .getTableName(), 0, aSqlCreateTable.getPartScheme(),
                aSqlCreateTable.partColumn, client.getCurrentUser(), -1, -1,
                null);
        // see if we should also get partition info

        // ----------------------------------------------------------
        // get column information
        int colseq = -1;
        for (SqlCreateTableColumn column : aSqlCreateTable.columnDefinitions) {
            SysColumn aSysColumn = new SysColumn(aSysTable, getNextColumnID(),
                    ++colseq, column.columnName, column.getColumnType(),
                    (column.getColumnLength() > 0 ? column.getColumnLength()
                            : 0), (column.getColumnScale() > 0 ? column
                            .getColumnScale() : 0), (column
                            .getColumnPrecision() > 0 ? column
                            .getColumnPrecision() : 0),
                    (column.isnullable != 0), column.isSerial(), column
                            .rebuildString(), 1, "");
            aSysTable.addSysColumn(aSysColumn);
        }
        aSysTable.setPartitioning(aSysTable.getPartitionColumn(),
                aSysTable.getPartitionScheme(),
                aSqlCreateTable.getPartitionMap());
        createdTable = aSysTable;
        database.addSysTempTable(aSysTable);
        refresh();
        aSysTable.refreshAssociatedInfo();
    }

    /**
     *
     * @throws Exception
     */
    public void refresh() throws Exception {
        synchronized (database.getScheduler()) {
            database.getLm().add(createdTable, client);
        }
    }

    /**
     * Gives the temp table ID
     *
     * @return the next ID
     */

    private int getNextTempTableID(SysDatabase database) {
        int tempid = 0;
        tempid = tempTableID++;
        if (tempTableID >= MAX_TEMP_TABLE_ID) {
            // recycle

            tempid = tempTableID = MIN_TEMP_TABLE_ID;
        }
        // Check if the temp table has already been given to some
        // other table
        int tries = 0;
        while (database.getSysTable(tempid) != null) {
            tempid++;
            if (tempTableID >= MAX_TEMP_TABLE_ID) {
                tempid = tempTableID = MIN_TEMP_TABLE_ID;
            }

            tries++;

            if (tries > 4095) {
                throw new XDBServerException("Out of temp table name space");
            }
        }

        tempTableID = tempid;
        return tempid;
    }

    /**
     * The column IDs
     *
     * @return the next ID
     */
    private int getNextColumnID() {
        if (tempColumnID >= MAX_COLUMNID) {
            tempColumnID = MIN_COLUMN_ID;
            return tempColumnID;
        } else {
            tempColumnID++;
            return tempColumnID;
        }
    }
}
