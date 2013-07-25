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

// import standard java packeages used
import java.util.Vector;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.SqlAddGeometryColumn;
import org.postgresql.stado.parser.SqlAlterAddColumn;
import org.postgresql.stado.parser.SqlCreateTableColumn;

// import other packages

/**
 * SyncAlterTableAddColumn class synchornizes the MetaData DB after a ALTER
 * TABLE ADD COLUMN has been successful on the user DB
 * 
 * It implements IMetaDataUpdate. These methods are called from
 * Engine.executeDDLOnMultipleNodes();
 */

public class SyncAlterTableAddColumn implements IMetaDataUpdate {

    private SysDatabase database;

    private SqlCreateTableColumn aSqlCreateTableColumn;

    SysTable table;

    boolean DEBUG;

    /**
     * Constructor Creates an Object for Updating and refresing the MetaData DB
     * as per the SqlAlterAddColumn (CREATE TABLE ...)
     */
    public SyncAlterTableAddColumn(SqlAlterAddColumn aSqlAlterAddColumn) {
        table = aSqlAlterAddColumn.getParent().getTable();
        aSqlCreateTableColumn = aSqlAlterAddColumn.getColDef();
    }

    public SyncAlterTableAddColumn(SqlAddGeometryColumn aSqlAddGeometryColumn) {
        table = aSqlAddGeometryColumn.getTable();
        aSqlCreateTableColumn = aSqlAddGeometryColumn.getColDef();
    }

    /**
     * Method execute() Updates the MetaData DB as per the ALTER TABLE ADD COL
     * statement stored in aSqlAlterAddColumn
     */
    public void execute(XDBSessionContext client) throws Exception {
        database = client.getSysDatabase();
        // -----------------------------
        // update the xsyscolumn table etc
        // -----------------------------
        Vector colDefs = new Vector();
        colDefs.addElement(aSqlCreateTableColumn);

        // which table
        SysTable sysTable = table;
        int tableid = sysTable.getSysTableid();
        // request MetaData to add a table column
        // first find out the maximum seq number in the columns
        // and use that for beginSeq - to add the column to the end
        int maxSeq = MetaUtils.getMaxColSeqNum(database, table.getTableName());
        MetaUtils.addTableColumns(maxSeq, colDefs, tableid);
    }

    /**
     * refresh() Refreshes the MetaData cahce by reading in the table
     * information just created
     */
    public void refresh() throws Exception {
        // refresh MetaData cache for new table added
        table.readTableInfo();
    }
}
