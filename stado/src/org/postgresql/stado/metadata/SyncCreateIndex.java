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
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.parser.SqlCreateIndex;
import org.postgresql.stado.parser.SqlCreateIndexKey;


/**
 * SyncCreateIndex class synchornizes the MetaData DB after a CREATE INDEX has
 * been successful on the user DB
 *
 * It implements IMetaDataUpdate. These methods are called from
 * Engine.executeDDLOnMultipleNodes();
 */

public class SyncCreateIndex implements IMetaDataUpdate {
    public final static String SYSTEM_CREATED = "XSYSINDEX_SYSTEM_CREATED";

    private SqlCreateIndex aSqlCreateIndex;

    private SysTable aSysTable;

    private Map<String, Integer> colMap;

    /**
     * Constructor Creates an Object for Updating and refresing the MetaData DB
     * as per the SqlCreateIndex (CREATE INDEX ...)
     */
    public SyncCreateIndex(SqlCreateIndex aSqlCreateIndex, SysDatabase database) {
        this.aSqlCreateIndex = aSqlCreateIndex;
        aSysTable = database.getSysTable(aSqlCreateIndex.getIndexTableName());
        if (aSysTable == null) {
            throw new XDBServerException(
                    "The table specified does not exist in the database");
        }
        colMap = new HashMap<String, Integer>();
        List<SysColumn> columns = aSysTable.getColumns();
        for (SysColumn aSysColumn : columns) {
            colMap.put(aSysColumn.getColName(), aSysColumn.getColID());
        }
    }

    /**
     * Method execute() Updates the MetaData DB as per the CREATE INDEX
     * statement stored in aSqlCreateIndex
     */
    public void execute(XDBSessionContext client) throws Exception {
        int tableID = aSysTable.getTableId();
        List<SqlCreateIndexKey> indexKeys = new LinkedList<SqlCreateIndexKey>();
        for (SqlCreateIndexKey keyDef : aSqlCreateIndex.getIndexKeyDefinitions()) {
            if (keyDef.getKeyColumnId() == -1) {
                for (String colName : keyDef.getKeyColumnNames()) {
                    indexKeys.add(new SqlCreateIndexKey(colMap.get(colName),
                            colName, keyDef.isAscending() ? "ASC" : "DESC",
                            keyDef.getColOperator()));
                }
            } else {
                indexKeys.add(keyDef);
            }
        }
        int indexid = MetaUtils.createIndex(-1, aSqlCreateIndex.getIndexName(),
                tableID, false,
                indexKeys.toArray(new SqlCreateIndexKey[indexKeys.size()]),
                aSqlCreateIndex.getTablespace() == null ? -1 : aSqlCreateIndex
                        .getTablespace().getTablespaceID(), aSqlCreateIndex
                        .getUsingType(), aSqlCreateIndex.getWherePred());

        // -----------------------------------
        // xsysconstraints for unique indexes
        // -----------------------------------
        if (aSqlCreateIndex.isUnique()) {

            int constid;
            String sqlCommand = "SELECT max(constid) FROM xsysconstraints";
            ResultSet rs = MetaData.getMetaData().executeQuery(sqlCommand);
            try {
                rs.next();
                constid = rs.getInt(1) + 1;
            } finally {
                rs.close();
            }

            // checkissoft
            String xsysconstraints = "INSERT INTO xsysconstraints "
                    + "(constid, tableid, consttype, Idxid, issoft) "
                    + "VALUES (" + constid + "," + tableID + "," + "'U'" + ","
                    + indexid + "," + "0)";

            MetaData.getMetaData().executeUpdate(xsysconstraints);
        }
    }

    /**
     * refresh() Refreshes the MetaData cache by reading in the table
     * information
     */
    public void refresh() throws Exception {
        // refresh MetaData structure, adding new index
        aSysTable.readTableInfo();
    }
}
