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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.SqlAlterInherit;
import org.postgresql.stado.parser.SqlAlterTable;


/**
 * SyncAlterTablePrimaryKey class synchornizes the MetaData DB after a ALTER
 * TABLE PRIMARY KEY has been successful on the user DB
 *
 * It implements IMetaDataUpdate. These methods are called from
 * Engine.executeDDLOnMultipleNodes();
 */

public class SyncAlterTableInherit implements IMetaDataUpdate {
    // This is not used anymore - executeDDLOnMultipleNodes takes care of
    // updates to MetaData DB
    private SqlAlterInherit aSqlAlterInherit;

    private SqlAlterTable aSqlAlterTable;

    /**
     * Constructor Creates an Object for Updating and refresing the MetaData DB
     * as per the SqlAlterTablePrimaryKey (CREATE TABLE ...)
     */
    public SyncAlterTableInherit(SqlAlterInherit aSqlAlterInherit) {
        this.aSqlAlterInherit = aSqlAlterInherit;
        aSqlAlterTable = aSqlAlterInherit.getParent();
    }

    /**
     * Method execute() Updates the MetaData DB as per the CREATE TABLE
     * statement stored in aSqlAlterTablePrimaryKey
     */
    public void execute(XDBSessionContext client) throws Exception {
        // -------------------------------
        // xsysconstraints for check
        // -------------------------------
        SysTable table = aSqlAlterTable.getTable();
        SysTable parent = aSqlAlterInherit.getTable();
        if (aSqlAlterInherit.isNoInherit()) {
            String sqlCommand = "UPDATE xsystables SET parentid = null, "
                    + "partcol = "
                    + (parent.getPartitionColumn() == null ? "null, " : "'"
                            + parent.getPartitionColumn() + "', ")
                    + "partscheme = " + parent.getPartitionScheme()
                    + "WHERE tableid = " + table.getSysTableid();
            MetaData.getMetaData().executeUpdate(sqlCommand);
            // Copy over columns from former parent
//            int colid;
            sqlCommand = "UPDATE xsyscolumns SET colseq = colseq + "
                    + parent.getColumns().size() + " WHERE tableid = "
                    + table.getSysTableid();
            MetaData.getMetaData().executeUpdate(sqlCommand);

            int colseq = 1;
            // We will need to update Index Keys that may reference inherited columns
            HashMap<Integer, Integer> colIdMap = new HashMap<Integer, Integer>();
            for (SysColumn column : parent.getColumns()) {
                sqlCommand = "INSERT INTO xsyscolumns ("
                        + "tableid, colseq, colname, coltype, collength, "
                        + "colscale, colprecision, isnullable, isserial, "
                        + "defaultexpr, selectivity, nativecoldef) VALUES ("
                        + table.getTableId()
                        + ", "
                        + colseq++
                        + ", '"
                        + column.getColName()
                        + "', "
                        + column.getColType()
                        + ", "
                        + column.getColLength()
                        + ", "
                        + column.getColScale()
                        + ", "
                        + column.getColPrecision()
                        + ", "
                        + (column.isNullable() ? "1" : "0")
                        + ", "
                        + (column.isSerial() ? "1" : "0")
                        + ", "
                        + (column.getDefaultExpr() == null ? "null, " : "'"
                                + column.getDefaultExpr() + "', ")
                        + column.getSelectivity()
                        + ", '"
                        + column.getNativeColDef() + "') RETURNING colid";
                ResultSet keys = MetaData.getMetaData().executeUpdateReturning(sqlCommand);

                int newColID;
                if (keys.next()) {
                	newColID = keys.getInt(1);
                } else {
                	throw new Exception("Error altering table");
                }

                colIdMap.put(column.getColID(), newColID);
            }
            Collection<SysIndex> indexes = table.getSysIndexList();
            if (indexes != null && !indexes.isEmpty()) {
                // Update Index Keys that may reference inherited columns
                sqlCommand = "UPDATE xsysindexkeys SET colid = ? WHERE idxkeyid = ?";
                PreparedStatement ps = MetaData.getMetaData().prepareStatement(
                        sqlCommand);
                for (SysIndex index : indexes) {
                    for (SysIndexKey idxKey : index.getIndexKeys()) {
                        Integer newColID = colIdMap.get(idxKey.colid);
                        // Update only if reference on inherited column
                        if (newColID != null) {
                            ps.setInt(1, newColID);
                            ps.setInt(2, idxKey.idxkeyid);
                            ps.addBatch();
                        }
                    }
                }
                ps.executeBatch();
            }
            // Copy over partitioning info
            int partid = 0;
            sqlCommand = "SELECT max(partid) FROM xsystabparts";
            ResultSet rs = MetaData.getMetaData().executeQuery(sqlCommand);
            try {
                rs.next();
                partid = rs.getInt(1) + 1;
            } finally {
                rs.close();
            }
            sqlCommand = "INSERT INTO xsystabparts (partid, tableid, dbid, nodeid) VALUES (?, ?, ?, ?)";
            PreparedStatement ps = MetaData.getMetaData().prepareStatement(
                    sqlCommand);
            ps.setInt(2, table.getSysTableid());
            ps.setInt(3, client.getSysDatabase().getDbid());
            sqlCommand = "SELECT nodeid FROM xsystabparts WHERE tableid = "
                    + parent.getSysTableid() + " ORDER BY partid";
            rs = MetaData.getMetaData().executeQuery(sqlCommand);
            try {
                while (rs.next()) {
                    ps.setInt(1, partid++);
                    ps.setInt(4, rs.getInt(1));
                    ps.addBatch();
                }
            } finally {
                rs.close();
            }
            ps.executeBatch();
            int parthashid = 0;
            sqlCommand = "SELECT max(parthashid) FROM xsystabparthash";
            rs = MetaData.getMetaData().executeQuery(sqlCommand);
            try {
                rs.next();
                parthashid = rs.getInt(1) + 1;
            } finally {
                rs.close();
            }
            sqlCommand = "INSERT INTO xsystabparthash (parthashid, tableid, dbid, hashvalue, nodeid) VALUES (?, ?, ?, ?, ?)";
            ps = MetaData.getMetaData().prepareStatement(
                    sqlCommand);
            ps.setInt(2, table.getSysTableid());
            ps.setInt(3, client.getSysDatabase().getDbid());
            sqlCommand = "SELECT hashvalue, nodeid FROM xsystabparthash WHERE tableid = "
                    + parent.getSysTableid() + " ORDER BY parthashid";
            rs = MetaData.getMetaData().executeQuery(sqlCommand);
            try {
                while (rs.next()) {
                    ps.setInt(1, parthashid++);
                    ps.setInt(4, rs.getInt(1));
                    ps.setInt(5, rs.getInt(2));
                    ps.addBatch();
                }
            } finally {
                rs.close();
            }
            ps.executeBatch();
        } else {
            String sqlCommand = "UPDATE xsystables SET parentid = "
                    + parent.getSysTableid() + " WHERE tableid = "
                    + table.getSysTableid();
            MetaData.getMetaData().executeUpdate(sqlCommand);

            // Change index keys that may reference inherited columns
            Collection<SysIndex> indexes = table.getSysIndexList();
            if (indexes != null && !indexes.isEmpty()) {
                HashMap<Integer, Integer> colIdMap = new HashMap<Integer, Integer>();
                for (int i = 0; i < parent.getColumns().size(); i++) {
                    SysColumn parentCol = parent.getColumns().get(i);
                    SysColumn childCol = table.getColumns().get(i);
                    colIdMap.put(childCol.getColID(), parentCol.getColID());
                }
                // Update Index Keys that may reference inherited columns
                sqlCommand = "UPDATE xsysindexkeys SET colid = ? WHERE idxkeyid = ?";
                PreparedStatement ps = MetaData.getMetaData().prepareStatement(
                        sqlCommand);
                for (SysIndex index : indexes) {
                    for (SysIndexKey idxKey : index.getIndexKeys()) {
                        Integer newColID = colIdMap.get(idxKey.colid);
                        // Update only if reference on inherited column
                        if (newColID != null) {
                            ps.setInt(1, newColID);
                            ps.setInt(2, idxKey.idxkeyid);
                            ps.addBatch();
                        }
                    }
                }
                ps.executeBatch();
            }

            // Remove columns common with the new parent
            sqlCommand = "DELETE FROM xsyscolumns WHERE colseq <= "
                    + parent.getColumns().size() + " AND tableid = "
                    + table.getSysTableid();
            MetaData.getMetaData().executeUpdate(sqlCommand);
            sqlCommand = "UPDATE xsyscolumns SET colseq = colseq - "
                    + parent.getColumns().size() + " WHERE tableid = "
                    + table.getSysTableid();
            MetaData.getMetaData().executeUpdate(sqlCommand);
            // Remove partitioning info (will be used from parent)
            sqlCommand = "DELETE FROM xsystabparts WHERE tableid = "
                    + table.getSysTableid();
            MetaData.getMetaData().executeUpdate(sqlCommand);
            sqlCommand = "DELETE FROM xsystabparthash WHERE tableid = "
                    + table.getSysTableid();
            MetaData.getMetaData().executeUpdate(sqlCommand);
        }
    }

    /**
     * refresh() Refreshes the MetaData cahce by reading in the table
     * information just created
     */
    public void refresh() throws Exception {
        aSqlAlterTable.getTable().setParentTableID(
                aSqlAlterInherit.isNoInherit() ? -1
                        : aSqlAlterInherit.getTable().getTableId());
        if (aSqlAlterInherit.isNoInherit()) {
            aSqlAlterTable.getTable().setPartitioning(
                    aSqlAlterInherit.getTable().getPartitionColumn(),
                    aSqlAlterInherit.getTable().getPartitionScheme(),
                    aSqlAlterInherit.getTable().getPartitionMap());
        }
        // to refresh columns
        aSqlAlterTable.getTable().readTableInfo(true);
    }
}
