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

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;

import org.postgresql.stado.parser.SqlCreateIndexKey;
import org.postgresql.stado.parser.SqlCreateTableColumn;


/**
 * MetUtils prcvides utility functions to manipulate the MetaData DB.
 */

public class MetaUtils {

    /**
     * Utility function to add column definitions to the MetatData DB tables
     * 
     * This is used from the CREATE TABLE Also used for ALTER TABLE ADD COLUMN.
     * 
     * It maintains the Restrictions imposed on execute() from IMetaDataUpdate.
     */
    static public int[] addTableColumns(int begColSeq,
            List<SqlCreateTableColumn> columnDefinitions, int tableid)
            throws Exception {
        int[] colIDs = new int[columnDefinitions.size()];
        SqlCreateTableColumn aSqlCreateTableColumn;
        String sqlStatement;
        String xsyscolumnsBaseStr = "INSERT INTO xsyscolumns" + "("
                + "tableid, " + "colseq," + "colname, " + "coltype, "
                + "collength, " + "colscale, " + "colprecision,"
                + "isnullable," + "isserial," + "DEFAULTEXPR, " + "CHECKEXPR, "
                + "nativecoldef)" + " values ";

        for (int i = 0, seq = begColSeq + 1; i < columnDefinitions.size(); i++, seq++) {
            aSqlCreateTableColumn = columnDefinitions.get(i);
            sqlStatement = xsyscolumnsBaseStr
                    + "("
                    + tableid
                    + ","
                    + seq
                    + ","
                    + "'"
                    + aSqlCreateTableColumn.columnName
                    + "',"
                    + aSqlCreateTableColumn.getColumnType()
                    + ","
                    + (aSqlCreateTableColumn.getColumnLength() > -1 ? aSqlCreateTableColumn
                            .getColumnLength()
                            + ","
                            : "null,")
                    + (aSqlCreateTableColumn.getColumnScale() > -1 ? aSqlCreateTableColumn
                            .getColumnScale()
                            + ","
                            : "null,")
                    + (aSqlCreateTableColumn.getColumnPrecision() > -1 ? aSqlCreateTableColumn
                            .getColumnPrecision()
                            + ","
                            : "null,") + aSqlCreateTableColumn.isnullable + ","
                    + (aSqlCreateTableColumn.isSerial() ? "1" : "0") + ",";

            String defaultValue = aSqlCreateTableColumn.getDefaultValue();
            if (defaultValue != null) {
                defaultValue = "'" + defaultValue.replaceAll("'", "''") + "'";
            }
            sqlStatement += defaultValue + ", ";

            String checkString = aSqlCreateTableColumn
                    .getcheckConditionString();
            if (checkString != null) {
                checkString = checkString.replaceAll("'", "''");
            }
            sqlStatement += "'" + checkString + "' ,";

            sqlStatement += "'"
                    + aSqlCreateTableColumn.rebuildString().replaceAll("'",
                            "''") + "') RETURNING colid";

            ResultSet keys = MetaData.getMetaData().executeUpdateReturning(sqlStatement);
            if (keys.next()) {
            	colIDs[i] = keys.getInt(1);
            } else {
            	throw new Exception("Error creating table");
            }

        }
        return colIDs;
    }

    /**
     * Utility function to modify column definitions to the MetatData DB tables
     * 
     * This is used from the ALTER TABLE MODIFY
     * 
     */
    static public void modifyTableColumn(SqlCreateTableColumn columnDefinition,
            SysTable targetTable) throws Exception {
        StringBuffer sbStatement = new StringBuffer("UPDATE xsyscolumns SET ");
        SysColumn aSysCol = targetTable
                .getSysColumn(columnDefinition.columnName);
        sbStatement.append("coltype = ").append(
                columnDefinition.getColumnType()).append(", ");
        sbStatement.append("collength = ");
        int colLength = columnDefinition.getColumnLength();
        if (colLength > -1) {
            sbStatement.append(colLength).append(", ");
        } else {
            sbStatement.append("null, ");
        }
        sbStatement.append("colscale = ");
        int colScale = columnDefinition.getColumnScale();
        if (colScale > -1) {
            sbStatement.append(colScale).append(", ");
        } else {
            sbStatement.append("null, ");
        }
        sbStatement.append("colprecision = ");
        int colPrecision = columnDefinition.getColumnPrecision();
        if (colPrecision > -1) {
            sbStatement.append(colPrecision).append(", ");
        } else {
            sbStatement.append("null, ");
        }
        sbStatement.append("isnullable = ").append(columnDefinition.isnullable)
                .append(", ");
        sbStatement.append("isserial = ").append(
                columnDefinition.isSerial() ? "1" : "0").append(", ");
        sbStatement.append("defaultexpr = ");
        String defaultExpr = columnDefinition.getDefaultValue();
        if (defaultExpr == null) {
            sbStatement.append("null, ");
        } else {
            sbStatement.append("'").append(defaultExpr.replaceAll("'", "''"))
                    .append("', ");
        }
        sbStatement.append("nativecoldef = '").append(
                columnDefinition.rebuildString().replaceAll("'", "''")).append(
                "' ");
        sbStatement.append("WHERE colid = ").append(aSysCol.getColID());
        MetaData.getMetaData().executeUpdate(sbStatement.toString());
    }

    /**
     * Returns the Maximum sequence number assigned to a column of the table
     * with name 'tableName' in the MetaData DB. Works off the cache only.
     * 
     * Returns -ve value if table is not found in the MetaData.
     * 
     */
    static public int getMaxColSeqNum(SysDatabase database, String tableName) {
        int maxSeq = -1000;

        SysTable sysTab = database.getSysTable(tableName);

        if (sysTab != null) {
            for (Iterator it = sysTab.getColumns().iterator(); it.hasNext();) {
                SysColumn aSysColumn = (SysColumn) it.next();
                if (aSysColumn.getColSeq() > maxSeq) {
                    maxSeq = aSysColumn.getColSeq();
                }
            }
        }

        return maxSeq; // will be -ve if tableName not found

    }

    static public int createIndex(int indexId, String indexName, int tableId,
            boolean sysCreated, SqlCreateIndexKey[] indexKeys,
            int tablespaceID, String usingType, String wherePred)
            throws Exception {
        String sqlCommand;
        if (indexId < 0) {
            sqlCommand = "SELECT max(idxid) FROM xsysindexes";
            ResultSet rs = MetaData.getMetaData().executeQuery(sqlCommand);
            try {
                rs.next();
                indexId = rs.getInt(1) + 1;
            } finally {
                rs.close();
            }
        }

        String xsysindexesStr = "insert into xsysindexes "
                + "(idxid, "
                + "idxname, "
                + "tableid,"
                + "keycnt, "
                + "issyscreated, "
                + "tablespaceid, "
                + "usingtype, "
                + "wherepred ) "
                + " values "
                + "("
                + indexId
                + ","
                + "'"
                + indexName
                + "',"
                + tableId
                + ","
                + indexKeys.length
                + ","
                + (sysCreated ? 1 : 0)
                + ","
                + (tablespaceID == -1 ? "null" : "" + tablespaceID)
                + ","
                + (usingType == null ? "null" : "'"
                        + usingType.replace("'", "''") + "'")
                + ","
                + (wherePred == null ? "null" : "'"
                        + wherePred.replace("'", "''") + "'") + ")";

        MetaData.getMetaData().executeUpdate(xsysindexesStr);

        String xsysindexkeysBaseStr = "INSERT INTO xsysindexkeys"
                + "(idxkeyid, " + "idxid, " + "idxkeyseq," + "idxascdesc,"
                + "colid, " + "coloperator)" + " values ";

        int indexKeyId;
        sqlCommand = "SELECT max(idxkeyid) FROM xsysindexkeys";
        ResultSet rs = MetaData.getMetaData().executeQuery(sqlCommand);
        try {
            rs.next();
            indexKeyId = rs.getInt(1) + 1;
        } finally {
            rs.close();
        }

        for (int keySeq = 0; keySeq < indexKeys.length; keySeq++) {
            sqlCommand = xsysindexkeysBaseStr
                    + "("
                    + (indexKeyId++)
                    + ","
                    + indexId
                    + ","
                    + (keySeq + 1)
                    + ","
                    + (indexKeys[keySeq].isAscending() ? 0 : 1)
                    + ","
                    + indexKeys[keySeq].getKeyColumnId()
                    + ","
                    + (indexKeys[keySeq].getColOperator() == null ? "null"
                            : "'" + indexKeys[keySeq].getColOperator() + "'")
                    + ")";
            MetaData.getMetaData().executeUpdate(sqlCommand);
        }

        return indexId;
    }

    /**
     * Performs a Drop index on the MetaData for the indexId specified.
     */
    static public void dropIndex(int indexId) throws Exception {
        // ---------------------------
        // xSysIndexKeys
        // ---------------------------
        String xsysindexkeysStr = "delete from xsysindexkeys where idxid = "
                + indexId;
        MetaData.getMetaData().executeUpdate(xsysindexkeysStr);

        // -------------------------
        // xSysIndexes
        // -------------------------
        String xSysIndexes = "delete from xsysindexes where idxid = " + indexId;
        MetaData.getMetaData().executeUpdate(xSysIndexes);
    }
}
