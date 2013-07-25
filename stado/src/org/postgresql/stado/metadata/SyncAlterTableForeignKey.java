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
 * SyncAlterTableForeignKey.java
 *
 * This class takes care of updating the MetaData after
 * ALTER TABLE ADD FOREIGN KEY is successful.
 *
 *  
 */

package org.postgresql.stado.metadata;

import java.sql.ResultSet;
import java.util.List;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.parser.SqlAlterAddForeignKey;
import org.postgresql.stado.parser.SqlCreateIndexKey;


/**
 * 
 * 
 */
public class SyncAlterTableForeignKey implements IMetaDataUpdate {

    private SysDatabase database;

    private SqlAlterAddForeignKey aSqlAlterAddForeignKey;

    /** Creates a new instance of SyncAlterTableForeignKey */
    public SyncAlterTableForeignKey(SqlAlterAddForeignKey aSqlAlterAddForeignKey) {
        this.aSqlAlterAddForeignKey = aSqlAlterAddForeignKey;
    }

    public void execute(XDBSessionContext client) throws Exception {

        /*
         * Use Case comments 1. If this table and its parent and table are both
         * partitioned on the same column which is the primary key we are
         * referencing, or the referenced table is REPLICATED (lookup), we can
         * execute the command on all concerned nodes via the multi-node
         * executor. 2. We also add an index on the referenced table keys, if
         * one does not yet exist. 3. The foreign key info is added to
         * xsysconstraints and xsysindexes, xsysreferences and xsysforeignkeys.
         * 
         * Alternate - If the foreign key references a primary key and is not a
         * common partitioning key, then we cannot add this constraint at the
         * individual nodes because they will be inconsistent. Instead, we
         * perform an exhaustive check across nodes to make sure that the data
         * is consistent, then add the info to the xsys tables.
         */
        database = client.getSysDatabase();
        String tableName = aSqlAlterAddForeignKey.getParent().getTableName();
        String refTableName = aSqlAlterAddForeignKey.getReferedTableName();

        List<String> columnNames = aSqlAlterAddForeignKey.getColumnNames();
        List<String> refColumnNames = aSqlAlterAddForeignKey
                .getReferedColumnNames();

        SysTable refSysTable = database.getSysTable(refTableName);
        int refTableid = refSysTable.getSysTableid();
        SysTable aSysTable = database.getSysTable(tableName);
        int tableid = aSysTable.getSysTableid();
        int indexIdUsed = aSqlAlterAddForeignKey.getIndexIDUsed();
        int referingIndexId = aSqlAlterAddForeignKey.getReferingIndexID();

        if (referingIndexId < 0) {
            // we created the index on the refering table
            // so update meta data

            // ----------------------------------
            // xsysindexes
            // ----------------------------------

            // the index needs to be created on the refering table
            SqlCreateIndexKey[] indexKeys = new SqlCreateIndexKey[columnNames
                    .size()];
            for (int i = 0; i < indexKeys.length; i++) {
                String colName = columnNames.get(i);
                indexKeys[i] = new SqlCreateIndexKey(aSysTable.getSysColumn(
                        colName).getColID(), colName, null, null); // coloperator
            }
            referingIndexId = MetaUtils.createIndex(referingIndexId,
                    aSqlAlterAddForeignKey.getIndexName(), tableid, true,
                    indexKeys, aSysTable.getTablespaceID(), null, null); // usingtype,
                                                                            // wherepred
        } // done with index

        // ----------------------------
        // xsysconstraints
        // ----------------------------

        String constraintName = aSqlAlterAddForeignKey.getConstraintName();

        if ((constraintName == null) || (constraintName.length() == 0)) {
            // reuse index name as contraint_name
            constraintName = "C_" + aSqlAlterAddForeignKey.getIndexName();
        }

        int constid;
        String sqlCommand = "SELECT max(constid) FROM xsysconstraints";
        ResultSet rs = MetaData.getMetaData().executeQuery(sqlCommand);
        try {
            rs.next();
            constid = rs.getInt(1) + 1;
        } finally {
            rs.close();
        }

        String xsysConstraintStr = "insert into xsysconstraints "
                + "(constid, " + "constname, " + "tableid, " + "Consttype, "
                + "idxid, issoft) " + " VALUES " + "(" + constid + "," + "'"
                + constraintName + "'," + tableid + "," + "'R'" + ","
                + referingIndexId + ", "
                + (aSqlAlterAddForeignKey.isSoftConstraint() ? "1 )" : "0 )");

        MetaData.getMetaData().executeUpdate(xsysConstraintStr);

        // --------------------------
        // XsysReferences
        // --------------------------
        int refid;
        sqlCommand = "SELECT max(refid) FROM xsysreferences";
        rs = MetaData.getMetaData().executeQuery(sqlCommand);
        try {
            rs.next();
            refid = rs.getInt(1) + 1;
        } finally {
            rs.close();
        }

        String xSysRefrencesStr = "INSERT INTO xsysreferences "
                + "(refid, constid, reftableid, refidxid) VALUES (" + refid
                + "," + constid + "," + refTableid + "," + indexIdUsed + ")";

        MetaData.getMetaData().executeUpdate(xSysRefrencesStr);

        // ----------------------------
        // XSysForeignKeys
        // ----------------------------
        int fkeyid;
        sqlCommand = "SELECT max(fkeyid)FROM xsysforeignkeys";
        rs = MetaData.getMetaData().executeQuery(sqlCommand);
        try {
            rs.next();
            fkeyid = rs.getInt(1) + 1;
        } finally {
            rs.close();
        }

        String foreignKeysBase = "INSERT INTO xsysforeignkeys "
                + "(fkeyid, Refid, FKeyseq, Colid, Refcolid ) VALUES ";

        int refColumnid;
        int columnid;
        for (int i = 0; i < refColumnNames.size(); i++) {
            SysColumn refSysColumn = refSysTable.getSysColumn(refColumnNames
                    .get(i));
            refColumnid = refSysColumn.getColID();
            SysColumn aSysColumn = aSysTable.getSysColumn(columnNames.get(i));
            columnid = aSysColumn.getColID();
            // build value list for insert statement
            String sql = foreignKeysBase + "(" + fkeyid++ + "," + refid + ","
                    + (i + 1) + "," + columnid + "," + refColumnid + ")";

            MetaData.getMetaData().executeUpdate(sql);
        }
    } // end of execute()

    public void refresh() throws Exception {
        aSqlAlterAddForeignKey.getParent().getTable().readTableInfo();
        database.getSysTable(aSqlAlterAddForeignKey.getReferedTableName())
                .updateCrossReferences();
    }
}
