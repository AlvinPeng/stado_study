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

// import standard java packages used
import java.sql.ResultSet;
import java.util.*;

import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.parser.SqlCreateIndexKey;
import org.postgresql.stado.parser.SqlCreateTable;
import org.postgresql.stado.parser.SqlCreateTableColumn;
import org.postgresql.stado.parser.handler.ForeignKeyHandler;


/**
 * SyncCreateTable class synchronizes the MetaData DB after a CREATE TABLE has
 * been successful on the user DB
 * 
 * It implements IMetaDataUpdate. These methods are called from
 * Engine.executeDDLOnMultipleNodes();
 */

public class SyncCreateTable implements IMetaDataUpdate {

    private XDBSessionContext client;

    private SysDatabase database;

    private int tableid;

    private Hashtable<String, String> colNameColid = new Hashtable<String, String>();

    //private SqlCreateTable aSqlCreateTable;
    
    private String tableName;
    
    private short partScheme;
    
    private String partColumn;
    
    private PartitionMap partMap;
    
    private SysTable parentTable;
    
    private int parentTableID = -1;
    
    private int tableSpaceID = -1;
    
    private List<SqlCreateTableColumn> columnDefinitions;
    
    private boolean possibleCreateIndex;
    
    private String rowidIndexName;

    private SqlCreateTableColumn serialCol;
    
    private List<String> pkList;
    
    private String pkConstraintName;
    
    private List<ForeignKeyHandler> fkDefinitions;
    
    private List<String> checkList;
    
    private List<String> checkConstraintNames;
    
    /**
     * Constructor Creates an Object for Updating and refresing the MetaData DB
     * as per the SqlCreateTable (CREATE TABLE ...)
     */
    public SyncCreateTable(SqlCreateTable aSqlCreateTable) {
        tableName = aSqlCreateTable.getReferenceName();
        partScheme = aSqlCreateTable.getPartScheme();
        partMap = aSqlCreateTable.getPartitionMap();
        partColumn = aSqlCreateTable.partColumn;
        parentTable = aSqlCreateTable.getParentTable(); 
        if (parentTable != null) {
        	parentTableID = parentTable.getTableId();
        }
        if (aSqlCreateTable.getTablespace() != null) {
        	tableSpaceID = aSqlCreateTable.getTablespace().getTablespaceID();
        }
        columnDefinitions = aSqlCreateTable.columnDefinitions;
        possibleCreateIndex = aSqlCreateTable.isPossibleToCreateIndex();
        if (possibleCreateIndex) {
        	rowidIndexName = aSqlCreateTable.getRowidIndexName();
        }
        serialCol = aSqlCreateTable.getSerialColumnDef();
        pkList = aSqlCreateTable.getPrimaryKeyNameColList();
        pkConstraintName = aSqlCreateTable.getPKConstraintName();
        fkDefinitions = aSqlCreateTable.getForeignKeyDefs();
        checkList = aSqlCreateTable.getChecks();
        checkConstraintNames = aSqlCreateTable.getCheckConstraintName();
    }

    
    public SyncCreateTable(SysTable aSysTable, List<SqlCreateTableColumn> columnDefinitions) {
    	tableName = aSysTable.getTableName();
    	partScheme = aSysTable.getPartitionScheme();
    	partMap = aSysTable.getPartitionMap();
    	partColumn = aSysTable.getPartitionColumn();
    	this.columnDefinitions = columnDefinitions;
        possibleCreateIndex = !aSysTable.isTemporary();
        if (possibleCreateIndex) {
        	rowidIndexName = null;
        }
        tableSpaceID = aSysTable.getTablespaceID();
    }
    
    /**
     * Method execute() Updates the MetaData DB as per the CREATE TABLE
     * statement stored in aSqlCreateTable
     */
    public void execute(XDBSessionContext client) throws Exception {
        this.client = client;
        database = MetaData.getMetaData().getSysDatabase(client.getDBName());

        String sqlStatement;
        ResultSet aTableRS;

        int indexid = -1;
        int refid = -1;
        int fkeyid = -1;
        int constid = -1;

        // ----------------------------------
        // xsystables
        // ----------------------------------
        sqlStatement = "insert into xsystables"
                + "(dbid,"
                + "tablename, "
                + "numrows,"
                + "partscheme, "
                + "partcol, "
                + "parthash, "
                + "owner,"
                + "parentid,"
                + "tablespaceid)"
                + " values "
                + "("
                + database.getDbid()
                + ","
                + "'"
                + tableName
                + "',"
                + "0,"
                + partScheme
                + ","
                + (partColumn == null ? "null," : "'" + partColumn + "',")
                +
                // set to 256 for now.
                "256, "
                + client.getCurrentUser().getUserID()
                + ","
                + (parentTableID == -1 ? "null" : "" + parentTableID)
                + ","
                + (tableSpaceID == -1 ? "null" : "" + tableSpaceID)
                + ") RETURNING tableid";

        ResultSet keys = MetaData.getMetaData().executeUpdateReturning(sqlStatement);
        if (keys.next()) {
        	tableid = keys.getInt(1);
        } else {
        	throw new Exception("Error creating table");
        }
        
        // ----------------------------------
        // xsystabprivs
        // ----------------------------------
        if (parentTable == null) {
            // ----------------------------------
            // Partition maps
            // ----------------------------------
            partMap.storeMapToMetadataDB(MetaData.getMetaData(), database, tableid);
        }

        // -----------------------------
        // xsyscolumns
        // -----------------------------
        int[] assignedIDs = MetaUtils.addTableColumns(0, columnDefinitions, tableid);

        /*
         * fill in the column ID information
         */
        for (int i = 0; i < assignedIDs.length; i++) {
            String colid = "" + assignedIDs[i];
            String columnName = columnDefinitions.get(i).columnName;
            colNameColid.put(columnName.toUpperCase(), colid);
        }
        // Add inherited columns
        if (parentTable != null) {
            for (Iterator<SysColumn> it = parentTable.getColumns().iterator(); it.hasNext();) {
                SysColumn column = it.next();
                colNameColid.put(column.getColName().toUpperCase(), ""
                        + column.getColID());
            }
        }

        if (possibleCreateIndex) {
            if (rowidIndexName != null) {
                SqlCreateIndexKey[] indexKeys = new SqlCreateIndexKey[1];
                indexKeys[0] = new SqlCreateIndexKey(Integer
                        .parseInt(colNameColid.get(
                                SqlCreateTableColumn.XROWID_NAME.toUpperCase())
                                .toString()), SqlCreateTableColumn.XROWID_NAME,
                        null, null);

                indexid = MetaUtils.createIndex(indexid, rowidIndexName, 
                		tableid, true, indexKeys, tableSpaceID, null, null);

                if (constid == -1) {
                    String sqlCommand = "SELECT max(constid) FROM xsysconstraints";
                    ResultSet rs = MetaData.getMetaData().executeQuery(
                            sqlCommand);
                    try {
                        rs.next();
                        constid = rs.getInt(1);
                    } finally {
                        rs.close();
                    }
                }
                String xsysconstraints = "INSERT INTO xsysconstraints "
                        + "(constid, tableid, consttype, Idxid, issoft) "
                        + "VALUES (" + ++constid + "," + tableid + "," + "'U'"
                        + "," + (indexid++) + "," + "0)";

                MetaData.getMetaData().executeUpdate(xsysconstraints);
            }

            // Inorder to update the data in metadata
            if (serialCol != null) {
                SqlCreateIndexKey[] indexKeys = { new SqlCreateIndexKey(
                        Integer.parseInt(colNameColid.get(
                                serialCol.columnName.toUpperCase()).toString()),
                        serialCol.columnName, null, null) };

                indexid = MetaUtils.createIndex(indexid,
                        SqlCreateTableColumn.IDX_SERIAL_NAME, tableid, true,
                        indexKeys, tableSpaceID, null, null) + 1;
            }
        }

        if (parentTable == null) {
            // We might have a user, who has not specified any primary key to
            // the table
            if (pkList != null && !pkList.isEmpty()) {
                SqlCreateIndexKey[] indexKeys = new SqlCreateIndexKey[pkList
                        .size()];
                for (int i = 0; i < indexKeys.length; i++) {
                    String colName = pkList.get(i);
                    indexKeys[i] = new SqlCreateIndexKey(Integer
                            .parseInt(colNameColid.get(colName.toUpperCase())
                                    .toString()), colName, null, null);
                }
                indexid = MetaUtils.createIndex(indexid, "PK_IDX_" + tableName, 
                		tableid, true, indexKeys, tableSpaceID, null, null);

                // -------------------------------
                // xsysconstraints for primary key
                // -------------------------------
                if (constid == -1) {
                    String sqlCommand = "SELECT max(constid) FROM xsysconstraints";
                    ResultSet rs = MetaData.getMetaData().executeQuery(
                            sqlCommand);
                    try {
                        rs.next();
                        constid = rs.getInt(1);
                    } finally {
                        rs.close();
                    }
                }
                // checkissoft
                String xsysconstraints = "INSERT INTO xsysconstraints "
                        + "(constid, tableid, consttype, Idxid, issoft, constname) "
                        + "VALUES (" + ++constid + "," + tableid + "," + "'P'"
                        + "," + (indexid++) + "," + "0" + "," +
                        "'" + pkConstraintName + "'" + ")";
                MetaData.getMetaData().executeUpdate(xsysconstraints);
            }
        }

        if (parentTable == null) {
            if (fkDefinitions != null && fkDefinitions.size() > 0) {
                for (ForeignKeyHandler aForeignKeyDefinition : fkDefinitions) {
                    // we created the index on the refering table
                    // so update meta data

                    // the index needs to be created on the refering table
                    List<String> columnNames = aForeignKeyDefinition
                            .getLocalColumnNames();
                    SqlCreateIndexKey[] indexKeys = new SqlCreateIndexKey[columnNames
                            .size()];
                    for (int i = 0; i < indexKeys.length; i++) {
                        String colName = (String) columnNames.get(i);
                        indexKeys[i] = new SqlCreateIndexKey(Integer
                                .parseInt(colNameColid.get(
                                        colName.toUpperCase()).toString()),
                                colName, null, null);
                    }
                    indexid = MetaUtils.createIndex(indexid,
                            aForeignKeyDefinition.getConstraintName(), tableid,
                            true, indexKeys, tableSpaceID, null, null);
                    // done with index

                    // ----------------------------
                    // xsysconstraints
                    // ----------------------------

                    String constraintName = aForeignKeyDefinition
                            .getConstraintName();
                    SysTable referedTable = aForeignKeyDefinition
                            .getForeignTable();
                    if (constid == -1) {
                        String sqlCommand = "SELECT max(constid) FROM xsysconstraints";
                        ResultSet rs = MetaData.getMetaData().executeQuery(
                                sqlCommand);
                        try {
                            rs.next();
                            constid = rs.getInt(1);
                        } finally {
                            rs.close();
                        }
                    }

                    String xsysconstraints = "INSERT INTO xsysconstraints "
                            + "(constid, constname, tableid, consttype, idxid, issoft) "
                            + "VALUES ("
                            + ++constid
                            + ","
                            + "'"
                            + constraintName
                            + "',"
                            + tableid
                            + ","
                            + "'R'"
                            + ","
                            + (indexid++)
                            + ", "
                            + (aForeignKeyDefinition.isSoft(partColumn, partMap) ? "1" : "0")
                            + ")";
                    MetaData.getMetaData().executeUpdate(xsysconstraints);

                    // --------------------------
                    // XsysReferences
                    // --------------------------
                    int indexIdToUse;
                    SysIndex indexToUse = referedTable
                            .getPrimaryOrUniqueIndex(aForeignKeyDefinition
                                    .getForeignColumnNames());
                    if (indexToUse == null
                            || (indexIdToUse = indexToUse.idxid) == -1) {
                        throw new XDBServerException(
                                ErrorMessageRepository.NO_PRIMARY_UNQIUE_INDEX
                                        + "( " + referedTable.getTableName()
                                        + " ) ",
                                0,
                                ErrorMessageRepository.NO_PRIMARY_UNQIUE_INDEX_CODE);
                    }

                    if (refid < 0) {
                        sqlStatement = "SELECT max(refid) FROM xsysreferences";
                        ResultSet rs = MetaData.getMetaData().executeQuery(
                                sqlStatement);
                        try {
                            rs.next();
                            refid = rs.getInt(1) + 1;
                        } finally {
                            rs.close();
                        }
                    } else {
                        refid++;
                    }
                    String xSysRefrencesStr = "INSERT INTO xsysreferences "
                            + "(refid, constid, reftableid, refidxid) VALUES ("
                            + refid + "," + constid + ","
                            + referedTable.getTableId() + "," + indexIdToUse
                            + ")";

                    MetaData.getMetaData().executeUpdate(xSysRefrencesStr);

                    // ----------------------------
                    // XSysForeignKeys
                    // ----------------------------
                    if (fkeyid < 0) {
                        sqlStatement = "SELECT max(fkeyid) FROM xsysforeignkeys";
                        ResultSet rs = MetaData.getMetaData().executeQuery(
                                sqlStatement);
                        try {
                            rs.next();
                            fkeyid = rs.getInt(1) + 1;
                        } finally {
                            rs.close();
                        }
                    }
                    String foreignKeysBase = "INSERT INTO xsysforeignkeys "
                            + "(fkeyid, Refid, FKeyseq, Colid, Refcolid ) VALUES ";

                    for (int i = 0; i < aForeignKeyDefinition
                            .getForeignColumnNames().size(); i++) {
                        SysColumn refSysColumn = referedTable
                                .getSysColumn(aForeignKeyDefinition
                                        .getForeignColumnNames().get(i));
                        int refColumnid = refSysColumn.getColID();
                        Object columnid = colNameColid
                                .get(aForeignKeyDefinition
                                        .getLocalColumnNames().get(i)
                                        .toUpperCase());

                        // build value list for insert statement
                        String anSql = foreignKeysBase + "(" + (fkeyid++) + ","
                                + refid + "," + (i + 1) + "," + columnid + ","
                                + refColumnid + ")";

                        MetaData.getMetaData().executeUpdate(anSql);
                    }
                }
            }
        }
        //
        // xsyschecks
        //
        if (checkList != null && checkList.size() > 0) {
            String insertConstraint = "INSERT INTO xsysconstraints "
                    + "(constid, tableid, consttype, idxid, issoft,constname) VALUES ";
            String insertCheck = "INSERT INTO xsyschecks "
                    + "(checkid, constid, seqno, checkstmt) VALUES ";
            int checkid;
            if (constid == -1) {
                ResultSet rs = MetaData.getMetaData().executeQuery(
                        "select max(constid) from xsysconstraints");
                try {
                    rs.next();
                    constid = rs.getInt(1);
                } finally {
                    rs.close();
                }
            }
            ResultSet rs = MetaData.getMetaData().executeQuery(
                    "select max(checkid) from xsyschecks");
            try {
                rs.next();
                checkid = rs.getInt(1);
            } finally {
                rs.close();
            }
            int i = 0;
            for (Iterator<String> it = checkList.iterator(); it.hasNext();) {
                String checkDef = it.next();
                sqlStatement = insertConstraint + "(" + ++constid + ", "
                        + tableid + ", 'C', null, 0 ," + "'"
                        + checkConstraintNames.get(i)
                        + "'" + ")";
                MetaData.getMetaData().executeUpdate(sqlStatement);
                sqlStatement = insertCheck + "(" + ++checkid + ", " + constid
                        + ", 1, '" + checkDef.replaceAll("'", "''") + "')";
                MetaData.getMetaData().executeUpdate(sqlStatement);
                i++;
            }
        }
    }

    /**
     * refresh() Refreshes the MetaData cahce by reading in the table
     * information just created
     */
    public void refresh() throws Exception {
        SysTable aSysTable = new SysTable(database, tableid, tableName, 0, 
        		(short) partScheme, partColumn, client.getCurrentUser(),
                parentTableID, tableSpaceID, null);
        synchronized (database.getScheduler()) {
            database.getLm().add(aSysTable, client);
        }
        database.addSysTable(aSysTable);
        // Also add the table to the lock manager
        aSysTable.readTableInfo();
        if (fkDefinitions != null) {
	        for (ForeignKeyHandler aFkDef : fkDefinitions) {
	            aSysTable = aFkDef.getForeignTable();
	            aSysTable.updateCrossReferences();
	        }
        }
    }
}
