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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBSecurityException;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.partitions.HashPartitionMap;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.metadata.partitions.ReplicatedPartitionMap;
import org.postgresql.stado.metadata.partitions.RobinPartitionMap;
import org.postgresql.stado.parser.SqlCreateTableColumn;
import org.postgresql.stado.parser.handler.IdentifierHandler;


public class SysTable {
    private static final XLogger logger = XLogger.getLogger(SysTable.class);

    public static final int PAGE_SIZE = 8096;

    // list partition types
    // for now, only worry about first 2
    public static final short PTYPE_ONE = 1; // not partitioned; all on one
    // node

    public static final short PTYPE_LOOKUP = 2; // lookup table, a copy is on
    // each node

    public static final short PTYPE_HASH = 3; // hash function

    public static final short PTYPE_RANGE = 4; // range/expression definition

    public static final short PTYPE_ROBIN = 5; // round robin

    /**
     * The default policy is as follows 1.If the table has a primary key then
     * choose the first primary key. -- primary keys cannot have blobs in them.
     *
     * 2.If the table does not have a primary key choose the first non-blob or
     * long column.
     *
     * 3.If the table does not have any such column make it on the lowest valued
     * node. - this is never used.
     */
    public static final short PTYPE_DEFAULT = 6;

    // Given a comma separated string of tables, determines which
    // table to base partitioning on
    // ------------------------------------------------------------------------
    static public SysTable getPartitionTable(String tableString,
            SysDatabase database) {
        String checkTables[];
        SysTable checkSysTab = null;
        SysTable sysTab = null;
        // Could be dealing with LOOKUPs only,
        // or partitioned + with a lookup
        // or partitioned with parent

        checkTables = tableString.split(",");

        // Handle multiple lookups, by looking at all tables in list
        // and try to choose the non-lookup table as the one to use
        // We do not need to worry about cases like PTYPE_ONE and PTYPE_HASH
        // together, they should never occur.
        for (int i = 0; i < checkTables.length; i++) {
            checkSysTab = database.getSysTable(checkTables[i]);

            if (i == 0) {
                sysTab = checkSysTab;
            } else {
                if (sysTab.getPartitionScheme() == SysTable.PTYPE_LOOKUP) {
                    if (checkSysTab.getPartitionScheme() != SysTable.PTYPE_LOOKUP) {
                        sysTab = checkSysTab;
                    }
                }
            }
        }

        return sysTab;
    }

    // Immutable fields, parameters of the constructor.
    // No need to synchronize
    private SysDatabase database = null;

    private int tableid;

    private String tableName;

    // for now, user can partition only on a single column
    // (no expressions, etc)
    private short partitionScheme;

    private String partitionColumn = "";

    private PartitionMap partitionMap;

    // private HashMap partitions;

    private long numrows = 0;

    // Estimate of the number of rows per page that will fit on a data page
    private int estRowsPerPage = 0;

    // approx number of chars per row.
    private int rowSize = 0;

    private SysUser owner = null;

    // TODO This is not persistently liniked to the
    // database - there fore if an alter table does something this is not
    // reflected here -- Infact this is true for all data in the meta data
    private SysColumn serialColumn = null;

    private SysSerialIDHandler serialIdHandler;

    private SysRowIDHandler rowIDHandler;

    // Columns
    // We use both here, one to preserve the order,
    // The other to allow parsing to be faster
    private List<SysColumn> sysColumnList;

    private Hashtable<String,SysColumn> sysColumnTable;

    // Keys, indexes
    private SysConstraint primaryConstraint = null;

    private List<SysColumn> rowID = null;

    private List<SysConstraint> sysConstraintsList; // list of constraints defined on this
    // table

    private Vector<SysReference> sysFkReferenceList;// list of references which the
    // constraintList refers too

    private Collection<SysIndex> sysIndexList; // list of indexes defined on
    // this table

    private Vector<SysReference> sysReferencesList; // list of references defined on this
    // table

    private HashMap<SysUser,SysPermission> sysPermissions; // list of permissions granted on this table

    private List<SysCheck> sysChecks;

    private List<SysTable> childrenTables;

    private String clusteridx;

    private boolean tableIsTemporary = false;

    private boolean tableIsUnlogged = false;

    private boolean isTrueRowID = false;

    /**
     * For subtables (INHERIT)
     */
    private int parentTableID = -1;

    private SysTable parentTable = null;

    private int tablespaceID = -1;

    private SysTablespace tablespace = null;

    public SysTable(SysDatabase database, int tableID, String tableName,
            long numRows, short partitionScheme, String partitionColumn,
            SysUser owner, int parentID, int tablespaceID, String clusteridx) {
        this.database = database;
        tableid = tableID;
        this.tableName = tableName;
        this.numrows = numRows;
        this.partitionScheme = partitionScheme;
        this.partitionColumn = partitionColumn;
        if (partitionScheme == SysTable.PTYPE_LOOKUP) {
            partitionMap = new ReplicatedPartitionMap();
        } else if (partitionScheme == SysTable.PTYPE_HASH) {
            partitionMap = new HashPartitionMap();
        } else if (partitionScheme == SysTable.PTYPE_ROBIN) {
            partitionMap = new RobinPartitionMap();
        } else if (partitionScheme == SysTable.PTYPE_ONE) {
            partitionMap = new ReplicatedPartitionMap();
        }
        this.owner = owner;
        this.parentTableID = parentID;
        this.tablespaceID = tablespaceID;
        this.clusteridx = clusteridx;

        if (owner != null) {
            owner.addOwned(this);
        }

        sysColumnList = new ArrayList<SysColumn>();
        sysColumnTable = new Hashtable<String,SysColumn>();
        sysIndexList = new ArrayList<SysIndex>();
        primaryConstraint = null;
        rowID = null;
        // This reference list only point to the table which
        // references this table - for e.g.

        /**
         * Orders refers Customer
         *
         * Orders customer sysRefList null orders sysFkList customer NULL
         *
         */
        sysReferencesList = new Vector<SysReference>();
        sysConstraintsList = new Vector<SysConstraint>();
        // This will contain the sysreferences of the tables which this table
        // refers
        // for eg. the orders - will refer to customer
        // Then we will have the information about the sysreferences
        sysFkReferenceList = new Vector<SysReference>();
        sysPermissions = new HashMap<SysUser,SysPermission>();
        sysChecks = new ArrayList<SysCheck>();
        childrenTables = new ArrayList<SysTable>();
    }

    private boolean loaded = false;

    void readTableInfo() throws XDBServerException {
        readTableInfo(true);
    }

    /**
     * Reads table info and adds to SysTableList This is also used after a new
     * table is added with CREATE TABLE Must be called from code synchronized on
     * database, before the table appears in the database's list, so do not do
     * any additional synchronization.
     */
    void readTableInfo(boolean force) throws XDBServerException {
        final String method = "readTableInfo";
        logger.entering(method);
        try {

            if (loaded && !force) {
                return;
            }
            // Clear existing data
            sysPermissions = new HashMap<SysUser,SysPermission>();
            // partitions = new HashMap();
            // partitionMap = new PartitionMap(256);
            sysColumnList = new ArrayList<SysColumn>();
            sysColumnTable = new Hashtable<String,SysColumn>();
            sysIndexList = new ArrayList<SysIndex>();
            sysReferencesList = new Vector<SysReference>();
            sysConstraintsList = new Vector<SysConstraint>();
            sysFkReferenceList = new Vector<SysReference>();
            sysChecks = new ArrayList<SysCheck>();
            childrenTables = new ArrayList<SysTable>();
            primaryConstraint = null;
            serialColumn = null;
            rowID = null;
            isTrueRowID = false;
            loaded = true;

            updateParentTableReference();
            readPermissionsInfo();
            readPartitioningInfo();
            readColumnInfo();
            readIndexInfo(); // responsible for reading indexkeys as well
            readConstraintsInfo(); // verifies indexes that must exist for the
            // constraint
            refreshAssociatedInfo(); // get numrows, etc
        } catch (Exception se) {

            throw new XDBServerException(
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR, se,
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR_CODE);

        }

        finally {
            logger.exiting(method);
        }
    }

    /**
     * @throws XDBServerException
     */
    public void readPartitioningInfo() throws XDBServerException {
        final String method = "readPartitioningInfo";
        logger.entering(method);
        try {

            if (parentTableID > -1) {
                return;
            }
            partitionMap.readMapFromMetadataDB(MetaData.getMetaData(), this);
        } catch (SQLException e) {
            throw new XDBServerException(
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR, e,
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR_CODE);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @throws XDBServerException
     */
    private void readColumnInfo() throws XDBServerException {
        final String method = "readColumnInfo";
        logger.entering(method);
        try {
            // get column information
            ResultSet aColumnRS;
            aColumnRS = MetaData.getMetaData().executeQuery(
                    "SELECT * from xsyscolumns WHERE tableid = " + tableid
                    + " order by colseq");

            SysColumn aSysColumn;
            while (aColumnRS.next()) {
                String defaultexpr = aColumnRS.getString("defaultexpr");
                if (defaultexpr != null) {
                    defaultexpr = defaultexpr.trim();
                }

                String nativecoldef = aColumnRS.getString("nativecoldef");
                if (nativecoldef != null) {
                    nativecoldef = nativecoldef.trim();
                }

                aSysColumn = new SysColumn(this, aColumnRS.getInt("colid"),
                        aColumnRS.getInt("colseq"), aColumnRS.getString(
                        "colname").trim(), aColumnRS.getInt("coltype"),
                        aColumnRS.getInt("collength"), aColumnRS
                        .getInt("colscale"), aColumnRS
                        .getInt("colprecision"), aColumnRS
                        .getBoolean("isnullable"), aColumnRS
                        .getBoolean("isserial"), nativecoldef,
                        aColumnRS.getFloat("selectivity"), defaultexpr);

                //set isWithTimeZone true if column definition contains "WITH TIME ZONE"
                if(nativecoldef.toUpperCase().contains("WITH TIME ZONE")) {
                    aSysColumn.isWithTimeZone = true;
                }

                addSysColumn(aSysColumn);
            }
        } catch (SQLException e) {
            throw new XDBServerException(
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR, e,
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR_CODE);
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @throws XDBServerException
     */
    void readPermissionsInfo() throws XDBServerException {
        final String method = "readPermissionsInfo";
        logger.entering(method);
        try {

            if (parentTableID > -1) {
                return;
            }
            ResultSet aRS = MetaData.getMetaData().executeQuery(
                    "SELECT * from xsystabprivs WHERE tableid = " + tableid);
            sysPermissions.clear();
            while (aRS.next()) {
                SysUser user = aRS.getString("userid") == null ? null :
                    database.getSysUser(aRS.getInt("userid"));
                SysPermission aSysPermission = new SysPermission(this, user, aRS
                        .getInt("privid"), aRS.getString("selectpriv"), aRS
                        .getString("insertpriv"), aRS.getString("updatepriv"),
                        aRS.getString("deletepriv"), aRS
                        .getString("referencespriv"), aRS
                        .getString("indexpriv"), aRS
                        .getString("alterpriv"));

                sysPermissions.put(user, aSysPermission);
                if (user != null)
                    user.addGranted(this);
            }
        } catch (SQLException e) {
            throw new XDBServerException(
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR, e,
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR_CODE);
        } finally {
            logger.exiting(method);
        }
    }

    public void ensurePermission(SysUser user, short privilege)
    throws XDBSecurityException {
        if (!checkPermission(user, privilege)) {
            XDBSecurityException ex = new XDBSecurityException("Table "
                    + tableName + ": Access denied");
            logger.throwing(ex);
            throw ex;
        }
    }

    public boolean checkPermission(SysUser user, short privilege) {
        if (isTemporary()
                // Bypass all security checks for DBA users
                || user != null
                && user.getUserClass() == SysLogin.USER_CLASS_DBA
                // Only owner has rights on table if no permission was set
                || sysPermissions.isEmpty() && user == owner) {
            return true;
        }
        SysPermission userPerm = getSysPermission(user);
        if (userPerm != null && userPerm.checkPermission(privilege)) {
            return true;
        } else {
            // Check public permission
            userPerm = getSysPermission(null);
            return userPerm != null && userPerm.checkPermission(privilege);
        }
    }

    public Collection<SysIndex> getSysIndexList() {
        return sysIndexList;
    }

    /**
     * read and add indexes defined on this table
     **/
    private void readIndexInfo() throws Exception {

        String sqlquery = "SELECT * FROM xsysindexes " + " WHERE tableid = "
        + tableid;
        ResultSet rs = MetaData.getMetaData().executeQuery(sqlquery);

        sysIndexList.clear();

        while (rs.next()) {
            SysIndex anIndex = new SysIndex(this);
            anIndex.idxid = rs.getInt("idxid");
            anIndex.idxname = rs.getString("idxname").trim();
            anIndex.tableid = tableid;
            anIndex.keycnt = rs.getInt("keycnt");
            anIndex.issyscreated = rs.getInt("issyscreated");
            anIndex.tablespaceID = rs.getInt("tablespaceid");
            anIndex.usingtype = rs.getString("usingtype");
            anIndex.wherepred = rs.getString("wherepred");

            if (rs.wasNull()) {
                anIndex.tablespaceID = -1;
            }
            // get the keys info for this index
            anIndex.readIndexKeysInfo();

            // set if the index is constrained or not.
            // contrained means index is used for either
            // primary or foreign keys.
            anIndex.is_constrained = isIndexConstrained(anIndex.idxid);

            // add this index to list of indexes
            sysIndexList.add(anIndex);
        }
    }

    /**
     * Specifies if index is constrained in any way interface if the index is
     * used for references or primary keys
     */
    private boolean isIndexConstrained(int idxid) throws XDBServerException {

        try {
            // get constrained info correclty
            ResultSet rs = MetaData.getMetaData().executeQuery(
                    "SELECT COUNT(*) FROM xsysreferences WHERE refidxid="
                    + idxid);

            if (rs.next()) {
                int cntRefs = rs.getInt(1);
                if (cntRefs > 0) {
                    return true;
                }
            }

            // also check if this index participates in a primary key
            rs = MetaData.getMetaData().executeQuery(
                    "SELECT COUNT(*) FROM xsysconstraints WHERE consttype='P' AND idxid="
                    + idxid);

            if (rs.next()) {
                int cntPrim = rs.getInt(1);
                if (cntPrim > 0) {
                    return true;
                }
            }
        } catch (SQLException se) {
            String errorString = ErrorMessageRepository.SYSINDEX_READ_FAILURE
            + " ( " + idxid + " )";
            throw new XDBServerException(errorString, se,
                    ErrorMessageRepository.SYSINDEX_READ_FAILURE_CODE);
        }

        return false;
    }

    /**
     * read and add Constraints defined on this table
     **/
    private void readConstraintsInfo() throws Exception {
        String sqlquery = "SELECT * FROM xsysconstraints "
            + " WHERE tableid = " + tableid;

        ResultSet rs = MetaData.getMetaData().executeQuery(sqlquery);

        // empty all constraints (if any)
        sysConstraintsList.clear();

        while (rs.next()) {
            String constname = rs.getString("constname");
            if (constname != null) {
                constname = constname.trim();
            }

            SysConstraint aConstraint = new SysConstraint(this, rs
                    .getInt("constid"), constname, rs.getString("consttype")
                    .charAt(0), rs.getInt("idxid"), rs.getInt("issoft")); // soft
            // constraint

            // verify that the index-id matches one on the table

            SysIndex sysIndex = null;
            int idxid = aConstraint.getIdxID();
            if (idxid > 0) {
                sysIndex = getSysIndex(idxid);
            }
            sysConstraintsList.add(aConstraint);

            // set the idxtype correctly for the index obtained
            // set the indextype to be stronger
            // P rimary > U unique > R reference
            switch (aConstraint.getConstType()) {
            case 'P':
                sysIndex.idxtype = 'P';
                break;
            case 'U':
                if (sysIndex.idxtype != 'P') {
                    sysIndex.idxtype = 'U';
                }
                break;
            case 'R':
                if (sysIndex.idxtype != 'P' && sysIndex.idxtype != 'U') {
                    sysIndex.idxtype = 'R';
                }
                break;
            default:
                // Do nothing
            }

            // additionally, if this constraint is a Primary key
            // update the primaryKey value
            if (aConstraint.getConstType() == 'P') {
                primaryConstraint = aConstraint;
            }

            // This function will fill the sys fk reference list for this table
            // thus giving us information about the tables to which this table
            // refers
            String sqlReferenceQuery = "SELECT * FROM xsysreferences "
                + " WHERE constid = " + aConstraint.getConstID();
            ResultSet rsReferenceList = MetaData.getMetaData().executeQuery(
                    sqlReferenceQuery);
            while (rsReferenceList.next()) {
                SysReference aSysReference = new SysReference(aConstraint,
                        rsReferenceList.getInt("refid"), rsReferenceList
                        .getInt("reftableid"), rsReferenceList
                        .getInt("refidxid"));
                aSysReference.readForeignKeysInfo();
                sysFkReferenceList.add(aSysReference);
            }
            String sqlChecksQuery = "SELECT * FROM xsyschecks "
                + " WHERE constid = " + aConstraint.getConstID()
                + " ORDER BY seqno";
            ResultSet rsCheckList = MetaData.getMetaData().executeQuery(
                    sqlChecksQuery);
            while (rsCheckList.next()) {
                SysCheck aSysCheck = new SysCheck(this, rsCheckList
                        .getInt("checkid"), rsCheckList.getInt("constid"),
                        rsCheckList.getInt("seqno"), rsCheckList.getString(
                        "checkstmt").trim());
                sysChecks.add(aSysCheck);
            }
        }
    }

    /**
     * add foreign keys info in this table - The infromation in this table
     */
    private void readReferencesInfo() throws XDBServerException {
        String sqlquery = "SELECT tableid, xsysconstraints.constid "
            + "FROM xsysconstraints JOIN xsysreferences "
            + "ON xsysconstraints.constid = xsysreferences.constid "
            + "WHERE reftableid = " + tableid;
        ResultSet rs = MetaData.getMetaData().executeQuery(sqlquery);

        // empty all old reference data (if any)
        if (sysReferencesList.size() > 0) {
            sysReferencesList.removeAllElements();
        }
        try {
            try {
                while (rs.next()) {
                    int tableid = rs.getInt("tableid");
                    int constid = rs.getInt("constid");
                    SysReference aRef = database.getSysTable(tableid)
                    .getFkSysReference(constid);
                    // get the reference keys info for this index
                    aRef.readForeignKeysInfo();

                    // add this index to list of indexes
                    sysReferencesList.addElement(aRef);
                }
            } finally {
                rs.close();
            }
        } catch (SQLException se) {
            String errorMessage = ErrorMessageRepository.TABLE_DEF_CORRUPTED
            + " ( " + tableName + " )";
            throw new XDBServerException(errorMessage, se,
                    XDBServerException.SEVERITY_HIGH);
        }
    }

    /**
     * Caches index, contraints and foreign keys references assoicated to this
     * table.
     *
     * This is done in the following order -
     *
     * 1) Reads Indexes defined on this table. 2) Reads Constraints defined on
     * this table. 3) Reads foriegn references defined on this table.
     *
     * PRE-CONDITION tableid must be set correctly.
     */
    void refreshAssociatedInfo() throws Exception {
        updateColumnIndexInfo();
        // Next method requires all tables have been loaded
        // Actually it is called from SysDatabase.readDatabaseInfo
        // updateDistributedForeignKeyChecks();
        updateSizeInfo();
    }

    /**
     * get index information for each column
     * NOTE - This is done from the cache
     **/
    private void updateColumnIndexInfo() {
        // Loop through and update Column info based on index information
        int indexType;

        for (SysIndex theIndex : sysIndexList) {
            List<SysIndexKey> indexKeys = theIndex.getIndexKeys();
            for (int ikeys = 0; ikeys < indexKeys.size(); ikeys++) {
                SysIndexKey theKey = indexKeys.get(ikeys);
                indexType = MetaData.INDEX_TYPE_NONE;
                if (theIndex.keycnt == 1 && theKey.idxkeyseq == 0) {
                    if (theIndex.idxtype == 'P') {
                        indexType = MetaData.INDEX_TYPE_PRIMARY_KEY;
                    } else if (theIndex.idxtype == 'U') {
                        indexType = MetaData.INDEX_TYPE_UNIQUE;
                    } else {
                        indexType = MetaData.INDEX_TYPE_SINGLE;
                    }
                } else if (theIndex.keycnt > 1 && theKey.idxkeyseq == 0) {
                    indexType = MetaData.INDEX_TYPE_FIRST_IN_COMPOSITE;
                } else if (theIndex.keycnt > 1 && theKey.idxkeyseq > 0) {
                    indexType = MetaData.INDEX_TYPE_NOT_FIRST_IN_COMPOSITE;
                }

                // Get the column so we can compare
                SysColumn aSysColumn = getSysColumn(theKey.colid);

                // If this one is better, update column
                if (indexType < aSysColumn.getIndexType()) {
                    aSysColumn.setIndexType(indexType);
                }
            }
        }
    }

    void updateCrossReferences() {
        readReferencesInfo(); // also reads the foriegn keys info
    }

    /**
     * Updates table row size info
     */
    private void updateSizeInfo() {
        int rowLength = 0;
        int colLength;

        for (Object element : getColumns()) {
            SysColumn aSysColumn = (SysColumn) element;

            colLength = aSysColumn.getColumnLength();

            rowLength += colLength;
        }

        // Save table info
        rowSize = rowLength;

        // estimate the number of rows per page
        estRowsPerPage = rowSize == 0 ? 0 : PAGE_SIZE / rowSize;
    }

    /**
     * For reading or when new table is created
     *
     * @return The PartitionMap of this table
     */
    public PartitionMap getPartitionMap() {
        return parentTable == null ? partitionMap : parentTable
                .getPartitionMap();
    }

    void setPartitioning(String partitionColumn, short partititonScheme,
            PartitionMap partitionMap) {
        if (parentTable == null) {
            this.partitionColumn = partitionColumn;
            this.partitionScheme = partititonScheme;
            this.partitionMap = partitionMap;
        }
    }

    /**
     * @return Returns the partitionScheme.
     */
    public short getPartitionScheme() {
        return parentTable == null ? partitionScheme : parentTable
                .getPartitionScheme();
    }

    /**
     * @return Returns the partitionColumn.
     */
    public String getPartitionColumn() {
        return parentTable == null ? partitionColumn : parentTable
                .getPartitionColumn();
    }

    /**
     * This will return the partitioned column to the caller and will throw a
     * XDBServerException if it finds that the table is not supposed to have a
     * partition column.
     *
     * It assumes that the partion information is always set right.
     *
     * @return
     */
    public SysColumn getPartitionedColumn() {
        if (parentTable == null) {
            if (partitionScheme == PTYPE_HASH || partitionScheme == PTYPE_RANGE) {
                return this.getSysColumn(partitionColumn);
            } else {
                return null;
            }
        } else {
            return parentTable.getPartitionedColumn();
        }
    }

    private Collection<DBNode> nodeIDs2DBNodes(Collection<Integer> nodeIDs) {
        Collection<DBNode> result = new ArrayList<DBNode>(nodeIDs.size());
        for (Integer nodeID : nodeIDs) {
            result.add(database.getDBNode(nodeID));
        }
        return result;
    }

    /**
     * return a Vector containg all nodes used
     **/
    public Collection<DBNode> getNodeList() {
        if (parentTable == null) {
            return nodeIDs2DBNodes(partitionMap.allPartitions());
        } else {
            return parentTable.getNodeList();
        }
    }

    public Collection<DBNode> getJoinNodeList() {
        if (parentTable == null) {
            return nodeIDs2DBNodes(partitionMap.joinPartitions());
        } else {
            return parentTable.getJoinNodeList();
        }
    }

    public Collection<DBNode> getNode(String sValue) {
        if (parentTable == null) {
            return nodeIDs2DBNodes(partitionMap.getPartitions(sValue));
        } else {
            return parentTable.getNode(sValue);
        }
    }

    public Collection<DBNode> findNode(String sValue) {
        if (parentTable == null) {
            return nodeIDs2DBNodes(partitionMap.findPartitions(sValue));
        } else {
            return parentTable.getNode(sValue);
        }
    }

    /**
     *
     */
    void addSysColumn(SysColumn aSysColumn) {
        if (getSysColumn(aSysColumn.getColName()) == null) {
            sysColumnTable.put(aSysColumn.getColName(), aSysColumn);
            sysColumnList.add(aSysColumn);
        } else {
            XDBServerException ex = new XDBServerException("Column \""
                    + aSysColumn.getColName()
                    + "\" already exists in the table \"" + tableName + "\"");
            logger.throwing(ex);
            throw ex;
        }
    }


    /**
     *
     */
    public SysColumn getSysColumn(int colid) {
        SysColumn aSysColumn = null;
        for (int i = 0; i < sysColumnList.size(); i++) {
            aSysColumn = sysColumnList.get(i);
            if (aSysColumn.getColID() == colid) {
                return aSysColumn;
            }
        }
        return parentTable == null ? null : parentTable.getSysColumn(colid);
    }

    // ------------------------------------------------------------------------
    // GetSysColumn
    public SysColumn getSysColumn(String sColumnName) {
        SysColumn aSysColumn = sysColumnTable.get(sColumnName);
        if (aSysColumn == null && parentTable != null) {
            aSysColumn = parentTable.getSysColumn(sColumnName);
        }
        return aSysColumn;
    }

    /*
     * Simply returns the column list.
     */
    public List<SysColumn> getColumns() {
        List<SysColumn> columns;
        if (parentTable == null) {
            columns = sysColumnList;
        } else {
            columns = new ArrayList<SysColumn>();
            columns.addAll(parentTable.getColumns());
            columns.addAll(sysColumnList);
        }
        return Collections.unmodifiableList(columns);
    }

    // Any table can have atmost one serial column - this function
    // will get hold of that
    // particular sys column.
    public SysColumn getSerialColumn() {
        if (serialColumn == null) {
            Enumeration colListEnumeration = this.sysColumnTable.elements();
            while (colListEnumeration.hasMoreElements()) {
                SysColumn aSysColumn = (SysColumn) colListEnumeration
                .nextElement();
                if (aSysColumn.isSerial() == true) {
                    serialColumn = aSysColumn;
                    break;
                }
            }
        }
        if (serialColumn == null && parentTable != null) {
            serialColumn = parentTable.getSerialColumn();
        }
        return serialColumn;
    }

    /**
     * @return the SysIndex used for the primary key
     *
     * Method - iterate over the constraints list to find a Primary index.
     * Note - the index used for the primary key may not be
     * flagged as 'P' since it may be an existing unique
     * index used for the key; hence use the conatraints to find it.
     **/
    public SysIndex getPrimaryIndex() {
        return primaryConstraint == null ? null : getSysIndex(primaryConstraint
                .getIdxID());
    }

    /**
     * Get the primary key columns for this table SysColumn
     */
    public List<SysColumn> getPrimaryKey() {
        SysIndex index = getPrimaryIndex();
        return index == null ? null : index.getKeyColumns();
    }

    /**
     * Returns a SysIndex object having indexid = idxid
     */
    public SysIndex getSysIndex(int idxid) {
        if (sysIndexList != null) {
            for (SysIndex anIndex : sysIndexList) {
                if (anIndex != null) {
                    if (anIndex.idxid == idxid) {
                        return anIndex;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Returns a SysIndex object having index name = idxName
     */
    public SysIndex getSysIndex(String idxName) {
        if (sysIndexList != null) {
            for (SysIndex anIndex : sysIndexList) {
                if (anIndex != null) {
                    if (anIndex.idxname.equalsIgnoreCase(idxName)) {
                        return anIndex;
                    }
                }
            }
        }
        return null;
    }

    /**
     * @return a Vector of SysIndex's on this table having
     * the index keys defined exactly as columnNames
     */
    public List<SysIndex> getSysIndexes(List<String> columnNames) {
        List<SysIndex> result = new ArrayList<SysIndex>();
        // convert columnNames to ids for faster lookup
        int colids[] = new int[columnNames.size()];

        for (int i = 0; i < colids.length; i++) {
            String colName = columnNames.get(i);
            SysColumn aCol = getSysColumn(colName);
            colids[i] = aCol.getColID();
        }

        // now lets checkup the indexes
        iterateIndexes: for (SysIndex anIndex : sysIndexList) {
            // we have an index
            // check if this index has number of keys= #cols requested
            if (anIndex.keycnt == colids.length) {
                // this may be of interest to us - check if colids match
                List<SysIndexKey> keys = anIndex.getIndexKeys();
                for (int k = 0; k < anIndex.keycnt; k++) {
                    SysIndexKey sysKey = keys.get(k);
                    if (sysKey.colid != colids[k]) {
                        continue iterateIndexes;
                    }
                }
                // if all colids matched - rethurn this index
                result.add(anIndex);
            }
        }
        // return all indexes found
        return result;
    }

    /**
     * @return the SysIndex defined on the columsNames
     * of table Tablename that is a Primary Index (preferably) if not
     * it returns a 'unique index' (CREATE UNIQUE INDEX ...)
     * if one exists, otherwise null
     */
    public SysIndex getPrimaryOrUniqueIndex(List<String> columnNames) {
        SysIndex candidate = null;
        SysIndex primaryIndex = getPrimaryIndex();
        for (SysIndex anIndex : getSysIndexes(columnNames)) {
            if (anIndex == primaryIndex) {
                return primaryIndex;
            } else if (anIndex.idxtype == 'U') {
                if (candidate == null
                        || anIndex.getIndexLength() < candidate
                        .getIndexLength()) {
                    candidate = anIndex;
                }
            }
        }
        return candidate;
    }

    /**
     * This function returns unique index and primary index defined on this
     * table
     *
     */
    public Vector<SysIndex> getAllUniqueAndPrimarySysIndexes() {
        Vector<SysIndex> indexesToReturn = new Vector<SysIndex>();
        SysIndex primaryIndex = getPrimaryIndex();
        if (primaryIndex != null) {
            indexesToReturn.add(primaryIndex);
        }
        for (SysIndex aSysIndex : sysIndexList) {
            if (aSysIndex.idxtype == 'U') {
                indexesToReturn.add(aSysIndex);
            }
        }
        return indexesToReturn;
    }

    public List<SysColumn> getRowID() {
        if (rowID == null) {
            SysColumn xrowid = getSysColumn(SqlCreateTableColumn.XROWID_NAME);
            if (xrowid != null) {
                rowID = Collections.singletonList(xrowid);
                isTrueRowID = true;
            } else {
                rowID = getPrimaryKey();
                if (rowID == null) {
                    SysIndex bestIndex = null;
                    int bestLength = 0;
                    for (SysIndex index : sysIndexList) {
                        if (index.idxtype == 'U') {
                            if (bestIndex == null
                                    || bestLength > index.getIndexLength()) {
                                bestIndex = index;
                                bestLength = index.getIndexLength();
                            }
                        }
                    }
                    if (bestIndex == null) {
                        rowID = sysColumnList;
                    } else {
                        rowID = bestIndex.getKeyColumns();
                        isTrueRowID = true;
                    }
                }
            }
        }
        return rowID;
    }

    /**
     * @return true if the index specified by indexid
     * is referenced somewhere
     **/
    public boolean isIndexReferenced(int indexId) {
        for (int i = 0; i < sysReferencesList.size(); i++) {
            SysReference aRef = sysReferencesList.elementAt(i);
            if (aRef.getRefIdxID() == indexId) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if theIndex is referenced somewhere
     **/
    public boolean isIndexReferenced(SysIndex theIndex) {
        return isIndexReferenced(theIndex.idxid);
    }

    /**
     * @return a constraint on this table by the given name
     **/
    public SysConstraint getConstraint(String constraintName) {
        for (int i = 0; i < sysConstraintsList.size(); i++) {
            SysConstraint aConstraint = sysConstraintsList
            .get(i);
            if (aConstraint.getConstName() != null) {
                if (constraintName.compareToIgnoreCase(aConstraint
                        .getConstName()) == 0) {
                    return aConstraint;
                }
            }
        }
        return null;
    }

    /**
     * @return a constraint on this table by the constraintId
     **/
    public SysConstraint getConstraint(int constraintId) {
        for (int i = 0; i < sysConstraintsList.size(); i++) {
            SysConstraint aConstraint = sysConstraintsList
            .get(i);
            if (constraintId == aConstraint.getConstID()) {
                return aConstraint;
            }
        }
        return null;
    }

    public List<SysConstraint> getConstraintList() {
        return sysConstraintsList;
    }

    /**
     * @return the SysConstraint coressponding to the
     * primary key defined on this table.
     **/
    public SysConstraint getPrimaryConstraint() {
        return primaryConstraint;
    }

    public Vector<SysConstraint> getFkConstList() {
        Vector<SysConstraint> vConstListFk = new Vector<SysConstraint>();
        for (SysConstraint aSysConstraint : sysConstraintsList) {
            if (aSysConstraint.getConstType() == 'R') {
                // We has a FK constraint
                vConstListFk.add(aSysConstraint);
            }
        }

        return vConstListFk;
    }

    /**
     * This will return the sys reference object
     *
     * @param constId
     * @return
     */
    public SysReference getFkSysReference(int constId) {
        Enumeration listOfFkRef = sysFkReferenceList.elements();
        while (listOfFkRef.hasMoreElements()) {
            SysReference aSysReference = (SysReference) listOfFkRef
            .nextElement();
            if (aSysReference.getConstraint().getConstID() == constId) {
                return aSysReference;
            }
        }
        return null;
    }

    /*
     * Returns information about foreign references
     */
    public Vector<SysReference> getSysFkReferenceList() {
        return sysFkReferenceList;
    }

    // ------------------------------------------------------------------------
    // returns a vector of Sysreferences
    public Vector<SysReference> getSysReferences() {
        return sysReferencesList;
    }

    /**
     * This should return us the reffering sys columns , that implies columns
     * belonging to this table and which are reffering to some column from the
     * other table
     *
     * @return
     */
    public Hashtable<SysColumn,SysColumn> getReferringSys_ReferrencedColumns_Map() {
        Hashtable<SysColumn,SysColumn> hashMap = new Hashtable<SysColumn,SysColumn>();
        // Get all the SysReference Objects from this list
        Enumeration aListOfReference = sysFkReferenceList.elements();
        // Incase we sme references
        while (aListOfReference.hasMoreElements()) {
            SysReference aSysReference = (SysReference) aListOfReference
            .nextElement();
            Vector fkKeys = aSysReference.getForeignKeys();
            Enumeration fkKeysEn = fkKeys.elements();
            // while we have more elements
            while (fkKeysEn.hasMoreElements()) {
                SysForeignKey aSysForeignKey = (SysForeignKey) fkKeysEn
                .nextElement();
                SysColumn aReferringColumn = aSysForeignKey
                .getReferringSysColumn(database);
                SysColumn aReferencedColumn = aSysForeignKey
                .getReferencedSysColumn(database);
                hashMap.put(aReferringColumn, aReferencedColumn);
            }
        }
        return hashMap;
        // 1. It can be possible that there are no sys references, we need to
        // take care of this condtion
        // in that case we will have the hashtable empty.. and that is ok
    }

    /**
     * @return Returns the estRowsPerPage.
     */
    public float getEstRowsPerPage() {
        return parentTable == null ? estRowsPerPage
                : rowSize == 0 ? parentTable.getEstRowsPerPage() : PAGE_SIZE
                        / getRowSize();
    }

    /**
     * @return Returns the rowSize.
     */
    public int getRowSize() {
        return parentTable == null ? rowSize : rowSize
                + parentTable.getRowSize();
    }

    public long getRowCount() {
        return numrows;
    }

    public SysRowIDHandler getRowIDHandler() {
        if (parentTable == null) {
            if (rowIDHandler == null) {
                rowIDHandler = new SysRowIDHandler(this);
            }
            return rowIDHandler;
        } else {
            return parentTable.getRowIDHandler();
        }
    }

    /**
     * What if the user specifies a value for the serial which is already in the
     * list What if the serial value reaches a limit What if the se
     *
     * @param client
     * @return
     */
    public synchronized SysSerialIDHandler getSerialHandler() {
        if (serialIdHandler == null && parentTable != null) {
            serialIdHandler = parentTable.getSerialHandler();
        }
        if (serialIdHandler == null) {
            SysColumn aSysColumn = getSerialColumn();
            if (aSysColumn != null) {
                serialIdHandler = new SysSerialIDHandler(aSysColumn);
            }
        }
        return serialIdHandler;
    }

    public SysDatabase getSysDatabase() {
        return database;
    }

    /*
     * This Function Returns the table ID. If we do not already have the ID. It
     * assumes that the table exists,
     * //------------------------------------------------------------------------incase
     * it is not able to find the tableid, it will throw a SQLException
     */
    public int getSysTableid() throws SQLException, Exception {
        return tableid;
    }

    /**
     * This function will recreate the sys table string according to the
     * criterias specified
     */
    public String getTableDef(boolean addPartitioningInfo) {
        StringBuffer sbCreate = new StringBuffer();

        if (this.isTemporary()) {
            sbCreate.append(Props.XDB_SQLCOMMAND_CREATETEMPTABLE_START).append(
            " ");
        } else if (this.isUnlogged()) {
            sbCreate.append("CREATE UNLOGGED TABLE ");
        } else {
            sbCreate.append("CREATE TABLE ");
        }

        sbCreate.append(IdentifierHandler.quote(tableName)).append(" (");
        // Child table can have empty column list
        if (sysColumnList.size() > 0) {
            for (Object element : sysColumnList) {
                SysColumn column = (SysColumn) element;
                sbCreate.append(column.getColumnDefinition()).append(", ");
            }
            // TODO Index, constraint definitions ?
            sbCreate.setLength(sbCreate.length() - 2);
        }
        sbCreate.append(") ");

        if (addPartitioningInfo) {
            if (partitionScheme == PTYPE_HASH) {
                sbCreate.append(" partinioning key ").append(
                        getPartitionColumn());
                sbCreate.append(" on nodes (");
                for (Object element : getNodeList()) {
                    DBNode node = (DBNode) element;
                    sbCreate.append(node.getNodeId()).append(", ");
                }
                sbCreate.setLength(sbCreate.length() - 2);
                sbCreate.append(")");
            } else if (this.partitionScheme == PTYPE_ONE) {
                sbCreate.append(" on node ");
                DBNode node = getNodeList().iterator().next();
                sbCreate.append(node.getNodeId());
            } else if (this.partitionScheme == PTYPE_LOOKUP) {
                sbCreate.append(" replicated");
            }
        } else if (parentTable != null) {
            sbCreate.append(" inherits (").append(parentTable.getTableName())
            .append(")");
        }
        if (isTemporary()) {
            sbCreate.append(Props.XDB_SQLCOMMAND_CREATETEMPTABLE_SUFFIX);
        }
        return sbCreate.toString();
    }

    // returns the tableid of this table
    // does not do a metadata lookup in db - use getSysTableId() for that.
    public int getTableId() {
        return tableid;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * @return if the table is lookup/replicated
     **/
    public boolean isLookup() {
        return parentTable == null ? partitionScheme == SysTable.PTYPE_LOOKUP
                : parentTable.isLookup();
    }

    /**
     * This function will return the information regarding the partition nature
     * of the table.If the table is distributed on various nodes we consider it
     * to be partitioned.
     *
     *
     * @return
     */
    public boolean isPartitioned() {
        return parentTable == null ? partitionScheme == PTYPE_HASH
                || partitionScheme == PTYPE_RANGE
                || partitionScheme == PTYPE_ROBIN : parentTable.isPartitioned();
    }

    /**
     * Checks if the table is partitioned and if the specified column name is
     * the partitioning column.
     */
    public boolean isPartitionedColumn(String columnName) {
        return partitionScheme == PTYPE_HASH
        && columnName.compareToIgnoreCase(getPartitionColumn()) == 0;
    }

    /**
     * onSameNodes returns true if this table and the other table specified in
     * the arument are defined on the same subset of nodes. It is also checked
     * if the nodes have the corresponding have the same hash values defined for
     * them.
     */
    public boolean onSameNodes(SysTable otherTable) {
        Collection myPartitions = getNodeList();
        Collection otherPartitions = otherTable.getNodeList();

        if (myPartitions.size() != otherPartitions.size()) {
            // trivial
            return false;
        }

        return getPartitionMap().equals(otherTable.getPartitionMap());
    }

    /**
     * @param numrows
     *            The numrows to set.
     */
    public void setNumrows(long numrows) {
        long delta = numrows - this.numrows;
        this.numrows = numrows;
        if (parentTable != null) {
            parentTable.setNumrows(parentTable.getRowCount() + delta);
        }
    }

    public boolean isTemporary() {
        return tableIsTemporary;
    }

    public void setTableTemporary(boolean temporary) {
        tableIsTemporary = temporary;
    }

    public boolean isUnlogged() {
        return tableIsUnlogged;
    }

    public void setTableUnlogged(boolean unlogged) {
        tableIsUnlogged = unlogged;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SysTable) {
            SysTable compare = (SysTable) obj;
            return compare.getTableId() == this.getTableId();
        } else {
            return false;
        }
    }

    /**
     * To String
     *
     * @return
     */
    @Override
    public String toString() {
        return tableName + ":" + tableid;
    }

    /**
     * @param newTableName
     */
    void setName(String newTableName) {
        tableName = newTableName;
    }

    /**
     * @param rowIDHandler
     */
    public void setRowIDHandler(SysRowIDHandler rowIDHandler) {
        this.rowIDHandler = rowIDHandler;
    }

    /**
     * @param serialIDHandler
     */
    void setSerialIDHandler(SysSerialIDHandler serialIDHandler) {
        this.serialIdHandler = serialIDHandler;
    }

    public SysUser getOwner() {
        return owner;
    }

    void setOwner(SysUser newOwner) {
        if (owner != null) {
            owner.removeOwned(this);
        }
        owner = newOwner;
        if (owner != null) {
            owner.addOwned(this);
        }
    }

    /**
     * @param user
     * @return
     */
    SysPermission getSysPermission(SysUser user) {
        if (parentTable == null) {
            return sysPermissions.get(user);
        } else {
            return parentTable.getSysPermission(user);
        }
    }

    Collection<SysPermission>  getSysPermissions() {
        return sysPermissions.values();
    }

    public SysTable getParentTable() {
        return parentTable;
    }

    void setParentTableID(int parentID) {
        parentTableID = parentID;
        updateParentTableReference();
    }

    int getParentTableID() {
        return parentTableID;
    }

    private void updateParentTableReference() {
        if (parentTable != null) {
            parentTable.removeChildTable(this);
        }
        if (parentTableID == -1) {
            parentTable = null;
        } else {
            parentTable = database.getSysTable(parentTableID);
            parentTable.addChildTable(this);
            parentTable.readTableInfo(false);
        }
    }

    private void addChildTable(SysTable child) {
        childrenTables.add(child);
    }

    private void removeChildTable(SysTable child) {
        childrenTables.remove(child);
    }

    public List getChildrenTables() {
        return childrenTables;
    }

    public int getTablespaceID() {
        return tablespaceID;
    }

    public Collection<SysCheck> getSysChecks() {
        return sysChecks;
    }

    /**
     * return the number of "checks" or subtables that this table has,
     * considering only the bottom-most elements.
     */
    public int getSubtableCount() {
        int totalBase = 0;

        Iterator itChildren = childrenTables.iterator();

        while (itChildren.hasNext()) {
            SysTable childTable = (SysTable) itChildren.next();

            totalBase += childTable.getSubtableCount();
        }

        if (childrenTables.size() == 0) {
            totalBase = 1;
        }

        return totalBase;
    }

    /**
     * @param tablespaceID2
     */
    void setTablespaceID(int tablespaceID) {
        this.tablespaceID = tablespaceID;
    }

    /**
     * Get SysTablespace object which represents a Tablespace the table belongs to
     * @return
     */
    public SysTablespace getTablespace() {
        if (tablespace == null && tablespaceID != -1) {
            for (SysTablespace aTablespace : MetaData.getMetaData().getTablespaces()) {
                if (aTablespace.getTablespaceID() == tablespaceID) {
                    tablespace = aTablespace;
                    break;
                }
            }
        }
        return tablespace;
    }

    /**
     * Set SysTablespace object which represents a Tablespace the table belongs to
     * @param tablespace
     */
    void setTablespace(SysTablespace tablespace) {
        this.tablespace = tablespace;
        tablespaceID = tablespace == null ? -1 : tablespace.getTablespaceID();
    }

    /**
     * Checks to see if specified column is a primary key. PK must be defined on
     * only that one column
     */
    public boolean isPrimaryKey(String columnName) {
        List<SysColumn> primaryKey = getPrimaryKey();
        if (primaryKey == null || primaryKey.size() != 1) {
            return false;
        }

        if (columnName.equalsIgnoreCase(primaryKey.get(0).getColName())) {
            return true;
        }

        return false;
    }

    /**
     * Checks to see if specified column is a unique index Index must be defined
     * on only that one column
     */
    public boolean isUniqueIndex(String columnName) {
        Iterator itIndex = this.sysIndexList.iterator();

        while (itIndex.hasNext()) {
            SysIndex sysIndex = (SysIndex) itIndex.next();

            if (sysIndex.idxtype != 'U') {
                continue;
            }

            if (sysIndex.getKeyColumns().size() != 1) {
                continue;
            }

            SysColumn sysColumn = sysIndex.getKeyColumns().get(0);

            if (columnName.equalsIgnoreCase(sysColumn.getColName())) {
                return true;
            }
        }

        return false;
    }

    public String getClusteridx() {
        return clusteridx;
    }

    public void setClusteridx(String clusteridx) {
        this.clusteridx = clusteridx;
    }

    /**
     * @return whether or not the rowid is a xrowid, primary key, or unique
     *         index. If false, it is a list of all of the columns.
     */
    public boolean isTrueRowID() {
        return isTrueRowID;
    }

}
