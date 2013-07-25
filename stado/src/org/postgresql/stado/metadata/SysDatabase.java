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
 * SysDatabase.java
 */

package org.postgresql.stado.metadata;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.partitions.PartitionMap;
import org.postgresql.stado.metadata.scheduler.Balancer;
import org.postgresql.stado.metadata.scheduler.LockManager;
import org.postgresql.stado.metadata.scheduler.Scheduler;
import org.postgresql.stado.parser.SqlCreateTableColumn;


/**
 *
 * DS
 */
public class SysDatabase {

    private static final XLogger logger = XLogger.getLogger(SysDatabase.class);

    private static final int MIN_TEMP_TABLE_ID = 0x7FFFF000;

    private static final int MAX_TEMP_TABLE_ID = 0x7FFFFFFF;

    private static SysDatabase ADMIN_DATABASE;
    
    private static boolean ReadOnly = false;

    static SysDatabase getAdminDatabase() {
        if (ADMIN_DATABASE == null) {
            ADMIN_DATABASE = new SysDatabase(-1, Props.XDB_ADMIN_DATABASE, false);
            ADMIN_DATABASE.started = true;
            ADMIN_DATABASE.readLoginInfo();
        }
        return ADMIN_DATABASE;
    }

    private int dbid;

    private String dbname;

    private Hashtable<Integer, SysTable> sysTableIndex;

    private Hashtable<String, SysTable> sysTables;

    private Hashtable<String, SysView> sysViews;

    private Hashtable<Integer, SysView> sysViewsIndex;

    private Hashtable<String, SysTable> sysTempTables;

    private Hashtable<Integer, SysUser> sysUserList;

    private TreeMap<Integer, DBNode> dbNodeList;

    private int tempTableID = MIN_TEMP_TABLE_ID;

    // Modifier for temp table name
    private long counter = 0;

    private boolean started = false;

    private boolean spatial = false;

    private int coordinatorNodeID = 0;

    /** Creates a new instance of SysDatabase */
    // ------------------------------------------------------------------------
    SysDatabase(int dbID, String dbName, boolean isSpatial) {
        dbid = dbID;
        dbname = dbName;
        spatial = isSpatial;
        sysTableIndex = new Hashtable<Integer, SysTable>();
        sysTables = new Hashtable<String, SysTable>();
        sysViews = new Hashtable<String, SysView>();
        sysViewsIndex = new Hashtable<Integer, SysView>();
        sysTempTables = new Hashtable<String, SysTable>();
        dbNodeList = new TreeMap<Integer, DBNode>();
        sysUserList = new Hashtable<Integer, SysUser>();

        initSchedulingMechanism();
        
        initLockManager();

        initBalancer();
    }

    // ------------- getters for immutable fields (not synchronized)

    /**
     * @return Returns the dbid.
     */
    public int getDbid() {
        return dbid;
    }

    void setDbid(int dbid) {
        if (this.dbid == -1) {
            this.dbid = dbid;
        }
    }

    /**
     * @return Returns the first node in order (may not be coordinator node).
     */
    public int getCoordinatorNodeID() {
        if (coordinatorNodeID == 0) {

            if (dbNodeList.isEmpty()) {
                readDatabaseInfo();
            }

            coordinatorNodeID = chooseTempNodeId();
        }
        return coordinatorNodeID;
    }

    /**
     * @return Returns the dbname.
     */
    public String getDbname() {
        return dbname;
    }

    public boolean isSpatial() {
    	return spatial;
    }
    
    // ------------- Sheduler, Notifier, LockManager

    // Scheduler
    private Scheduler sc;

    private Balancer balancer;

    // lockmanager
    private LockManager lm;

    public LockManager getLm() {
        return lm;
    }

    private void initSchedulingMechanism() {
        sc = new Scheduler();
        Thread schedulerThread = new Thread(sc, dbname + " Scheduler Thread");
        schedulerThread.setDaemon(true);
        schedulerThread.start();
    }

    private void initLockManager() {
    	lm = new LockManager();
    }
    
    /**
     *
     */
    private void initBalancer() {
        balancer = new Balancer(this);
    }

    /**
     * Returns the scheduler for this particular metadata
     *
     * @return
     */
    public Scheduler getScheduler() {
        return sc;
    }

    /**
     *
     * @return
     */
    public Balancer getBalancer() {
        return balancer;
    }

    // ------------- SysDatabase startup and initialization

    public void admin() {
        Object startupLock = MetaData.getMetaData().getStartupLock();
        synchronized (startupLock) {
            if (dbNodeList.isEmpty() && this != ADMIN_DATABASE) {
                readDatabaseInfo();
            }
            startupLock.notifyAll();
        }
    }

    /**
     * @return
     */
    public Collection<DBNode> start() {
        LinkedList<DBNode> dbNodesToConnect = new LinkedList<DBNode>();
        synchronized (MetaData.getMetaData().getStartupLock()) {
            started = true;
            if (dbNodeList.isEmpty()) {
                readDatabaseInfo();
            }
            for (Object element : getDBNodeList()) {
                DBNode dbNode = (DBNode) element;
                if (!dbNode.isOnline() && dbNode.getNode().isUp()) {
                    dbNodesToConnect.add(dbNode);
                }
            }
        }
        return dbNodesToConnect;
    }

    /**
     * @return
     */
    public Collection<DBNode> stop() {
        LinkedList<DBNode> dbNodesToDisconnect = new LinkedList<DBNode>();
        synchronized (MetaData.getMetaData().getStartupLock()) {
            started = false;
            for (DBNode dbNode : getDBNodeList()) {
                if (dbNode.isOnline() && dbNode.getNode().isUp()) {
                    dbNodesToDisconnect.add(dbNode);
                }
            }
        }
        return dbNodesToDisconnect;
    }

    /**
     * @return Returns the started.
     */
    public boolean isStarted() {
        synchronized (MetaData.getMetaData().getStartupLock()) {
            return started;
        }
    }

    synchronized void addDbNode(DBNode dbNode) {
        dbNodeList.put(dbNode.getNode().getNodeid(), dbNode);
    }

    /**
     *
     * @return
     */
    public boolean isOnline() {
        synchronized (MetaData.getMetaData().getStartupLock()) {
            if (!started) {
                return false;
            }
            for (DBNode node : dbNodeList.values()) {
                if (!node.isOnline()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     *
     * @param timeout
     * @return
     */
    public boolean waitForOnline(long timeout) {
        Object startupLock = MetaData.getMetaData().getStartupLock();
        synchronized (startupLock) {
            long startWait = System.currentTimeMillis();
            long toWait = timeout;
            while (toWait > 0) {
                if (isOnline()) {
                    return true;
                } else {
                    try {
                        startupLock.wait(toWait);
                    } catch (InterruptedException ignore) {
                    }
                }
                toWait = startWait + timeout - System.currentTimeMillis();
            }
            LinkedList<DBNode> offline = new LinkedList<DBNode>();
            for (DBNode dbNode : dbNodeList.values()) {
                if (!dbNode.isOnline()) {
                    offline.add(dbNode);
                }
            }
            if (!offline.isEmpty()) {
                XLogger.getLogger("Server").log(Level.INFO,
                        "DB Nodes are not online: %0%",
                        new Object[] { offline });
            }
        }
        return isOnline();
    }

    // ----------- Getters, Setters, etc.

    /**
     * This method will read the login info and add it to the database.
     *
     */
    private synchronized void readLoginInfo() throws XDBServerException {
        final String method = "readLoginIno";
        logger.entering(method);
        try {
            MetaData meta = MetaData.getMetaData();

            // TODO load users
            for (SysLogin login : meta.getSysLogins()) {
                // TODO userID is NOT loginID
                int userID = login.getLoginID();
                SysUser aSysUser = new SysUser(userID, login, this);
                // Add it to the database list.
                insertUser(aSysUser);
            }
        } finally {
            logger.exiting(method);
        }
    }

    /**
     * This function will read the database information and will allow to pull
     * in all the information pertaining to a particular database
     *
     * @throws XDBServerException
     *                 This exception can be thrown for the following reasons 1.
     *                 Node from xsysdbnodes table not found
     *
     */
    private synchronized void readDatabaseInfo() throws XDBServerException {
        final String method = "readDatabaseInfo";
        logger.entering(method);
        try {

            MetaData meta = MetaData.getMetaData();

            // TODO load users
            readLoginInfo();

            // load dbnodes
            String sNodeQuery = "SELECT nodeid FROM xsysdbnodes WHERE dbid = "
                + dbid;
            ResultSet aNodeRS = meta.executeQuery(sNodeQuery);
            while (aNodeRS.next()) {
                Node node = meta.getNode(aNodeRS.getInt("nodeid"));
                new DBNode(node, this);
            }

            // load tables
            ResultSet aTableRS = meta.executeQuery
            ("SELECT * from xsystables WHERE dbid = " + dbid);
            while (aTableRS.next()) {
                String partCol = aTableRS.getString("partcol");
                String clusteridx = aTableRS.getString("clusteridx");
                if (partCol != null) {
                    partCol = partCol.trim();
                }
                SysUser owner = null;
                int ownerid = aTableRS.getInt("owner");
                if (ownerid > 0) {
                    owner = getSysUser(ownerid);
                }
                int parentID = aTableRS.getInt("parentid");
                if (parentID <= 0) {
                    parentID = -1;
                }
                int tablespaceID = aTableRS.getInt("tablespaceid");
                if (tablespaceID <= 0) {
                    tablespaceID = -1;
                }
                SysTable aSysTable = new SysTable(this, aTableRS
                        .getInt("tableid"), aTableRS.getString("tablename")
                        .trim(), aTableRS.getLong("numrows"), aTableRS
                        .getShort("partscheme"), partCol, owner, parentID,
                        tablespaceID, clusteridx);
                addSysTable(aSysTable);
            }
            // Do read table info in separate loop to resolve parent reference
            // properly

            // We first only want to read parent tables, then we can do the
            // children
            // otherwise we may get a NPE.
            for (SysTable aSysTable : sysTables.values()) {
                aSysTable.readTableInfo(false);
            }
            /*
             * // Now we can get tables with children for (Iterator it =
             * sysTables.values().iterator(); it.hasNext();) { SysTable
             * aSysTable = (SysTable) it.next();
             *
             * if (aSysTable.getParentTableID() >= 0) {
             * aSysTable.readTableInfo(); } }
             */
            // We loop through again and refresh constraint checking info,
            // now that we have read in all tables.
            Enumeration enumTable = getAllTables();

            while (enumTable.hasMoreElements()) {
                SysTable aSysTable = (SysTable) enumTable.nextElement();

                aSysTable.updateCrossReferences();
            }

            // load views
            ResultSet aViewRS = meta
            .executeQuery("SELECT * from xsysviews WHERE dbid = "
                    + dbid);
            while (aViewRS.next()) {
                SysView aSysView = new SysView(this, aViewRS.getInt("viewid"),
                        aViewRS.getString("viewname").trim(), aViewRS
                        .getString("viewtext").trim());
                addSysView(aSysView);
            }

            // Re-create lock manager with new table list
            initLockManager();

            initBalancer();
        } catch (SQLException se) {
            logger.catching(se);
            XDBServerException ex = new XDBServerException(
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR, se,
                    ErrorMessageRepository.METADATA_DB_INFO_READ_ERROR_CODE);
            logger.throwing(ex);
            throw ex;
        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param userId
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public synchronized SysUser getSysUser(int userId)
    throws XDBServerException {
        final String method = "getSysUser";
        Integer userIdInt = new Integer(userId);
        logger.entering(method, new Object[] { userIdInt });
        try {

            SysUser user = sysUserList.get(userIdInt);
            if (user == null) {
                XDBServerException ex = new XDBServerException("User " + userId
                        + " is not found");
                logger.throwing(ex);
                throw ex;
            }
            return user;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     *
     * @param userName
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public synchronized SysUser getSysUser(String userName)
    throws XDBServerException {
        final String method = "getSysUser";
        logger.entering(method, new Object[] { userName });
        try {

            for (Object element : sysUserList.values()) {
                SysUser user = (SysUser) element;
                if (userName.equals(user.getName())) {
                    return user;
                }
            }
            XDBServerException ex = new XDBServerException("User \"" + userName
                    + "\" is not found");
            logger.throwing(ex);
            throw ex;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @return
     */
    public Collection<SysUser> getSysUsers() {
        return sysUserList.values();
    }

    /**
     *
     * @return
     */
    public Collection<SysView> getSysViews() {
        return sysViews.values();
    }

    // ------------------------------------------------------------------------
    /**
     *
     * @param aSysTable
     */
    public synchronized void addSysTable(SysTable aSysTable) {
        sysTables.put(aSysTable.getTableName(), aSysTable);
        sysTableIndex.put(aSysTable.getTableId(), aSysTable);
    }

    /**
     *
     * @param aSysView
     */
    public synchronized void addSysView(SysView aSysView) {
        sysViews.put(aSysView.getViewName(), aSysView);
        sysViewsIndex.put(new Integer(aSysView.getViewid()), aSysView);
    }

    // ------------------------------------------------------------------------
    /**
     *
     * @param aSysTable
     */
    public synchronized void addSysTempTable(SysTable aSysTable) {
        sysTempTables.put(aSysTable.getTableName(), aSysTable);
        sysTableIndex.put(new Integer(aSysTable.getTableId()), aSysTable);
    }

    // ------------------------------------------------------------------------
    /**
     *
     * @param tableName
     * @return
     */
    public synchronized SysTable getSysTable(String tableName) {
        SysTable aSysTable = null;
        aSysTable = sysTables.get(tableName);
        // If we find that the systables is null , we must check if
        // the table is a temp table
        if (aSysTable == null) {
            aSysTable = sysTempTables.get(tableName);
        }
        if (aSysTable == null) {
            XDBServerException ex = new XDBServerException("Table " + tableName
                    + " has not been found in database " + dbname);
            // Don't log at throwing level, just debug level.
            // Let the calling method deal with it.
            // logger.throwing(ex);
            logger.debug(ex.getMessage());
            throw ex;
        }
        return aSysTable;
    }

    /**
     * This method has the same functionality as that of getSysTable except that
     * if the sys table is not found it returns null instead of throwing
     * exception.
     */
    public synchronized SysTable checkForSysTable(String tableName) {
        if (tableName == null || tableName.equals("")) {
            return null;
        }

        SysTable aSysTable = null;
        aSysTable = sysTables.get(tableName);
        // If we find that the systables is null , we must check if
        // the table is a temp table
        if (aSysTable == null) {
            aSysTable = sysTempTables.get(tableName);
        }

        return aSysTable;
    }

    /**
     *
     * @param viewName
     * @return
     */
    public synchronized SysView getSysView(String viewName) {
        SysView aSysView = null;
        aSysView = sysViews.get(viewName);
        // If we find that the systables is null , we must check if
        // the table is a temp table
        if (aSysView == null) {
            XDBServerException ex = new XDBServerException("View " + viewName
                    + " has not been found in database " + dbname);
            logger.throwing(ex);
            throw ex;
        }
        return aSysView;
    }

    // Returns a systable corresponding to an id
    /**
     *
     * @param tableId
     * @return
     */
    public synchronized SysTable getSysTable(int tableId) {
        return sysTableIndex.get(new Integer(tableId));
    }

    /**
     * Returns a systable corresponding to an id
     * @param viewId
     * @return
     */
    public synchronized SysView getSysView(int viewId) {
        return sysViewsIndex.get(new Integer(viewId));
    }

    /**
     * drops specified sysTable
     * @param tableName
     */
    public synchronized void dropSysTable(String tableName) {
        SysTable aSysTable;
        if (!sysTempTables.containsKey(tableName)) {
            aSysTable = sysTables.remove(tableName);
        } else {
            aSysTable = sysTempTables.remove(tableName);
        }
        if (aSysTable != null) {
            sysTableIndex.remove(new Integer(aSysTable.getTableId()));
            SysUser owner = aSysTable.getOwner();
            if (owner != null) {
                owner.removeOwned(aSysTable);
            }
            for (SysPermission permission : aSysTable.getSysPermissions()) {
                permission.getUser().removeGranted(aSysTable);
            }
            aSysTable.setParentTableID(-1);
        }
    }

    /**
     *
     * @param viewName
     */
    public synchronized void dropSysView(String viewName) {
        SysView aSysView;

        aSysView = sysViews.remove(viewName);
        if (aSysView != null) {
            sysViewsIndex.remove(new Integer(aSysView.getViewid()));
        }
    }

    /**
     * This should only return permanent tables
     *
     * @return the enumeration of all tables
     */
    public synchronized Enumeration getAllTables() {
        return Collections.enumeration(sysTableIndex.values());
    }

    /**
     *
     * @return
     */
    public synchronized Enumeration getAllViews() {
        return Collections.enumeration(sysViewsIndex.values());
    }

    /**
     * Since indexes are only allowed on temp tables we need only to search the
     * systables
     *
     * @param indexName
     * @return
     */
    public synchronized HelperSysIndex getSysIndexByName(String indexName)
    throws XDBServerException {
        HelperSysIndex helpIndex = null;
        Enumeration tableList = sysTables.elements();
        while (tableList.hasMoreElements()) {
            SysTable aTable = (SysTable) tableList.nextElement();
            SysIndex aSysIndex = aTable.getSysIndex(indexName);
            if (aSysIndex != null) {
                // Skip index if it is inherited
                if (aTable.getParentTable() != null
                        && aTable.getParentTable().getSysIndex(aSysIndex.idxid) == aSysIndex) {
                    continue;
                }
                if (helpIndex != null) {
                    throw new XDBServerException("Ambiguous index name.");
                }
                helpIndex = new HelperSysIndex(aTable, aSysIndex);
            }
        }
        return helpIndex;
    }

    /**
     *
     * @return
     */
    public synchronized Collection<DBNode> getDBNodeList() {
        return dbNodeList.values();
    }

    /**
     *
     * @param nodeID
     * @throws org.postgresql.stado.exception.XDBServerException
     * @return
     */
    public synchronized DBNode getDBNode(int nodeID) throws XDBServerException {
        DBNode dbNode = dbNodeList.get(nodeID);
        if (dbNode == null) {
            XDBServerException ex = new XDBServerException("Node " + nodeID
                    + " is not used by Database " + dbname);
            logger.throwing(ex);
            throw ex;
        }
        return dbNode;
    }

    /**
     * @param nodeid
     */
    synchronized void removeDBNode(int nodeID) {
        dbNodeList.remove(nodeID);
    }

    /**
     * Each table has a counter which it uses to issue unique integers
     *
     * @param prefix
     * @param suffix
     * @param client
     * @return
     */
    public String getUniqueTableName(String prefix, String suffix,
            XDBSessionContext client) {
        return getUniqueTempTableName(prefix);
    }

    /**
     *
     * @param basename
     * @return
     */
    public String getUniqueTempTableName(String basename) {
        if (!basename.startsWith(Props.XDB_TEMPTABLEPREFIX)) {
            basename = Props.XDB_TEMPTABLEPREFIX + basename;
        }
        String candidate = basename;
        while (isTableExists(candidate)) {
            if (counter == Long.MAX_VALUE) {
                counter = 0;
            }
            candidate = basename + "_" + counter++;
        }
        return candidate;
    }

    private int getNextTempTableID() {
        int attempts = MAX_TEMP_TABLE_ID - MIN_TEMP_TABLE_ID;
        while (getSysTable(tempTableID == MAX_TEMP_TABLE_ID ? tempTableID = MIN_TEMP_TABLE_ID
                : ++tempTableID) != null) {
            if (attempts-- < 0) {
                throw new XDBServerException("Out of temp table name space");
            }
        }
        return tempTableID;
    }

    /**
     *
     * @param baseName
     * @param partitionType
     * @param partitionMap
     * @param partitionColumn
     * @param serialIDHandler
     * @param xrowIDHandler
     * @param sysColumns
     * @throws java.lang.Exception
     * @return
     */
    public synchronized SysTable createTempSysTable(String baseName,
            short partitionType, PartitionMap partitionMap,
            String partitionColumn, SysSerialIDHandler serialIDHandler,
            SysRowIDHandler xrowIDHandler, Collection<SysColumn> sysColumns)
    throws Exception {
        SysTable tempTable = new SysTable(this, getNextTempTableID(),
                getUniqueTempTableName(baseName), 0, partitionType,
                partitionColumn, null, -1, -1, null);
        tempTable.setTableTemporary(true);
        tempTable.setPartitioning(partitionColumn, partitionType, partitionMap);
        int colID = 1;
        for (SysColumn srcCol : sysColumns) {
            SysColumn column = new SysColumn(tempTable, colID, colID,
                    srcCol.getColName(), srcCol.getColType(),
                    srcCol.getColLength(), srcCol.getColScale(),
                    srcCol.getColPrecision(), srcCol.isNullable(),
                    srcCol.isSerial(), srcCol.getNativeColDef(),
                    srcCol.getSelectivity(), srcCol.getDefaultExpr());
            tempTable.addSysColumn(column);
            colID++;
        }
        tempTable.refreshAssociatedInfo();
        tempTable.setSerialIDHandler(serialIDHandler);
        tempTable.setRowIDHandler(xrowIDHandler);
        addSysTempTable(tempTable);

        return tempTable;
    }

    /**
     *
     * @param name
     * @param partitionType
     * @param partitionMap
     * @param partitionColumn
     * @param serialIDHandler
     * @param xrowIDHandler
     * @param sysColumns
     * @param tablespace
     * @param temporary
     * @param client
     * @throws java.lang.Exception
     * @return
     */
    public synchronized SysTable createSysTable(String name,
            short partitionType, PartitionMap partitionMap,
            String partitionColumn, SysSerialIDHandler serialIDHandler,
            SysRowIDHandler xrowIDHandler, List<SqlCreateTableColumn> sysColumns,
            SysTablespace tablespace, boolean temporary, XDBSessionContext client)
    throws Exception {
        SysTable table = new SysTable(this, getNextTempTableID(),
                temporary ? getUniqueTempTableName(name) : name, 0,
                        partitionType, partitionColumn, null, -1, -1, null);
        table.setTableTemporary(temporary);
        table.setPartitioning(partitionColumn, partitionType, partitionMap);
        int colID = 1;
        for (SqlCreateTableColumn srcCol : sysColumns) {
            SysColumn column = new SysColumn(table, colID, colID,
                    srcCol.columnName, srcCol.getColumnType(),
                    srcCol.getColumnLength(), srcCol.getColumnScale(),
                    srcCol.getColumnPrecision(), true, false,
                    srcCol.rebuildString(), 0, null);
            table.addSysColumn(column);
            colID++;
        }
        table.refreshAssociatedInfo();
        table.setSerialIDHandler(serialIDHandler);
        table.setRowIDHandler(xrowIDHandler);
        table.setTablespace(tablespace);
        table.setOwner(client.getCurrentUser());

        return table;
    }

    /**
     * @param tableName
     * @return
     */
    public synchronized boolean isTableExists(String tableName) {
        return sysTables.containsKey(tableName)
        || sysTempTables.containsKey(tableName);
    }

    /**
     *
     * @param viewName
     * @return
     */
    public synchronized boolean isViewExists(String viewName) {
        return sysViews.containsKey(viewName);
    }

    /**
     * Get the lowest node in the system.
     *
     * @return the lowest node in the system.
     */
    public DBNode getLowestNode() {
        Iterator nodesIt = getDBNodeList().iterator();
        DBNode lowestPrevNode = null;

        while (nodesIt.hasNext()) {
            DBNode presentNode = (DBNode) nodesIt.next();
            if (lowestPrevNode == null) {
                lowestPrevNode = presentNode;
            } else {
                if (lowestPrevNode.getNodeId() > presentNode.getNodeId()) {
                    lowestPrevNode = presentNode;
                }
            }
        }
        // Incase we have a situation where lowestPrevNode == null
        // then...implying nodes in the system
        return lowestPrevNode;
    }

    /**
     * @param oldTableName
     * @param newTableName
     */
    public void renameSysTable(String oldTableName, String newTableName) {
        SysTable table = sysTables.remove(oldTableName);
        table.setName(newTableName);
        sysTables.put(newTableName, table);
    }

    /**
     * Returns all persistent tables in the database
     *
     * @return the list of tables
     */
    public synchronized Collection<SysTable> getSysTables() {
        return sysTables.values();
    }

    /**
     * check if user with specified name already exists
     *
     * @param userName
     * @return <code>TRUE</code> if exists, <code>FALSE</code> otherwise
     */
    public synchronized boolean hasSysUser(String userName) {
        String userNameUpper = userName.toUpperCase();
        for (Object element : sysUserList.values()) {
            SysUser user = (SysUser) element;
            if (userNameUpper.equals(user.getName().toUpperCase())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param user
     */
    public void insertUser(SysUser user) {
        sysUserList.put(new Integer(user.getUserID()), user);
    }

    /**
     * @param user
     */
    public void removeUser(String userName) {
        for (Iterator<SysUser> it = sysUserList.values().iterator(); it
        .hasNext();) {
            if (userName.equals(it.next().getName())) {
                it.remove();
            }
        }
    }

    /**
     *
     * @return
     */
    @Override
    public String toString() {
        return "Db:" + dbname;
    }

    /**
     * @param nodeList
     * @return collection of newly created DBNodes
     */
    public Collection<DBNode> createDBNodes(Collection<Integer> nodeList) {
        final String method = "createDBNodes";
        logger.entering(method, new Object[] { nodeList });
        try {

            Collection<DBNode> dbNodes = new ArrayList<DBNode>(nodeList.size());
            Object startupLock = MetaData.getMetaData().getStartupLock();
            synchronized (startupLock) {
                synchronized (this) {
                    MetaData meta = MetaData.getMetaData();
                    for (Integer integer : nodeList) {
                        // Conctructor of DBNode puts new entry into dbNodeList
                        dbNodes.add(new DBNode(meta.getNode(integer), this));
                    }
                }
                startupLock.notifyAll();
            }
            // List of nodes has been changed so update the Balancer
            initBalancer();
            return dbNodes;

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * @param dbNodeList
     */
    public synchronized void deleteDBNodes(Collection<DBNode> dbNodeList) {
        final String method = "deleteDBNodes";
        logger.entering(method, new Object[] { dbNodeList });
        try {

            Object startupLock = MetaData.getMetaData().getStartupLock();
            synchronized (startupLock) {
                synchronized (this) {
                    for (DBNode nodeToDelete : dbNodeList) {
                        this.dbNodeList.remove(nodeToDelete.getNodeId());
                    }
                }
                startupLock.notifyAll();
            }
            // List of nodes has been changed so update the Balancer
            initBalancer();

        } finally {
            logger.exiting(method);
        }
    }

    /**
     * Determines which node id to use for this databases temp coordinator
     * database.
     *
     * @return the node id to use for the coordinator node
     */
    private synchronized int chooseTempNodeId() {
        Hashtable<Integer, Integer> nodeIdList = new Hashtable<Integer, Integer>();

        for (SysTable sysTable : sysTables.values()) {
            for (DBNode aDBNode : sysTable.getNodeList()) {
                Integer currentCount = nodeIdList.get(aDBNode.getNodeId());
                if (currentCount == null) {
                    nodeIdList.put(aDBNode.getNodeId(), 1);
                } else {
                    nodeIdList.put(aDBNode.getNodeId(), currentCount + 1);
                }
            }
        }

        // Pick the one that has the most tables.
        // If the traditional coordinator node has equal to the most tables,
        // use that.
        int maxCount = 0;
        int highNodeId = 0;
        for (Integer nodeId : nodeIdList.keySet()) {
            Integer theCount = nodeIdList.get(nodeId);
            if (theCount > maxCount) {
                highNodeId = nodeId;
                maxCount = theCount;
            } else if (theCount == maxCount
                    && nodeId == Props.XDB_COORDINATOR_NODE) {
                highNodeId = nodeId;
            }
        }

        // Work-around for when creating a new database
        if (highNodeId == 0) {
            for (DBNode aDBNode : getDBNodeList()) {
                if (aDBNode.getNodeId() == Props.XDB_COORDINATOR_NODE) {
                    return Props.XDB_COORDINATOR_NODE;
                } else {
                    highNodeId = aDBNode.getNodeId();
                }
            }
        }

        return highNodeId;
    }

	public static void setReadOnly(boolean readOnly) {
		ReadOnly = readOnly;
	}

	public static boolean isReadOnly() {
		return ReadOnly;
	}

}
